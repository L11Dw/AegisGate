#include "aegisgate/runtime/ReloadController.h"

#include <cerrno>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <system_error>
#include <unistd.h>
#include <sys/eventfd.h>

#include "aegisgate/config/Config.h"

namespace aegisgate::runtime {

ReloadController::ReloadController(std::string config_path)
    : config_path_(std::move(config_path)) {
  const int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(), "eventfd");
  }
  wake_fd_ = fd;
}

ReloadController::~ReloadController() { Stop(); }

bool ReloadController::Request() {
  {
    std::lock_guard<std::mutex> guard(mu_);
    if (stopping_) return false;
    if (parsing_) {
      // A parse is already running — mark that another is pending.
      pending_ = true;
      return true;
    }
    parsing_ = true;
    pending_ = false;
  }
  // Start the background parse thread.
  parser_ = std::thread([this] { ParseLoop(); });
  return true;
}

std::vector<ReloadController::Result> ReloadController::Drain() {
  std::vector<Result> result;
  {
    std::lock_guard<std::mutex> guard(mu_);
    result.assign(results_.begin(), results_.end());
    results_.clear();
  }
  // Consume the eventfd counter.
  std::uint64_t counter = 0;
  for (;;) {
    const ssize_t n = ::read(wake_fd_, &counter, sizeof(counter));
    if (n == sizeof(counter)) break;
    if (n < 0 && errno == EINTR) continue;
    break; // EAGAIN
  }
  return result;
}

void ReloadController::Stop() noexcept {
  {
    std::lock_guard<std::mutex> guard(mu_);
    if (stopping_) return;
    stopping_ = true;
    pending_ = false;
  }
  if (parser_.joinable()) parser_.join();
  if (wake_fd_ >= 0) {
    (void)::close(wake_fd_);
    wake_fd_ = -1;
  }
}

int ReloadController::wake_fd() const noexcept {
  std::lock_guard<std::mutex> guard(mu_);
  return stopping_ ? -1 : wake_fd_;
}

void ReloadController::ParseLoop() noexcept {
  for (;;) {
    // Check if we should stop.
    {
      std::lock_guard<std::mutex> guard(mu_);
      if (stopping_) {
        parsing_ = false;
        return;
      }
    }

    // Perform the parse (blocking I/O).
    Result result;
    result.sequence = next_sequence_++;
    try {
      std::ifstream ifs(config_path_);
      if (!ifs.is_open()) {
        result.error = "cannot open config file: " + config_path_;
      } else {
        std::ostringstream oss;
        oss << ifs.rdbuf();
        result.candidate = config::LoadFromYaml(oss.str());
      }
    } catch (const std::exception &e) {
      result.error = e.what();
    } catch (...) {
      result.error = "unknown parse error";
    }

    // Deliver the result and check for a pending request.
    bool should_continue = false;
    {
      std::lock_guard<std::mutex> guard(mu_);
      if (stopping_) {
        parsing_ = false;
        return;
      }
      // Latest-wins: keep only the most recent result.
      results_.push_back(std::move(result));
      if (pending_) {
        // Another request came in while we were parsing — loop again.
        pending_ = false;
        should_continue = true;
      } else {
        parsing_ = false;
      }
    }

    // Wake the control loop.
    const std::uint64_t one = 1;
    for (;;) {
      const ssize_t n = ::write(wake_fd_, &one, sizeof(one));
      if (n == sizeof(one)) break;
      if (n < 0 && errno == EINTR) continue;
      break; // EAGAIN
    }

    if (!should_continue) return;
  }
}

} // namespace aegisgate::runtime
