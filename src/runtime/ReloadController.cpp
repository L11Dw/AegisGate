#include "aegisgate/runtime/ReloadController.h"

#include <cerrno>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <sys/eventfd.h>
#include <unistd.h>

namespace aegisgate::runtime {
namespace {

std::string ReadFile(const std::string &path) {
  std::ifstream input(path);
  if (!input) throw std::runtime_error("failed to open configuration file");
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

} // namespace

ReloadController::ReloadController(std::string config_path) : config_path_(std::move(config_path)) {
  if (config_path_.empty()) throw std::invalid_argument("reload configuration path must not be empty");
  wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (wake_fd_ < 0) throw std::system_error(errno, std::generic_category(), "eventfd");
}

ReloadController::~ReloadController() { Stop(); }

bool ReloadController::Request() {
  std::thread completed;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (stopping_) return false;
    if (parsing_) {
      pending_ = true;
      return true;
    }
    if (parser_.joinable()) completed = std::move(parser_);
    parsing_ = true;
    try {
      parser_ = std::thread([this] { ParseLoop(); });
    } catch (...) {
      // Keep the state machine truthful when the OS rejects thread creation:
      // a later Request() may retry and no caller is left believing a parser
      // is running when there is none.
      parsing_ = false;
      throw;
    }
  }
  if (completed.joinable()) completed.join();
  return true;
}

std::vector<ReloadController::Result> ReloadController::Drain() {
  int fd = -1;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    fd = wake_fd_;
  }
  if (fd >= 0) {
    std::uint64_t counter = 0;
    for (;;) {
      const ssize_t count = ::read(fd, &counter, sizeof(counter));
      if (count == static_cast<ssize_t>(sizeof(counter))) continue;
      if (count < 0 && errno == EINTR) continue;
      break;
    }
  }
  std::deque<Result> queued;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    queued.swap(results_);
  }
  return {std::make_move_iterator(queued.begin()), std::make_move_iterator(queued.end())};
}

void ReloadController::Stop() noexcept {
  std::thread parser;
  int fd = -1;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (stopping_ && wake_fd_ < 0) return;
    stopping_ = true;
    pending_ = false;
    parser = std::move(parser_);
  }
  if (parser.joinable()) parser.join();
  {
    std::lock_guard<std::mutex> guard(mutex_);
    fd = std::exchange(wake_fd_, -1);
  }
  if (fd >= 0) (void)::close(fd);
}

int ReloadController::wake_fd() const noexcept {
  std::lock_guard<std::mutex> guard(mutex_);
  return wake_fd_;
}

void ReloadController::ParseLoop() noexcept {
  for (;;) {
    Result result;
    result.parser_thread = std::this_thread::get_id();
    try {
      result.candidate = config::LoadFromYaml(ReadFile(config_path_));
    } catch (const std::exception &error) {
      result.error = error.what();
    } catch (...) {
      result.error = "unknown configuration parse failure";
    }

    bool repeat = false;
    bool publish = false;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      if (!stopping_) {
        try {
          result.sequence = next_sequence_++;
          results_.push_back(std::move(result));
          publish = WakeLocked();
          if (!publish) results_.pop_back();
        } catch (...) {
          // A result must not corrupt the parser thread.  The Gateway keeps
          // the old generation unless it receives a fully materialized value.
        }
      }
      repeat = pending_ && !stopping_;
      pending_ = false;
      if (!repeat) parsing_ = false;
    }
    if (!repeat) return;
  }
}

bool ReloadController::WakeLocked() noexcept {
  if (wake_fd_ < 0) return false;
  const std::uint64_t counter = 1;
  for (;;) {
    const ssize_t count = ::write(wake_fd_, &counter, sizeof(counter));
    if (count == static_cast<ssize_t>(sizeof(counter))) return true;
    if (count < 0 && errno == EINTR) continue;
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
    return false;
  }
}

} // namespace aegisgate::runtime
