#include "aegisgate/runtime/ReloadWatcher.h"

#include <array>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <pthread.h>
#include <signal.h>
#include <sys/inotify.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"

namespace aegisgate::runtime {
namespace {

std::pair<std::string, std::string> SplitPath(const std::string &path) {
  const std::size_t slash = path.find_last_of('/');
  if (slash == std::string::npos) return {".", path};
  if (slash + 1 == path.size()) throw std::invalid_argument("reload path names a directory");
  return {slash == 0 ? "/" : path.substr(0, slash), path.substr(slash + 1)};
}

} // namespace

void ReloadWatcher::BlockSighupForProcess() {
  sigset_t signals;
  if (::sigemptyset(&signals) != 0 || ::sigaddset(&signals, SIGHUP) != 0) {
    throw std::system_error(errno, std::generic_category(), "block SIGHUP");
  }
  const int error = ::pthread_sigmask(SIG_BLOCK, &signals, nullptr);
  if (error != 0) throw std::system_error(error, std::generic_category(), "block SIGHUP");
}

ReloadWatcher::ReloadWatcher(net::EventLoop &loop, std::string config_path,
                             std::function<void()> trigger, bool watch_sighup)
    : loop_(loop), trigger_(std::move(trigger)), timers_(std::make_unique<net::TimerQueue>(loop)) {
  if (!loop_.IsOwnerThread()) throw std::logic_error("reload watcher requires control loop owner");
  if (config_path.empty() || !trigger_) throw std::invalid_argument("reload watcher needs path and trigger");
  try {
    auto [directory, filename] = SplitPath(config_path);
    directory_ = std::move(directory);
    filename_ = std::move(filename);
    inotify_fd_ = ::inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
    if (inotify_fd_ < 0) throw std::system_error(errno, std::generic_category(), "inotify_init1");
    watch_descriptor_ = ::inotify_add_watch(
        inotify_fd_, directory_.c_str(), IN_CLOSE_WRITE | IN_MOVED_TO | IN_ATTRIB);
    if (watch_descriptor_ < 0) {
      throw std::system_error(errno, std::generic_category(), "inotify_add_watch");
    }
    inotify_channel_ = std::make_unique<net::Channel>(loop_, inotify_fd_);
    inotify_channel_->SetReadCallback([this] { HandleInotify(); });
    inotify_channel_->EnableReading();

    if (!watch_sighup) return;
    sigset_t signals;
    if (::sigemptyset(&signals) != 0 || ::sigaddset(&signals, SIGHUP) != 0) {
      throw std::system_error(errno, std::generic_category(), "make SIGHUP signal set");
    }
    sighup_fd_ = ::signalfd(-1, &signals, SFD_NONBLOCK | SFD_CLOEXEC);
    if (sighup_fd_ < 0) throw std::system_error(errno, std::generic_category(), "signalfd");
    sighup_channel_ = std::make_unique<net::Channel>(loop_, sighup_fd_);
    sighup_channel_->SetReadCallback([this] { HandleSighup(); });
    sighup_channel_->EnableReading();
  } catch (...) {
    Stop();
    throw;
  }
}

ReloadWatcher::~ReloadWatcher() { Stop(); }

void ReloadWatcher::HandleInotify() {
  std::array<char, 4096> bytes{};
  for (;;) {
    const ssize_t count = ::read(inotify_fd_, bytes.data(), bytes.size());
    if (count < 0 && errno == EINTR) continue;
    if (count <= 0) return;  // EAGAIN or close
    std::size_t offset = 0;
    while (offset < static_cast<std::size_t>(count)) {
      const auto *event = reinterpret_cast<const inotify_event *>(bytes.data() + offset);
      const std::size_t event_size = sizeof(inotify_event) + event->len;
      if (event_size == 0 || offset + event_size > static_cast<std::size_t>(count)) return;
      if (event->wd == watch_descriptor_ && event->len != 0 &&
          filename_ == std::string_view(event->name) &&
          (event->mask & (IN_CLOSE_WRITE | IN_MOVED_TO | IN_ATTRIB)) != 0) {
        Debounce();
      }
      offset += event_size;
    }
  }
}

void ReloadWatcher::HandleSighup() {
  signalfd_siginfo information{};
  for (;;) {
    const ssize_t count = ::read(sighup_fd_, &information, sizeof(information));
    if (count == static_cast<ssize_t>(sizeof(information))) {
      if (information.ssi_signo == SIGHUP) Debounce();
      continue;
    }
    if (count < 0 && errno == EINTR) continue;
    return;
  }
}

void ReloadWatcher::Debounce() {
  if (debounce_timer_ != 0) (void)timers_->Cancel(debounce_timer_);
  debounce_timer_ = timers_->ScheduleAfter(std::chrono::milliseconds(200), [this] {
    debounce_timer_ = 0;
    try {
      trigger_();
    } catch (...) {
      // A reload trigger must not take down the control loop.  The current
      // generation remains live; observability/logging is handled above it.
    }
  });
}

void ReloadWatcher::Stop() noexcept {
  if (debounce_timer_ != 0 && timers_) (void)timers_->Cancel(debounce_timer_);
  debounce_timer_ = 0;
  sighup_channel_.reset();
  inotify_channel_.reset();
  timers_.reset();
  if (watch_descriptor_ >= 0 && inotify_fd_ >= 0) {
    (void)::inotify_rm_watch(inotify_fd_, watch_descriptor_);
  }
  watch_descriptor_ = -1;
  if (sighup_fd_ >= 0) (void)::close(std::exchange(sighup_fd_, -1));
  if (inotify_fd_ >= 0) (void)::close(std::exchange(inotify_fd_, -1));
}

} // namespace aegisgate::runtime
