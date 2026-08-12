#include "aegisgate/runtime/ReloadWatcher.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string_view>
#include <system_error>

#include <fcntl.h>
#include <sys/inotify.h>
#include <sys/signalfd.h>
#include <sys/timerfd.h>
#include <signal.h>
#include <unistd.h>

namespace aegisgate::runtime {

namespace {
constexpr std::uint32_t kInotifyMask =
    IN_CLOSE_WRITE | IN_MOVED_TO | IN_ATTRIB | IN_CREATE;

int MakeSignalfd() {
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, SIGHUP);
  // Block SIGHUP so it's delivered via signalfd instead of terminating.
  sigprocmask(SIG_BLOCK, &mask, nullptr);
  const int fd = ::signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
  return fd;
}

int MakeTimerfd() {
  const int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  return fd;
}

void ArmTimer(int fd, std::uint64_t nanoseconds) {
  itimerspec spec{};
  spec.it_value.tv_sec = nanoseconds / 1'000'000'000;
  spec.it_value.tv_nsec = nanoseconds % 1'000'000'000;
  (void)::timerfd_settime(fd, 0, &spec, nullptr);
}

} // namespace

ReloadWatcher::ReloadWatcher(std::string config_path, ReloadCallback callback)
    : config_path_(std::move(config_path)), callback_(std::move(callback)) {
  // Determine the directory to watch (parent of the config file).
  std::filesystem::path p(config_path_);
  watch_dir_ = p.parent_path().string();
  if (watch_dir_.empty()) watch_dir_ = ".";

  // Create signalfd for SIGHUP.
  sighup_fd_ = MakeSignalfd();

  // Create inotify watch on the directory.
  inotify_fd_ = ::inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
  if (inotify_fd_ >= 0) {
    watch_descriptor_ = ::inotify_add_watch(inotify_fd_, watch_dir_.c_str(), kInotifyMask);
    if (watch_descriptor_ < 0) {
      (void)::close(inotify_fd_);
      inotify_fd_ = -1;
    }
  }

  // Create debounce timer.
  timer_fd_ = MakeTimerfd();
}

ReloadWatcher::~ReloadWatcher() { Stop(); }

void ReloadWatcher::Stop() noexcept {
  if (stopped_) return;
  stopped_ = true;
  if (timer_fd_ >= 0) { (void)::close(timer_fd_); timer_fd_ = -1; }
  if (inotify_fd_ >= 0) {
    if (watch_descriptor_ >= 0) {
      (void)::inotify_rm_watch(inotify_fd_, watch_descriptor_);
      watch_descriptor_ = -1;
    }
    (void)::close(inotify_fd_);
    inotify_fd_ = -1;
  }
  if (sighup_fd_ >= 0) { (void)::close(sighup_fd_); sighup_fd_ = -1; }
}

void ReloadWatcher::HandleSighup() {
  if (stopped_) return;
  // Drain the signalfd.
  signalfd_siginfo info{};
  for (;;) {
    const ssize_t n = ::read(sighup_fd_, &info, sizeof(info));
    if (n == sizeof(info)) continue;
    break; // EAGAIN or error
  }
  ScheduleDebounce();
}

void ReloadWatcher::HandleInotify() {
  if (stopped_) return;
  // Drain the inotify events.
  char buf[4096];
  bool relevant = false;
  for (;;) {
    const ssize_t n = ::read(inotify_fd_, buf, sizeof(buf));
    if (n <= 0) break;
    // Check if any event matches the config file name.
    const std::string filename = std::filesystem::path(config_path_).filename().string();
    for (const char *ptr = buf; ptr < buf + n;) {
      const auto *event = reinterpret_cast<const inotify_event *>(ptr);
      if (event->len > 0 &&
          std::string_view(event->name, strnlen(event->name, event->len)) == filename) {
        relevant = true;
      }
      // Also watch for directory recreation (IN_CREATE with no name).
      if (event->mask & IN_CREATE && event->len == 0) {
        // Re-arm the watch in case the directory was recreated.
        if (watch_descriptor_ >= 0) {
          (void)::inotify_rm_watch(inotify_fd_, watch_descriptor_);
        }
        watch_descriptor_ = ::inotify_add_watch(inotify_fd_, watch_dir_.c_str(), kInotifyMask);
      }
      ptr += sizeof(inotify_event) + event->len;
    }
  }
  if (relevant) {
    ScheduleDebounce();
  }
}

void ReloadWatcher::HandleTimer() {
  if (stopped_) return;
  // Drain the timerfd counter.
  std::uint64_t expirations = 0;
  for (;;) {
    const ssize_t n = ::read(timer_fd_, &expirations, sizeof(expirations));
    if (n == sizeof(expirations)) continue;
    break; // EAGAIN
  }
  if (callback_) callback_();
}

void ReloadWatcher::ScheduleDebounce() {
  if (stopped_ || timer_fd_ < 0) return;
  // Arm (or re-arm) the debounce timer for 200ms.
  ArmTimer(timer_fd_, 200'000'000);
}

} // namespace aegisgate::runtime
