#pragma once

#include <cstdint>
#include <functional>
#include <string>

namespace aegisgate::runtime {

// Watches a config file for changes via SIGHUP (signalfd) and inotify.
// Runs on the control EventLoop; triggers a reload callback on change.
//
// - SIGHUP: uses signalfd to convert the signal into a Channel event.
// - inotify: watches the config file's directory (not the file inode)
//   for close-write, moved-to, and attrib events — compatible with
//   atomic rename write patterns.
// - Debounce: 200ms after the first event, fires the reload callback.
//   Multiple events within the debounce window coalesce into one reload.
// - Watch recovery: if the directory is deleted and recreated, the
//   watcher re-arms the inotify watch.
class ReloadWatcher {
public:
  using ReloadCallback = std::function<void()>;

  // config_path: the YAML file to watch.
  // callback: called on the control loop after debounce.
  ReloadWatcher(std::string config_path, ReloadCallback callback);
  ~ReloadWatcher();

  ReloadWatcher(const ReloadWatcher &) = delete;
  ReloadWatcher &operator=(const ReloadWatcher &) = delete;

  // Stops watching and closes all fds.  Idempotent.  After Stop,
  // no further callbacks are fired.
  void Stop() noexcept;

  // File descriptors for epoll registration.
  // Returns -1 if the component is not available or stopped.
  [[nodiscard]] int sighup_fd() const noexcept { return sighup_fd_; }
  [[nodiscard]] int inotify_fd() const noexcept { return inotify_fd_; }
  // Debounce timer fd.  When it fires, the callback should be invoked.
  [[nodiscard]] int timer_fd() const noexcept { return timer_fd_; }

  // Event handlers for the control loop's Channel callbacks.
  void HandleSighup();
  void HandleInotify();
  void HandleTimer();

private:
  void ScheduleDebounce();

  std::string config_path_;
  std::string watch_dir_;
  ReloadCallback callback_;
  int sighup_fd_ = -1;
  int inotify_fd_ = -1;
  int watch_descriptor_ = -1;
  // Debounce timer fd.  -1 if not armed.
  int timer_fd_ = -1;
  bool stopped_ = false;
};

} // namespace aegisgate::runtime
