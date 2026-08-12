#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "aegisgate/net/TimerQueue.h"

namespace aegisgate::net {
class Channel;
class EventLoop;
} // namespace aegisgate::net

namespace aegisgate::runtime {

// Control-loop-owned reload trigger sources.  inotify watches the containing
// directory (rather than the file inode) so an editor's atomic rename is seen;
// SIGHUP arrives through signalfd after the process blocks it before worker
// threads are created.  Neither callback parses configuration: both merely
// coalesce into one 200ms debounce callback.
class ReloadWatcher {
public:
  static void BlockSighupForProcess();

  ReloadWatcher(net::EventLoop &loop, std::string config_path,
                std::function<void()> trigger, bool watch_sighup = true);
  ~ReloadWatcher();

  ReloadWatcher(const ReloadWatcher &) = delete;
  ReloadWatcher &operator=(const ReloadWatcher &) = delete;

private:
  void HandleInotify();
  void HandleSighup();
  void Debounce();
  void TryRewatch();
  void Stop() noexcept;

  net::EventLoop &loop_;
  std::string directory_;
  std::string filename_;
  std::function<void()> trigger_;
  std::unique_ptr<net::TimerQueue> timers_;
  std::unique_ptr<net::Channel> inotify_channel_;
  std::unique_ptr<net::Channel> sighup_channel_;
  int inotify_fd_ = -1;
  int watch_descriptor_ = -1;
  int sighup_fd_ = -1;
  net::TimerQueue::TimerId debounce_timer_ = 0;
};

} // namespace aegisgate::runtime
