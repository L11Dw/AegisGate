// A6: ReloadWatcher tests.
// ReloadWatcher uses signalfd (SIGHUP) and inotify to detect config file
// changes and trigger a debounced reload callback.

#include <chrono>
#include <fstream>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/inotify.h>
#include <unistd.h>

#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/TimerQueue.h"
#include "aegisgate/runtime/ReloadWatcher.h"

namespace aegisgate::runtime {
namespace {

std::string WriteTempConfig(const std::string &content, const std::string &name) {
  const std::string path = "/tmp/aegisgate_watcher_" + name;
  std::ofstream ofs(path);
  ofs << content;
  ofs.close();
  return path;
}

// ---------------------------------------------------------------------------
// ReloadWatcher basic tests
// ---------------------------------------------------------------------------

TEST(ReloadWatcherTest, SighupFdIsValidAfterConstruction) {
  const std::string path = WriteTempConfig("test", "sighup_fd.yaml");
  int reload_count = 0;
  ReloadWatcher watcher(path, [&] { ++reload_count; });
  EXPECT_GE(watcher.sighup_fd(), 0);
  EXPECT_GE(watcher.inotify_fd(), 0);
}

TEST(ReloadWatcherTest, StopClosesAllFds) {
  const std::string path = WriteTempConfig("test", "stop_closes.yaml");
  int reload_count = 0;
  ReloadWatcher watcher(path, [&] { ++reload_count; });
  const int sighup = watcher.sighup_fd();
  const int inotify = watcher.inotify_fd();
  EXPECT_GE(sighup, 0);
  EXPECT_GE(inotify, 0);

  watcher.Stop();
  EXPECT_EQ(watcher.sighup_fd(), -1);
  EXPECT_EQ(watcher.inotify_fd(), -1);

  // Fds should be closed — fcntl should return EBADF.
  EXPECT_EQ(::fcntl(sighup, F_GETFD), -1);
  EXPECT_EQ(::fcntl(inotify, F_GETFD), -1);
}

TEST(ReloadWatcherTest, StopIsIdempotent) {
  const std::string path = WriteTempConfig("test", "stop_idempotent.yaml");
  int reload_count = 0;
  ReloadWatcher watcher(path, [&] { ++reload_count; });
  watcher.Stop();
  watcher.Stop(); // must not crash
}

// Helper: run an EventLoop with watcher Channels until a condition or timeout.
void RunWatcherLoop(net::EventLoop &loop, ReloadWatcher &watcher,
                    std::function<bool()> done, int timeout_ms = 1000) {
  net::Channel sighup_ch(loop, watcher.sighup_fd());
  net::Channel inotify_ch(loop, watcher.inotify_fd());
  net::Channel timer_ch(loop, watcher.timer_fd());

  sighup_ch.SetReadCallback([&] { watcher.HandleSighup(); });
  inotify_ch.SetReadCallback([&] { watcher.HandleInotify(); });
  timer_ch.SetReadCallback([&] {
    watcher.HandleTimer();
    if (done()) loop.Quit();
  });

  sighup_ch.EnableReading();
  inotify_ch.EnableReading();
  timer_ch.EnableReading();

  // Quit after timeout even if condition not met.
  net::TimerQueue timers(loop);
  (void)timers.ScheduleAfter(std::chrono::milliseconds(timeout_ms), [&] { loop.Quit(); });

  loop.Loop();

  sighup_ch.Remove();
  inotify_ch.Remove();
  timer_ch.Remove();
}

TEST(ReloadWatcherTest, InotifyTriggersDebouncedCallback) {
  const std::string path = WriteTempConfig("initial", "inotify_trigger.yaml");
  int reload_count = 0;
  ReloadWatcher watcher(path, [&] { ++reload_count; });

  net::EventLoop loop;
  // Write to the config file after a short delay (give the loop time to start).
  std::thread writer([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::ofstream ofs(path);
    ofs << "updated content";
    ofs.close();
  });

  RunWatcherLoop(loop, watcher, [&] { return reload_count >= 1; });
  writer.join();
  EXPECT_GE(reload_count, 1);
}

TEST(ReloadWatcherTest, InotifyAtomicRenameTriggersCallback) {
  const std::string path = WriteTempConfig("initial", "atomic_rename.yaml");
  int reload_count = 0;
  ReloadWatcher watcher(path, [&] { ++reload_count; });

  net::EventLoop loop;
  std::thread writer([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const std::string tmp = path + ".tmp";
    {
      std::ofstream ofs(tmp);
      ofs << "new content via rename";
      ofs.close();
    }
    EXPECT_EQ(::rename(tmp.c_str(), path.c_str()), 0);
  });

  RunWatcherLoop(loop, watcher, [&] { return reload_count >= 1; });
  writer.join();
  EXPECT_GE(reload_count, 1);
}

TEST(ReloadWatcherTest, StopPreventsLateCallback) {
  const std::string path = WriteTempConfig("initial", "stop_prevents.yaml");
  int reload_count = 0;
  ReloadWatcher watcher(path, [&] { ++reload_count; });
  watcher.Stop();

  // Write after stop — fds are closed, no callback possible.
  {
    std::ofstream ofs(path);
    ofs << "after stop";
    ofs.close();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(reload_count, 0);
}

TEST(ReloadWatcherTest, MultipleChangesDebounceIntoOneCallback) {
  const std::string path = WriteTempConfig("initial", "debounce.yaml");
  int reload_count = 0;
  ReloadWatcher watcher(path, [&] { ++reload_count; });

  net::EventLoop loop;
  std::thread writer([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    for (int i = 0; i < 5; ++i) {
      std::ofstream ofs(path);
      ofs << "update " << i;
      ofs.close();
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  });

  RunWatcherLoop(loop, watcher, [&] { return reload_count >= 1; }, 2000);
  writer.join();
  // Debounce should coalesce rapid writes.
  EXPECT_GE(reload_count, 1);
  EXPECT_LE(reload_count, 3);
}

TEST(ReloadWatcherTest, SighupTriggersCallback) {
  const std::string path = WriteTempConfig("initial", "sighup_trigger.yaml");
  int reload_count = 0;
  ReloadWatcher watcher(path, [&] { ++reload_count; });

  net::EventLoop loop;
  // Send SIGHUP to the current process after a short delay.
  std::thread sender([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(::kill(::getpid(), SIGHUP), 0);
  });

  RunWatcherLoop(loop, watcher, [&] { return reload_count >= 1; });
  sender.join();
  EXPECT_GE(reload_count, 1);
}

} // namespace
} // namespace aegisgate::runtime
