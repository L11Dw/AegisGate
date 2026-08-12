#include <array>
#include <cerrno>
#include <chrono>
#include <string>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/TimerQueue.h"
#include "aegisgate/runtime/ReloadWatcher.h"

namespace aegisgate::runtime {
namespace {

// Helper: creates a unique temp directory and returns its path.
std::string MakeTempDir() {
  char buf[] = "/tmp/aegisgate-watch-XXXXXX";
  char *result = ::mkdtemp(buf);
  if (!result) return {};
  return result;
}

TEST(ReloadWatcherTest, AtomicRenameOfConfiguredFileTriggersOneDebouncedReload) {
  const std::string dir = MakeTempDir();
  ASSERT_FALSE(dir.empty());
  const std::string path = dir + "/config.yaml";
  const int config = ::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
  ASSERT_GE(config, 0);
  ASSERT_EQ(::write(config, "old\n", 4), 4);
  ASSERT_EQ(::close(config), 0);

  char replacement[] = "/tmp/aegisgate-watch-replacement-XXXXXX";
  const int staged = ::mkstemp(replacement);
  ASSERT_GE(staged, 0);
  ASSERT_EQ(::write(staged, "new\n", 4), 4);
  ASSERT_EQ(::close(staged), 0);

  net::EventLoop loop;
  net::TimerQueue timeout(loop);
  std::size_t triggers = 0;
  ReloadWatcher watcher(loop, path, [&] {
    ++triggers;
    loop.Quit();
  }, /*watch_sighup=*/false);
  bool timed_out = false;
  (void)timeout.ScheduleAfter(std::chrono::seconds(5), [&] {
    timed_out = true;
    loop.Quit();
  });
  ASSERT_EQ(::rename(replacement, path.c_str()), 0);
  loop.Loop();

  EXPECT_FALSE(timed_out);
  EXPECT_EQ(triggers, 1U);
  EXPECT_EQ(::unlink(path.c_str()), 0);
  EXPECT_EQ(::rmdir(dir.c_str()), 0);
}

TEST(ReloadWatcherTest, WatchRecoveryAfterDirectoryRecreation) {
  const std::string dir = MakeTempDir();
  ASSERT_FALSE(dir.empty());
  const std::string path = dir + "/config.yaml";
  const int config_fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
  ASSERT_GE(config_fd, 0);
  ASSERT_EQ(::write(config_fd, "old\n", 4), 4);
  ASSERT_EQ(::close(config_fd), 0);

  net::EventLoop loop;
  net::TimerQueue timeout(loop);
  std::size_t triggers = 0;
  ReloadWatcher watcher(loop, path, [&] { ++triggers; }, /*watch_sighup=*/false);

  // First trigger: write to the config file.
  const int fd1 = ::open(path.c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC);
  ASSERT_GE(fd1, 0);
  ASSERT_EQ(::write(fd1, "new1\n", 5), 5);
  ASSERT_EQ(::close(fd1), 0);

  // Wait for the debounce to fire.
  bool timed_out = false;
  (void)timeout.ScheduleAfter(std::chrono::seconds(5), [&] {
    timed_out = true;
    loop.Quit();
  });
  auto check = std::make_shared<std::function<void()>>();
  *check = [&] {
    if (triggers > 0 || timed_out) {
      loop.Quit();
      return;
    }
    (void)timeout.ScheduleAfter(std::chrono::milliseconds(10), *check);
  };
  (void)timeout.ScheduleAfter(std::chrono::milliseconds(10), *check);
  loop.Loop();
  EXPECT_FALSE(timed_out);
  EXPECT_EQ(triggers, 1U);

  // Delete the directory (this invalidates the inotify watch).
  ASSERT_EQ(::unlink(path.c_str()), 0);
  ASSERT_EQ(::rmdir(dir.c_str()), 0);

  // Recreate the directory with the same path.  The watcher's retry timer
  // (kWatchRetryDelay) will re-arm the watch once the directory exists again.
  // After re-arm, TryRewatch calls Debounce() to pick up any file changes
  // that happened during the directory absence.
  ASSERT_EQ(::mkdir(dir.c_str(), 0700), 0);
  // Write a file before the retry fires, so Debounce on recovery picks it up.
  const int fd2 = ::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
  ASSERT_GE(fd2, 0);
  ASSERT_EQ(::write(fd2, "new2\n", 5), 5);
  ASSERT_EQ(::close(fd2), 0);

  // Wait for the trigger (from Debounce on recovery).
  timed_out = false;
  triggers = 0;
  (void)timeout.ScheduleAfter(std::chrono::seconds(5), [&] {
    timed_out = true;
    loop.Quit();
  });
  auto check2 = std::make_shared<std::function<void()>>();
  *check2 = [&] {
    if (triggers > 0 || timed_out) {
      loop.Quit();
      return;
    }
    (void)timeout.ScheduleAfter(std::chrono::milliseconds(10), *check2);
  };
  (void)timeout.ScheduleAfter(std::chrono::milliseconds(10), *check2);
  loop.Loop();
  EXPECT_FALSE(timed_out);
  EXPECT_EQ(triggers, 1U);

  // Cleanup.
  EXPECT_EQ(::unlink(path.c_str()), 0);
  EXPECT_EQ(::rmdir(dir.c_str()), 0);
}

TEST(ReloadWatcherTest, WatchRetryIsRateLimitedAndDoesNotDuplicateTriggers) {
  const std::string dir = MakeTempDir();
  ASSERT_FALSE(dir.empty());
  const std::string path = dir + "/config.yaml";
  const int config_fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
  ASSERT_GE(config_fd, 0);
  ASSERT_EQ(::write(config_fd, "old\n", 4), 4);
  ASSERT_EQ(::close(config_fd), 0);

  net::EventLoop loop;
  net::TimerQueue timeout(loop);
  std::size_t triggers = 0;
  ReloadWatcher watcher(loop, path, [&] { ++triggers; }, /*watch_sighup=*/false);

  // Delete the directory to invalidate the watch.
  ASSERT_EQ(::unlink(path.c_str()), 0);
  ASSERT_EQ(::rmdir(dir.c_str()), 0);

  // Wait 2 seconds.  The retry timer fires every kWatchRetryDelay (500ms),
  // so we expect roughly 4 attempts (2000/500).  The exact count depends on
  // timing, but it must be > 0 and <= 5 (no infinite spin).
  bool timed_out = false;
  (void)timeout.ScheduleAfter(std::chrono::seconds(2), [&] {
    timed_out = true;
    loop.Quit();
  });
  auto wait = std::make_shared<std::function<void()>>();
  *wait = [&] {
    if (timed_out) {
      loop.Quit();
      return;
    }
    (void)timeout.ScheduleAfter(std::chrono::milliseconds(50), *wait);
  };
  (void)timeout.ScheduleAfter(std::chrono::milliseconds(50), *wait);
  loop.Loop();
  EXPECT_TRUE(timed_out);
  // No triggers (directory was deleted, no file writes).
  EXPECT_EQ(triggers, 0U);
  // Rewatch attempts should be rate-limited: at least 1, at most ~5.
  const std::size_t attempts = watcher.RewatchAttemptCount();
  EXPECT_GE(attempts, 1U);
  EXPECT_LE(attempts, 6U);

  // Now recreate the directory and write to the config file.
  ASSERT_EQ(::mkdir(dir.c_str(), 0700), 0);
  timed_out = false;
  triggers = 0;
  (void)timeout.ScheduleAfter(std::chrono::seconds(5), [&] {
    timed_out = true;
    loop.Quit();
  });
  // Schedule a write after the retry delay.
  (void)timeout.ScheduleAfter(std::chrono::milliseconds(700), [&] {
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
    if (fd >= 0) {
      [[maybe_unused]] auto _w = ::write(fd, "new\n", 4);
      (void)::close(fd);
    }
  });
  auto check = std::make_shared<std::function<void()>>();
  *check = [&] {
    if (triggers > 0 || timed_out) {
      loop.Quit();
      return;
    }
    (void)timeout.ScheduleAfter(std::chrono::milliseconds(10), *check);
  };
  (void)timeout.ScheduleAfter(std::chrono::milliseconds(10), *check);
  loop.Loop();
  EXPECT_FALSE(timed_out);
  // Exactly one trigger (no duplicate).
  EXPECT_EQ(triggers, 1U);

  // Cleanup.
  (void)::unlink(path.c_str());
  (void)::rmdir(dir.c_str());
}

TEST(ReloadWatcherTest, StopPreventsFurtherRewatchAndTriggers) {
  const std::string dir = MakeTempDir();
  ASSERT_FALSE(dir.empty());
  const std::string path = dir + "/config.yaml";
  const int config_fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
  ASSERT_GE(config_fd, 0);
  ASSERT_EQ(::write(config_fd, "old\n", 4), 4);
  ASSERT_EQ(::close(config_fd), 0);

  net::EventLoop loop;
  net::TimerQueue timeout(loop);
  std::size_t triggers = 0;
  auto watcher = std::make_unique<ReloadWatcher>(
      loop, path, [&] { ++triggers; }, /*watch_sighup=*/false);

  // Delete the directory to invalidate the watch.
  ASSERT_EQ(::unlink(path.c_str()), 0);
  ASSERT_EQ(::rmdir(dir.c_str()), 0);

  // Stop the watcher immediately.
  watcher.reset();

  // Wait 2 seconds.  No triggers or rewatch attempts should occur.
  bool timed_out = false;
  (void)timeout.ScheduleAfter(std::chrono::seconds(2), [&] {
    timed_out = true;
    loop.Quit();
  });
  auto wait = std::make_shared<std::function<void()>>();
  *wait = [&] {
    if (timed_out) {
      loop.Quit();
      return;
    }
    (void)timeout.ScheduleAfter(std::chrono::milliseconds(50), *wait);
  };
  (void)timeout.ScheduleAfter(std::chrono::milliseconds(50), *wait);
  loop.Loop();
  EXPECT_TRUE(timed_out);
  EXPECT_EQ(triggers, 0U);

  // Cleanup.
  (void)::mkdir(dir.c_str(), 0700);
  EXPECT_EQ(::rmdir(dir.c_str()), 0);
}

} // namespace
} // namespace aegisgate::runtime
