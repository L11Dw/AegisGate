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

TEST(ReloadWatcherTest, AtomicRenameOfConfiguredFileTriggersOneDebouncedReload) {
  char path[] = "/tmp/aegisgate-watch-XXXXXX";
  const int config = ::mkstemp(path);
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
  ASSERT_EQ(::rename(replacement, path), 0);
  loop.Loop();

  EXPECT_FALSE(timed_out);
  EXPECT_EQ(triggers, 1U);
  EXPECT_EQ(::unlink(path), 0);
}

TEST(ReloadWatcherTest, WatchRecoveryAfterDirectoryRecreation) {
  // Use a fixed path that we can delete and recreate.
  const std::string dir_path = "/tmp/aegisgate-watch-recovery-test";
  const std::string path = dir_path + "/config.yaml";

  // Clean up any stale state from previous runs.
  (void)::unlink(path.c_str());
  (void)::rmdir(dir_path.c_str());

  // Create the directory and config file.
  ASSERT_EQ(::mkdir(dir_path.c_str(), 0700), 0);
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
  ASSERT_EQ(::rmdir(dir_path.c_str()), 0);

  // Recreate the directory with the same path.  The watcher's retry timer
  // (500ms) will re-arm the watch once the directory exists again.
  ASSERT_EQ(::mkdir(dir_path.c_str(), 0700), 0);

  // Wait for the retry timer to fire and re-arm the watch.
  // Then write to the config file to trigger the debounce.
  timed_out = false;
  triggers = 0;
  (void)timeout.ScheduleAfter(std::chrono::seconds(5), [&] {
    timed_out = true;
    loop.Quit();
  });
  // Schedule a write after the retry delay has had time to fire.
  (void)timeout.ScheduleAfter(std::chrono::milliseconds(700), [&] {
    const int fd2 = ::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
    if (fd2 >= 0) {
      (void)::write(fd2, "new2\n", 5);
      (void)::close(fd2);
    }
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
  EXPECT_EQ(::rmdir(dir_path.c_str()), 0);
}

TEST(ReloadWatcherTest, WatchRetryDoesNotSpinOrDuplicateTriggers) {
  // Prove that:
  // 1. When the directory is deleted, the retry timer fires once and does not
  //    spin infinitely.
  // 2. When the directory is recreated, exactly one trigger fires (no duplicate).
  const std::string dir_path = "/tmp/aegisgate-watch-nospin-test";
  const std::string path = dir_path + "/config.yaml";

  // Clean up any stale state from previous runs.
  (void)::unlink(path.c_str());
  (void)::rmdir(dir_path.c_str());

  ASSERT_EQ(::mkdir(dir_path.c_str(), 0700), 0);
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
  ASSERT_EQ(::rmdir(dir_path.c_str()), 0);

  // Wait for the retry timer to fire (kWatchRetryDelay = 500ms).
  // The retry will fail because the directory is gone, but it should not spin.
  // Wait 2 seconds to prove no infinite retry.
  bool timed_out = false;
  (void)timeout.ScheduleAfter(std::chrono::seconds(2), [&] {
    timed_out = true;
    loop.Quit();
  });
  auto wait_for_timeout = std::make_shared<std::function<void()>>();
  *wait_for_timeout = [&] {
    if (timed_out) {
      loop.Quit();
      return;
    }
    (void)timeout.ScheduleAfter(std::chrono::milliseconds(50), *wait_for_timeout);
  };
  (void)timeout.ScheduleAfter(std::chrono::milliseconds(50), *wait_for_timeout);
  loop.Loop();
  EXPECT_TRUE(timed_out);
  // No triggers should have fired (directory was deleted, no file writes).
  EXPECT_EQ(triggers, 0U);

  // Now recreate the directory and write to the config file.
  // The retry timer should have re-armed the watch by now.
  ASSERT_EQ(::mkdir(dir_path.c_str(), 0700), 0);
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
      (void)::write(fd, "new\n", 4);
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
  // Exactly one trigger should have fired (no duplicate).
  EXPECT_EQ(triggers, 1U);

  // Cleanup.
  EXPECT_EQ(::unlink(path.c_str()), 0);
  EXPECT_EQ(::rmdir(dir_path.c_str()), 0);
}

} // namespace
} // namespace aegisgate::runtime
