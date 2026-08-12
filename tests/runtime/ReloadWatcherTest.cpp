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

} // namespace
} // namespace aegisgate::runtime
