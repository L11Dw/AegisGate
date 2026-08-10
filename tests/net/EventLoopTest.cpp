#include <array>
#include <cerrno>
#include <chrono>
#include <exception>
#include <memory>
#include <thread>
#include <type_traits>

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"

namespace aegisgate::net {
namespace {

static_assert(!std::is_copy_constructible_v<Channel>);
static_assert(!std::is_copy_assignable_v<Channel>);
static_assert(!std::is_move_constructible_v<Channel>);
static_assert(!std::is_move_assignable_v<Channel>);

TEST(EventLoopTest, DispatchesReadableSocketpairChannelAndQuits) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  Channel channel(loop, sockets[0]);
  bool called = false;
  channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(sockets[0], &byte, 1), 1);
    EXPECT_EQ(byte, 'x');
    called = true;
    loop.Quit();
  });
  channel.EnableReading();

  ASSERT_EQ(::write(sockets[1], "x", 1), 1);
  loop.Loop();

  EXPECT_TRUE(called);
  channel.Remove();
  EXPECT_EQ(::close(sockets[0]), 0);
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(EventLoopTest, DispatchesPeerCloseToReadCallbackAndQuits) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  Channel channel(loop, sockets[0]);
  bool called = false;
  channel.SetReadCallback([&] {
    char byte = '\0';
    EXPECT_EQ(::read(sockets[0], &byte, 1), 0);
    called = true;
    loop.Quit();
  });
  channel.EnableReading();

  ASSERT_EQ(::close(sockets[1]), 0);
  loop.Loop();

  EXPECT_TRUE(called);
  channel.Remove();
  EXPECT_EQ(::close(sockets[0]), 0);
}

TEST(EventLoopTest, KeepsReadInterestWhenWritableInterestIsDisabled) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  Channel channel(loop, sockets[0]);
  bool write_called = false;
  bool read_called = false;
  channel.SetWriteCallback([&] {
    write_called = true;
    channel.DisableWriting();
    ASSERT_EQ(::write(sockets[1], "r", 1), 1);
  });
  channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(sockets[0], &byte, 1), 1);
    EXPECT_EQ(byte, 'r');
    read_called = true;
    loop.Quit();
  });
  channel.EnableReading();
  channel.EnableWriting();

  loop.Loop();

  EXPECT_TRUE(write_called);
  EXPECT_TRUE(read_called);
  channel.Remove();
  EXPECT_EQ(::close(sockets[0]), 0);
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(EventLoopTest, DoesNotDispatchChannelAfterItIsDestroyed) {
  std::array<int, 2> stale_sockets{};
  std::array<int, 2> wake_sockets{};
  ASSERT_EQ(
      ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, stale_sockets.data()),
      0);
  ASSERT_EQ(
      ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_sockets.data()),
      0);

  EventLoop loop;
  bool destroyed_channel_called = false;
  {
    Channel channel(loop, stale_sockets[0]);
    channel.SetReadCallback([&] { destroyed_channel_called = true; });
    channel.EnableReading();
  }

  Channel wake_channel(loop, wake_sockets[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(wake_sockets[0], &byte, 1), 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  ASSERT_EQ(::write(stale_sockets[1], "x", 1), 1);
  std::thread wake_loop([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(::write(wake_sockets[1], "q", 1), 1);
  });
  loop.Loop();
  wake_loop.join();

  EXPECT_FALSE(destroyed_channel_called);
  wake_channel.Remove();
  EXPECT_EQ(::close(stale_sockets[0]), 0);
  EXPECT_EQ(::close(stale_sockets[1]), 0);
  EXPECT_EQ(::close(wake_sockets[0]), 0);
  EXPECT_EQ(::close(wake_sockets[1]), 0);
}

TEST(EventLoopTest, DestroysRemovedChannelAfterEventLoop) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  std::unique_ptr<Channel> channel;
  {
    auto loop = std::make_unique<EventLoop>();
    channel = std::make_unique<Channel>(*loop, sockets[0]);
    channel->EnableReading();
    channel->Remove();

    loop.reset();
  }

  channel.reset();
  EXPECT_EQ(::close(sockets[0]), 0);
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(EventLoopTest, SkipsDestroyedChannelFromTheSameEpollBatch) {
  std::array<int, 2> first_sockets{};
  std::array<int, 2> second_sockets{};
  ASSERT_EQ(
      ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, first_sockets.data()),
      0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0,
                         second_sockets.data()),
            0);

  EventLoop loop;
  auto second_channel = std::make_unique<Channel>(loop, second_sockets[0]);
  bool destroyed_channel_called = false;
  int first_callback_count = 0;
  second_channel->SetReadCallback([&] { destroyed_channel_called = true; });

  Channel first_channel(loop, first_sockets[0]);
  first_channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(first_sockets[0], &byte, 1), 1);
    ++first_callback_count;
    if (first_callback_count == 1) {
      EXPECT_EQ(byte, 'a');
      second_channel.reset();
    } else {
      EXPECT_EQ(byte, 'q');
      loop.Quit();
    }
  });

  // Register and make the first socket ready first so both events are returned
  // by one epoll_wait batch, with the first callback destroying the second.
  first_channel.EnableReading();
  second_channel->EnableReading();
  ASSERT_EQ(::write(first_sockets[1], "a", 1), 1);
  ASSERT_EQ(::write(second_sockets[1], "b", 1), 1);

  std::thread quit_loop([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(::write(first_sockets[1], "q", 1), 1);
  });
  loop.Loop();
  quit_loop.join();

  EXPECT_FALSE(destroyed_channel_called);
  EXPECT_EQ(first_callback_count, 2);
  first_channel.Remove();
  EXPECT_EQ(::close(first_sockets[0]), 0);
  EXPECT_EQ(::close(first_sockets[1]), 0);
  EXPECT_EQ(::close(second_sockets[0]), 0);
  EXPECT_EQ(::close(second_sockets[1]), 0);
}

TEST(EventLoopTest, DrainsTasksQueuedByDeferredTaskBeforeLeavingSafePoint) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()), 0);

  EventLoop loop;
  Channel channel(loop, sockets[0]);
  bool outer_ran = false;
  bool inner_ran = false;
  channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(sockets[0], &byte, 1), 1);
    loop.QueueAfterCurrentBatch([&] {
      outer_ran = true;
      loop.QueueAfterCurrentBatch([&] { inner_ran = true; });
      loop.Quit();
    });
  });
  channel.EnableReading();

  ASSERT_EQ(::write(sockets[1], "x", 1), 1);
  loop.Loop();

  EXPECT_TRUE(outer_ran);
  EXPECT_TRUE(inner_ran);
  channel.Remove();
  EXPECT_EQ(::close(sockets[0]), 0);
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(EventLoopTest, RejectsDeferredTaskQueuedFromAnotherThread) {
  EventLoop loop;
  std::exception_ptr error;
  std::thread other([&] {
    try {
      loop.QueueAfterCurrentBatch([] {});
    } catch (...) {
      error = std::current_exception();
    }
  });
  other.join();
  ASSERT_NE(error, nullptr);
  try {
    std::rethrow_exception(error);
  } catch (const std::logic_error &) {
  } catch (...) {
    FAIL() << "wrong exception type";
  }
}

} // namespace
} // namespace aegisgate::net
