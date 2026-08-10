#include <array>
#include <cerrno>

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"

namespace aegisgate::net {
namespace {

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

} // namespace
} // namespace aegisgate::net
