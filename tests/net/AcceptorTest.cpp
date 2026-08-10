#include <fcntl.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "aegisgate/net/Acceptor.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/Socket.h"

namespace aegisgate::net {
namespace {

TEST(AcceptorTest, DispatchesLoopbackClientWithNonblockingDescriptor) {
  EventLoop loop;
  Acceptor acceptor(loop, "127.0.0.1", 0);
  ASSERT_NE(acceptor.port(), 0U);

  int accepted_fd = -1;
  acceptor.SetNewConnectionCallback([&](int fd) {
    accepted_fd = fd;
    EXPECT_TRUE(Socket::IsNonblocking(fd));
    loop.Quit();
  });
  acceptor.Listen();

  Socket client = Socket::ConnectLoopback(acceptor.port());
  loop.Loop();

  ASSERT_NE(accepted_fd, -1);
  EXPECT_EQ(::close(accepted_fd), 0);
}

TEST(AcceptorTest, CallbackCanTakeOwnershipOfAcceptedConnection) {
  EventLoop loop;
  Acceptor acceptor(loop, "127.0.0.1", 0);
  Socket accepted_connection;
  acceptor.SetNewConnectionCallback([&](int fd) {
    accepted_connection = Socket(fd);
    loop.Quit();
  });
  acceptor.Listen();

  Socket client = Socket::ConnectLoopback(acceptor.port());
  loop.Loop();

  ASSERT_TRUE(accepted_connection.Valid());
  EXPECT_NE(::fcntl(accepted_connection.Fd(), F_GETFD), -1);
}

} // namespace
} // namespace aegisgate::net
