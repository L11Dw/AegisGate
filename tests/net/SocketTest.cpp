#include <unistd.h>

#include <poll.h>

#include <gtest/gtest.h>

#include "aegisgate/net/Socket.h"

namespace aegisgate::net {
namespace {

TEST(SocketTest, AcceptsLoopbackClientWithNonblockingDescriptor) {
  Socket listener = Socket::ListenLoopback();
  ASSERT_NE(listener.BoundPort(), 0U);
  EXPECT_EQ(listener.Accept(), -1);

  Socket client = Socket::ConnectLoopback(listener.BoundPort());
  const int accepted_fd = listener.Accept();
  ASSERT_NE(accepted_fd, -1);

  EXPECT_TRUE(client.IsNonblocking());
  EXPECT_TRUE(Socket::IsNonblocking(accepted_fd));
  EXPECT_EQ(::close(accepted_fd), 0);
}

TEST(SocketTest, CreatesAReusableNonblockingTcpSocket) {
  Socket listener = Socket::ListenLoopback();
  Socket client = Socket::CreateNonblockingTcp();
  EXPECT_TRUE(client.IsNonblocking());
  const Socket::ConnectResult result = client.ConnectToLoopback(listener.BoundPort());
  EXPECT_NE(result, Socket::ConnectResult::kError);
}

TEST(SocketTest, LiteralIpv4ConnectDoesNotSubstituteLoopback) {
  Socket listener = Socket::ListenLoopback();
  Socket client = Socket::CreateNonblockingTcp();

  const Socket::ConnectResult result = client.ConnectToIpv4({127, 0, 0, 2}, listener.BoundPort());
  if (result == Socket::ConnectResult::kInProgress) {
    pollfd descriptor{client.Fd(), POLLOUT, 0};
    ASSERT_GT(::poll(&descriptor, 1, 1000), 0);
    EXPECT_NE(client.PendingError(), 0);
  } else {
    EXPECT_EQ(result, Socket::ConnectResult::kError);
  }
  EXPECT_EQ(listener.Accept(), -1);
}

} // namespace
} // namespace aegisgate::net
