#include <unistd.h>

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

  EXPECT_TRUE(Socket::IsNonblocking(accepted_fd));
  EXPECT_EQ(::close(accepted_fd), 0);
}

} // namespace
} // namespace aegisgate::net
