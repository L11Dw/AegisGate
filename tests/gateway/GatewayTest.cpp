#include <array>
#include <cerrno>
#include <chrono>
#include <string>
#include <string_view>
#include <thread>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/config/Config.h"
#include "aegisgate/gateway/Gateway.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/Socket.h"

namespace aegisgate::gateway {
namespace {

using Deadline = std::chrono::steady_clock::time_point;

Deadline TestDeadline() { return std::chrono::steady_clock::now() + std::chrono::seconds(5); }

int RemainingMilliseconds(Deadline deadline) {
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
      deadline - std::chrono::steady_clock::now());
  return remaining.count() > 0 ? static_cast<int>(remaining.count()) : 0;
}

bool WaitFor(int fd, short events, Deadline deadline) {
  pollfd descriptor{fd, events, 0};
  return ::poll(&descriptor, 1, RemainingMilliseconds(deadline)) > 0;
}

int AcceptUntil(const net::Socket &listener, Deadline deadline, std::string &error) {
  for (;;) {
    const int fd = listener.Accept();
    if (fd >= 0) {
      const int flags = ::fcntl(fd, F_GETFL);
      if (flags >= 0) (void)::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
      return fd;
    }
    if (!WaitFor(listener.Fd(), POLLIN, deadline)) {
      error = "upstream accept timed out";
      return -1;
    }
  }
}

bool WriteAll(int fd, std::string_view bytes, Deadline deadline, std::string &error) {
  std::size_t sent = 0;
  while (sent != bytes.size()) {
    const ssize_t count = ::write(fd, bytes.data() + sent, bytes.size() - sent);
    if (count > 0) {
      sent += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) continue;
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) &&
        WaitFor(fd, POLLOUT, deadline)) continue;
    error = "write failed";
    return false;
  }
  return true;
}

std::string ReadExact(int fd, std::size_t size, Deadline deadline, std::string &error) {
  std::string result(size, '\0');
  std::size_t received = 0;
  while (received != size) {
    if (!WaitFor(fd, POLLIN | POLLHUP, deadline)) {
      error = "read timed out";
      return {};
    }
    const ssize_t count = ::read(fd, result.data() + received, size - received);
    if (count > 0) {
      received += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) continue;
    error = "unexpected EOF or read error";
    return {};
  }
  return result;
}

TEST(GatewayTest, RoutesTwoSequentialKeepAliveRequestsOverRealTcp) {
  net::Socket upstream_listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1},
                                  upstream_listener.BoundPort(), 1};
  const config::Config config{{{"api", "gateway.test", "/v1", {endpoint}, 10, 10, 4}}};

  std::string upstream_error;
  std::array<std::string, 2> upstream_requests;
  std::thread upstream([&] {
    const int fd = AcceptUntil(upstream_listener, TestDeadline(), upstream_error);
    if (fd < 0) return;
    constexpr std::array<std::string_view, 2> expected{
        "GET /v1/one HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\n"
        "Connection: keep-alive\r\n\r\n",
        "GET /v1/two HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\n"
        "Connection: keep-alive\r\n\r\n"};
    constexpr std::array<std::string_view, 2> responses{
        "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\none",
        "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo"};
    for (std::size_t index = 0; index != expected.size(); ++index) {
      upstream_requests[index] = ReadExact(fd, expected[index].size(), TestDeadline(), upstream_error);
      if (!upstream_error.empty()) break;
      if (!WriteAll(fd, responses[index], TestDeadline(), upstream_error)) break;
    }
    (void)::close(fd);
  });

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(wake_fds[0], &byte, 1), 1);
    loop.Quit();
  });
  wake_channel.EnableReading();
  Gateway gateway(loop, config, "127.0.0.1", 0);
  gateway.Start();

  std::string client_error;
  std::array<std::string, 2> client_responses;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::array<std::string_view, 2> requests{
        "GET /v1/one HTTP/1.1\r\nHost: gateway.test\r\n\r\n",
        "GET /v1/two HTTP/1.1\r\nHost: gateway.test\r\n\r\n"};
    constexpr std::array<std::string_view, 2> responses{
        "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\none",
        "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo"};
    for (std::size_t index = 0; index != requests.size(); ++index) {
      if (!WriteAll(socket.Fd(), requests[index], TestDeadline(), client_error)) break;
      client_responses[index] = ReadExact(socket.Fd(), responses[index].size(), TestDeadline(), client_error);
      if (!client_error.empty()) break;
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });

  loop.Loop();
  client.join();
  upstream.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(upstream_error.empty()) << upstream_error;
  EXPECT_EQ(upstream_requests[0], "GET /v1/one HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n");
  EXPECT_EQ(upstream_requests[1], "GET /v1/two HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n");
  EXPECT_EQ(client_responses[0], "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\none");
  EXPECT_EQ(client_responses[1], "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo");

  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, ReapsClosedClientAfterItsChannelCallbackReturns) {
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1}, 1, 1};
  const config::Config config{{{"api", "gateway.test", "/", {endpoint}, 10, 10, 4}}};
  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  Gateway gateway(loop, config, "127.0.0.1", 0);
  gateway.Start();
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(wake_fds[0], &byte, 1), 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  std::string client_error;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request =
        "GET / HTTP/1.1\r\nHost: unknown.test\r\nConnection: close\r\n\r\n";
    constexpr std::string_view response =
        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
    if (WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      EXPECT_EQ(ReadExact(socket.Fd(), response.size(), TestDeadline(), client_error), response);
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });

  loop.Loop();
  client.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_EQ(gateway.ClientCount(), 0U);
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

} // namespace
} // namespace aegisgate::gateway
