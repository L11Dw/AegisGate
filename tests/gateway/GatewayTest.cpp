#include <array>
#include <cerrno>
#include <chrono>
#include <string>
#include <string_view>
#include <thread>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <poll.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/config/Config.h"
#include "aegisgate/gateway/Gateway.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/Socket.h"

#include "../support/WakeFd.h"

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

std::string ReadUntilContains(int fd, std::string_view needle, Deadline deadline,
                              std::string &error) {
  std::string result;
  std::array<char, 4096> bytes{};
  while (result.find(needle) == std::string::npos) {
    if (!WaitFor(fd, POLLIN | POLLHUP, deadline)) {
      error = "read timed out";
      return {};
    }
    const ssize_t count = ::read(fd, bytes.data(), bytes.size());
    if (count > 0) {
      result.append(bytes.data(), static_cast<std::size_t>(count));
      continue;
    }
    if (count < 0 && errno == EINTR) continue;
    error = "unexpected EOF or read error";
    return {};
  }
  return result;
}

net::Socket ListenWithBacklog(int backlog) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) throw std::system_error(errno, std::generic_category(), "socket");
  net::Socket listener(fd);
  int reuse_address = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse_address, sizeof(reuse_address)) < 0) {
    throw std::system_error(errno, std::generic_category(), "setsockopt");
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (::bind(fd, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) < 0) {
    throw std::system_error(errno, std::generic_category(), "bind");
  }
  if (::listen(fd, backlog) < 0) {
    throw std::system_error(errno, std::generic_category(), "listen");
  }
  return listener;
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
  EXPECT_NE(gateway.MetricsText().find("aegisgate_requests_total{route=\"api\",status=\"200\",upstream=\"127.0.0.1:" +
                                       std::to_string(endpoint.port) + "\"} 2\n"),
            std::string::npos);

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

TEST(GatewayTest, ReturnsGatewayTimeoutWhenUpstreamSendsNoFirstByte) {
  net::Socket upstream_listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1}, upstream_listener.BoundPort(), 1};
  config::Config config{{{"api", "gateway.test", "/", {endpoint}, 10, 10, 4}}};
  config.routes[0].connect_timeout_ms = 200;
  config.routes[0].first_byte_timeout_ms = 200;
  config.routes[0].total_timeout_ms = 500;

  std::string upstream_error;
  std::thread upstream([&] {
    const int fd = AcceptUntil(upstream_listener, TestDeadline(), upstream_error);
    if (fd < 0) return;
    constexpr std::string_view request =
        "GET / HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    (void)ReadExact(fd, request.size(), TestDeadline(), upstream_error);
    pollfd descriptor{fd, POLLHUP | POLLIN, 0};
    if (::poll(&descriptor, 1, RemainingMilliseconds(TestDeadline())) <= 0 && upstream_error.empty()) {
      upstream_error = "gateway did not close timed-out upstream";
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
  std::string client_response;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view response =
        "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n";
    if (WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      client_response = ReadExact(socket.Fd(), response.size(), TestDeadline(), client_error);
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  upstream.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(upstream_error.empty()) << upstream_error;
  EXPECT_EQ(client_response, "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n");
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, ReturnsGatewayTimeoutWhenConnectStaysPending) {
  // A backlog of zero holds the first completed handshake.  The next local
  // nonblocking connect remains pending until the listener accepts, giving
  // the connect deadline a deterministic in-process target.
  net::Socket upstream_listener = ListenWithBacklog(0);
  net::Socket backlog_filler = net::Socket::ConnectLoopback(upstream_listener.BoundPort());
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1},
                                  upstream_listener.BoundPort(), 1};
  config::Config config{{{"api", "gateway.test", "/", {endpoint}, 10, 10, 4}}};
  config.routes[0].connect_timeout_ms = 30;
  config.routes[0].first_byte_timeout_ms = 2000;
  config.routes[0].total_timeout_ms = 2500;

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
  std::string client_response;
  std::chrono::steady_clock::duration elapsed{};
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view response =
        "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n";
    const auto started = std::chrono::steady_clock::now();
    if (WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      client_response = ReadExact(socket.Fd(), response.size(), TestDeadline(), client_error);
    }
    elapsed = std::chrono::steady_clock::now() - started;
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_EQ(client_response, "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n");
  EXPECT_LT(elapsed, std::chrono::milliseconds(500));
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, TotalDeadlineWinsAfterFirstByteOfAnIncompleteResponse) {
  net::Socket upstream_listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1}, upstream_listener.BoundPort(), 1};
  config::Config config{{{"api", "gateway.test", "/", {endpoint}, 10, 10, 4}}};
  config.routes[0].connect_timeout_ms = 500;
  config.routes[0].first_byte_timeout_ms = 500;
  config.routes[0].total_timeout_ms = 40;

  std::string upstream_error;
  std::thread upstream([&] {
    const int fd = AcceptUntil(upstream_listener, TestDeadline(), upstream_error);
    if (fd < 0) return;
    constexpr std::string_view request =
        "GET / HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    (void)ReadExact(fd, request.size(), TestDeadline(), upstream_error);
    constexpr std::string_view partial = "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nx";
    if (upstream_error.empty()) (void)WriteAll(fd, partial, TestDeadline(), upstream_error);
    pollfd descriptor{fd, POLLHUP | POLLIN, 0};
    if (::poll(&descriptor, 1, RemainingMilliseconds(TestDeadline())) <= 0 && upstream_error.empty()) {
      upstream_error = "gateway did not close total-timeout upstream";
    }
    (void)::close(fd);
  });

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] { char byte = '\0'; ASSERT_EQ(::read(wake_fds[0], &byte, 1), 1); loop.Quit(); });
  wake_channel.EnableReading();
  Gateway gateway(loop, config, "127.0.0.1", 0);
  gateway.Start();
  std::string client_error;
  std::string client_response;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view response = "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n";
    if (WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      client_response = ReadExact(socket.Fd(), response.size(), TestDeadline(), client_error);
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  upstream.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(upstream_error.empty()) << upstream_error;
  EXPECT_EQ(client_response, "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n");
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, DoesNotRetryGetAfterACompleteResponseHeader) {
  net::Socket first_listener = net::Socket::ListenLoopback();
  net::Socket second_listener = net::Socket::ListenLoopback();
  const config::Endpoint first{"127.0.0.1", {127, 0, 0, 1}, first_listener.BoundPort(), 1};
  const config::Endpoint second{"127.0.0.1", {127, 0, 0, 1}, second_listener.BoundPort(), 1};
  config::Config config{{{"api", "gateway.test", "/", {first, second}, 10, 10, 4}}};
  std::string first_error;
  std::thread first_backend([&] {
    const int fd = AcceptUntil(first_listener, TestDeadline(), first_error);
    if (fd < 0) return;
    constexpr std::string_view request =
        "GET /header HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    (void)ReadExact(fd, request.size(), TestDeadline(), first_error);
    constexpr std::string_view header_only = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n";
    if (first_error.empty()) (void)WriteAll(fd, header_only, TestDeadline(), first_error);
    (void)::close(fd);
  });

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] { char byte = '\0'; ASSERT_EQ(::read(wake_fds[0], &byte, 1), 1); loop.Quit(); });
  wake_channel.EnableReading();
  Gateway gateway(loop, config, "127.0.0.1", 0);
  gateway.Start();
  std::string client_error;
  std::string client_response;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request = "GET /header HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view response = "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n";
    if (WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      client_response = ReadExact(socket.Fd(), response.size(), TestDeadline(), client_error);
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  first_backend.join();

  pollfd descriptor{second_listener.Fd(), POLLIN, 0};
  EXPECT_EQ(::poll(&descriptor, 1, 100), 0) << "GET with response header retried a second endpoint";
  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(first_error.empty()) << first_error;
  EXPECT_EQ(client_response, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n");
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, TotalTimeoutCancelsActiveConnectionBeforeTheNextBorrow) {
  net::Socket upstream_listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1}, upstream_listener.BoundPort(), 1};
  config::Config config{{{"api", "gateway.test", "/", {endpoint}, 10, 10, 4}}};
  config.routes[0].connect_timeout_ms = 500;
  config.routes[0].first_byte_timeout_ms = 500;
  config.routes[0].total_timeout_ms = 40;
  std::string upstream_error;
  std::thread upstream([&] {
    constexpr std::string_view first_request =
        "GET /first HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    constexpr std::string_view second_request =
        "GET /second HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    const int first_fd = AcceptUntil(upstream_listener, TestDeadline(), upstream_error);
    if (first_fd < 0) return;
    (void)ReadExact(first_fd, first_request.size(), TestDeadline(), upstream_error);
    constexpr std::string_view partial = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nx";
    if (upstream_error.empty()) (void)WriteAll(first_fd, partial, TestDeadline(), upstream_error);
    if (!upstream_error.empty() || !WaitFor(first_fd, POLLHUP | POLLIN, TestDeadline())) {
      if (upstream_error.empty()) upstream_error = "gateway did not close timed-out connection";
      (void)::close(first_fd);
      return;
    }
    (void)::close(first_fd);
    const int second_fd = AcceptUntil(upstream_listener, TestDeadline(), upstream_error);
    if (second_fd < 0) return;
    (void)ReadExact(second_fd, second_request.size(), TestDeadline(), upstream_error);
    constexpr std::string_view success = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (upstream_error.empty()) (void)WriteAll(second_fd, success, TestDeadline(), upstream_error);
    (void)::close(second_fd);
  });

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] { char byte = '\0'; ASSERT_EQ(::read(wake_fds[0], &byte, 1), 1); loop.Quit(); });
  wake_channel.EnableReading();
  Gateway gateway(loop, config, "127.0.0.1", 0);
  gateway.Start();
  std::string client_error;
  std::array<std::string, 2> responses;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view first = "GET /first HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view second = "GET /second HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view timeout = "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n";
    constexpr std::string_view success = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (WriteAll(socket.Fd(), first, TestDeadline(), client_error)) {
      responses[0] = ReadExact(socket.Fd(), timeout.size(), TestDeadline(), client_error);
    }
    if (client_error.empty() && WriteAll(socket.Fd(), second, TestDeadline(), client_error)) {
      responses[1] = ReadExact(socket.Fd(), success.size(), TestDeadline(), client_error);
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  upstream.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(upstream_error.empty()) << upstream_error;
  EXPECT_EQ(responses[0], "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n");
  EXPECT_EQ(responses[1], "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, RetriesGetOnceOnADifferentEndpointBeforeResponseHeader) {
  net::Socket first_listener = net::Socket::ListenLoopback();
  net::Socket second_listener = net::Socket::ListenLoopback();
  const config::Endpoint first{"127.0.0.1", {127, 0, 0, 1}, first_listener.BoundPort(), 1};
  const config::Endpoint second{"127.0.0.1", {127, 0, 0, 1}, second_listener.BoundPort(), 1};
  config::Config config{{{"api", "gateway.test", "/", {first, second}, 10, 10, 4}}};
  config.routes[0].connect_timeout_ms = 500;
  config.routes[0].first_byte_timeout_ms = 500;
  config.routes[0].total_timeout_ms = 1000;

  std::string first_error;
  std::string second_error;
  constexpr std::string_view request =
      "GET /retry HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
  std::thread first_backend([&] {
    const int fd = AcceptUntil(first_listener, TestDeadline(), first_error);
    if (fd < 0) return;
    (void)ReadExact(fd, request.size(), TestDeadline(), first_error);
    (void)::close(fd);
  });
  std::thread second_backend([&] {
    const int fd = AcceptUntil(second_listener, TestDeadline(), second_error);
    if (fd < 0) return;
    (void)ReadExact(fd, request.size(), TestDeadline(), second_error);
    constexpr std::string_view response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (second_error.empty() && !WriteAll(fd, response, TestDeadline(), second_error)) {}
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
  std::string client_response;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view inbound = "GET /retry HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (WriteAll(socket.Fd(), inbound, TestDeadline(), client_error)) {
      client_response = ReadExact(socket.Fd(), response.size(), TestDeadline(), client_error);
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  first_backend.join();
  second_backend.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(first_error.empty()) << first_error;
  EXPECT_TRUE(second_error.empty()) << second_error;
  EXPECT_EQ(client_response, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, DoesNotRetryPostAfterUpstreamFailure) {
  net::Socket first_listener = net::Socket::ListenLoopback();
  net::Socket second_listener = net::Socket::ListenLoopback();
  const config::Endpoint first{"127.0.0.1", {127, 0, 0, 1}, first_listener.BoundPort(), 1};
  const config::Endpoint second{"127.0.0.1", {127, 0, 0, 1}, second_listener.BoundPort(), 1};
  const config::Config config{{{"api", "gateway.test", "/", {first, second}, 10, 10, 4}}};
  std::string first_error;
  std::thread first_backend([&] {
    const int fd = AcceptUntil(first_listener, TestDeadline(), first_error);
    if (fd < 0) return;
    constexpr std::string_view request =
        "POST /no-retry HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    (void)ReadExact(fd, request.size(), TestDeadline(), first_error);
    (void)::close(fd);
  });

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] { char byte = '\0'; ASSERT_EQ(::read(wake_fds[0], &byte, 1), 1); loop.Quit(); });
  wake_channel.EnableReading();
  Gateway gateway(loop, config, "127.0.0.1", 0);
  gateway.Start();
  std::string client_error;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request = "POST /no-retry HTTP/1.1\r\nHost: gateway.test\r\nContent-Length: 0\r\n\r\n";
    constexpr std::string_view response = "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n";
    if (WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      EXPECT_EQ(ReadExact(socket.Fd(), response.size(), TestDeadline(), client_error), response);
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  first_backend.join();

  pollfd descriptor{second_listener.Fd(), POLLIN, 0};
  EXPECT_EQ(::poll(&descriptor, 1, 100), 0) << "POST failure retried a second endpoint";
  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(first_error.empty()) << first_error;
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, DoesNotStartFirstByteDeadlineUntilLargeRequestIsFullyWritten) {
  net::Socket upstream_listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1}, upstream_listener.BoundPort(), 1};
  config::Config config{{{"api", "gateway.test", "/", {endpoint}, 10, 10, 4}}};
  config.routes[0].connect_timeout_ms = 500;
  config.routes[0].first_byte_timeout_ms = 200;
  config.routes[0].total_timeout_ms = 1000;
  std::array<int, 2> gate{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, gate.data()), 0);
  std::string upstream_error;
  std::thread upstream([&] {
    const int fd = AcceptUntil(upstream_listener, TestDeadline(), upstream_error);
    if (fd < 0) return;
    if (::write(gate[1], "a", 1) != 1) { upstream_error = "accept gate failed"; (void)::close(fd); return; }
    char command = '\0';
    if (::read(gate[1], &command, 1) != 1 || command != 'd') {
      upstream_error = "drain gate failed";
      (void)::close(fd);
      return;
    }
    std::string bytes;
    constexpr std::size_t kBodySize = 1024 * 1024;
    const auto deadline = TestDeadline();
    while (bytes.find("\r\n\r\n") == std::string::npos ||
           bytes.size() < bytes.find("\r\n\r\n") + 4 + kBodySize) {
      if (!WaitFor(fd, POLLIN | POLLHUP, deadline)) { upstream_error = "drain timed out"; break; }
      std::array<char, 64 * 1024> chunk{};
      const ssize_t count = ::read(fd, chunk.data(), chunk.size());
      if (count > 0) { bytes.append(chunk.data(), static_cast<std::size_t>(count)); continue; }
      if (count < 0 && errno == EINTR) continue;
      upstream_error = "unexpected EOF while draining";
      break;
    }
    constexpr std::string_view response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (upstream_error.empty() && !WriteAll(fd, response, TestDeadline(), upstream_error)) {}
    (void)::close(fd);
  });

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] { char byte = '\0'; ASSERT_EQ(::read(wake_fds[0], &byte, 1), 1); loop.Quit(); });
  wake_channel.EnableReading();
  Gateway gateway(loop, config, "127.0.0.1", 0);
  gateway.Start();
  std::string client_error;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::size_t kBodySize = 1024 * 1024;
    std::string request = "POST /blocked HTTP/1.1\r\nHost: gateway.test\r\nContent-Length: " +
                          std::to_string(kBodySize) + "\r\n\r\n" + std::string(kBodySize, 'x');
    if (!WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    char accepted = '\0';
    if (::read(gate[0], &accepted, 1) != 1 || accepted != 'a') {
      client_error = "accept gate read failed";
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    pollfd descriptor{socket.Fd(), POLLIN | POLLHUP, 0};
    if (::poll(&descriptor, 1, 300) != 0) {
      client_error = "first-byte timeout armed before blocked write drained";
      if (::write(gate[0], "d", 1) != 1 && client_error.empty()) client_error = "drain gate write failed";
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    if (::write(gate[0], "d", 1) != 1) {
      client_error = "drain gate write failed";
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    constexpr std::string_view response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    EXPECT_EQ(ReadExact(socket.Fd(), response.size(), TestDeadline(), client_error), response);
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  upstream.join();
  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(upstream_error.empty()) << upstream_error;
  wake_channel.Remove();
  EXPECT_EQ(::close(gate[0]), 0);
  EXPECT_EQ(::close(gate[1]), 0);
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, RecordsActualUnmatchedOutcomeAndServesPrometheusEndpoint) {
  net::Socket upstream_listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1}, upstream_listener.BoundPort(), 1};
  const config::Config config{{{"known", "gateway.test", "/", {endpoint}, 10, 10, 4}}};

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
  std::string metrics_response;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view missing =
        "GET /missing HTTP/1.1\r\nHost: unknown.test\r\n\r\n";
    constexpr std::string_view missing_response =
        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
    if (!WriteAll(socket.Fd(), missing, TestDeadline(), client_error) ||
        ReadExact(socket.Fd(), missing_response.size(), TestDeadline(), client_error) !=
            missing_response) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    constexpr std::string_view metrics = "GET /metrics HTTP/1.1\r\nHost: ignored.test\r\n\r\n";
    if (!WriteAll(socket.Fd(), metrics, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    metrics_response = ReadUntilContains(socket.Fd(), "aegisgate_inflight_requests 0\n",
                                         TestDeadline(), client_error);
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_NE(metrics_response.find("HTTP/1.1 200 OK\r\n"), std::string::npos);
  EXPECT_NE(metrics_response.find("aegisgate_requests_total{route=\"_unmatched\",status=\"404\",upstream=\"\"} 1\n"),
            std::string::npos);
  EXPECT_NE(metrics_response.find("aegisgate_active_connections 1\n"), std::string::npos);
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

} // namespace
} // namespace aegisgate::gateway
