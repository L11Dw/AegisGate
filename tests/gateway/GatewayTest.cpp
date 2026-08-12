#include <array>
#include <cerrno>
#include <chrono>
#include <memory>
#include <optional>
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
#include "aegisgate/health/EndpointHealth.h"
#include "aegisgate/resilience/CircuitBreaker.h"

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
  // The worker reaps the closed client asynchronously on its own thread:
  // poll the aggregated count until the reap lands.
  const auto reap_deadline = TestDeadline();
  while (gateway.ClientCount() != 0U && std::chrono::steady_clock::now() < reap_deadline) {
  }
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
    // Streaming semantics: the committed head plus the partial body byte are
    // already on the wire, so the total deadline truncates the connection
    // instead of replacing them with a 504 status line.
    constexpr std::string_view committed = "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nx";
    if (WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      client_response = ReadExact(socket.Fd(), committed.size(), TestDeadline(), client_error);
    }
    if (client_error.empty()) {
      pollfd descriptor{socket.Fd(), POLLIN | POLLHUP, 0};
      if (::poll(&descriptor, 1, RemainingMilliseconds(TestDeadline())) <= 0) {
        client_error = "truncated connection did not reach EOF";
      } else {
        std::array<char, 8> extra{};
        if (::read(socket.Fd(), extra.data(), extra.size()) != 0) {
          client_error = "expected EOF after truncated response";
        }
      }
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  upstream.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(upstream_error.empty()) << upstream_error;
  EXPECT_EQ(client_response, "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nx");
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
    // Streaming semantics: the validated head is committed downstream as soon
    // as it arrives, so the body-mid EOF truncates the connection instead of
    // replacing the committed response with a 502.
    constexpr std::string_view committed = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n";
    if (WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      client_response = ReadExact(socket.Fd(), committed.size(), TestDeadline(), client_error);
    }
    if (client_error.empty()) {
      pollfd descriptor{socket.Fd(), POLLIN | POLLHUP, 0};
      if (::poll(&descriptor, 1, RemainingMilliseconds(TestDeadline())) <= 0) {
        client_error = "truncated connection did not reach EOF";
      } else {
        std::array<char, 8> extra{};
        if (::read(socket.Fd(), extra.data(), extra.size()) != 0) {
          client_error = "expected EOF after truncated response";
        }
      }
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
  EXPECT_EQ(client_response, "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n");
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
    // Streaming semantics: the committed head plus the partial body byte are
    // already on the wire, so the total timeout truncates the connection
    // instead of replacing them with a 504 status line.
    constexpr std::string_view committed = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nx";
    constexpr std::string_view success = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (WriteAll(socket.Fd(), first, TestDeadline(), client_error)) {
      responses[0] = ReadExact(socket.Fd(), committed.size(), TestDeadline(), client_error);
    }
    if (client_error.empty()) {
      pollfd descriptor{socket.Fd(), POLLIN | POLLHUP, 0};
      if (::poll(&descriptor, 1, RemainingMilliseconds(TestDeadline())) <= 0) {
        client_error = "truncated connection did not reach EOF";
      } else {
        std::array<char, 8> extra{};
        if (::read(socket.Fd(), extra.data(), extra.size()) != 0) {
          client_error = "expected EOF after truncated response";
        }
      }
    }
    // The truncation closed the first connection: the second request runs on
    // a fresh connection (the gateway borrows a new upstream descriptor).
    if (client_error.empty()) {
      net::Socket second_socket = net::Socket::ConnectLoopback(gateway.port());
      if (WriteAll(second_socket.Fd(), second, TestDeadline(), client_error)) {
        responses[1] = ReadExact(second_socket.Fd(), success.size(), TestDeadline(), client_error);
      }
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  upstream.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(upstream_error.empty()) << upstream_error;
  EXPECT_EQ(responses[0], "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nx");
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


TEST(GatewayTest, ForwardsHeadRequestWithoutWaitingForBody) {
  net::Socket upstream_listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1},
                                  upstream_listener.BoundPort(), 1};
  const config::Config config{{{"api", "gateway.test", "/v1", {endpoint}, 10, 10, 4}}};

  constexpr std::string_view expected_upstream =
      "HEAD /v1/head HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\n"
      "Connection: keep-alive\r\n\r\n";
  std::string upstream_error;
  std::string received_request;
  std::thread upstream([&] {
    const int fd = AcceptUntil(upstream_listener, TestDeadline(), upstream_error);
    if (fd < 0) return;
    received_request = ReadExact(fd, expected_upstream.size(), TestDeadline(), upstream_error);
    if (upstream_error.empty()) {
      constexpr std::string_view response = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n";
      if (!WriteAll(fd, response, TestDeadline(), upstream_error)) {}
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
    constexpr std::string_view head_request =
        "HEAD /v1/head HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view expected = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n";
    if (!WriteAll(socket.Fd(), head_request, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    client_response = ReadExact(socket.Fd(), expected.size(), TestDeadline(), client_error);
    pollfd descriptor{socket.Fd(), POLLIN | POLLHUP, 0};
    if (client_error.empty() && ::poll(&descriptor, 1, 100) != 0) {
      client_error = "received bytes after head response";
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  upstream.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(upstream_error.empty()) << upstream_error;
  EXPECT_EQ(received_request, expected_upstream);
  EXPECT_EQ(client_response, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n");
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}


TEST(GatewayTest, SkipsUnhealthyEndpointWith503) {
  net::Socket upstream_listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1},
                                  upstream_listener.BoundPort(), 1};
  config::Route route{"api", "gateway.test", "/v1", {endpoint}, 10, 10, 4};
  route.health_check = config::HealthCheckSettings{1000, 200};
  const config::Config config{{route}};

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

  // The backend answers the health check with 503, then signals completion.
  std::string backend_error;
  std::thread backend([&] {
    const int fd = AcceptUntil(upstream_listener, TestDeadline(), backend_error);
    if (fd < 0) return;
    (void)ReadExact(fd, 54, TestDeadline(), backend_error);
    constexpr std::string_view unhealthy = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
    if (!WriteAll(fd, unhealthy, TestDeadline(), backend_error)) {}
    (void)::close(fd);
    if (::write(wake_fds[1], "c", 1) != 1 && backend_error.empty()) backend_error = "check wake failed";
  });
  loop.Loop();  // First pass: the health check completes and marks unhealthy.
  backend.join();
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  const std::optional<std::size_t> matched = gateway.Routes().Match("gateway.test", "/v1/x");
  ASSERT_TRUE(matched.has_value());
  // The health result is committed asynchronously on the coordinator loop:
  // poll the snapshot until the endpoint becomes unhealthy.
  const auto health_deadline = TestDeadline();
  while (gateway.EndpointHealthy(*matched, 0) &&
         std::chrono::steady_clock::now() < health_deadline) {
  }
  EXPECT_FALSE(gateway.EndpointHealthy(*matched, 0));

  std::string client_error;
  std::string client_response;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request = "GET /v1/x HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view expected = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
    if (!WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    client_response = ReadExact(socket.Fd(), expected.size(), TestDeadline(), client_error);
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();  // Second pass: the request is answered with 503, no connect.
  client.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_EQ(client_response, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n");
  pollfd descriptor{upstream_listener.Fd(), POLLIN, 0};
  EXPECT_EQ(::poll(&descriptor, 1, 100), 0) << "503 opened an upstream connection";
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, SkipsOpenEndpointWithoutConnecting) {
  net::Socket upstream_listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1},
                                  upstream_listener.BoundPort(), 1};
  config::Route route{"api", "gateway.test", "/v1", {endpoint}, 10, 10, 4};
  route.circuit_breaker = config::CircuitBreakerSettings{10, 5, 500, 5, 1};
  const config::Config config{{route}};

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

  const std::optional<std::size_t> matched = gateway.Routes().Match("gateway.test", "/v1/x");
  ASSERT_TRUE(matched.has_value());
  // Drive the endpoint's breaker open through the coordinator seam; each
  // submission blocks until the coordinator processed it and republished.
  for (int i = 0; i < 5; ++i) {
    gateway.SubmitResultAndWait(*matched, 0, /*success=*/false);
  }
  EXPECT_EQ(gateway.BreakerState(*matched, 0),
            resilience::CircuitBreaker::State::kOpen);

  std::string client_error;
  std::string client_response;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request = "GET /v1/x HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view expected = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
    if (!WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    client_response = ReadExact(socket.Fd(), expected.size(), TestDeadline(), client_error);
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_EQ(client_response, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n");
  pollfd descriptor{upstream_listener.Fd(), POLLIN, 0};
  EXPECT_EQ(::poll(&descriptor, 1, 100), 0) << "503 opened an upstream connection";
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, RouteIsolatedBreakerState) {
  net::Socket upstream_listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1},
                                  upstream_listener.BoundPort(), 1};
  config::Route route_a{"a", "a.test", "/", {endpoint}, 10, 10, 4};
  route_a.circuit_breaker = config::CircuitBreakerSettings{10, 5, 500, 5, 1};
  config::Route route_b{"b", "b.test", "/", {endpoint}, 10, 10, 4};
  route_b.circuit_breaker = config::CircuitBreakerSettings{10, 5, 500, 5, 1};
  const config::Config config{{route_a, route_b}};

  std::string upstream_error;
  std::thread upstream([&] {
    const int fd = AcceptUntil(upstream_listener, TestDeadline(), upstream_error);
    if (fd < 0) return;
    (void)ReadExact(fd, 57, TestDeadline(), upstream_error);
    constexpr std::string_view response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (!WriteAll(fd, response, TestDeadline(), upstream_error)) {}
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

  // Open route a's breaker; route b shares the same endpoint but keeps its
  // own state.
  const std::optional<std::size_t> matched_a = gateway.Routes().Match("a.test", "/x");
  ASSERT_TRUE(matched_a.has_value());
  for (int i = 0; i < 5; ++i) {
    gateway.SubmitResultAndWait(*matched_a, 0, /*success=*/false);
  }
  EXPECT_EQ(gateway.BreakerState(*matched_a, 0),
            resilience::CircuitBreaker::State::kOpen);

  std::string client_error;
  std::string a_response;
  std::string b_response;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request_a = "GET /a HTTP/1.1\r\nHost: a.test\r\n\r\n";
    constexpr std::string_view service_unavailable = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
    if (WriteAll(socket.Fd(), request_a, TestDeadline(), client_error)) {
      a_response = ReadExact(socket.Fd(), service_unavailable.size(), TestDeadline(), client_error);
    }
    constexpr std::string_view request_b = "GET /b HTTP/1.1\r\nHost: b.test\r\n\r\n";
    constexpr std::string_view ok = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (client_error.empty() && WriteAll(socket.Fd(), request_b, TestDeadline(), client_error)) {
      b_response = ReadExact(socket.Fd(), ok.size(), TestDeadline(), client_error);
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  upstream.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(upstream_error.empty()) << upstream_error;
  EXPECT_EQ(a_response, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n");
  EXPECT_EQ(b_response, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}


TEST(GatewayTest, RetrySkipsOpenCandidateWithoutConnecting) {
  net::Socket first_listener = net::Socket::ListenLoopback();
  net::Socket open_listener = net::Socket::ListenLoopback();
  const config::Endpoint first{"127.0.0.1", {127, 0, 0, 1}, first_listener.BoundPort(), 1};
  const config::Endpoint open_endpoint{"127.0.0.1", {127, 0, 0, 1}, open_listener.BoundPort(), 1};
  config::Route route{"api", "gateway.test", "/v1", {first, open_endpoint}, 10, 10, 4};
  route.circuit_breaker = config::CircuitBreakerSettings{10, 5, 500, 5, 1};
  route.retry_budget = 1;
  const config::Config config{{route}};

  // The first endpoint accepts and immediately closes: a retryable EOF.
  std::string first_error;
  std::thread first_backend([&] {
    const int fd = AcceptUntil(first_listener, TestDeadline(), first_error);
    if (fd < 0) return;
    (void)ReadExact(fd, 53, TestDeadline(), first_error);
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

  // Drive the second endpoint's breaker open through the coordinator seam.
  const std::optional<std::size_t> matched = gateway.Routes().Match("gateway.test", "/v1/x");
  ASSERT_TRUE(matched.has_value());
  for (int i = 0; i < 5; ++i) {
    gateway.SubmitResultAndWait(*matched, 1, /*success=*/false);
  }
  EXPECT_EQ(gateway.BreakerState(*matched, 1),
            resilience::CircuitBreaker::State::kOpen);

  std::string client_error;
  std::string client_response;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request = "GET /v1/x HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view expected = "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n";
    if (!WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    client_response = ReadExact(socket.Fd(), expected.size(), TestDeadline(), client_error);
    pollfd descriptor{socket.Fd(), POLLIN | POLLHUP, 0};
    if (client_error.empty() && ::poll(&descriptor, 1, 100) != 0) {
      client_error = "received bytes after terminal 502";
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  first_backend.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(first_error.empty()) << first_error;
  EXPECT_EQ(client_response, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n");
  // The open candidate must never have been connected to.
  pollfd descriptor{open_listener.Fd(), POLLIN, 0};
  EXPECT_EQ(::poll(&descriptor, 1, 100), 0) << "retry connected to an open endpoint";
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}


TEST(GatewayTest, RetryFailureAccountsExactlyOnce) {
  net::Socket first_listener = net::Socket::ListenLoopback();
  net::Socket open_listener = net::Socket::ListenLoopback();
  const config::Endpoint first{"127.0.0.1", {127, 0, 0, 1}, first_listener.BoundPort(), 1};
  const config::Endpoint open_endpoint{"127.0.0.1", {127, 0, 0, 1}, open_listener.BoundPort(), 1};
  config::Route route{"api", "gateway.test", "/v1", {first, open_endpoint}, 10, 10, 4};
  route.circuit_breaker = config::CircuitBreakerSettings{10, 2, 500, 5, 1};
  route.retry_budget = 1;
  const config::Config config{{route}};

  std::string first_error;
  std::thread first_backend([&] {
    const int fd = AcceptUntil(first_listener, TestDeadline(), first_error);
    if (fd < 0) return;
    (void)ReadExact(fd, 53, TestDeadline(), first_error);
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

  const std::optional<std::size_t> matched = gateway.Routes().Match("gateway.test", "/v1/x");
  ASSERT_TRUE(matched.has_value());
  // Drive the backup endpoint open with its own two failures (min_requests=2).
  for (int i = 0; i < 2; ++i) {
    gateway.SubmitResultAndWait(*matched, 1, /*success=*/false);
  }
  EXPECT_EQ(gateway.BreakerState(*matched, 1),
            resilience::CircuitBreaker::State::kOpen);

  std::string client_error;
  std::string client_response;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request = "GET /v1/x HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view expected = "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n";
    if (!WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    client_response = ReadExact(socket.Fd(), expected.size(), TestDeadline(), client_error);
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  first_backend.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(first_error.empty()) << first_error;
  EXPECT_EQ(client_response, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n");
  // The single retryable failure of the first endpoint was accounted exactly
  // once: with min_requests=2 the breaker must still be closed.  A double
  // count would open it.
  EXPECT_EQ(gateway.BreakerState(*matched, 0),
            resilience::CircuitBreaker::State::kClosed);
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, LeastActiveRoutesToLessBusyEndpoint) {
  // Three requests with equal weights: the rotation would send the third
  // request back to the first (busy) endpoint, but least-active must keep it
  // on the second endpoint while the first request is still in flight.
  net::Socket first_listener = net::Socket::ListenLoopback();
  net::Socket second_listener = net::Socket::ListenLoopback();
  const config::Endpoint first{"127.0.0.1", {127, 0, 0, 1}, first_listener.BoundPort(), 1};
  const config::Endpoint second{"127.0.0.1", {127, 0, 0, 1}, second_listener.BoundPort(), 1};
  config::Route route{"api", "gateway.test", "/v1", {first, second}, 10, 10, 4};
  route.balance = config::BalancePolicy::kLeastActive;
  const config::Config config{{route}};

  std::array<int, 2> gate{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, gate.data()), 0);
  std::string first_error;
  std::string second_error;
  std::thread first_backend([&] {
    const int fd = AcceptUntil(first_listener, TestDeadline(), first_error);
    if (fd < 0) return;
    constexpr std::string_view request =
        "GET /v1/held HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    (void)ReadExact(fd, request.size(), TestDeadline(), first_error);
    if (::write(gate[1], "a", 1) != 1) {
      first_error = "first accept gate failed";
      (void)::close(fd);
      return;
    }
    char command = '\0';
    if (::read(gate[1], &command, 1) != 1 || command != 'd') {
      first_error = "first release gate failed";
      (void)::close(fd);
      return;
    }
    constexpr std::string_view response = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\none";
    if (first_error.empty() && !WriteAll(fd, response, TestDeadline(), first_error)) {}
    (void)::close(fd);
  });
  // The second endpoint answers two requests (requests two and three).
  std::thread second_backend([&] {
    constexpr std::string_view request =
        "GET /v1/held HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    constexpr std::string_view response = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo";
    for (int index = 0; index != 2; ++index) {
      const int fd = AcceptUntil(second_listener, TestDeadline(), second_error);
      if (fd < 0) return;
      (void)ReadExact(fd, request.size(), TestDeadline(), second_error);
      if (second_error.empty() && !WriteAll(fd, response, TestDeadline(), second_error)) {}
      (void)::close(fd);
    }
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
  std::string first_response;
  std::string second_response;
  std::string third_response;
  std::thread client([&] {
    constexpr std::string_view held = "GET /v1/held HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view one = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\none";
    constexpr std::string_view two = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo";
    // Request 1 occupies the first endpoint's active slot.
    net::Socket first_client = net::Socket::ConnectLoopback(gateway.port());
    if (!WriteAll(first_client.Fd(), held, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    char accepted = '\0';
    if (::read(gate[0], &accepted, 1) != 1 || accepted != 'a') {
      client_error = "first accept gate read failed";
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    net::Socket second_client = net::Socket::ConnectLoopback(gateway.port());
    if (!WriteAll(second_client.Fd(), held, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    second_response = ReadExact(second_client.Fd(), two.size(), TestDeadline(), client_error);
    // The third request must also stay on the second endpoint while the first
    // is still in flight; a rotation-based choice would pick the busy first.
    if (client_error.empty() && !WriteAll(second_client.Fd(), held, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    third_response = ReadExact(second_client.Fd(), two.size(), TestDeadline(), client_error);
    // Release the first request; it completes on the first endpoint.
    if (::write(gate[0], "d", 1) != 1) {
      client_error = "first release gate write failed";
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    first_response = ReadExact(first_client.Fd(), one.size(), TestDeadline(), client_error);
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  first_backend.join();
  second_backend.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(first_error.empty()) << first_error;
  EXPECT_TRUE(second_error.empty()) << second_error;
  EXPECT_EQ(first_response, "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\none");
  EXPECT_EQ(second_response, "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo");
  EXPECT_EQ(third_response, "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo");
  // No connection may have been opened to the first endpoint after its held
  // request (a rotation-based third choice would leave one pending here).
  pollfd descriptor{first_listener.Fd(), POLLIN, 0};
  EXPECT_EQ(::poll(&descriptor, 1, 100), 0) << "third request connected to the busy endpoint";
  wake_channel.Remove();
  EXPECT_EQ(::close(gate[0]), 0);
  EXPECT_EQ(::close(gate[1]), 0);
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

// Regression guard: the least-active provider must apply the same eligibility
// rules as the weighted provider.  Initially green because the weighted path
// already enforces them; the least-active branch must keep them.
TEST(GatewayTest, LeastActiveSkipsUnhealthyAndOpen) {
  net::Socket unhealthy_listener = net::Socket::ListenLoopback();
  net::Socket healthy_listener = net::Socket::ListenLoopback();
  const config::Endpoint unhealthy{"127.0.0.1", {127, 0, 0, 1}, unhealthy_listener.BoundPort(), 1};
  const config::Endpoint healthy{"127.0.0.1", {127, 0, 0, 1}, healthy_listener.BoundPort(), 1};
  config::Route route{"api", "gateway.test", "/v1", {unhealthy, healthy}, 10, 10, 4};
  route.balance = config::BalancePolicy::kLeastActive;
  route.health_check = config::HealthCheckSettings{1000, 200};
  route.circuit_breaker = config::CircuitBreakerSettings{10, 5, 500, 5, 1};
  const config::Config config{{route}};

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  std::string unhealthy_error;
  std::thread unhealthy_backend([&] {
    const int fd = AcceptUntil(unhealthy_listener, TestDeadline(), unhealthy_error);
    if (fd < 0) return;
    (void)ReadUntilContains(fd, "\r\n\r\n", TestDeadline(), unhealthy_error);
    constexpr std::string_view probe_fail =
        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
    if (unhealthy_error.empty() &&
        !WriteAll(fd, probe_fail, TestDeadline(), unhealthy_error)) {}
    (void)::close(fd);
    // Signal the first loop pass: the check has been answered and unhealthy.
    if (::write(wake_fds[1], "c", 1) != 1 && unhealthy_error.empty()) {
      unhealthy_error = "check wake failed";
    }
  });
  std::string healthy_error;
  std::thread healthy_backend([&] {
    // Health probe first, then the one real request.
    int fd = AcceptUntil(healthy_listener, TestDeadline(), healthy_error);
    if (fd < 0) return;
    (void)ReadUntilContains(fd, "\r\n\r\n", TestDeadline(), healthy_error);
    constexpr std::string_view probe_ok = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    if (healthy_error.empty() && !WriteAll(fd, probe_ok, TestDeadline(), healthy_error)) {}
    (void)::close(fd);
    fd = AcceptUntil(healthy_listener, TestDeadline(), healthy_error);
    if (fd < 0) return;
    (void)ReadUntilContains(fd, "\r\n\r\n", TestDeadline(), healthy_error);
    constexpr std::string_view ok = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (healthy_error.empty() && !WriteAll(fd, ok, TestDeadline(), healthy_error)) {}
    (void)::close(fd);
  });

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

  // Pass 1: the health checkers mark the first endpoint unhealthy.
  loop.Loop();
  const std::optional<std::size_t> matched = gateway.Routes().Match("gateway.test", "/v1/x");
  ASSERT_TRUE(matched.has_value());
  const auto health_deadline = TestDeadline();
  while (gateway.EndpointHealthy(*matched, 0) &&
         std::chrono::steady_clock::now() < health_deadline) {
  }
  EXPECT_FALSE(gateway.EndpointHealthy(*matched, 0));

  // Phase 1: the request must reach the healthy endpoint.
  std::string client_error;
  std::string ok_response;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request = "GET /v1/x HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view ok = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (!WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    ok_response = ReadExact(socket.Fd(), ok.size(), TestDeadline(), client_error);
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  healthy_backend.join();
  unhealthy_backend.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_TRUE(healthy_error.empty()) << healthy_error;
  EXPECT_TRUE(unhealthy_error.empty()) << unhealthy_error;
  EXPECT_EQ(ok_response, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");

  // Open the healthy endpoint's breaker: no candidate remains, so the gateway
  // answers a unique 503 without connecting anywhere.
  for (int i = 0; i < 5; ++i) {
    gateway.SubmitResultAndWait(*matched, 1, /*success=*/false);
  }

  std::string unavailable_response;
  std::thread unavailable_client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view request = "GET /v1/x HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    constexpr std::string_view expected =
        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
    if (!WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    unavailable_response = ReadExact(socket.Fd(), expected.size(), TestDeadline(), client_error);
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  unavailable_client.join();

  EXPECT_TRUE(client_error.empty()) << client_error;
  EXPECT_EQ(unavailable_response, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n");
  pollfd unhealthy_pending{unhealthy_listener.Fd(), POLLIN, 0};
  EXPECT_EQ(::poll(&unhealthy_pending, 1, 100), 0) << "503 connected to the unhealthy endpoint";
  pollfd healthy_pending{healthy_listener.Fd(), POLLIN, 0};
  EXPECT_EQ(::poll(&healthy_pending, 1, 100), 0) << "503 connected to the open endpoint";
  wake_channel.Remove();
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

// R-040 regression: the gateway is destroyed while the first attempt's EOF is
// pending but unprocessed.  Every late upstream event must never touch the
// destroyed gateway (timers_ / provider captures); no retry connect may happen
// and the transaction must terminate cleanly.  Note: this covers "EOF
// unprocessed at destruction"; a retry task already queued but not yet drained
// cannot be observed in the current EventLoop (batch-end drain is
// unconditional), so that window is defended by the GatewayDown checks rather
// than by a dedicated test.
TEST(GatewayTest, GatewayDestructionWithPendingUpstreamEofIsSafe) {
  net::Socket first_listener = net::Socket::ListenLoopback();
  net::Socket second_listener = net::Socket::ListenLoopback();
  const config::Endpoint first{"127.0.0.1", {127, 0, 0, 1}, first_listener.BoundPort(), 1};
  const config::Endpoint second{"127.0.0.1", {127, 0, 0, 1}, second_listener.BoundPort(), 1};
  config::Route route{"api", "gateway.test", "/v1", {first, second}, 10, 10, 4};
  route.balance = config::BalancePolicy::kLeastActive;
  route.retry_budget = 0;
  const config::Config config{{route}};

  std::array<int, 2> gate{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, gate.data()), 0);
  std::string first_error;
  std::thread first_backend([&] {
    const int fd = AcceptUntil(first_listener, TestDeadline(), first_error);
    if (fd < 0) return;
    constexpr std::string_view request =
        "GET /v1/x HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    (void)ReadExact(fd, request.size(), TestDeadline(), first_error);
    if (::write(gate[1], "a", 1) != 1) {
      first_error = "accept gate failed";
      (void)::close(fd);
      return;
    }
    char command = '\0';
    if (::read(gate[1], &command, 1) != 1 || command != 'c') {
      first_error = "close gate failed";
      (void)::close(fd);
      return;
    }
    (void)::close(fd);  // the EOF may land before or during gateway shutdown
    if (::write(gate[1], "d", 1) != 1 && first_error.empty()) {
      first_error = "closed gate failed";
    }
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
  auto gateway = std::make_unique<Gateway>(loop, config, "127.0.0.1", 0);
  gateway->Start();

  std::string client_error;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway->port());
    constexpr std::string_view request = "GET /v1/x HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
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
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();  // Pass 1: the request is in flight on the first endpoint.
  client.join();
  EXPECT_TRUE(client_error.empty()) << client_error;

  // Close the first endpoint now.  With always-running workers the EOF is
  // processed as soon as it is ready (no constructible "pending but
  // unprocessed" window), so the destruction races the EOF delivery: both
  // orders must be safe.  retry_budget 0 makes "no second connect" the
  // deterministic observable contract; the late-retry window itself is
  // defended by the GatewayDown checks in StartUpstream.
  if (::write(gate[0], "c", 1) != 1) {
    FAIL() << "close gate write failed";
  }
  char closed = '\0';
  if (::read(gate[0], &closed, 1) != 1 || closed != 'd') {
    FAIL() << "close gate read failed";
  }
  first_backend.join();

  // Destroy the gateway with the EOF possibly still in flight.  The destroy
  // task terminates the exchange on the worker thread (CancelAll); any late
  // upstream event must never touch the destroyed gateway, and no connect may
  // follow.  (Transaction-termination evidence lives in
  // ProxyTransactionTest.CancelAllTerminatesInFlightTransaction.)
  gateway.reset();

  // Pump briefly so a (buggy) retry connect would be observed; the watchdog
  // drives the loop and is only a failure backstop.
  std::array<int, 2> stop{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, stop.data()), 0);
  std::string watchdog_error;
  std::thread watchdog([&] {
    pollfd descriptor{stop[0], POLLIN, 0};
    while (::poll(&descriptor, 1, 100) == 0) {
      if (::write(wake_fds[1], "q", 1) != 1) {
        watchdog_error = "watchdog wake failed";
        return;
      }
    }
  });
  const auto pump_end = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
  while (std::chrono::steady_clock::now() < pump_end) {
    loop.Loop();
  }
  (void)test::SignalWakeFd(stop[1], 's', watchdog_error);
  watchdog.join();
  EXPECT_TRUE(watchdog_error.empty()) << watchdog_error;
  pollfd retry_pending{second_listener.Fd(), POLLIN, 0};
  EXPECT_EQ(::poll(&retry_pending, 1, 100), 0)
      << "retry connected to the second endpoint after gateway destruction";
  wake_channel.Remove();
  EXPECT_EQ(::close(gate[0]), 0);
  EXPECT_EQ(::close(gate[1]), 0);
  EXPECT_EQ(::close(stop[0]), 0);
  EXPECT_EQ(::close(stop[1]), 0);
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

// R-040 companion: an in-flight attempt (no retry) whose upstream completes
// after the gateway is destroyed must terminate without touching timers_.
TEST(GatewayTest, GatewayDestructionDuringInFlightAttemptIsSafe) {
  net::Socket listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1}, listener.BoundPort(), 1};
  config::Route route{"api", "gateway.test", "/v1", {endpoint}, 10, 10, 4};
  route.retry_budget = 0;
  const config::Config config{{route}};

  std::array<int, 2> gate{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, gate.data()), 0);
  std::string backend_error;
  std::thread backend([&] {
    const int fd = AcceptUntil(listener, TestDeadline(), backend_error);
    if (fd < 0) return;
    constexpr std::string_view request =
        "GET /v1/x HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    (void)ReadExact(fd, request.size(), TestDeadline(), backend_error);
    if (::write(gate[1], "a", 1) != 1) {
      backend_error = "accept gate failed";
      (void)::close(fd);
      return;
    }
    char command = '\0';
    if (::read(gate[1], &command, 1) != 1 || command != 'c') {
      backend_error = "close gate failed";
      (void)::close(fd);
      return;
    }
    (void)::close(fd);  // EOF: the attempt's terminal event arrives late.
    if (::write(gate[1], "d", 1) != 1 && backend_error.empty()) {
      backend_error = "closed gate failed";
    }
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
  auto gateway = std::make_unique<Gateway>(loop, config, "127.0.0.1", 0);
  gateway->Start();

  std::string client_error;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway->port());
    constexpr std::string_view request = "GET /v1/x HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
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
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  EXPECT_TRUE(client_error.empty()) << client_error;

  if (::write(gate[0], "c", 1) != 1) {
    FAIL() << "close gate write failed";
  }
  char closed = '\0';
  if (::read(gate[0], &closed, 1) != 1 || closed != 'd') {
    FAIL() << "close gate read failed";
  }
  backend.join();
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  gateway.reset();

  // The late EOF must terminate the in-flight attempt without touching the
  // destroyed gateway.  (Its own EOF is the backend-observed signal in the
  // never-responds variant; here the contract is a clean ASan run, so just
  // pump briefly with the watchdog as a backstop.)
  std::array<int, 2> stop{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, stop.data()), 0);
  std::string watchdog_error;
  std::thread watchdog([&] {
    pollfd descriptor{stop[0], POLLIN, 0};
    while (::poll(&descriptor, 1, 100) == 0) {
      if (::write(wake_fds[1], "q", 1) != 1) {
        watchdog_error = "watchdog wake failed";
        return;
      }
    }
  });
  const auto pump_end = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
  while (std::chrono::steady_clock::now() < pump_end) {
    loop.Loop();
  }
  (void)test::SignalWakeFd(stop[1], 's', watchdog_error);
  watchdog.join();
  EXPECT_TRUE(watchdog_error.empty()) << watchdog_error;
  wake_channel.Remove();
  EXPECT_EQ(::close(gate[0]), 0);
  EXPECT_EQ(::close(gate[1]), 0);
  EXPECT_EQ(::close(stop[0]), 0);
  EXPECT_EQ(::close(stop[1]), 0);
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

// R-040 companion (v2): an upstream that never responds nor closes must not
// keep the transaction alive after the gateway is destroyed.  The gateway's
// shutdown terminates the exchange (no waiting for EOF or a timeout); the
// backend observes the connection close (the observable completion signal)
// and the transaction's admission reference expires.
TEST(GatewayTest, GatewayDestructionTerminatesInFlightAttempt) {
  net::Socket listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1}, listener.BoundPort(), 1};
  config::Route route{"api", "gateway.test", "/v1", {endpoint}, 10, 10, 4};
  route.retry_budget = 0;
  const config::Config config{{route}};

  std::array<int, 2> gate{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, gate.data()), 0);
  std::string backend_error;
  std::thread backend([&] {
    const int fd = AcceptUntil(listener, TestDeadline(), backend_error);
    if (fd < 0) return;
    constexpr std::string_view request =
        "GET /v1/x HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    (void)ReadExact(fd, request.size(), TestDeadline(), backend_error);
    if (::write(gate[1], "a", 1) != 1) {
      backend_error = "accept gate failed";
      (void)::close(fd);
      return;
    }
    // Never respond and never close on our own: the gateway's shutdown must
    // terminate the exchange.  A poll hit alone is not EOF: recv() must
    // return 0 to prove the gateway closed the connection.
    pollfd descriptor{fd, POLLHUP | POLLIN, 0};
    bool saw_eof = false;
    if (::poll(&descriptor, 1, RemainingMilliseconds(TestDeadline())) > 0) {
      char byte = '\0';
      saw_eof = ::recv(fd, &byte, 1, 0) == 0;
    }
    if (!saw_eof && backend_error.empty()) {
      backend_error = "gateway did not close the in-flight connection";
    }
    (void)::close(fd);
    if (::write(gate[1], "d", 1) != 1 && backend_error.empty()) {
      backend_error = "closed gate failed";
    }
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
  auto gateway = std::make_unique<Gateway>(loop, config, "127.0.0.1", 0);
  gateway->Start();

  std::string client_error;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway->port());
    constexpr std::string_view request = "GET /v1/x HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
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
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  EXPECT_TRUE(client_error.empty()) << client_error;

  // Destroy the gateway while the exchange is still in flight and the backend
  // never responds: shutdown must terminate it without waiting.
  gateway.reset();

  // Completion signal: the backend observes the gateway closing the in-flight
  // connection.  The watchdog only pumps the loop as a failure backstop.
  std::array<int, 2> stop{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, stop.data()), 0);
  std::string watchdog_error;
  std::thread watchdog([&] {
    pollfd descriptor{stop[0], POLLIN, 0};
    while (::poll(&descriptor, 1, 100) == 0) {
      if (::write(wake_fds[1], "q", 1) != 1) {
        watchdog_error = "watchdog wake failed";
        return;
      }
    }
  });
  pollfd gate_descriptor{gate[0], POLLIN, 0};
  const auto deadline = TestDeadline();
  while (::poll(&gate_descriptor, 1, 0) == 0 && std::chrono::steady_clock::now() < deadline) {
    loop.Loop();
  }
  (void)test::SignalWakeFd(stop[1], 's', watchdog_error);
  watchdog.join();
  EXPECT_TRUE(watchdog_error.empty()) << watchdog_error;
  char closed = '\0';
  ASSERT_EQ(::read(gate[0], &closed, 1), 1);
  EXPECT_EQ(closed, 'd');
  backend.join();
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  wake_channel.Remove();
  EXPECT_EQ(::close(gate[0]), 0);
  EXPECT_EQ(::close(gate[1]), 0);
  EXPECT_EQ(::close(stop[0]), 0);
  EXPECT_EQ(::close(stop[1]), 0);
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

TEST(GatewayTest, GatewayDestroyDuringStreamingIsSafe) {
  net::Socket listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1}, listener.BoundPort(), 1};
  config::Route route{"api", "gateway.test", "/v1", {endpoint}, 10, 10, 4};
  route.retry_budget = 0;
  const config::Config config{{route}};

  std::array<int, 2> gate{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, gate.data()), 0);
  std::string backend_error;
  std::thread backend([&] {
    const int fd = AcceptUntil(listener, TestDeadline(), backend_error);
    if (fd < 0) return;
    constexpr std::string_view request =
        "GET /v1/x HTTP/1.1\r\nhost: gateway.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    (void)ReadExact(fd, request.size(), TestDeadline(), backend_error);
    // Start a streaming response: committed header plus a body chunk, then
    // hold the connection open.  The gateway must terminate the exchange when
    // destroyed, without waiting for EOF or a timeout.
    constexpr std::string_view partial =
        "HTTP/1.1 200 OK\r\nContent-Length: 524288\r\n\r\nstream";
    if (backend_error.empty() &&
        ::write(fd, partial.data(), partial.size()) != static_cast<ssize_t>(partial.size())) {
      backend_error = "failed to write partial response";
    }
    if (::write(gate[1], "a", 1) != 1) {
      backend_error = "accept gate failed";
      (void)::close(fd);
      return;
    }
    pollfd descriptor{fd, POLLHUP | POLLIN, 0};
    bool saw_eof = false;
    if (::poll(&descriptor, 1, RemainingMilliseconds(TestDeadline())) > 0) {
      char byte = '\0';
      saw_eof = ::recv(fd, &byte, 1, 0) == 0;
    }
    if (!saw_eof && backend_error.empty()) {
      backend_error = "gateway did not close the streaming connection";
    }
    (void)::close(fd);
    if (::write(gate[1], "d", 1) != 1 && backend_error.empty()) {
      backend_error = "closed gate failed";
    }
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
  auto gateway = std::make_unique<Gateway>(loop, config, "127.0.0.1", 0);
  gateway->Start();

  std::string client_error;
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway->port());
    constexpr std::string_view request = "GET /v1/x HTTP/1.1\r\nHost: gateway.test\r\n\r\n";
    if (!WriteAll(socket.Fd(), request, TestDeadline(), client_error)) {
      (void)test::SignalWakeFd(wake_fds[1], 'q', client_error);
      return;
    }
    // Read the committed head, leaving the stream mid-body.
    constexpr std::string_view committed = "HTTP/1.1 200 OK\r\nContent-Length: 524288\r\n\r\n";
    const auto received = ReadExact(socket.Fd(), committed.size(), TestDeadline(), client_error);
    if (received != committed && client_error.empty()) {
      client_error = "unexpected streaming head";
    }
    // Consume the accept gate so the later 'd' completion signal is next.
    char accepted = '\0';
    if (::read(gate[0], &accepted, 1) != 1 || accepted != 'a') {
      if (client_error.empty()) client_error = "accept gate read failed";
    }
    if (::write(wake_fds[1], "q", 1) != 1 && client_error.empty()) client_error = "wake failed";
  });
  loop.Loop();
  client.join();
  EXPECT_TRUE(client_error.empty()) << client_error;

  // Destroy the gateway mid-stream: CancelAll must terminate the exchange.
  gateway.reset();

  std::array<int, 2> stop{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, stop.data()), 0);
  std::string watchdog_error;
  std::thread watchdog([&] {
    pollfd descriptor{stop[0], POLLIN, 0};
    while (::poll(&descriptor, 1, 100) == 0) {
      if (::write(wake_fds[1], "q", 1) != 1) {
        watchdog_error = "watchdog wake failed";
        return;
      }
    }
  });
  pollfd gate_descriptor{gate[0], POLLIN, 0};
  const auto deadline = TestDeadline();
  while (::poll(&gate_descriptor, 1, 0) == 0 && std::chrono::steady_clock::now() < deadline) {
    loop.Loop();
  }
  (void)test::SignalWakeFd(stop[1], 's', watchdog_error);
  watchdog.join();
  EXPECT_TRUE(watchdog_error.empty()) << watchdog_error;
  char closed = '\0';
  ASSERT_EQ(::read(gate[0], &closed, 1), 1);
  EXPECT_EQ(closed, 'd');
  backend.join();
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  wake_channel.Remove();
  EXPECT_EQ(::close(gate[0]), 0);
  EXPECT_EQ(::close(gate[1]), 0);
  EXPECT_EQ(::close(stop[0]), 0);
  EXPECT_EQ(::close(stop[1]), 0);
  EXPECT_EQ(::close(wake_fds[0]), 0);
  EXPECT_EQ(::close(wake_fds[1]), 0);
}

} // namespace
} // namespace aegisgate::gateway
