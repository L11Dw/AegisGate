#include <array>
#include <cerrno>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/http/HttpRequestSerializer.h"
#include "aegisgate/net/ClientConnection.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/Socket.h"
#include "aegisgate/net/TimerQueue.h"
#include "aegisgate/proxy/ProxyTransaction.h"
#include "aegisgate/proxy/UpstreamPool.h"
#include "aegisgate/resilience/RouteAdmission.h"

#include "../support/WakeFd.h"

namespace aegisgate::proxy {
namespace {

using Deadline = std::chrono::steady_clock::time_point;

Deadline TestDeadline() { return std::chrono::steady_clock::now() + std::chrono::seconds(5); }

int RemainingMilliseconds(Deadline deadline) {
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
      deadline - std::chrono::steady_clock::now());
  return remaining.count() > 0 ? static_cast<int>(remaining.count()) : 0;
}

int AcceptUntil(const net::Socket &listener, Deadline deadline, std::string &error) {
  for (;;) {
    const int fd = listener.Accept();
    if (fd >= 0) {
      const int flags = ::fcntl(fd, F_GETFL);
      if (flags >= 0) (void)::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
      return fd;
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
      error = "accept failed";
      return -1;
    }
    pollfd descriptor{listener.Fd(), POLLIN, 0};
    if (::poll(&descriptor, 1, RemainingMilliseconds(deadline)) <= 0) {
      error = "accept timed out";
      return -1;
    }
  }
}

std::optional<std::string> ReadExactUntil(int fd, std::size_t size, Deadline deadline,
                                          std::string &error) {
  std::string value(size, '\0');
  std::size_t received = 0;
  while (received != size) {
    pollfd descriptor{fd, POLLIN | POLLHUP, 0};
    if (::poll(&descriptor, 1, RemainingMilliseconds(deadline)) <= 0) {
      error = "read timed out";
      return std::nullopt;
    }
    const ssize_t count = ::read(fd, value.data() + received, size - received);
    if (count > 0) {
      received += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) continue;
    error = "unexpected EOF or read error";
    return std::nullopt;
  }
  return value;
}

bool WaitForPeerEofUntil(int fd, Deadline deadline, std::string &error) {
  pollfd descriptor{fd, POLLIN | POLLHUP, 0};
  if (::poll(&descriptor, 1, RemainingMilliseconds(deadline)) <= 0) {
    error = "upstream EOF timed out";
    return false;
  }
  char byte = '\0';
  for (;;) {
    const ssize_t count = ::read(fd, &byte, 1);
    if (count == 0) return true;
    if (count < 0 && errno == EINTR) continue;
    error = "upstream did not close";
    return false;
  }
}

struct WorkerResult {
  std::string received;
  std::string error;
  bool wire_ok = false;
};

class LoopWatchdog {
public:
  explicit LoopWatchdog(int wake_fd, std::string &error) : wake_fd_(wake_fd), error_(&error) {
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, stop_fds_.data()) != 0) {
      throw std::system_error(errno, std::generic_category(), "socketpair watchdog");
    }
    thread_ = std::thread([this] {
      pollfd descriptor{stop_fds_[0], POLLIN, 0};
      if (::poll(&descriptor, 1, 5000) == 0) {
        (void)test::SignalWakeFd(wake_fd_, 't', *error_);
      }
    });
  }

  ~LoopWatchdog() {
    if (thread_.joinable()) Stop();
    (void)::close(stop_fds_[0]);
    (void)::close(stop_fds_[1]);
  }

  void Stop() {
    (void)test::SignalWakeFd(stop_fds_[1], 's', *error_);
    thread_.join();
  }

private:
  int wake_fd_;
  std::string *error_;
  std::array<int, 2> stop_fds_{};
  std::thread thread_;
};

TEST(ProxyTransactionTest, RejectsRouteAdmissionBeforeStartingUpstream) {
  net::Socket listener = net::Socket::ListenLoopback();
  const auto now = std::chrono::steady_clock::now();
  const config::Route route{"limited", "test", "/", {}, 1, 1, 1};
  const auto admission = std::make_shared<resilience::RouteAdmission>(route, now);
  auto held = admission->TryAcquire(now);
  ASSERT_TRUE(held);

  std::array<int, 2> sockets{};
  std::array<int, 2> wake_sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()), 0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_sockets.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_sockets[0]);
  bool watchdog_fired = false;
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    if (::read(wake_sockets[0], &byte, 1) != 1 || byte == 't') watchdog_fired = true;
    loop.Quit();
  });
  wake_channel.EnableReading();

  std::shared_ptr<ProxyTransaction> transaction;
  net::ClientConnection client(loop, sockets[0], [&](net::ClientConnection &connection,
                                                       const http::HttpRequest &parsed) {
    transaction = ProxyTransaction::Start(loop, connection, listener.BoundPort(), parsed, admission);
  });
  client.Start();
  constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: test\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()), static_cast<ssize_t>(request.size()));

  WorkerResult peer_result;
  constexpr std::string_view expected =
      "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n";
  std::thread peer([&] {
    const auto response = ReadExactUntil(sockets[1], expected.size(), TestDeadline(), peer_result.error);
    if (!response) return;
    peer_result.received = *response;
    peer_result.wire_ok = ::write(wake_sockets[1], "q", 1) == 1;
    if (!peer_result.wire_ok) peer_result.error = "failed to wake event loop";
  });
  std::string watchdog_error;
  LoopWatchdog watchdog(wake_sockets[1], watchdog_error);
  loop.Loop();
  watchdog.Stop();
  peer.join();

  pollfd descriptor{listener.Fd(), POLLIN, 0};
  EXPECT_EQ(::poll(&descriptor, 1, 100), 0) << "rejected request opened an upstream connection";
  EXPECT_FALSE(watchdog_fired);
  EXPECT_TRUE(watchdog_error.empty()) << watchdog_error;
  EXPECT_TRUE(peer_result.error.empty()) << peer_result.error;
  EXPECT_EQ(peer_result.received, expected);
  EXPECT_TRUE(peer_result.wire_ok);
  EXPECT_TRUE(transaction);
  held.reset();
  client.Close();
  wake_channel.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
  EXPECT_EQ(::close(wake_sockets[0]), 0);
  EXPECT_EQ(::close(wake_sockets[1]), 0);
}

TEST(ProxyTransactionTest, ForwardsPostAndSegmentedResponseToClient) {
  net::Socket listener = net::Socket::ListenLoopback();
  // Framing and every hop-by-hop field, including fields nominated by
  // Connection, belong to the downstream hop and must not reach the origin.
  constexpr std::string_view wire_request =
      "POST /submit HTTP/1.1\r\nHost: origin.test\r\nContent-Length: 5\r\n"
      "Connection: X-Request-Scope, Keep-Alive\r\nX-Request-Scope: secret\r\n"
      "Connection: X-Request-Second\r\nX-Request-Second: secret\r\n"
      "Keep-Alive: timeout=5\r\nProxy-Connection: keep-alive\r\nTE: trailers\r\n"
      "Trailer: X-Trailer\r\nUpgrade: websocket\r\n\r\nhello";
  // HttpRequestParser normalizes incoming field names to lowercase; the
  // upstream serializer rebuilds framing and its own Connection header.
  constexpr std::string_view expected_request =
      "POST /submit HTTP/1.1\r\nhost: origin.test\r\nContent-Length: 5\r\n"
      "Connection: keep-alive\r\n\r\nhello";
  WorkerResult server_result;
  std::thread server([&] {
    const int fd = AcceptUntil(listener, TestDeadline(), server_result.error);
    if (fd < 0) return;
    const auto received = ReadExactUntil(fd, expected_request.size(), TestDeadline(), server_result.error);
    if (!received) { (void)::close(fd); return; }
    server_result.received = *received;
    constexpr std::string_view first =
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n"
        "Connection: X-Response-Scope, Keep-Alive\r\nX-Response-Scope: secret\r\n"
        "Connection: X-Response-Second\r\nX-Response-Second: secret\r\n"
        "Keep-Alive: timeout=5\r\nProxy-Connection: keep-alive\r\nTE: trailers\r\n"
        "Trailer: X-Trailer\r\nUpgrade: websocket\r\nX-Origin: test\r\n\r\nhe";
    server_result.wire_ok = ::write(fd, first.data(), first.size()) == static_cast<ssize_t>(first.size()) &&
                            ::write(fd, "llo", 3) == 3;
    if (!server_result.wire_ok) server_result.error = "failed to write upstream response";
    (void)::close(fd);
  });

  std::array<int, 2> sockets{};
  std::array<int, 2> wake_sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()), 0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_sockets.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_sockets[0]);
  bool watchdog_fired = false;
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    if (::read(wake_sockets[0], &byte, 1) != 1) watchdog_fired = true;
    if (byte == 't') watchdog_fired = true;
    loop.Quit();
  });
  wake_channel.EnableReading();
  const config::Route route{"success", "origin.test", "/", {}, 2, 2, 1};
  const auto admission = std::make_shared<resilience::RouteAdmission>(
      route, std::chrono::steady_clock::now());
  std::weak_ptr<ProxyTransaction> transaction;
  bool request_callback_called = false;
  net::ClientConnection client(loop, sockets[0], [&](net::ClientConnection &connection,
                                                       const http::HttpRequest &parsed) {
    request_callback_called = true;
    transaction = ProxyTransaction::Start(loop, connection, listener.BoundPort(), parsed, admission);
  });
  client.Start();
  ASSERT_EQ(::write(sockets[1], wire_request.data(), wire_request.size()),
            static_cast<ssize_t>(wire_request.size()));

  WorkerResult peer_result;
  std::thread peer([&] {
    const auto response = ReadExactUntil(sockets[1], 59, TestDeadline(), peer_result.error);
    if (!response) return;
    peer_result.received = *response;
    peer_result.wire_ok = ::write(wake_sockets[1], "q", 1) == 1;
    if (!peer_result.wire_ok) peer_result.error = "failed to wake event loop";
  });
  std::string watchdog_error;
  LoopWatchdog watchdog(wake_sockets[1], watchdog_error);
  loop.Loop();
  watchdog.Stop();
  peer.join();
  server.join();

  EXPECT_FALSE(watchdog_fired);
  EXPECT_TRUE(watchdog_error.empty()) << watchdog_error;
  EXPECT_TRUE(request_callback_called);
  EXPECT_TRUE(transaction.expired());
  // The upstream completion terminates the transaction before downstream I/O
  // resumes; its route reservation must already be available again.
  EXPECT_TRUE(admission->TryAcquire(std::chrono::steady_clock::now()));
  EXPECT_TRUE(server_result.error.empty()) << server_result.error;
  EXPECT_EQ(server_result.received, expected_request);
  EXPECT_TRUE(server_result.wire_ok);
  EXPECT_TRUE(peer_result.error.empty()) << peer_result.error;
  EXPECT_EQ(peer_result.received, "HTTP/1.1 200 OK\r\nX-Origin: test\r\nContent-Length: 5\r\n\r\nhello");
  EXPECT_TRUE(peer_result.wire_ok);
  client.Close();
  wake_channel.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
  EXPECT_EQ(::close(wake_sockets[0]), 0);
  EXPECT_EQ(::close(wake_sockets[1]), 0);
}

TEST(ProxyTransactionTest, MapsUpstreamFailuresToExactBadGateway) {
  enum class Failure { kConnect, kEof, kUnsupported };
  for (const auto &[failure, upstream_response] : std::array{
           std::pair{Failure::kConnect, std::string_view{}},
           std::pair{Failure::kEof, std::string_view{}},
           std::pair{Failure::kUnsupported,
                     std::string_view{"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"}}}) {
    net::Socket listener = net::Socket::ListenLoopback();
    const std::uint16_t port = listener.BoundPort();
    std::thread server;
    WorkerResult server_result;
    if (failure != Failure::kConnect) {
      server = std::thread([&] {
        const int fd = AcceptUntil(listener, TestDeadline(), server_result.error);
        if (fd < 0) return;
        if (failure == Failure::kEof) {
          constexpr std::string_view expected =
              "GET / HTTP/1.1\r\nhost: test\r\nContent-Length: 0\r\n"
              "Connection: keep-alive\r\n\r\n";
          const auto received = ReadExactUntil(fd, expected.size(), TestDeadline(), server_result.error);
          if (received) {
            server_result.received = *received;
            server_result.wire_ok = true;
          }
        } else {
          server_result.wire_ok = ::write(fd, upstream_response.data(), upstream_response.size()) ==
                                  static_cast<ssize_t>(upstream_response.size());
          if (!server_result.wire_ok) server_result.error = "failed to write upstream response";
        }
        (void)::close(fd);
      });
    } else {
      listener.Close();
    }
    std::array<int, 2> sockets{};
    std::array<int, 2> wake_sockets{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()), 0);
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_sockets.data()), 0);
    net::EventLoop loop;
    net::Channel wake_channel(loop, wake_sockets[0]);
    bool watchdog_fired = false;
    wake_channel.SetReadCallback([&] {
      char byte = '\0';
      if (::read(wake_sockets[0], &byte, 1) != 1) watchdog_fired = true;
      if (byte == 't') watchdog_fired = true;
      loop.Quit();
    });
    wake_channel.EnableReading();
    std::shared_ptr<ProxyTransaction> transaction;
    net::ClientConnection client(loop, sockets[0], [&](net::ClientConnection &connection,
                                                         const http::HttpRequest &parsed) {
      transaction = ProxyTransaction::Start(loop, connection, port, parsed);
    });
    client.Start();
    constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: test\r\n\r\n";
    ASSERT_EQ(::write(sockets[1], request.data(), request.size()), static_cast<ssize_t>(request.size()));
    WorkerResult peer_result;
    std::thread peer([&] {
      const auto response = ReadExactUntil(sockets[1], 47, TestDeadline(), peer_result.error);
      if (!response) return;
      peer_result.received = *response;
      pollfd descriptor{sockets[1], POLLIN | POLLHUP, 0};
      if (::poll(&descriptor, 1, 100) != 0) {
        peer_result.error = "received bytes after terminal 502";
        return;
      }
      peer_result.wire_ok = ::write(wake_sockets[1], "q", 1) == 1;
      if (!peer_result.wire_ok) peer_result.error = "failed to wake event loop";
    });
    std::string watchdog_error;
  LoopWatchdog watchdog(wake_sockets[1], watchdog_error);
    loop.Loop();
    watchdog.Stop();
    peer.join();
    if (server.joinable()) server.join();
    EXPECT_FALSE(watchdog_fired);
  EXPECT_TRUE(watchdog_error.empty()) << watchdog_error;
    EXPECT_TRUE(server_result.error.empty()) << server_result.error;
    EXPECT_TRUE(server_result.wire_ok || failure == Failure::kConnect);
    if (failure == Failure::kEof) {
      EXPECT_EQ(server_result.received,
                "GET / HTTP/1.1\r\nhost: test\r\nContent-Length: 0\r\n"
                "Connection: keep-alive\r\n\r\n");
    }
    EXPECT_TRUE(peer_result.error.empty()) << peer_result.error;
    EXPECT_EQ(peer_result.received, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n");
    EXPECT_TRUE(peer_result.wire_ok);
    client.Close();
    wake_channel.Remove();
    EXPECT_EQ(::close(sockets[1]), 0);
    EXPECT_EQ(::close(wake_sockets[0]), 0);
    EXPECT_EQ(::close(wake_sockets[1]), 0);
  }
}

TEST(ProxyTransactionTest, DoesNotDereferenceClientDestroyedAfterRequestCallback) {
  net::Socket listener = net::Socket::ListenLoopback();
  constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: test\r\n\r\n";
  constexpr std::string_view expected_upstream_request =
      "GET / HTTP/1.1\r\nhost: test\r\nContent-Length: 0\r\n"
      "Connection: keep-alive\r\n\r\n";
  std::array<int, 2> client_sockets{};
  std::array<int, 2> wake_sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, client_sockets.data()), 0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_sockets.data()), 0);
  WorkerResult server_result;
  std::thread server([&] {
    const int fd = AcceptUntil(listener, TestDeadline(), server_result.error);
    if (fd < 0) return;
    const auto received = ReadExactUntil(fd, expected_upstream_request.size(), TestDeadline(),
                                         server_result.error);
    if (!received) { (void)::close(fd); return; }
    server_result.received = *received;
    constexpr std::string_view response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    server_result.wire_ok = ::write(fd, response.data(), response.size()) == static_cast<ssize_t>(response.size()) &&
                            WaitForPeerEofUntil(fd, TestDeadline(), server_result.error) &&
                            ::write(wake_sockets[1], "q", 1) == 1;
    if (!server_result.wire_ok && server_result.error.empty()) server_result.error = "failed to complete upstream exchange";
    (void)::close(fd);
  });

  net::EventLoop loop;
  std::weak_ptr<void> client_lifetime;
  std::weak_ptr<ProxyTransaction> transaction;
  std::unique_ptr<net::ClientConnection> client;
  net::Channel wake_channel(loop, wake_sockets[0]);
  bool watchdog_fired = false;
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    if (::read(wake_sockets[0], &byte, 1) != 1) watchdog_fired = true;
    if (byte == 't') watchdog_fired = true;
    loop.Quit();
  });
  wake_channel.EnableReading();
  client = std::make_unique<net::ClientConnection>(
      loop, client_sockets[0], [&](net::ClientConnection &connection, const http::HttpRequest &request) {
        client_lifetime = connection.LifetimeToken();
        transaction = ProxyTransaction::Start(loop, connection, listener.BoundPort(), request);
        client.reset();
      });
  client->Start();
  ASSERT_EQ(::write(client_sockets[1], request.data(), request.size()), static_cast<ssize_t>(request.size()));
  std::string watchdog_error;
  LoopWatchdog watchdog(wake_sockets[1], watchdog_error);
  loop.Loop();
  watchdog.Stop();
  server.join();

  EXPECT_FALSE(watchdog_fired);
  EXPECT_TRUE(watchdog_error.empty()) << watchdog_error;
  EXPECT_TRUE(server_result.error.empty()) << server_result.error;
  EXPECT_EQ(server_result.received, expected_upstream_request);
  EXPECT_TRUE(server_result.wire_ok);
  EXPECT_TRUE(client_lifetime.expired());
  EXPECT_TRUE(transaction.expired());
  wake_channel.Remove();
  EXPECT_EQ(::close(client_sockets[1]), 0);
  EXPECT_EQ(::close(wake_sockets[0]), 0);
  EXPECT_EQ(::close(wake_sockets[1]), 0);
}

TEST(ProxyTransactionTest, CancelsAllDeadlinesAfterARetrySucceeds) {
  net::Socket first_listener = net::Socket::ListenLoopback();
  net::Socket second_listener = net::Socket::ListenLoopback();
  const config::Endpoint first{"127.0.0.1", {127, 0, 0, 1}, first_listener.BoundPort(), 1};
  const config::Endpoint second{"127.0.0.1", {127, 0, 0, 1}, second_listener.BoundPort(), 1};
  constexpr std::string_view request =
      "GET /retry HTTP/1.1\r\nhost: test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
  std::string first_error;
  std::string second_error;
  std::thread first_backend([&] {
    const int fd = AcceptUntil(first_listener, TestDeadline(), first_error);
    if (fd < 0) return;
    (void)ReadExactUntil(fd, request.size(), TestDeadline(), first_error);
    (void)::close(fd);
  });
  std::thread second_backend([&] {
    const int fd = AcceptUntil(second_listener, TestDeadline(), second_error);
    if (fd < 0) return;
    (void)ReadExactUntil(fd, request.size(), TestDeadline(), second_error);
    constexpr std::string_view response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (second_error.empty() &&
        ::write(fd, response.data(), response.size()) != static_cast<ssize_t>(response.size())) {
      second_error = "failed to write retry response";
    }
    (void)::close(fd);
  });

  std::array<int, 2> client_sockets{};
  std::array<int, 2> wake_sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, client_sockets.data()), 0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_sockets.data()), 0);
  net::EventLoop loop;
  net::TimerQueue timers(loop);
  auto pool = std::make_shared<UpstreamPool>(loop);
  net::Channel wake_channel(loop, wake_sockets[0]);
  bool watchdog_fired = false;
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    if (::read(wake_sockets[0], &byte, 1) != 1 || byte == 't') watchdog_fired = true;
    loop.Quit();
  });
  wake_channel.EnableReading();
  std::shared_ptr<ProxyTransaction> transaction;
  net::ClientConnection client(loop, client_sockets[0],
                               [&](net::ClientConnection &connection, const http::HttpRequest &parsed) {
    UpstreamPolicy policy;
    policy.connect_timeout = std::chrono::seconds(1);
    policy.first_byte_timeout = std::chrono::seconds(1);
    policy.total_timeout = std::chrono::seconds(2);
    policy.retry_budget = 1;
    policy.retry_endpoints = {first, second};
    transaction = ProxyTransaction::Start(loop, connection, first, parsed, pool, nullptr, &timers,
                                          std::move(policy));
  });
  client.Start();
  constexpr std::string_view inbound = "GET /retry HTTP/1.1\r\nHost: test\r\n\r\n";
  ASSERT_EQ(::write(client_sockets[1], inbound.data(), inbound.size()),
            static_cast<ssize_t>(inbound.size()));
  WorkerResult peer_result;
  std::thread peer([&] {
    constexpr std::string_view response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    const auto received = ReadExactUntil(client_sockets[1], response.size(), TestDeadline(), peer_result.error);
    if (!received) return;
    peer_result.received = *received;
    peer_result.wire_ok = ::write(wake_sockets[1], "q", 1) == 1;
    if (!peer_result.wire_ok) peer_result.error = "failed to wake event loop";
  });
  std::string watchdog_error;
  LoopWatchdog watchdog(wake_sockets[1], watchdog_error);
  loop.Loop();
  watchdog.Stop();
  peer.join();
  first_backend.join();
  second_backend.join();

  EXPECT_FALSE(watchdog_fired);
  EXPECT_TRUE(watchdog_error.empty()) << watchdog_error;
  EXPECT_TRUE(first_error.empty()) << first_error;
  EXPECT_TRUE(second_error.empty()) << second_error;
  EXPECT_TRUE(peer_result.error.empty()) << peer_result.error;
  EXPECT_EQ(peer_result.received, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
  EXPECT_TRUE(peer_result.wire_ok);
  EXPECT_TRUE(transaction);
  EXPECT_EQ(timers.PendingCount(), 0U);
  client.Close();
  wake_channel.Remove();
  EXPECT_EQ(::close(client_sockets[1]), 0);
  EXPECT_EQ(::close(wake_sockets[0]), 0);
  EXPECT_EQ(::close(wake_sockets[1]), 0);
}


TEST(ProxyTransactionTest, DoesNotRetryWithoutADifferentCandidate) {
  net::Socket listener = net::Socket::ListenLoopback();
  const std::uint16_t port = listener.BoundPort();
  listener.Close();
  const config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1}, port, 1};

  std::array<int, 2> client_sockets{};
  std::array<int, 2> wake_sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, client_sockets.data()), 0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_sockets.data()), 0);
  net::EventLoop loop;
  auto pool = std::make_shared<UpstreamPool>(loop);
  net::Channel wake_channel(loop, wake_sockets[0]);
  bool watchdog_fired = false;
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    if (::read(wake_sockets[0], &byte, 1) != 1 || byte == 't') watchdog_fired = true;
    loop.Quit();
  });
  wake_channel.EnableReading();

  std::weak_ptr<ProxyTransaction> transaction;
  net::ClientConnection client(loop, client_sockets[0],
                               [&](net::ClientConnection &connection, const http::HttpRequest &parsed) {
    UpstreamPolicy policy;
    policy.connect_timeout = std::chrono::seconds(1);
    policy.first_byte_timeout = std::chrono::seconds(1);
    policy.total_timeout = std::chrono::seconds(2);
    policy.retry_budget = 1;
    policy.retry_endpoints = {endpoint};
    transaction = ProxyTransaction::Start(loop, connection, endpoint, parsed, pool, nullptr, nullptr,
                                          std::move(policy));
  });
  client.Start();
  constexpr std::string_view inbound = "GET /retry HTTP/1.1\r\nHost: test\r\n\r\n";
  ASSERT_EQ(::write(client_sockets[1], inbound.data(), inbound.size()),
            static_cast<ssize_t>(inbound.size()));

  WorkerResult peer_result;
  std::thread peer([&] {
    constexpr std::string_view response = "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n";
    const auto received = ReadExactUntil(client_sockets[1], response.size(), TestDeadline(), peer_result.error);
    if (!received) return;
    peer_result.received = *received;
    pollfd descriptor{client_sockets[1], POLLIN | POLLHUP, 0};
    if (::poll(&descriptor, 1, 100) != 0) {
      peer_result.error = "received bytes after terminal 502";
      return;
    }
    peer_result.wire_ok = ::write(wake_sockets[1], "q", 1) == 1;
    if (!peer_result.wire_ok) peer_result.error = "failed to wake event loop";
  });
  std::string watchdog_error;
  LoopWatchdog watchdog(wake_sockets[1], watchdog_error);
  loop.Loop();
  watchdog.Stop();
  peer.join();

  EXPECT_FALSE(watchdog_fired);
  EXPECT_TRUE(watchdog_error.empty()) << watchdog_error;
  EXPECT_TRUE(peer_result.error.empty()) << peer_result.error;
  EXPECT_EQ(peer_result.received, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n");
  EXPECT_TRUE(peer_result.wire_ok);
  EXPECT_TRUE(transaction.expired());
  client.Close();
  wake_channel.Remove();
  EXPECT_EQ(::close(client_sockets[1]), 0);
  EXPECT_EQ(::close(wake_sockets[0]), 0);
  EXPECT_EQ(::close(wake_sockets[1]), 0);
}
} // namespace
} // namespace aegisgate::proxy
