#include <array>
#include <cerrno>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <mutex>
#include <thread>
#include <utility>

#include <gtest/gtest.h>

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/config/Config.h"
#include "aegisgate/gateway/Gateway.h"
#include "aegisgate/mock/MockBackend.h"

#include "../support/WakeFd.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/Socket.h"

namespace aegisgate::integration {
namespace {

using Deadline = std::chrono::steady_clock::time_point;

Deadline TestDeadline() { return std::chrono::steady_clock::now() + std::chrono::seconds(5); }

int RemainingMilliseconds(Deadline deadline) {
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
  return remaining.count() > 0 ? static_cast<int>(remaining.count()) : 0;
}

bool WaitFor(int fd, short events, Deadline deadline) {
  pollfd descriptor{fd, events, 0};
  return ::poll(&descriptor, 1, RemainingMilliseconds(deadline)) > 0;
}

bool WriteAll(int fd, std::string_view bytes, Deadline deadline, std::string &error) {
  std::size_t sent = 0;
  while (sent < bytes.size()) {
    const ssize_t count = ::write(fd, bytes.data() + sent, bytes.size() - sent);
    if (count > 0) { sent += static_cast<std::size_t>(count); continue; }
    if (count < 0 && errno == EINTR) continue;
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) && WaitFor(fd, POLLOUT, deadline)) continue;
    error = "write failed";
    return false;
  }
  return true;
}

std::string ReadExact(int fd, std::size_t size, Deadline deadline, std::string &error) {
  std::string result(size, '\0');
  std::size_t received = 0;
  while (received < size) {
    if (!WaitFor(fd, POLLIN | POLLHUP, deadline)) { error = "read timed out"; return {}; }
    const ssize_t count = ::read(fd, result.data() + received, size - received);
    if (count > 0) { received += static_cast<std::size_t>(count); continue; }
    if (count < 0 && errno == EINTR) continue;
    error = "unexpected EOF or read error";
    return {};
  }
  return result;
}

std::string ReadUntil(int fd, std::string_view needle, Deadline deadline, std::string &error) {
  std::array<char, 4096> bytes{};
  std::string result;
  while (result.find(needle) == std::string::npos) {
    if (!WaitFor(fd, POLLIN | POLLHUP, deadline)) { error = "read timed out"; return {}; }
    const ssize_t count = ::read(fd, bytes.data(), bytes.size());
    if (count > 0) { result.append(bytes.data(), static_cast<std::size_t>(count)); continue; }
    if (count < 0 && errno == EINTR) continue;
    error = "unexpected EOF or read error";
    return {};
  }
  return result;
}

class BackendRunner {
public:
  explicit BackendRunner(mock::MockBackendOptions options, std::string &error)
      : error_(&error) {
    auto ready = std::make_shared<std::promise<std::pair<std::uint16_t, int>>>();
    auto future = ready->get_future();
    thread_ = std::thread([options, ready, this] {
      try {
        std::array<int, 2> wake{};
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake.data()) != 0) {
          throw std::system_error(errno, std::generic_category(), "socketpair");
        }
        net::EventLoop loop;
        net::Channel channel(loop, wake[0]);
        channel.SetReadCallback([&] {
          (void)test::ConsumeWakeFd(wake[0], 'q', *error_);
          loop.Quit();
        });
        channel.EnableReading();
        mock::MockBackend backend(loop, options, "127.0.0.1", 0);
        backend.Start();
        ready->set_value({backend.port(), wake[1]});
        loop.Loop();
        channel.Remove();
        (void)::close(wake[0]);
        (void)::close(wake[1]);
      } catch (...) {
        try { ready->set_exception(std::current_exception()); } catch (...) {}
      }
    });
    const auto [port, wake] = future.get();
    port_ = port;
    wake_ = wake;
  }

  ~BackendRunner() { Stop(); }

  // Signals the worker to exit, joins it, and makes later Stop() calls
  // idempotent so the caller can assert the error state afterwards.
  void Stop() {
    if (wake_ >= 0) {
      (void)test::SignalWakeFd(wake_, 'q', *error_);
      wake_ = -1;
    }
    if (thread_.joinable()) thread_.join();
  }

  [[nodiscard]] std::uint16_t port() const noexcept { return port_; }

private:
  std::thread thread_;
  std::uint16_t port_ = 0;
  int wake_ = -1;
  std::string *error_ = nullptr;
};


// One loopback backend answering each accepted connection from a sequence of
// responses; failures are recorded into the caller's error string
// (mutex-protected) and asserted by the main thread after Stop().
int AcceptBlocking(const net::Socket &listener, Deadline deadline) {
  for (;;) {
    const int fd = listener.Accept();
    if (fd >= 0) {
      const int flags = ::fcntl(fd, F_GETFL);
      if (flags >= 0) (void)::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
      return fd;
    }
    pollfd descriptor{listener.Fd(), POLLIN, 0};
    if (::poll(&descriptor, 1, RemainingMilliseconds(deadline)) <= 0) return -1;
  }
}

class SequencedBackend {
public:
  explicit SequencedBackend(std::vector<std::string> responses, std::string &error)
      : listener_(net::Socket::ListenLoopback()), responses_(std::move(responses)),
        error_(&error) {
    thread_ = std::thread([this] { Serve(); });
  }
  ~SequencedBackend() {
    Stop();
    listener_.Close();
  }
  void Stop() {
    if (thread_.joinable()) thread_.join();
  }
  [[nodiscard]] std::uint16_t port() const { return listener_.BoundPort(); }

private:
  void Fail(std::string message) {
    std::lock_guard<std::mutex> guard(mutex_);
    error_->append(std::move(message)).append("; ");
  }
  void Serve() {
    for (const auto &response : responses_) {
      const int fd = AcceptBlocking(listener_, TestDeadline());
      if (fd < 0) {
        Fail("accept timed out");
        return;
      }
      std::array<char, 512> request{};
      (void)::read(fd, request.data(), request.size());
      if (::write(fd, response.data(), response.size()) != static_cast<ssize_t>(response.size())) {
        Fail("write failed");
      }
      (void)::close(fd);
    }
  }
  net::Socket listener_;
  std::vector<std::string> responses_;
  std::string *error_;
  std::mutex mutex_;
  std::thread thread_;
};

config::Endpoint Endpoint(std::uint16_t port) { return {"127.0.0.1", {127, 0, 0, 1}, port, 1}; }

config::Route Route(std::string name, std::string host, config::Endpoint endpoint,
                    std::uint32_t rate = 100, std::uint32_t burst = 100,
                    std::uint32_t max_inflight = 8, std::uint32_t first_byte_ms = 500) {
  config::Route route{std::move(name), std::move(host), "/", {std::move(endpoint)}, rate, burst, max_inflight};
  route.connect_timeout_ms = 200;
  route.first_byte_timeout_ms = first_byte_ms;
  route.total_timeout_ms = 1000;
  route.retry_budget = 0;
  return route;
}

template <typename Client>
void RunGateway(config::Config config, Client client) {
  std::array<int, 2> wake{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake[0]);
  wake_channel.SetReadCallback([&] { char byte = '\0'; ASSERT_EQ(::read(wake[0], &byte, 1), 1); loop.Quit(); });
  wake_channel.EnableReading();
  gateway::Gateway gateway(loop, std::move(config), "127.0.0.1", 0);
  gateway.Start();
  std::thread client_thread([&] { client(gateway.port(), wake[1]); });
  loop.Loop();
  client_thread.join();
  wake_channel.Remove();
  EXPECT_EQ(::close(wake[0]), 0);
  EXPECT_EQ(::close(wake[1]), 0);
}

TEST(EndToEndTest, ForwardsNormalAnd5xxMockResultsAndExportsMetrics) {
  std::string normal_error;
  std::string fault_error;
  BackendRunner normal(mock::MockBackendOptions{.status = 200}, normal_error);
  BackendRunner fault(mock::MockBackendOptions{.status = 503}, fault_error);
  std::string error;
  std::string normal_response;
  std::string fault_response;
  std::string metrics;
  RunGateway(config::Config{{Route("normal", "normal.e2e.test", Endpoint(normal.port())),
                             Route("fault", "fault.e2e.test", Endpoint(fault.port()))}},
             [&](std::uint16_t port, int wake) {
    net::Socket client = net::Socket::ConnectLoopback(port);
    constexpr std::string_view normal_request = "GET / HTTP/1.1\r\nHost: normal.e2e.test\r\n\r\n";
    constexpr std::string_view normal_expected = "HTTP/1.1 200 Mock Response\r\nContent-Length: 0\r\n\r\n";
    constexpr std::string_view fault_request = "GET / HTTP/1.1\r\nHost: fault.e2e.test\r\n\r\n";
    constexpr std::string_view fault_expected = "HTTP/1.1 503 Mock Failure\r\nContent-Length: 0\r\n\r\n";
    if (WriteAll(client.Fd(), normal_request, TestDeadline(), error)) normal_response = ReadExact(client.Fd(), normal_expected.size(), TestDeadline(), error);
    if (error.empty() && WriteAll(client.Fd(), fault_request, TestDeadline(), error)) fault_response = ReadExact(client.Fd(), fault_expected.size(), TestDeadline(), error);
    constexpr std::string_view request = "GET /metrics HTTP/1.1\r\nHost: ignored.test\r\n\r\n";
    if (error.empty() && WriteAll(client.Fd(), request, TestDeadline(), error)) metrics = ReadUntil(client.Fd(), "aegisgate_inflight_requests 0\n", TestDeadline(), error);
    if (::write(wake, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(normal_response, "HTTP/1.1 200 Mock Response\r\nContent-Length: 0\r\n\r\n");
  EXPECT_EQ(fault_response, "HTTP/1.1 503 Mock Failure\r\nContent-Length: 0\r\n\r\n");
  EXPECT_NE(metrics.find("aegisgate_requests_total{route=\"normal\",status=\"200\""), std::string::npos);
  EXPECT_NE(metrics.find("aegisgate_requests_total{route=\"fault\",status=\"503\""), std::string::npos);
  normal.Stop();
  fault.Stop();
  EXPECT_TRUE(normal_error.empty()) << normal_error;
  EXPECT_TRUE(fault_error.empty()) << fault_error;
}

TEST(EndToEndTest, MapsResetAndDelayedMocksTo502And504) {
  std::string reset_error;
  std::string delayed_error;
  BackendRunner reset(mock::MockBackendOptions{.reset = true}, reset_error);
  BackendRunner delayed(mock::MockBackendOptions{.delay = std::chrono::milliseconds(400)}, delayed_error);
  std::string error;
  std::string reset_response;
  std::string delayed_response;
  RunGateway(config::Config{{Route("reset", "reset.e2e.test", Endpoint(reset.port())),
                             Route("delayed", "delayed.e2e.test", Endpoint(delayed.port()), 100, 100, 8, 50)}},
             [&](std::uint16_t port, int wake) {
    net::Socket client = net::Socket::ConnectLoopback(port);
    constexpr std::string_view reset_request = "GET / HTTP/1.1\r\nHost: reset.e2e.test\r\n\r\n";
    constexpr std::string_view delayed_request = "GET / HTTP/1.1\r\nHost: delayed.e2e.test\r\n\r\n";
    constexpr std::string_view bad_gateway = "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n";
    constexpr std::string_view timeout = "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n";
    if (WriteAll(client.Fd(), reset_request, TestDeadline(), error)) reset_response = ReadExact(client.Fd(), bad_gateway.size(), TestDeadline(), error);
    if (error.empty() && WriteAll(client.Fd(), delayed_request, TestDeadline(), error)) delayed_response = ReadExact(client.Fd(), timeout.size(), TestDeadline(), error);
    if (::write(wake, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(reset_response, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n");
  EXPECT_EQ(delayed_response, "HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n");
  reset.Stop();
  delayed.Stop();
  EXPECT_TRUE(reset_error.empty()) << reset_error;
  EXPECT_TRUE(delayed_error.empty()) << delayed_error;
}

TEST(EndToEndTest, EnforcesRateAndInflightAdmissionAgainstRealMocks) {
  std::string fast_error;
  std::string slow_error;
  BackendRunner fast(mock::MockBackendOptions{.status = 200}, fast_error);
  BackendRunner slow(mock::MockBackendOptions{.delay = std::chrono::milliseconds(400)}, slow_error);
  std::string error;
  std::string rate_first;
  std::string rate_second;
  std::string inflight_second;
  RunGateway(config::Config{{Route("rate", "rate.e2e.test", Endpoint(fast.port()), 1, 1, 8),
                             Route("inflight", "inflight.e2e.test", Endpoint(slow.port()), 100, 100, 1, 800)}},
             [&](std::uint16_t port, int wake) {
    net::Socket rate = net::Socket::ConnectLoopback(port);
    constexpr std::string_view rate_request = "GET / HTTP/1.1\r\nHost: rate.e2e.test\r\n\r\n";
    constexpr std::string_view ok = "HTTP/1.1 200 Mock Response\r\nContent-Length: 0\r\n\r\n";
    constexpr std::string_view limited = "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n";
    if (WriteAll(rate.Fd(), rate_request, TestDeadline(), error)) rate_first = ReadExact(rate.Fd(), ok.size(), TestDeadline(), error);
    if (error.empty() && WriteAll(rate.Fd(), rate_request, TestDeadline(), error)) rate_second = ReadExact(rate.Fd(), limited.size(), TestDeadline(), error);
    net::Socket first = net::Socket::ConnectLoopback(port);
    constexpr std::string_view slow_request = "GET / HTTP/1.1\r\nHost: inflight.e2e.test\r\n\r\n";
    if (error.empty() && !WriteAll(first.Fd(), slow_request, TestDeadline(), error)) {}
    if (error.empty() && WaitFor(first.Fd(), POLLIN | POLLHUP, std::chrono::steady_clock::now() + std::chrono::milliseconds(75))) error = "slow response arrived too early";
    net::Socket second = net::Socket::ConnectLoopback(port);
    if (error.empty() && WriteAll(second.Fd(), slow_request, TestDeadline(), error)) inflight_second = ReadExact(second.Fd(), limited.size(), TestDeadline(), error);
    if (::write(wake, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(rate_first, "HTTP/1.1 200 Mock Response\r\nContent-Length: 0\r\n\r\n");
  EXPECT_EQ(rate_second, "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n");
  EXPECT_EQ(inflight_second, "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n");
  fast.Stop();
  slow.Stop();
  EXPECT_TRUE(fast_error.empty()) << fast_error;
  EXPECT_TRUE(slow_error.empty()) << slow_error;
}


TEST(EndToEndTest, CircuitBreakerOpensAndRecovers) {
  std::string backend_error;
  SequencedBackend backend(
      {"HTTP/1.1 503 Mock Failure\r\nContent-Length: 0\r\n\r\n",
       "HTTP/1.1 503 Mock Failure\r\nContent-Length: 0\r\n\r\n",
       "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"},
      backend_error);
  config::Route route = Route("guarded", "guarded.e2e.test", Endpoint(backend.port()));
  route.circuit_breaker = config::CircuitBreakerSettings{10, 2, 500, 1, 1};
  std::string error;
  std::string metrics;
  std::string recovered_metrics;
  RunGateway(config::Config{{route}}, [&](std::uint16_t port, int wake) {
    net::Socket client = net::Socket::ConnectLoopback(port);
    constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: guarded.e2e.test\r\n\r\n";
    constexpr std::string_view upstream_fail = "HTTP/1.1 503 Mock Failure\r\nContent-Length: 0\r\n\r\n";
    constexpr std::string_view gateway_fail = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
    constexpr std::string_view ok = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    // Two failing requests open the breaker (min_requests=2).
    for (int i = 0; i < 2; ++i) {
      if (error.empty() && WriteAll(client.Fd(), request, TestDeadline(), error)) {
        if (ReadExact(client.Fd(), upstream_fail.size(), TestDeadline(), error) != upstream_fail) {
          if (error.empty()) error = "expected upstream 503";
        }
      }
    }
    // Open: the gateway answers 503 without connecting.
    if (error.empty() && WriteAll(client.Fd(), request, TestDeadline(), error)) {
      if (ReadExact(client.Fd(), gateway_fail.size(), TestDeadline(), error) != gateway_fail) {
        if (error.empty()) error = "expected gateway 503 while open";
      }
    }
    // The metrics expose the open state (appended after the counters) and
    // the 503 reason; read until the circuit_state line so both are present.
    const std::string circuit_needle = "aegisgate_circuit_state{route=\"guarded\",upstream=\"127.0.0.1:" +
                                       std::to_string(backend.port()) + "\"} open\n";
    if (error.empty() && WriteAll(client.Fd(), "GET /metrics HTTP/1.1\r\nHost: ignored.test\r\n\r\n",
                                  TestDeadline(), error)) {
      metrics = ReadUntil(client.Fd(), circuit_needle, TestDeadline(), error);
    }
    // Poll until the open window elapses and the probe passes (backend 200);
    // keep retrying past transient 503 answers until the deadline.  A fresh
    // connection per attempt keeps the byte streams aligned.
    const auto deadline = TestDeadline();
    for (;;) {
      std::string attempt_error;
      net::Socket probe = net::Socket::ConnectLoopback(port);
      if (WriteAll(probe.Fd(), request, TestDeadline(), attempt_error)) {
        const std::string response = ReadExact(probe.Fd(), ok.size(), TestDeadline(), attempt_error);
        if (attempt_error.empty() && response == ok) break;
      }
      if (std::chrono::steady_clock::now() > deadline) {
        error = "breaker did not recover in time";
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    // The probe closed the breaker; the metrics now show closed, and the
    // 503 reason label is still recorded.
    if (error.empty() && WriteAll(client.Fd(), "GET /metrics HTTP/1.1\r\nHost: ignored.test\r\n\r\n",
                                  TestDeadline(), error)) {
      recovered_metrics = ReadUntil(client.Fd(), "aegisgate_inflight_requests 0\n", TestDeadline(), error);
    }
    if (::write(wake, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  backend.Stop();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  EXPECT_NE(metrics.find("aegisgate_circuit_state{route=\"guarded\",upstream=\"127.0.0.1:" +
                         std::to_string(backend.port()) + "\"} open\n"),
            std::string::npos);
  EXPECT_NE(metrics.find("aegisgate_requests_total{route=\"guarded\",status=\"503\",upstream=\"\",reason=\"no_healthy_endpoint\"} 1\n"),
            std::string::npos);
  EXPECT_NE(recovered_metrics.find("aegisgate_circuit_state{route=\"guarded\",upstream=\"127.0.0.1:" +
                                   std::to_string(backend.port()) + "\"} closed\n"),
            std::string::npos);
}

TEST(EndToEndTest, AllEndpointsUnavailableServesUnique503WithInflightZero) {
  std::string backend_error;
  SequencedBackend backend({"HTTP/1.1 503 Mock Failure\r\nContent-Length: 0\r\n\r\n"},
                           backend_error);
  config::Route route = Route("guarded", "guarded.e2e.test", Endpoint(backend.port()));
  route.circuit_breaker = config::CircuitBreakerSettings{10, 1, 500, 5, 1};
  std::string error;
  std::string metrics;
  std::string recovered_metrics;
  RunGateway(config::Config{{route}}, [&](std::uint16_t port, int wake) {
    net::Socket client = net::Socket::ConnectLoopback(port);
    constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: guarded.e2e.test\r\n\r\n";
    constexpr std::string_view upstream_fail = "HTTP/1.1 503 Mock Failure\r\nContent-Length: 0\r\n\r\n";
    constexpr std::string_view gateway_fail = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
    // One failing request opens the breaker (min_requests=1).
    if (WriteAll(client.Fd(), request, TestDeadline(), error)) {
      if (ReadExact(client.Fd(), upstream_fail.size(), TestDeadline(), error) != upstream_fail) {
        if (error.empty()) error = "expected upstream 503";
      }
    }
    // Open: the gateway answers 503 without connecting or retrying.
    if (error.empty() && WriteAll(client.Fd(), request, TestDeadline(), error)) {
      if (ReadExact(client.Fd(), gateway_fail.size(), TestDeadline(), error) != gateway_fail) {
        if (error.empty()) error = "expected gateway 503 while open";
      }
    }
    if (error.empty() && WriteAll(client.Fd(), "GET /metrics HTTP/1.1\r\nHost: ignored.test\r\n\r\n",
                                  TestDeadline(), error)) {
      metrics = ReadUntil(client.Fd(), "aegisgate_inflight_requests 0\n", TestDeadline(), error);
    }
    if (::write(wake, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  backend.Stop();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  EXPECT_NE(metrics.find("aegisgate_requests_total{route=\"guarded\",status=\"503\",upstream=\"\",reason=\"no_healthy_endpoint\"} 1\n"),
            std::string::npos);
  EXPECT_NE(metrics.find("aegisgate_inflight_requests 0\n"), std::string::npos);
}

} // namespace
} // namespace aegisgate::integration
