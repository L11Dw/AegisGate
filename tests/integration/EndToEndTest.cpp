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
#include "aegisgate/resilience/CircuitBreaker.h"
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

// The coordinator processes breaker results asynchronously on its own loop:
// a snapshot-state poll is the deterministic barrier the client side needs
// before it may rely on the breaker being open (or closed again).
bool WaitForBreakerState(const gateway::Gateway &gateway, const config::Route &route,
                         const config::Endpoint &endpoint,
                         resilience::CircuitBreaker::State wanted, Deadline deadline) {
  while (std::chrono::steady_clock::now() < deadline) {
    if (gateway.BreakerState(route, endpoint) == wanted) return true;
  }
  return false;
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

// Deadline-bounded accept that reports failure via the shared error string.
int AcceptUntil(const net::Socket &listener, Deadline deadline, std::string &error) {
  const int fd = AcceptBlocking(listener, deadline);
  if (fd < 0 && error.empty()) error = "accept timed out";
  return fd;
}

// Deadline-bounded exact read that reports failure via the shared error
// string; returns std::nullopt on any failure.
std::optional<std::string> ReadExactUntil(int fd, std::size_t size, Deadline deadline,
                                          std::string &error) {
  std::string result(size, '\0');
  std::size_t received = 0;
  while (received < size) {
    pollfd descriptor{fd, POLLIN | POLLHUP, 0};
    if (::poll(&descriptor, 1, RemainingMilliseconds(deadline)) <= 0) {
      if (error.empty()) error = "read timed out";
      return std::nullopt;
    }
    const ssize_t count = ::read(fd, result.data() + received, size - received);
    if (count > 0) {
      received += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) continue;
    if (error.empty()) error = "unexpected EOF or read error";
    return std::nullopt;
  }
  return result;
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
void RunGateway(config::Config config, Client client,
                net::StreamFlowControl flow_control = net::StreamFlowControl{}) {
  std::array<int, 2> wake{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake[0]);
  wake_channel.SetReadCallback([&] { char byte = '\0'; ASSERT_EQ(::read(wake[0], &byte, 1), 1); loop.Quit(); });
  wake_channel.EnableReading();
  gateway::Gateway gateway(loop, std::move(config), "127.0.0.1", 0, flow_control);
  gateway.Start();
  std::thread client_thread([&] { client(gateway, gateway.port(), wake[1]); });
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
             [&](gateway::Gateway &, std::uint16_t port, int wake) {
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
             [&](gateway::Gateway &, std::uint16_t port, int wake) {
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
             [&](gateway::Gateway &, std::uint16_t port, int wake) {
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
  RunGateway(config::Config{{route}}, [&](gateway::Gateway &gateway, std::uint16_t port, int wake) {
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
    // The coordinator processes the two failures asynchronously: request 3
    // must not race it (it would connect while the snapshot still says
    // closed and consume the backend's 200).  The snapshot-state poll is the
    // deterministic barrier before the request and before the metrics read.
    // The route pointer must come from the gateway's own config copy
    // (RouteIndexOf matches by pointer identity).
    const config::Route *matched = gateway.Routes().Match("guarded.e2e.test", "/");
    if (!WaitForBreakerState(gateway, *matched, matched->endpoints.front(),
                             resilience::CircuitBreaker::State::kOpen, TestDeadline())) {
      error = "breaker did not open in time";
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
                                       std::to_string(backend.port()) +
                                       "\",state=\"open\"} 1\n";
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
    // The probe closed the breaker asynchronously on the coordinator loop:
    // the snapshot-state poll is the barrier before the recovered metrics
    // read renders the closed gauge.
    if (error.empty() &&
        !WaitForBreakerState(gateway, *matched, matched->endpoints.front(),
                             resilience::CircuitBreaker::State::kClosed, TestDeadline())) {
      error = "breaker did not close after the probe";
    }
    const std::string closed_needle = "aegisgate_circuit_state{route=\"guarded\",upstream=\"127.0.0.1:" +
                                      std::to_string(backend.port()) +
                                      "\",state=\"closed\"} 1\n";
    if (error.empty() && WriteAll(client.Fd(), "GET /metrics HTTP/1.1\r\nHost: ignored.test\r\n\r\n",
                                  TestDeadline(), error)) {
      recovered_metrics = ReadUntil(client.Fd(), closed_needle, TestDeadline(), error);
    }
    if (::write(wake, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  backend.Stop();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  EXPECT_NE(metrics.find("aegisgate_circuit_state{route=\"guarded\",upstream=\"127.0.0.1:" +
                         std::to_string(backend.port()) + "\",state=\"open\"} 1\n"),
            std::string::npos);
  // The 503-no-connect counter accrued both from request 3 and from the
  // recovery loop's probes while the breaker was still open, so only the
  // label prefix (not an exact count) is asserted on the recovered metrics.
  EXPECT_NE(recovered_metrics.find("aegisgate_requests_total{route=\"guarded\",status=\"503\",upstream=\"\",reason=\"no_healthy_endpoint\"} "),
            std::string::npos);
  EXPECT_NE(recovered_metrics.find("aegisgate_circuit_state{route=\"guarded\",upstream=\"127.0.0.1:" +
                                   std::to_string(backend.port()) + "\",state=\"closed\"} 1\n"),
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
  RunGateway(config::Config{{route}}, [&](gateway::Gateway &, std::uint16_t port, int wake) {
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
    if (!error.empty() && error == "unexpected EOF or read error") {
      error += " (first response)";
    }
    // Open: the gateway answers 503 without connecting or retrying.
    if (error.empty() && WriteAll(client.Fd(), request, TestDeadline(), error)) {
      if (ReadExact(client.Fd(), gateway_fail.size(), TestDeadline(), error) != gateway_fail) {
        if (error.empty()) error = "expected gateway 503 while open";
      }
    }
    if (!error.empty() && error == "unexpected EOF or read error") {
      error += " (second response)";
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

TEST(EndToEndTest, LeastActiveSelectsBackendWithFewestInFlight) {
  // The slow backend holds its request for 400ms; with equal weights the
  // rotation would send the third request back to it, but least-active keeps
  // it on the fast backend while the slow attempt is still in flight.
  std::string slow_error;
  std::string fast_error;
  BackendRunner slow(mock::MockBackendOptions{.status = 503,
                                              .delay = std::chrono::milliseconds(400)},
                     slow_error);
  BackendRunner fast(mock::MockBackendOptions{.status = 200}, fast_error);
  config::Route route = Route("least", "least.e2e.test", Endpoint(slow.port()));
  route.balance = config::BalancePolicy::kLeastActive;
  route.endpoints.push_back(Endpoint(fast.port()));
  route.first_byte_timeout_ms = 800;
  std::string error;
  std::string slow_response;
  std::string fast_response;
  std::string third_response;
  std::string metrics;
  RunGateway(config::Config{{route}}, [&](gateway::Gateway &, std::uint16_t port, int wake) {
    constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: least.e2e.test\r\n\r\n";
    constexpr std::string_view slow_expected =
        "HTTP/1.1 503 Mock Failure\r\nContent-Length: 0\r\n\r\n";
    constexpr std::string_view fast_expected =
        "HTTP/1.1 200 Mock Response\r\nContent-Length: 0\r\n\r\n";
    net::Socket slow_client = net::Socket::ConnectLoopback(port);
    if (WriteAll(slow_client.Fd(), request, TestDeadline(), error)) {
      // The first request occupies the slow endpoint's active slot; both
      // follow-up requests must land on the fast endpoint.
      net::Socket fast_client = net::Socket::ConnectLoopback(port);
      if (error.empty() && WriteAll(fast_client.Fd(), request, TestDeadline(), error)) {
        fast_response = ReadExact(fast_client.Fd(), fast_expected.size(), TestDeadline(), error);
      }
      if (error.empty() && WriteAll(fast_client.Fd(), request, TestDeadline(), error)) {
        third_response = ReadExact(fast_client.Fd(), fast_expected.size(), TestDeadline(), error);
      }
      slow_response = ReadExact(slow_client.Fd(), slow_expected.size(), TestDeadline(), error);
    }
    constexpr std::string_view metrics_request = "GET /metrics HTTP/1.1\r\nHost: ignored.test\r\n\r\n";
    if (error.empty() && WriteAll(slow_client.Fd(), metrics_request, TestDeadline(), error)) {
      metrics = ReadUntil(slow_client.Fd(), "aegisgate_inflight_requests 0\n", TestDeadline(), error);
    }
    if (::write(wake, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(slow_response, "HTTP/1.1 503 Mock Failure\r\nContent-Length: 0\r\n\r\n");
  EXPECT_EQ(fast_response, "HTTP/1.1 200 Mock Response\r\nContent-Length: 0\r\n\r\n");
  EXPECT_EQ(third_response, "HTTP/1.1 200 Mock Response\r\nContent-Length: 0\r\n\r\n");
  EXPECT_NE(metrics.find("aegisgate_requests_total{route=\"least\",status=\"200\",upstream=\"127.0.0.1:" +
                         std::to_string(fast.port()) + "\"} 2\n"),
            std::string::npos);
  EXPECT_NE(metrics.find("aegisgate_requests_total{route=\"least\",status=\"503\",upstream=\"127.0.0.1:" +
                         std::to_string(slow.port()) + "\"} 1\n"),
            std::string::npos);
  slow.Stop();
  fast.Stop();
  EXPECT_TRUE(slow_error.empty()) << slow_error;
  EXPECT_TRUE(fast_error.empty()) << fast_error;
}

// --- M3-C streaming end to end ---

TEST(EndToEndTest, ConnectionCloseRequestClosesAfterStreamingResponse) {
  std::string backend_error;
  SequencedBackend backend({"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"}, backend_error);
  std::string error;
  std::string received;
  RunGateway(config::Config{{Route("close", "close.e2e.test", Endpoint(backend.port()))}},
             [&](gateway::Gateway &, std::uint16_t port, int wake) {
    net::Socket client = net::Socket::ConnectLoopback(port);
    constexpr std::string_view request =
        "GET / HTTP/1.1\r\nHost: close.e2e.test\r\nConnection: close\r\n\r\n";
    constexpr std::string_view expected = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (WriteAll(client.Fd(), request, TestDeadline(), error)) {
      received = ReadExact(client.Fd(), expected.size(), TestDeadline(), error);
    }
    // R-048: the Connection: close connection must close after the full
    // streamed response.  This small response drains synchronously, so the
    // close must happen in FinishResponse() (no pending write re-enters
    // HandleWrite()).
    if (error.empty()) {
      pollfd descriptor{client.Fd(), POLLIN | POLLHUP, 0};
      if (::poll(&descriptor, 1, RemainingMilliseconds(TestDeadline())) <= 0) {
        error = "connection stayed open after a Connection: close response";
      } else {
        std::array<char, 8> extra{};
        if (::read(client.Fd(), extra.data(), extra.size()) != 0) {
          error = "expected EOF after Connection: close response";
        }
      }
    }
    if (::write(wake, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  backend.Stop();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  EXPECT_EQ(received, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
}

TEST(EndToEndTest, StreamsLargeResponseUnderSlowClient) {
  net::Socket listener = net::Socket::ListenLoopback();
  constexpr std::size_t kBodySize = 128 * 1024;
  std::array<int, 2> signal{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, signal.data()), 0);
  std::string backend_error;
  std::thread backend([&] {
    const int fd = AcceptUntil(listener, TestDeadline(), backend_error);
    if (fd < 0) return;
    constexpr std::string_view request =
        "GET /slow HTTP/1.1\r\nhost: slow.e2e.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    (void)ReadExactUntil(fd, request.size(), TestDeadline(), backend_error);
    const std::string header = "HTTP/1.1 200 OK\r\nContent-Length: " +
                               std::to_string(kBodySize) + "\r\n\r\n";
    if (backend_error.empty() &&
        ::write(fd, header.data(), header.size()) != static_cast<ssize_t>(header.size())) {
      backend_error = "failed to write header";
    }
    const std::string body(kBodySize, 'x');
    std::size_t written = 0;
    while (backend_error.empty() && written < body.size()) {
      const ssize_t count = ::write(fd, body.data() + written, body.size() - written);
      if (count > 0) {
        written += static_cast<std::size_t>(count);
        continue;
      }
      if (count < 0 && errno == EINTR) continue;
      if (backend_error.empty()) backend_error = "backend write failed";
      break;
    }
    (void)::close(fd);
    // The body is fully buffered upstream; the client has read nothing yet.
    (void)::write(signal[1], "d", 1);
  });

  config::Route route{"slow", "slow.e2e.test", "/", {Endpoint(listener.BoundPort())}, 100, 100, 8};
  route.total_timeout_ms = 5000;
  route.first_byte_timeout_ms = 1000;
  std::string error;
  std::string received;
  // A small hysteresis makes the pause deterministic: the 128 KiB response
  // far exceeds the 8 KiB high watermark once the kernel send queue (64 KiB)
  // is saturated by the not-reading client.
  RunGateway(config::Config{{route}}, [&](gateway::Gateway &, std::uint16_t port, int wake) {
    net::Socket client = net::Socket::ConnectLoopback(port);
    constexpr std::string_view request = "GET /slow HTTP/1.1\r\nHost: slow.e2e.test\r\n\r\n";
    if (!WriteAll(client.Fd(), request, TestDeadline(), error)) {
      (void)::write(wake, "q", 1);
      return;
    }
    // Do not read anything while the whole response is buffered: the gateway
    // pauses its upstream read above the high watermark instead of timing out
    // or dropping bytes.  Wait for the backend to finish writing first.
    pollfd signal_descriptor{signal[0], POLLIN, 0};
    if (::poll(&signal_descriptor, 1, RemainingMilliseconds(TestDeadline())) <= 0) {
      if (error.empty()) error = "backend never finished writing";
    } else {
      char byte = '\0';
      EXPECT_EQ(::read(signal[0], &byte, 1), 1);
    }
    // Drain everything: the low-water notification resumes the upstream read
    // and the complete response arrives exactly once (no loss, no duplicate,
    // no total timeout while the client was slow).
    constexpr std::size_t kExpected = 43 + kBodySize;
    while (error.empty() && received.size() < kExpected) {
      pollfd descriptor{client.Fd(), POLLIN | POLLHUP, 0};
      if (::poll(&descriptor, 1, RemainingMilliseconds(TestDeadline())) <= 0) {
        error = "read timed out draining response";
        break;
      }
      std::array<char, 64 * 1024> bytes{};
      const ssize_t count = ::read(client.Fd(), bytes.data(), bytes.size());
      if (count > 0) {
        received.append(bytes.data(), static_cast<std::size_t>(count));
        continue;
      }
      if (count < 0 && errno == EINTR) continue;
      error = "unexpected EOF while draining response";
      break;
    }
    // R-046 regression: after the full response, nothing more may arrive.
    // A pause/resume cycle that re-delivered a retained chunk would inflate
    // the total beyond kExpected.
    if (error.empty()) {
      pollfd descriptor{client.Fd(), POLLIN, 0};
      while (::poll(&descriptor, 1, 200) > 0) {
        std::array<char, 4096> bytes{};
        const ssize_t count = ::read(client.Fd(), bytes.data(), bytes.size());
        if (count > 0) {
          received.append(bytes.data(), static_cast<std::size_t>(count));
          continue;
        }
        if (count < 0 && errno == EINTR) continue;
        break;
      }
    }
    if (::write(wake, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  backend.join();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  EXPECT_EQ(received.size(), 43 + kBodySize);
  EXPECT_EQ(received.substr(0, 43),
            "HTTP/1.1 200 OK\r\nContent-Length: 131072\r\n\r\n");
  EXPECT_EQ(received.substr(43), std::string(kBodySize, 'x'));
  EXPECT_EQ(::close(signal[0]), 0);
  EXPECT_EQ(::close(signal[1]), 0);
}

TEST(EndToEndTest, CommittedBodyFailureDoesNotRetry) {
  net::Socket first_listener = net::Socket::ListenLoopback();
  net::Socket second_listener = net::Socket::ListenLoopback();
  constexpr std::string_view request =
      "GET /fail HTTP/1.1\r\nhost: fail.e2e.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
  std::string first_error;
  std::thread first_backend([&] {
    const int fd = AcceptUntil(first_listener, TestDeadline(), first_error);
    if (fd < 0) return;
    (void)ReadExactUntil(fd, request.size(), TestDeadline(), first_error);
    constexpr std::string_view partial = "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\npart";
    if (first_error.empty() &&
        ::write(fd, partial.data(), partial.size()) != static_cast<ssize_t>(partial.size())) {
      first_error = "failed to write partial response";
    }
    (void)::close(fd);
  });

  config::Route route{"fail", "fail.e2e.test", "/",
                      {Endpoint(first_listener.BoundPort()), Endpoint(second_listener.BoundPort())},
                      100, 100, 8};
  route.retry_budget = 1;
  std::string error;
  std::string received;
  RunGateway(config::Config{{route}}, [&](gateway::Gateway &, std::uint16_t port, int wake) {
    net::Socket client = net::Socket::ConnectLoopback(port);
    constexpr std::string_view inbound = "GET /fail HTTP/1.1\r\nHost: fail.e2e.test\r\n\r\n";
    constexpr std::string_view committed = "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\npart";
    if (WriteAll(client.Fd(), inbound, TestDeadline(), error)) {
      received = ReadExact(client.Fd(), committed.size(), TestDeadline(), error);
    }
    if (error.empty()) {
      pollfd descriptor{client.Fd(), POLLIN | POLLHUP, 0};
      if (::poll(&descriptor, 1, RemainingMilliseconds(TestDeadline())) <= 0) {
        error = "truncated connection did not reach EOF";
      } else {
        std::array<char, 8> extra{};
        if (::read(client.Fd(), extra.data(), extra.size()) != 0) {
          error = "expected EOF after truncated response";
        }
      }
    }
    if (::write(wake, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  first_backend.join();
  pollfd descriptor{second_listener.Fd(), POLLIN, 0};
  EXPECT_EQ(::poll(&descriptor, 1, 100), 0) << "committed body failure retried a second endpoint";
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_TRUE(first_error.empty()) << first_error;
  EXPECT_EQ(received, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\npart");
}

TEST(EndToEndTest, ClientDisconnectStopsUpstreamRead) {
  net::Socket listener = net::Socket::ListenLoopback();
  constexpr std::size_t kBodySize = 512 * 1024;
  std::array<int, 2> signal{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, signal.data()), 0);
  std::string backend_error;
  std::thread backend([&] {
    const int fd = AcceptUntil(listener, TestDeadline(), backend_error);
    if (fd < 0) return;
    constexpr std::string_view request =
        "GET /disconnect HTTP/1.1\r\nhost: disconnect.e2e.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    (void)ReadExactUntil(fd, request.size(), TestDeadline(), backend_error);
    const std::string header = "HTTP/1.1 200 OK\r\nContent-Length: " +
                               std::to_string(kBodySize) + "\r\n\r\n";
    if (backend_error.empty() &&
        ::write(fd, header.data(), header.size()) != static_cast<ssize_t>(header.size())) {
      backend_error = "failed to write header";
    }
    const std::string chunk(64 * 1024, 'd');
    std::size_t written = 0;
    while (backend_error.empty() && written < kBodySize) {
      const std::size_t want = std::min(chunk.size(), kBodySize - written);
      // MSG_NOSIGNAL: the gateway cancels this upstream while the blocking
      // write may still be in flight; EPIPE must not kill the test process
      // (d2b80d2 fixed the same pattern in ProxyTransactionTest).
      const ssize_t count = ::send(fd, chunk.data(), want, MSG_NOSIGNAL);
      if (count > 0) {
        written += static_cast<std::size_t>(count);
        continue;
      }
      if (count < 0 && errno == EINTR) continue;
      break;  // EAGAIN (gateway paused) or EPIPE (gateway cancelled)
    }
    // The gateway must cancel the exchange once the client is gone.
    pollfd descriptor{fd, POLLHUP | POLLIN, 0};
    if (::poll(&descriptor, 1, RemainingMilliseconds(TestDeadline())) <= 0) {
      if (backend_error.empty()) backend_error = "gateway did not close cancelled upstream";
    } else {
      char byte = '\0';
      const ssize_t count = ::recv(fd, &byte, 1, 0);
      const bool closed = count == 0 || (count < 0 && (errno == ECONNRESET || errno == EPIPE));
      if (!closed && backend_error.empty()) backend_error = "expected EOF on cancelled upstream";
    }
    (void)::close(fd);
    (void)::write(signal[1], "c", 1);
  });

  config::Route route{"disconnect", "disconnect.e2e.test", "/",
                      {Endpoint(listener.BoundPort())}, 100, 100, 8};
  route.total_timeout_ms = 5000;
  std::string error;
  RunGateway(config::Config{{route}}, [&](gateway::Gateway &, std::uint16_t port, int wake) {
    net::Socket client = net::Socket::ConnectLoopback(port);
    constexpr std::string_view inbound = "GET /disconnect HTTP/1.1\r\nHost: disconnect.e2e.test\r\n\r\n";
    if (!WriteAll(client.Fd(), inbound, TestDeadline(), error)) {
      (void)::write(wake, "q", 1);
      return;
    }
    // Wait for the response head to reach the kernel (the stream is
    // committed), then reset the connection WITHOUT reading: the gateway's
    // downstream write stays pending, so the reset is observable there and
    // must cancel the upstream exchange.
    pollfd descriptor{client.Fd(), POLLIN | POLLHUP, 0};
    if (::poll(&descriptor, 1, RemainingMilliseconds(TestDeadline())) <= 0) {
      if (error.empty()) error = "no response bytes arrived";
    }
    struct linger reset = {1, 0};
    (void)::setsockopt(client.Fd(), SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
    (void)::close(client.Fd());
    // The backend must observe the gateway closing the upstream exchange.
    pollfd signal_descriptor{signal[0], POLLIN, 0};
    if (::poll(&signal_descriptor, 1, RemainingMilliseconds(TestDeadline())) <= 0) {
      if (error.empty()) error = "upstream was not cancelled after client reset";
    } else {
      char byte = '\0';
      EXPECT_EQ(::read(signal[0], &byte, 1), 1);
    }
    if (::write(wake, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  backend.join();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  EXPECT_EQ(::close(signal[0]), 0);
  EXPECT_EQ(::close(signal[1]), 0);
}

TEST(EndToEndTest, UpstreamReuseIsIndependentOfDownstreamDrain) {
  net::Socket listener = net::Socket::ListenLoopback();
  constexpr std::string_view request =
      "GET /reuse HTTP/1.1\r\nhost: reuse.e2e.test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
  std::string backend_error;
  std::thread backend([&] {
    const int fd = AcceptUntil(listener, TestDeadline(), backend_error);
    if (fd < 0) return;
    (void)ReadExactUntil(fd, request.size(), TestDeadline(), backend_error);
    constexpr std::string_view first_response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (backend_error.empty() &&
        ::write(fd, first_response.data(), first_response.size()) !=
            static_cast<ssize_t>(first_response.size())) {
      backend_error = "failed to write first response";
    }
    // The same upstream connection serves the second request while the first
    // client has not yet read its response bytes off the wire.
    (void)ReadExactUntil(fd, request.size(), TestDeadline(), backend_error);
    constexpr std::string_view second_response = "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone";
    if (backend_error.empty() &&
        ::write(fd, second_response.data(), second_response.size()) !=
            static_cast<ssize_t>(second_response.size())) {
      backend_error = "failed to write second response";
    }
    (void)::close(fd);
  });

  config::Route route{"reuse", "reuse.e2e.test", "/", {Endpoint(listener.BoundPort())}, 100, 100, 8};
  std::string error;
  std::string first_wire;
  std::string second_wire;
  RunGateway(config::Config{{route}}, [&](gateway::Gateway &, std::uint16_t port, int wake) {
    net::Socket first_client = net::Socket::ConnectLoopback(port);
    constexpr std::string_view inbound = "GET /reuse HTTP/1.1\r\nHost: reuse.e2e.test\r\n\r\n";
    if (!WriteAll(first_client.Fd(), inbound, TestDeadline(), error)) {
      (void)::write(wake, "q", 1);
      return;
    }
    // The first response arriving in the kernel means the gateway has read
    // the whole upstream body and returned the connection to the idle pool.
    // Do NOT read the bytes off the wire yet: the second request must reuse
    // the upstream descriptor while the first client's wire is still pending.
    pollfd first_check{first_client.Fd(), POLLIN | POLLHUP, 0};
    if (::poll(&first_check, 1, RemainingMilliseconds(TestDeadline())) <= 0) {
      if (error.empty()) error = "first response never reached the gateway output";
    }
    net::Socket second_client = net::Socket::ConnectLoopback(port);
    constexpr std::string_view second_expected = "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone";
    if (error.empty() && WriteAll(second_client.Fd(), inbound, TestDeadline(), error)) {
      second_wire = ReadExact(second_client.Fd(), second_expected.size(), TestDeadline(), error);
    }
    constexpr std::string_view first_expected = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    if (error.empty()) {
      first_wire = ReadExact(first_client.Fd(), first_expected.size(), TestDeadline(), error);
    }
    if (::write(wake, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  backend.join();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  EXPECT_EQ(first_wire, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
  EXPECT_EQ(second_wire, "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone");
  // The backend accepted exactly one connection: the idle upstream descriptor
  // was reused while the first client's response was still undelivered.
  pollfd descriptor{listener.Fd(), POLLIN, 0};
  EXPECT_EQ(::poll(&descriptor, 1, 100), 0) << "second request opened a new upstream connection";
}

} // namespace
} // namespace aegisgate::integration
