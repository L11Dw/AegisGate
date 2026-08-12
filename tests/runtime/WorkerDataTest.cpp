#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/health/Coordinator.h"
#include "aegisgate/observability/Metrics.h"
#include "aegisgate/resilience/GlobalAdmission.h"
#include "aegisgate/runtime/ConfigSnapshot.h"
#include "aegisgate/runtime/WorkerData.h"
#include "aegisgate/net/Fd.h"
#include "aegisgate/runtime/WorkerRuntime.h"
#include "aegisgate/runtime/WorkerShared.h"

namespace aegisgate::runtime {
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

bool WriteAll(int fd, std::string_view bytes, Deadline deadline, std::string &error) {
  std::size_t sent = 0;
  while (sent < bytes.size()) {
    const ssize_t count = ::write(fd, bytes.data() + sent, bytes.size() - sent);
    if (count > 0) {
      sent += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) continue;
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) &&
        WaitFor(fd, POLLOUT, deadline)) {
      continue;
    }
    error = "write failed";
    return false;
  }
  return true;
}

std::string ReadExact(int fd, std::size_t size, Deadline deadline, std::string &error) {
  std::string result(size, '\0');
  std::size_t received = 0;
  while (received < size) {
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

// A rate-limited, breaker-less route whose endpoint is 127.0.0.1:1: admission
// draws a lease batch and the upstream connect fails, terminating with 502.
config::Route LeaseRoute() {
  return config::Route{"lease", "lease.test", "/",
                       {config::Endpoint{"127.0.0.1", {127, 0, 0, 1}, 1, 1}},
                       /*rate_limit=*/100, /*burst=*/1000, /*max_inflight=*/100};
}

struct Fixture {
  std::shared_ptr<config::Config> config;
  std::shared_ptr<health::Coordinator> coordinator;
  std::shared_ptr<WorkerShared> shared;
  std::shared_ptr<resilience::GlobalAdmission> admission;
  std::shared_ptr<std::atomic<std::uint64_t>> client_count;
  std::shared_ptr<observability::Metrics> metrics;
};

Fixture MakeFixture() {
  Fixture fixture;
  fixture.config = std::make_shared<config::Config>();
  fixture.config->routes = {LeaseRoute()};
  const auto snapshot = std::make_shared<const ConfigSnapshot>(ConfigSnapshot{1, *fixture.config});
  fixture.coordinator = std::make_shared<health::Coordinator>(
      fixture.config, health::Coordinator::Clock::now());
  fixture.shared = std::make_shared<WorkerShared>();
  fixture.shared->config_snapshot.store(snapshot, std::memory_order_release);
  fixture.shared->coordinator = fixture.coordinator;
  fixture.shared->worker_count = 1;
  fixture.shared->flow_control = net::StreamFlowControl{};
  fixture.shared->lifetime_token = std::make_shared<int>(0);
  fixture.shared->metrics_renderer = [] { return std::string{}; };
  const auto now = resilience::GlobalAdmission::Clock::now();
  fixture.admission =
      std::make_shared<resilience::GlobalAdmission>(fixture.config->routes[0], now);
  fixture.shared->admissions = {fixture.admission};
  fixture.client_count = std::make_shared<std::atomic<std::uint64_t>>(0);
  fixture.metrics = std::make_shared<observability::Metrics>();
  return fixture;
}

// R-055: a worker that stops returns its unspent lease balance to the global
// credit; repeated shutdown is idempotent, and the client count zeroes.
TEST(WorkerDataTest, LeaseReturnedOnWorkerStop) {
  Fixture fixture = MakeFixture();
  WorkerRuntime runtime;
  runtime.Start();
  std::shared_ptr<WorkerData> data;
  {
    std::promise<void> ready;
    auto future = ready.get_future();
    ASSERT_TRUE(runtime.PostWithLoop([&](net::EventLoop &loop) {
      data = std::make_shared<WorkerData>(loop, fixture.shared, /*worker_index=*/0,
                                          fixture.metrics, fixture.client_count);
      ready.set_value();
    }));
    future.get();
  }

  std::array<int, 2> fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds.data()), 0);
  ASSERT_TRUE(runtime.PostWithLoop([&](net::EventLoop &) { data->Accept(net::FdOwner(fds[1])); }));

  std::string error;
  constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: lease.test\r\n\r\n";
  ASSERT_TRUE(WriteAll(fds[0], request, TestDeadline(), error)) << error;
  constexpr std::string_view expected =
      "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n";
  EXPECT_EQ(ReadExact(fds[0], expected.size(), TestDeadline(), error), expected);
  EXPECT_TRUE(error.empty()) << error;
  (void)::close(fds[0]);

  constexpr std::int64_t kScale = 1'000'000'000;
  constexpr std::int64_t kBatch = 100;
  // One lease batch drawn (100 tokens), one spent by the request: 99 held.
  EXPECT_EQ(fixture.admission->credit(), (1000 - kBatch) * kScale);
  EXPECT_EQ(fixture.client_count->load(), 1U);

  // Shutdown returns the 99 unspent tokens; only the spent one stays gone.
  // The barrier makes the assertion observe the shutdown task having run.
  {
    std::promise<void> done;
    auto future = done.get_future();
    ASSERT_TRUE(runtime.PostWithLoop([&](net::EventLoop &) {
      data->Shutdown();
      done.set_value();
    }));
    future.get();
  }
  EXPECT_EQ(fixture.admission->credit(), (1000 - 1) * kScale);
  EXPECT_EQ(fixture.client_count->load(), 0U);

  // Idempotent repeated shutdown returns nothing extra.
  {
    std::promise<void> done;
    auto future = done.get_future();
    ASSERT_TRUE(runtime.PostWithLoop([&](net::EventLoop &) {
      data->Shutdown();
      done.set_value();
    }));
    future.get();
  }
  EXPECT_EQ(fixture.admission->credit(), (1000 - 1) * kScale);

  // The WorkerData is destroyed on its worker thread (gateway pattern).
  ASSERT_TRUE(runtime.PostWithLoop([&](net::EventLoop &) { data.reset(); }));
  runtime.Stop();
}

// R-055 exit-matrix row: GlobalAdmission is independent of the coordinator's
// lifecycle, so a worker still holding a lease after the coordinator stops
// returns it on shutdown.
TEST(WorkerDataTest, LeaseReturnedAfterCoordinatorStops) {
  Fixture fixture = MakeFixture();
  fixture.coordinator->Start();  // real coordinator loop, no admissions refill
  WorkerRuntime runtime;
  runtime.Start();
  std::shared_ptr<WorkerData> data;
  {
    std::promise<void> ready;
    auto future = ready.get_future();
    ASSERT_TRUE(runtime.PostWithLoop([&](net::EventLoop &loop) {
      data = std::make_shared<WorkerData>(loop, fixture.shared, 0, fixture.metrics,
                                          fixture.client_count);
      ready.set_value();
    }));
    future.get();
  }
  std::array<int, 2> fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds.data()), 0);
  ASSERT_TRUE(runtime.PostWithLoop([&](net::EventLoop &) { data->Accept(net::FdOwner(fds[1])); }));
  std::string error;
  constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: lease.test\r\n\r\n";
  ASSERT_TRUE(WriteAll(fds[0], request, TestDeadline(), error)) << error;
  constexpr std::string_view expected =
      "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n";
  EXPECT_EQ(ReadExact(fds[0], expected.size(), TestDeadline(), error), expected);
  EXPECT_TRUE(error.empty()) << error;
  (void)::close(fds[0]);

  constexpr std::int64_t kScale = 1'000'000'000;
  EXPECT_EQ(fixture.admission->credit(), (1000 - 100) * kScale);
  fixture.coordinator->Stop();
  // The worker still holds its balance; shutdown must still return it.
  {
    std::promise<void> done;
    auto future = done.get_future();
    ASSERT_TRUE(runtime.PostWithLoop([&](net::EventLoop &) {
      data->Shutdown();
      done.set_value();
    }));
    future.get();
  }
  EXPECT_EQ(fixture.admission->credit(), (1000 - 1) * kScale);
  ASSERT_TRUE(runtime.PostWithLoop([&](net::EventLoop &) { data.reset(); }));
  runtime.Stop();
}

} // namespace
} // namespace aegisgate::runtime
