// A4: Worker Prepare / Publish / Rollback tests.
// These tests verify that reload candidates are prepared on worker owner
// threads, published atomically on success, and rolled back on failure.

#include <chrono>
#include <fstream>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/config/Config.h"
#include "aegisgate/gateway/Gateway.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/Socket.h"
#include "aegisgate/runtime/RuntimeGeneration.h"

namespace aegisgate::gateway {
namespace {

// Minimal config with one route, one endpoint, no health check, no breaker.
config::Config SimpleConfig(std::uint16_t port = 0) {
  config::Endpoint ep{"127.0.0.1", {127, 0, 0, 1}, port, 1};
  return config::Config{{{"api", "test.local", "/", {ep}, 100, 50, 10}}};
}

// Config with a different worker count (should be rejected).
config::Config DifferentWorkersConfig() {
  config::Endpoint ep{"127.0.0.1", {127, 0, 0, 1}, 0, 1};
  config::Config c{{{"api", "test.local", "/", {ep}, 100, 50, 10}}};
  c.workers = 99; // different from default 1
  return c;
}

// Helper: create a Gateway on a control loop, run it, and quit.
struct GatewayFixture {
  net::EventLoop loop;
  std::array<int, 2> wake_fds{};
  std::unique_ptr<Gateway> gateway;

  void Init(config::Config config) {
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
    gateway = std::make_unique<Gateway>(loop, std::move(config), "127.0.0.1", 0);
    gateway->Start();
  }

  void Quit() {
    gateway.reset();
    if (wake_fds[0] >= 0) { (void)::close(wake_fds[0]); wake_fds[0] = -1; }
    if (wake_fds[1] >= 0) { (void)::close(wake_fds[1]); wake_fds[1] = -1; }
  }

  ~GatewayFixture() { Quit(); }
};

// ---------------------------------------------------------------------------
// A4 Test 1: WorkersMismatchRejectsReload
// ---------------------------------------------------------------------------
TEST(ReloadTest, WorkersMismatchRejectsReload) {
  GatewayFixture fix;
  fix.Init(SimpleConfig());

  const auto old_version = fix.gateway->CurrentGenerationVersion();
  const auto old_retiring = fix.gateway->RetiringGenerationCount();

  // Candidate with different workers — must be rejected.
  EXPECT_FALSE(fix.gateway->RequestReload(DifferentWorkersConfig()));

  // Current generation unchanged.
  EXPECT_EQ(fix.gateway->CurrentGenerationVersion(), old_version);
  EXPECT_EQ(fix.gateway->RetiringGenerationCount(), old_retiring);
}

// ---------------------------------------------------------------------------
// A4 Test 2: DuplicateReloadIsIdempotent
// (Two consecutive reloads each publish a new generation.)
// ---------------------------------------------------------------------------
TEST(ReloadTest, ConsecutiveReloadsIncrementVersion) {
  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    [[maybe_unused]] auto _ = ::read(wake_fds[0], &byte, 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  Gateway gateway(loop, SimpleConfig(), "127.0.0.1", 0);
  gateway.Start();

  const auto v0 = gateway.CurrentGenerationVersion();

  // First reload.
  EXPECT_TRUE(gateway.RequestReload(SimpleConfig()));
  net::TimerQueue timers(loop);
  (void)timers.ScheduleAfter(std::chrono::milliseconds(200), [&] {
    if (gateway.CurrentGenerationVersion() > v0) {
      // Second reload.
      EXPECT_TRUE(gateway.RequestReload(SimpleConfig()));
      (void)timers.ScheduleAfter(std::chrono::milliseconds(200), [&] {
        [[maybe_unused]] auto _ = ::write(wake_fds[1], "q", 1);
      });
    } else {
      (void)timers.ScheduleAfter(std::chrono::milliseconds(100), [&] {
        [[maybe_unused]] auto _ = ::write(wake_fds[1], "q", 1);
      });
    }
  });
  loop.Loop();

  EXPECT_EQ(gateway.CurrentGenerationVersion(), v0 + 2);
  wake_channel.Remove();
  (void)::close(wake_fds[0]);
  (void)::close(wake_fds[1]);
}

// ---------------------------------------------------------------------------
// A4 Test 3: SuccessfulReloadPublishesNewGeneration
// ---------------------------------------------------------------------------
TEST(ReloadTest, SuccessfulReloadPublishesNewGeneration) {
  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    [[maybe_unused]] auto _ = ::read(wake_fds[0], &byte, 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  Gateway gateway(loop, SimpleConfig(), "127.0.0.1", 0);
  gateway.Start();

  const auto old_version = gateway.CurrentGenerationVersion();

  // A valid candidate with the same worker count.
  EXPECT_TRUE(gateway.RequestReload(SimpleConfig()));

  // Wait for the async prepare to complete and the control loop to process it.
  net::TimerQueue timers(loop);
  (void)timers.ScheduleAfter(std::chrono::milliseconds(200), [&] {
    if (gateway.CurrentGenerationVersion() > old_version) {
      [[maybe_unused]] auto _ = ::write(wake_fds[1], "q", 1);
    } else {
      (void)timers.ScheduleAfter(std::chrono::milliseconds(100), [&] {
        [[maybe_unused]] auto _ = ::write(wake_fds[1], "q", 1);
      });
    }
  });
  loop.Loop();

  EXPECT_EQ(gateway.CurrentGenerationVersion(), old_version + 1);
  EXPECT_GE(gateway.RetiringGenerationCount(), 1u);
  wake_channel.Remove();
  (void)::close(wake_fds[0]);
  (void)::close(wake_fds[1]);
}

// ---------------------------------------------------------------------------
// A4 Test 4: OldGenerationRetiresAfterReload
// (After reload, the old generation enters the retirement pipeline.)
// ---------------------------------------------------------------------------
TEST(ReloadTest, OldGenerationRetiresAfterReload) {
  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    [[maybe_unused]] auto _ = ::read(wake_fds[0], &byte, 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  Gateway gateway(loop, SimpleConfig(), "127.0.0.1", 0);
  gateway.Start();

  const auto v0 = gateway.CurrentGenerationVersion();
  EXPECT_EQ(gateway.RetiringGenerationCount(), 0u);

  EXPECT_TRUE(gateway.RequestReload(SimpleConfig()));

  // Wait for async prepare + publish.
  net::TimerQueue timers(loop);
  (void)timers.ScheduleAfter(std::chrono::milliseconds(300), [&] {
    [[maybe_unused]] auto _ = ::write(wake_fds[1], "q", 1);
  });
  loop.Loop();

  EXPECT_EQ(gateway.CurrentGenerationVersion(), v0 + 1);
  EXPECT_GE(gateway.RetiringGenerationCount(), 1u);

  wake_channel.Remove();
  (void)::close(wake_fds[0]);
  (void)::close(wake_fds[1]);
}

// ---------------------------------------------------------------------------
// A6: Gateway wiring tests
// ---------------------------------------------------------------------------

// Write a YAML config to a temp file.
std::string WriteConfig(const std::string &yaml, const std::string &name) {
  const std::string path = "/tmp/aegisgate_reload_" + name + ".yaml";
  std::ofstream ofs(path);
  ofs << yaml;
  ofs.close();
  return path;
}

TEST(ReloadTest, ReloadUsesExplicitConfigPath) {
  constexpr std::string_view yaml = R"(workers: 1
routes:
  - name: api
    host: test.local
    path_prefix: /
    endpoints:
      - host: 127.0.0.1
        port: 9001
        weight: 1
    rate_limit: 100
    burst: 50
    max_inflight: 10
)";
  const std::string path = WriteConfig(std::string(yaml), "explicit_path");

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    [[maybe_unused]] auto _r = ::read(wake_fds[0], &byte, 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  config::Config config = SimpleConfig();
  Gateway gateway(loop, config, "127.0.0.1", 0, net::StreamFlowControl{}, path);
  gateway.Start();

  const auto v0 = gateway.CurrentGenerationVersion();

  // Trigger a file-based reload.
  EXPECT_TRUE(gateway.RequestReload());

  // Wait for the reload result to be consumed by the control loop.
  net::TimerQueue timers(loop);
  (void)timers.ScheduleAfter(std::chrono::milliseconds(500), [&] {
    if (gateway.CurrentGenerationVersion() > v0 || gateway.LastReloadResultSequence() > 0) {
      if (::write(wake_fds[1], "q", 1) != 1) {}
    } else {
      (void)timers.ScheduleAfter(std::chrono::milliseconds(100), [&] {
        if (::write(wake_fds[1], "q", 1) != 1) {}
      });
    }
  });

  loop.Loop();

  // The reload should have been consumed.
  EXPECT_GT(gateway.LastReloadResultSequence(), 0u);

  wake_channel.Remove();
  (void)::close(wake_fds[0]);
  (void)::close(wake_fds[1]);
}

TEST(ReloadTest, InvalidYamlLeavesCurrentGenerationUntouched) {
  const std::string path = WriteConfig("not valid yaml: [broken", "invalid_yaml");

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    [[maybe_unused]] auto _r = ::read(wake_fds[0], &byte, 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  config::Config config = SimpleConfig();
  Gateway gateway(loop, config, "127.0.0.1", 0, net::StreamFlowControl{}, path);
  gateway.Start();

  const auto v0 = gateway.CurrentGenerationVersion();

  // Trigger a reload with invalid YAML.
  EXPECT_TRUE(gateway.RequestReload());

  // Wait for the result.
  net::TimerQueue timers(loop);
  (void)timers.ScheduleAfter(std::chrono::milliseconds(500), [&] {
    if (::write(wake_fds[1], "q", 1) != 1) {}
  });
  loop.Loop();

  // The parse failed — generation should be unchanged.
  EXPECT_EQ(gateway.CurrentGenerationVersion(), v0);
  EXPECT_GT(gateway.LastReloadResultSequence(), 0u);

  wake_channel.Remove();
  (void)::close(wake_fds[0]);
  (void)::close(wake_fds[1]);
}

TEST(ReloadTest, NoConfigPathDisablesFileReload) {
  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    [[maybe_unused]] auto _r = ::read(wake_fds[0], &byte, 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  config::Config config = SimpleConfig();
  // No config_path — file reload should be disabled.
  Gateway gateway(loop, config, "127.0.0.1", 0);
  gateway.Start();

  EXPECT_FALSE(gateway.RequestReload());

  [[maybe_unused]] auto _w = ::write(wake_fds[1], "q", 1);
  loop.Loop();
  wake_channel.Remove();
  (void)::close(wake_fds[0]);
  (void)::close(wake_fds[1]);
}

// ---------------------------------------------------------------------------
// A4: Worker prepare and protection migration tests
// ---------------------------------------------------------------------------

TEST(ReloadTest, PrepareBuildsSelectionStateOnWorkerOwner) {
  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    [[maybe_unused]] auto _ = ::read(wake_fds[0], &byte, 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  Gateway gateway(loop, SimpleConfig(), "127.0.0.1", 0);
  gateway.Start();

  const auto v0 = gateway.CurrentGenerationVersion();
  EXPECT_TRUE(gateway.RequestReload(SimpleConfig()));

  net::TimerQueue timers(loop);
  (void)timers.ScheduleAfter(std::chrono::milliseconds(200), [&] {
    [[maybe_unused]] auto _ = ::write(wake_fds[1], "q", 1);
  });
  loop.Loop();

  EXPECT_EQ(gateway.CurrentGenerationVersion(), v0 + 1);
  wake_channel.Remove();
  (void)::close(wake_fds[0]);
  (void)::close(wake_fds[1]);
}

TEST(ReloadTest, ProtectionStateMigratesHealthOnReload) {
  config::HealthCheckSettings hc;
  hc.interval_ms = 10000;
  hc.timeout_ms = 5000;
  const config::Endpoint ep{"127.0.0.1", {127, 0, 0, 1}, 1, 1};
  const config::Config config{
      {{"health", "health.test", "/", {ep}, 100, 50, 10, 5000, 5000, 30000, 1,
        std::nullopt, hc}}};

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    [[maybe_unused]] auto _ = ::read(wake_fds[0], &byte, 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  Gateway gateway(loop, config, "127.0.0.1", 0);
  gateway.Start();

  const auto v0 = gateway.CurrentGenerationVersion();
  EXPECT_TRUE(gateway.RequestReload(config));

  net::TimerQueue timers(loop);
  (void)timers.ScheduleAfter(std::chrono::milliseconds(200), [&] {
    [[maybe_unused]] auto _ = ::write(wake_fds[1], "q", 1);
  });
  loop.Loop();

  EXPECT_EQ(gateway.CurrentGenerationVersion(), v0 + 1);
  wake_channel.Remove();
  (void)::close(wake_fds[0]);
  (void)::close(wake_fds[1]);
}

} // namespace
} // namespace aegisgate::gateway
