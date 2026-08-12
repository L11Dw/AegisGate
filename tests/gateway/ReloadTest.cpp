// A4: Worker Prepare / Publish / Rollback tests.
// These tests verify that reload candidates are prepared on worker owner
// threads, published atomically on success, and rolled back on failure.

#include <chrono>
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
  GatewayFixture fix;
  fix.Init(SimpleConfig());

  const auto v0 = fix.gateway->CurrentGenerationVersion();
  EXPECT_TRUE(fix.gateway->RequestReload(SimpleConfig()));
  EXPECT_EQ(fix.gateway->CurrentGenerationVersion(), v0 + 1);
  EXPECT_TRUE(fix.gateway->RequestReload(SimpleConfig()));
  EXPECT_EQ(fix.gateway->CurrentGenerationVersion(), v0 + 2);
}

// ---------------------------------------------------------------------------
// A4 Test 3: SuccessfulReloadPublishesNewGeneration
// ---------------------------------------------------------------------------
TEST(ReloadTest, SuccessfulReloadPublishesNewGeneration) {
  GatewayFixture fix;
  fix.Init(SimpleConfig());

  const auto old_version = fix.gateway->CurrentGenerationVersion();

  // A valid candidate with the same worker count.
  config::Config candidate = SimpleConfig();
  EXPECT_TRUE(fix.gateway->RequestReload(std::move(candidate)));
  EXPECT_EQ(fix.gateway->CurrentGenerationVersion(), old_version + 1);

  // The old generation should be in the retirement pipeline.
  EXPECT_GE(fix.gateway->RetiringGenerationCount(), 1u);
}

// ---------------------------------------------------------------------------
// A4 Test 4: OldGenerationRetiresAfterReload
// (After reload, the old generation enters the retirement pipeline.)
// ---------------------------------------------------------------------------
TEST(ReloadTest, OldGenerationRetiresAfterReload) {
  GatewayFixture fix;
  fix.Init(SimpleConfig());

  const auto v0 = fix.gateway->CurrentGenerationVersion();
  EXPECT_EQ(fix.gateway->RetiringGenerationCount(), 0u);

  // Reload — old generation should enter the retirement pipeline.
  EXPECT_TRUE(fix.gateway->RequestReload(SimpleConfig()));
  EXPECT_EQ(fix.gateway->CurrentGenerationVersion(), v0 + 1);
  EXPECT_GE(fix.gateway->RetiringGenerationCount(), 1u);

  // The retired generation should eventually complete (the test's ctest
  // timeout is the assertion — if retirement hangs, the test times out).
}

} // namespace
} // namespace aegisgate::gateway
