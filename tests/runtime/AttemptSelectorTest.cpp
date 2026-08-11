#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "aegisgate/health/Coordinator.h"
#include "aegisgate/runtime/AttemptSelector.h"
#include "aegisgate/runtime/ConfigSnapshot.h"
#include "aegisgate/runtime/SelectionState.h"
#include "aegisgate/runtime/WorkerShared.h"

namespace aegisgate::runtime {
namespace {

config::Route RouteWithEndpoint(const char *host, std::uint16_t port) {
  return config::Route{"r", "r.test", "/", {config::Endpoint{host, {127, 0, 0, 1}, port, 1}}};
}

config::Route RouteWithTwoEndpoints() {
  return config::Route{"r", "r.test", "/",
                       {config::Endpoint{"a.host", {127, 0, 0, 1}, 9001, 1},
                        config::Endpoint{"b.host", {127, 0, 0, 1}, 9002, 1}}};
}

std::shared_ptr<WorkerShared> MakeShared(std::shared_ptr<health::Coordinator> coordinator,
                                         ConfigSnapshotRef snapshot) {
  auto shared = std::make_shared<WorkerShared>();
  shared->config_snapshot.store(std::move(snapshot), std::memory_order_release);
  shared->coordinator = std::move(coordinator);
  shared->lifetime_token = std::make_shared<int>(0);
  return shared;
}

// R-054: the selector binds the request's snapshot at construction, so a
// reload of the global snapshot mid-request must not change what a retry
// selects — the initial attempt and every retry use the same configuration.
TEST(AttemptSelectorTest, RetryUsesRequestSnapshotAfterReload) {
  // V1 has two endpoints (the retry needs a second candidate once the first is
  // tried); V2 has a single different endpoint.
  auto config_v1 = std::make_shared<config::Config>();
  config_v1->routes = {RouteWithTwoEndpoints()};
  const auto v1 = std::make_shared<const ConfigSnapshot>(ConfigSnapshot{1, *config_v1});

  // The coordinator is never started: its constructor already publishes the
  // initial all-eligible snapshot matching config V1.
  auto coordinator = std::make_shared<health::Coordinator>(config_v1,
                                                           health::Coordinator::Clock::now());
  auto shared = MakeShared(coordinator, v1);
  SelectionState selection(v1->config, v1->version);
  AttemptSelector selector(selection, shared, /*route_index=*/0, v1);

  const auto initial = selector.Select(/*least_active=*/false);
  ASSERT_TRUE(initial.has_value());
  EXPECT_EQ(initial->endpoint.port, 9001);  // first weighted pick

  // Publish a new global snapshot with a single different endpoint.
  auto config_v2 = std::make_shared<config::Config>();
  config_v2->routes = {RouteWithEndpoint("c.host", 9003)};
  const auto v2 = std::make_shared<const ConfigSnapshot>(ConfigSnapshot{2, *config_v2});
  shared->config_snapshot.store(v2, std::memory_order_release);

  // The retry still selects from the request-bound V1 snapshot: the second V1
  // candidate, never the reloaded single endpoint.
  const auto retry = selector.Select(false);
  ASSERT_TRUE(retry.has_value());
  EXPECT_EQ(retry->endpoint.port, 9002);
  EXPECT_EQ(retry->snapshot->version, v1->version);
  EXPECT_NE(retry->endpoint.host, "c.host");
}

// R-054: a selection is a value-copied endpoint plus the request snapshot; no
// pointer into the snapshot's internal storage is ever handed out.
TEST(AttemptSelectorTest, SelectionCarriesSnapshotRefAndValueEndpoint) {
  auto config = std::make_shared<config::Config>();
  config->routes = {RouteWithEndpoint("a.host", 9001)};
  const auto snapshot = std::make_shared<const ConfigSnapshot>(ConfigSnapshot{7, *config});
  auto coordinator =
      std::make_shared<health::Coordinator>(config, health::Coordinator::Clock::now());
  auto shared = MakeShared(coordinator, snapshot);
  SelectionState selection(snapshot->config, snapshot->version);
  AttemptSelector selector(selection, shared, 0, snapshot);

  const auto selection_result = selector.Select(false);
  ASSERT_TRUE(selection_result.has_value());
  EXPECT_EQ(selection_result->snapshot->version, 7U);
  EXPECT_EQ(selection_result->endpoint.host, "a.host");
}

} // namespace
} // namespace aegisgate::runtime
