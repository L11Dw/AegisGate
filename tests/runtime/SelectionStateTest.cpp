#include <chrono>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "aegisgate/health/CoordinatorState.h"
#include "aegisgate/runtime/SelectionState.h"

using namespace std::chrono_literals;

namespace aegisgate::runtime {
namespace {

using Clock = health::CoordinatorState::Clock;
using State = resilience::CircuitBreaker::State;

config::Endpoint Loopback(std::uint16_t port, std::uint32_t weight = 1) {
  return {"127.0.0.1", {127, 0, 0, 1}, port, weight};
}

// One route with the given endpoints, optionally with health/breaker.
struct Fixture {
  std::shared_ptr<config::Config> config;
  std::size_t route_index = 0;
};

Fixture MakeFixture(std::vector<config::Endpoint> endpoints, bool protected_route = false) {
  config::Route route{"least", "la.test", "/", std::move(endpoints), 10, 10, 4};
  route.balance = config::BalancePolicy::kLeastActive;
  if (protected_route) {
    route.health_check = config::HealthCheckSettings{1000, 200};
    route.circuit_breaker = config::CircuitBreakerSettings{10, 5, 500, 5, 1};
  }
  auto config = std::make_shared<config::Config>();
  config->routes.push_back(std::move(route));
  return Fixture{std::move(config), 0};
}

// Eligibility predicate backed by a live CoordinatorState snapshot, mirroring
// the worker data plane (healthy + breaker state + probe availability).
class Eligibility {
public:
  explicit Eligibility(health::CoordinatorState &state, std::size_t route_index)
      : state_(&state), route_index_(route_index) {}

  std::function<bool(std::size_t)> Predicate() {
    const auto snapshot = state_->BuildSnapshot();
    return [this, snapshot](std::size_t endpoint_index) {
      const auto &decision = snapshot->endpoints[route_index_][endpoint_index];
      if (!decision.healthy) return false;
      if (decision.breaker_state == static_cast<std::uint8_t>(State::kOpen)) return false;
      if (decision.breaker_state == static_cast<std::uint8_t>(State::kHalfOpen)) {
        return state_->ProbeAvailable(route_index_, endpoint_index);
      }
      return true;
    };
  }

private:
  health::CoordinatorState *state_;
  std::size_t route_index_;
};

TEST(SelectionStateTest, PicksFewerActiveEndpoint) {
  // The busy endpoint owns the current rotation position and the higher
  // weight; the less busy one must still win.
  Fixture fixture = MakeFixture({Loopback(9001, 10), Loopback(9002, 1)});
  SelectionState selection(*fixture.config);
  auto eligible = [](std::size_t) { return true; };
  auto held = selection.AcquireActive(0, 0);
  EXPECT_EQ(selection.NextLeastActiveIndex(0, {}, eligible), 1U);
  held.Release();
}

TEST(SelectionStateTest, AlternatesEqualWeightTies) {
  Fixture fixture = MakeFixture({Loopback(9001), Loopback(9002)});
  SelectionState selection(*fixture.config);
  auto eligible = [](std::size_t) { return true; };
  std::size_t first = 0;
  std::size_t second = 0;
  for (int index = 0; index != 9; ++index) {
    const auto picked = selection.NextLeastActiveIndex(0, {}, eligible);
    ASSERT_TRUE(picked.has_value());
    if (*picked == 0) {
      ++first;
    } else {
      ++second;
    }
  }
  EXPECT_EQ(first, 5U);
  EXPECT_EQ(second, 4U);
}

TEST(SelectionStateTest, WeightsResolveTiesProportionally) {
  Fixture fixture = MakeFixture({Loopback(9001, 2), Loopback(9002, 1)});
  SelectionState selection(*fixture.config);
  auto eligible = [](std::size_t) { return true; };
  std::size_t first = 0;
  std::size_t second = 0;
  for (int index = 0; index != 9; ++index) {
    const auto picked = selection.NextLeastActiveIndex(0, {}, eligible);
    ASSERT_TRUE(picked.has_value());
    if (*picked == 0) {
      ++first;
    } else {
      ++second;
    }
  }
  EXPECT_EQ(first, 6U);
  EXPECT_EQ(second, 3U);
}

TEST(SelectionStateTest, SkipsTriedCandidates) {
  Fixture fixture = MakeFixture({Loopback(9001), Loopback(9002)});
  SelectionState selection(*fixture.config);
  auto eligible = [](std::size_t) { return true; };
  EXPECT_EQ(selection.NextLeastActiveIndex(0, std::set<std::size_t>{0}, eligible), 1U);
  EXPECT_EQ(selection.NextLeastActiveIndex(0, std::set<std::size_t>{0, 1}, eligible),
            std::nullopt);
}

TEST(SelectionStateTest, SkipsUnhealthyAndOpenCandidates) {
  Fixture fixture = MakeFixture({Loopback(9001), Loopback(9002)}, /*protected_route=*/true);
  SelectionState selection(*fixture.config);
  const auto now = Clock::now();
  health::CoordinatorState state(fixture.config, now);

  // Unhealthy endpoint 0 must never be selected.
  state.RecordHealth(0, 0, false);
  Eligibility eligibility(state, 0);
  EXPECT_EQ(selection.NextLeastActiveIndex(0, {}, eligibility.Predicate()), 1U);

  // Open breaker on endpoint 1: endpoint 0 becomes the only candidate again.
  state.RecordHealth(0, 0, true);
  const auto generation = state.BuildSnapshot()->endpoints[0][0].generation;
  state.RecordResult({0, 1, {false, generation, 0}, false}, now + 1ms);
  state.RecordResult({0, 1, {false, generation, 0}, false}, now + 2ms);
  state.RecordResult({0, 1, {false, generation, 0}, false}, now + 3ms);
  state.RecordResult({0, 1, {false, generation, 0}, false}, now + 4ms);
  state.RecordResult({0, 1, {false, generation, 0}, false}, now + 5ms);
  EXPECT_EQ(state.BreakerState(0, 1), State::kOpen);
  EXPECT_EQ(selection.NextLeastActiveIndex(0, {}, eligibility.Predicate()), 0U);

  // Both unavailable: no candidate at all.
  state.RecordHealth(0, 0, false);
  EXPECT_EQ(selection.NextLeastActiveIndex(0, {}, eligibility.Predicate()), std::nullopt);
}

TEST(SelectionStateTest, ActiveCountsDoNotCrossRoutes) {
  config::Route first_route{"a", "a.test", "/", {Loopback(9001)}, 10, 10, 4};
  first_route.balance = config::BalancePolicy::kLeastActive;
  config::Route second_route{"b", "b.test", "/", {Loopback(9001)}, 10, 10, 4};
  second_route.balance = config::BalancePolicy::kLeastActive;
  auto config = std::make_shared<config::Config>();
  config->routes.push_back(std::move(first_route));
  config->routes.push_back(std::move(second_route));
  SelectionState selection(*config);

  auto held = selection.AcquireActive(0, 0);
  EXPECT_EQ(selection.ActiveFor(0, 0), 1U);
  EXPECT_EQ(selection.ActiveFor(1, 0), 0U);
  held.Release();
  EXPECT_EQ(selection.ActiveFor(0, 0), 0U);
  EXPECT_EQ(selection.ActiveFor(1, 0), 0U);
}

// R-072: worker-local selection state is bound to the config snapshot version
// it was built for, so a reload rebuilds a fresh state per version instead of
// reusing cursors and route indices across versions.
TEST(SelectionStateTest, BindsSnapshotVersion) {
  Fixture fixture = MakeFixture({Loopback(9001, 1)});
  SelectionState selection(*fixture.config, /*version=*/42);
  EXPECT_EQ(selection.Version(), 42U);
  EXPECT_EQ(SelectionState(*fixture.config).Version(), 0U);
}

// R-041 regression: the weighted selector must be reachable through a stable
// index (never a pointer into the selector's internal copies).
TEST(SelectionStateTest, WeightedIndexFollowsSelectorOrder) {
  Fixture fixture = MakeFixture({Loopback(9001, 2), Loopback(9002, 1)});
  SelectionState selection(*fixture.config);
  std::vector<std::size_t> indices;
  for (int index = 0; index != 6; ++index) {
    const auto picked = selection.NextWeightedIndex(0);
    ASSERT_TRUE(picked.has_value());
    indices.push_back(*picked);
  }
  EXPECT_EQ(indices, (std::vector<std::size_t>{0, 0, 1, 0, 0, 1}));
}

} // namespace
} // namespace aegisgate::runtime
