#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "aegisgate/config/Config.h"
#include "aegisgate/health/Coordinator.h"
#include "aegisgate/health/CoordinatorState.h"

namespace aegisgate::health {
namespace {

using Clock = std::chrono::steady_clock;
using State = resilience::CircuitBreaker::State;
using namespace std::chrono_literals;

config::Route RouteWithBreaker(std::string name, std::uint32_t window_seconds,
                               std::uint32_t min_requests, std::uint32_t threshold_permille,
                               std::uint32_t open_seconds, std::uint32_t probes) {
  config::Route route{std::move(name), name + ".test", "/",
                      {config::Endpoint{name + ".host", {127, 0, 0, 1}, 9001, 1}}};
  // A breaker route's outcome channel capacity derives from max_inflight; the
  // coordinator requires a positive in-flight cap.
  route.max_inflight = 8;
  route.circuit_breaker =
      config::CircuitBreakerSettings{window_seconds, min_requests, threshold_permille,
                                     open_seconds, probes};
  return route;
}

config::Route PlainRoute(std::string name) {
  return config::Route{std::move(name), name + ".test", "/",
                       {config::Endpoint{name + ".host", {127, 0, 0, 1}, 9002, 1}}};
}

std::shared_ptr<config::Config> ConfigWith(std::vector<config::Route> routes) {
  auto config = std::make_shared<config::Config>();
  config->routes = std::move(routes);
  return config;
}

TEST(CoordinatorStateTest, HealthChangePublishesVersionedDecision) {
  CoordinatorState state(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 5, 1)}), Clock::now());
  auto first = state.BuildSnapshot();
  EXPECT_EQ(first->version, 1U);
  EXPECT_TRUE(first->endpoints[0][0].healthy);
  EXPECT_EQ(first->endpoints[0][0].breaker_state,
            static_cast<std::uint8_t>(State::kClosed));
  state.RecordHealth(0, 0, false);
  auto second = state.BuildSnapshot();
  EXPECT_EQ(second->version, 2U);
  EXPECT_FALSE(second->endpoints[0][0].healthy);
  state.RecordHealth(0, 0, true);
  EXPECT_TRUE(state.BuildSnapshot()->endpoints[0][0].healthy);
}

TEST(CoordinatorStateTest, FailureThresholdOpensBreakerAndBumpsGeneration) {
  const auto now = Clock::now();
  CoordinatorState state(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 5, 1)}), now);
  const auto first = state.BuildSnapshot();
  const std::uint64_t generation = first->endpoints[0][0].generation;
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 1ms);
  EXPECT_EQ(state.BreakerState(0, 0), State::kClosed);  // below min_requests
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 2ms);
  EXPECT_EQ(state.BreakerState(0, 0), State::kOpen);
  EXPECT_GT(state.Generation(0, 0), generation);
  EXPECT_EQ(state.BuildSnapshot()->endpoints[0][0].breaker_state,
            static_cast<std::uint8_t>(State::kOpen));
}

TEST(CoordinatorStateTest, StaleGenerationResultIsDropped) {
  const auto now = Clock::now();
  CoordinatorState state(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 5, 1)}), now);
  const std::uint64_t old_generation = state.BuildSnapshot()->endpoints[0][0].generation;
  state.RecordResult({0, 0, {false, old_generation, 0}, false}, now + 1ms);
  state.RecordResult({0, 0, {false, old_generation, 0}, false}, now + 2ms);
  EXPECT_EQ(state.BreakerState(0, 0), State::kOpen);
  // A success from the pre-open epoch must not close the open breaker.
  state.RecordResult({0, 0, {false, old_generation, 0}, true}, now + 3ms);
  EXPECT_EQ(state.BreakerState(0, 0), State::kOpen);
}

TEST(CoordinatorStateTest, BreakerStateIsRouteIsolated) {
  const auto now = Clock::now();
  CoordinatorState state(
      ConfigWith({RouteWithBreaker("a", 10, 2, 500, 5, 1), RouteWithBreaker("b", 10, 2, 500, 5, 1)}),
      now);
  const std::uint64_t generation = state.BuildSnapshot()->endpoints[0][0].generation;
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 1ms);
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 2ms);
  EXPECT_EQ(state.BreakerState(0, 0), State::kOpen);
  EXPECT_EQ(state.BreakerState(1, 0), State::kClosed);
}

TEST(CoordinatorStateTest, ArmHalfOpenIssuesQuotaSlotsWithClaimsBounded) {
  const auto now = Clock::now();
  CoordinatorState state(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 5, 2)}), now);
  const std::uint64_t generation = state.BuildSnapshot()->endpoints[0][0].generation;
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 1ms);
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 2ms);
  state.ArmHalfOpen(0, 0, now + 6s);
  auto armed = state.BuildSnapshot();
  EXPECT_EQ(armed->endpoints[0][0].breaker_state, static_cast<std::uint8_t>(State::kHalfOpen));
  EXPECT_EQ(armed->endpoints[0][0].probe_quota, 2U);
  EXPECT_NE(armed->endpoints[0][0].probe_base, 0U);
  // Two claims fit the quota; the third is refused.
  auto first = state.ClaimProbe(0, 0, *armed);
  auto second = state.ClaimProbe(0, 0, *armed);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_TRUE(first->probe);
  EXPECT_EQ(first->generation, armed->endpoints[0][0].generation);
  EXPECT_NE(first->probe_id, second->probe_id);
  EXPECT_FALSE(state.ClaimProbe(0, 0, *armed).has_value());
  EXPECT_FALSE(state.ProbeAvailable(0, 0));
}

TEST(CoordinatorStateTest, ProbeSuccessClosesOnlyAfterFullQuota) {
  const auto now = Clock::now();
  CoordinatorState state(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 5, 2)}), now);
  const std::uint64_t generation = state.BuildSnapshot()->endpoints[0][0].generation;
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 1ms);
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 2ms);
  state.ArmHalfOpen(0, 0, now + 6s);
  auto armed = state.BuildSnapshot();
  auto first = state.ClaimProbe(0, 0, *armed);
  auto second = state.ClaimProbe(0, 0, *armed);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  state.RecordResult({0, 0, *first, true}, now + 7s);
  EXPECT_EQ(state.BreakerState(0, 0), State::kHalfOpen);  // one of two: not closed
  state.RecordResult({0, 0, *second, true}, now + 8s);
  EXPECT_EQ(state.BreakerState(0, 0), State::kClosed);
}

TEST(CoordinatorStateTest, ProbeFailureReopensImmediately) {
  const auto now = Clock::now();
  CoordinatorState state(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 5, 1)}), now);
  const std::uint64_t generation = state.BuildSnapshot()->endpoints[0][0].generation;
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 1ms);
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 2ms);
  state.ArmHalfOpen(0, 0, now + 6s);
  auto armed = state.BuildSnapshot();
  auto probe = state.ClaimProbe(0, 0, *armed);
  ASSERT_TRUE(probe.has_value());
  state.RecordResult({0, 0, *probe, false}, now + 7s);
  EXPECT_EQ(state.BreakerState(0, 0), State::kOpen);
  EXPECT_GT(state.Generation(0, 0), armed->endpoints[0][0].generation);
}

// R-058: a stale snapshot's probe claim is voided once the coordinator has
// moved to a new HalfOpen cycle; no counters are rolled back.
TEST(CoordinatorStateTest, StaleSnapshotProbeClaimIsRejected) {
  const auto now = Clock::now();
  CoordinatorState state(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 5, 2)}), now);
  const std::uint64_t generation = state.BuildSnapshot()->endpoints[0][0].generation;
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 1ms);
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 2ms);
  state.ArmHalfOpen(0, 0, now + 6s);
  auto first_cycle = state.BuildSnapshot();
  auto probe = state.ClaimProbe(0, 0, *first_cycle);
  ASSERT_TRUE(probe.has_value());
  state.RecordResult({0, 0, *probe, false}, now + 7s);  // probe failure reopens
  state.ArmHalfOpen(0, 0, now + 13s);                   // second cycle
  auto second_cycle = state.BuildSnapshot();

  // The stale first-cycle snapshot claims from its own (now inactive) slot
  // object; the post-claim currency check voids it.
  EXPECT_FALSE(state.ClaimProbe(0, 0, *first_cycle).has_value());
  // The current cycle claims normally with matching base/generation/quota.
  auto claim = state.ClaimProbe(0, 0, *second_cycle);
  ASSERT_TRUE(claim.has_value());
  EXPECT_EQ(claim->generation, second_cycle->endpoints[0][0].generation);
  EXPECT_GE(claim->probe_id, second_cycle->endpoints[0][0].probe_base);
  EXPECT_LT(claim->probe_id,
            second_cycle->endpoints[0][0].probe_base + 2U);
}

// R-058: concurrent claims from one cycle never exceed the quota and never
// issue duplicate probe ids.
TEST(CoordinatorStateTest, ConcurrentClaimsBoundedByQuotaNoDuplicates) {
  const auto now = Clock::now();
  CoordinatorState state(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 5, 4)}), now);
  const std::uint64_t generation = state.BuildSnapshot()->endpoints[0][0].generation;
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 1ms);
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 2ms);
  state.ArmHalfOpen(0, 0, now + 6s);
  auto armed = state.BuildSnapshot();

  constexpr int kThreads = 4;
  constexpr int kAttempts = 200;
  std::atomic<int> successes{0};
  std::mutex ids_mutex;
  std::vector<std::uint64_t> ids;
  std::vector<std::thread> threads;
  for (int thread_index = 0; thread_index < kThreads; ++thread_index) {
    threads.emplace_back([&] {
      for (int attempt = 0; attempt < kAttempts; ++attempt) {
        const auto claim = state.ClaimProbe(0, 0, *armed);
        if (claim.has_value()) {
          ++successes;
          {
            std::lock_guard<std::mutex> guard(ids_mutex);
            ids.push_back(claim->probe_id);
          }
        }
      }
    });
  }
  for (auto &thread : threads) thread.join();
  EXPECT_EQ(successes.load(), 4);
  const std::set<std::uint64_t> distinct(ids.begin(), ids.end());
  EXPECT_EQ(distinct.size(), 4U);
  for (const std::uint64_t id : ids) {
    EXPECT_GE(id, armed->endpoints[0][0].probe_base);
    EXPECT_LT(id, armed->endpoints[0][0].probe_base + 4U);
  }
}

// R-058: concurrent stale and current-cycle claims — the stale ones are all
// voided by the currency check and no duplicate probe id is ever issued.
TEST(CoordinatorStateTest, ConcurrentStaleAndCurrentCycleClaims) {
  const auto now = Clock::now();
  CoordinatorState state(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 5, 4)}), now);
  const std::uint64_t generation = state.BuildSnapshot()->endpoints[0][0].generation;
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 1ms);
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 2ms);
  state.ArmHalfOpen(0, 0, now + 6s);
  auto first_cycle = state.BuildSnapshot();
  auto first_probe = state.ClaimProbe(0, 0, *first_cycle);
  ASSERT_TRUE(first_probe.has_value());
  state.RecordResult({0, 0, *first_probe, false}, now + 7s);  // reopen
  state.ArmHalfOpen(0, 0, now + 13s);                         // second cycle
  auto second_cycle = state.BuildSnapshot();

  std::atomic<int> stale_successes{0};
  std::atomic<int> current_successes{0};
  std::mutex ids_mutex;
  std::vector<std::uint64_t> ids;
  std::thread stale([&] {
    for (int attempt = 0; attempt < 200; ++attempt) {
      if (state.ClaimProbe(0, 0, *first_cycle).has_value()) ++stale_successes;
    }
  });
  std::thread current([&] {
    for (int attempt = 0; attempt < 200; ++attempt) {
      const auto claim = state.ClaimProbe(0, 0, *second_cycle);
      if (claim.has_value()) {
        ++current_successes;
        {
          std::lock_guard<std::mutex> guard(ids_mutex);
          ids.push_back(claim->probe_id);
        }
      }
    }
  });
  stale.join();
  current.join();
  EXPECT_EQ(stale_successes.load(), 0) << "stale cycle claims must all be voided";
  EXPECT_EQ(current_successes.load(), 4);
  const std::set<std::uint64_t> distinct(ids.begin(), ids.end());
  EXPECT_EQ(distinct.size(), 4U) << "no duplicate probe ids across concurrent claims";
}

TEST(CoordinatorStateTest, DuplicateProbeResultIsIgnored) {
  const auto now = Clock::now();
  CoordinatorState state(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 5, 2)}), now);
  const std::uint64_t generation = state.BuildSnapshot()->endpoints[0][0].generation;
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 1ms);
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 2ms);
  state.ArmHalfOpen(0, 0, now + 6s);
  auto armed = state.BuildSnapshot();
  auto first = state.ClaimProbe(0, 0, *armed);
  auto second = state.ClaimProbe(0, 0, *armed);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  state.RecordResult({0, 0, *first, true}, now + 7s);
  state.RecordResult({0, 0, *first, true}, now + 8s);  // duplicate: ignored
  EXPECT_EQ(state.BreakerState(0, 0), State::kHalfOpen);
  state.RecordResult({0, 0, *second, true}, now + 9s);
  EXPECT_EQ(state.BreakerState(0, 0), State::kClosed);
}

TEST(CoordinatorStateTest, RoutesWithoutBreakerAreAlwaysEligible) {
  const auto now = Clock::now();
  CoordinatorState state(ConfigWith({PlainRoute("plain")}), now);
  state.RecordResult({0, 0, {false, 1, 0}, false}, now + 1ms);  // inert
  auto snapshot = state.BuildSnapshot();
  EXPECT_TRUE(snapshot->endpoints[0][0].healthy);
  EXPECT_EQ(snapshot->endpoints[0][0].breaker_state, static_cast<std::uint8_t>(State::kClosed));
  EXPECT_EQ(snapshot->endpoints[0][0].generation, 0U);
  state.RecordHealth(0, 0, false);  // health still applies without a breaker
  EXPECT_FALSE(state.BuildSnapshot()->endpoints[0][0].healthy);
}

TEST(CoordinatorStateTest, HealthAndBreakerDecisionsStaySeparate) {
  const auto now = Clock::now();
  CoordinatorState state(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 5, 1)}), now);
  state.RecordHealth(0, 0, false);
  auto snapshot = state.BuildSnapshot();
  EXPECT_FALSE(snapshot->endpoints[0][0].healthy);
  EXPECT_EQ(snapshot->endpoints[0][0].breaker_state, static_cast<std::uint8_t>(State::kClosed));
  const std::uint64_t generation = snapshot->endpoints[0][0].generation;
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 1ms);
  state.RecordResult({0, 0, {false, generation, 0}, false}, now + 2ms);
  auto opened = state.BuildSnapshot();
  EXPECT_FALSE(opened->endpoints[0][0].healthy);
  EXPECT_EQ(opened->endpoints[0][0].breaker_state, static_cast<std::uint8_t>(State::kOpen));
}

// --- runtime level: real clock, short durations, seam-driven ---

using Deadline = std::chrono::steady_clock::time_point;

Deadline TestDeadline() { return std::chrono::steady_clock::now() + std::chrono::seconds(5); }

bool WaitForState(Coordinator &coordinator, std::size_t route, std::size_t endpoint,
                  State wanted, Deadline deadline) {
  while (std::chrono::steady_clock::now() < deadline) {
    const auto snapshot = coordinator.CurrentSnapshot();
    if (snapshot && snapshot->endpoints.size() > route &&
        snapshot->endpoints[route].size() > endpoint &&
        static_cast<State>(snapshot->endpoints[route][endpoint].breaker_state) == wanted) {
      return true;
    }
  }
  return false;
}

TEST(CoordinatorRuntimeTest, SubmitResultAndWaitDrivesOpenArmProbeAndClose) {
  const auto now = Clock::now();
  // open_seconds is in whole seconds: the arm timer transitions to half-open
  // about one second after the breaker opens.
  Coordinator coordinator(ConfigWith({RouteWithBreaker("a", 1, 2, 500, 1, 1)}), now);
  coordinator.Start();
  const std::uint64_t generation = coordinator.CurrentSnapshot()->endpoints[0][0].generation;
  coordinator.SubmitResultAndWait({0, 0, {false, generation, 0}, false});
  coordinator.SubmitResultAndWait({0, 0, {false, generation, 0}, false});
  EXPECT_TRUE(WaitForState(coordinator, 0, 0, State::kOpen, TestDeadline()));
  // The arm timer (open_duration 1s) transitions to half-open.
  EXPECT_TRUE(WaitForState(coordinator, 0, 0, State::kHalfOpen, TestDeadline()));
  auto snapshot = coordinator.CurrentSnapshot();
  auto probe = coordinator.ClaimProbe(0, 0, *snapshot);
  ASSERT_TRUE(probe.has_value());
  coordinator.SubmitResultAndWait({0, 0, *probe, true});
  EXPECT_TRUE(WaitForState(coordinator, 0, 0, State::kClosed, TestDeadline()));
  coordinator.Stop();
}

TEST(CoordinatorRuntimeTest, RecordHealthAndWaitPublishes) {
  const auto now = Clock::now();
  Coordinator coordinator(ConfigWith({PlainRoute("plain")}), now);
  coordinator.Start();
  coordinator.RecordHealthAndWait(0, 0, false);
  EXPECT_FALSE(coordinator.CurrentSnapshot()->endpoints[0][0].healthy);
  coordinator.Stop();
}

// R-053: a worker-side outcome reservation, published and drained on the
// coordinator loop, drives the breaker exactly like a direct submission.
TEST(CoordinatorRuntimeTest, ReserveOutcomeAndDrainRecordsToBreaker) {
  const auto now = Clock::now();
  Coordinator coordinator(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 1, 1)}), now);
  coordinator.Start();
  const std::uint64_t generation = coordinator.CurrentSnapshot()->endpoints[0][0].generation;
  auto first = coordinator.ReserveOutcome(0);
  auto second = coordinator.ReserveOutcome(0);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  first->Publish({0, 0, {false, generation, 0}, false});
  second->Publish({0, 0, {false, generation, 0}, false});
  // The published outcomes are drained by the coordinator loop's wake handler.
  EXPECT_TRUE(WaitForState(coordinator, 0, 0, State::kOpen, TestDeadline()));
  // Consuming each outcome restored a credit: the capacity is usable again.
  EXPECT_TRUE(coordinator.ReserveOutcome(0).has_value());
  coordinator.Stop();
}

// R-053/R-061: BeginStopping refuses new reservations and counts them; the
// rejected total is observable (outcome_reservation_rejected_total).
TEST(CoordinatorRuntimeTest, BeginOutcomeStoppingRejectsNewReserve) {
  const auto now = Clock::now();
  Coordinator coordinator(ConfigWith({RouteWithBreaker("a", 10, 2, 500, 1, 1)}), now);
  coordinator.Start();
  EXPECT_TRUE(coordinator.ReserveOutcome(0).has_value());
  coordinator.BeginOutcomeStopping();
  EXPECT_FALSE(coordinator.ReserveOutcome(0).has_value());
  EXPECT_GE(coordinator.OutcomeRejectedTotal(), 1U);
  // A route without a breaker has no outcome channel at all.
  coordinator.Stop();
  Coordinator plain(ConfigWith({PlainRoute("plain")}), now);
  plain.Start();
  EXPECT_FALSE(plain.ReserveOutcome(0).has_value());
  plain.Stop();
}

} // namespace
} // namespace aegisgate::health
