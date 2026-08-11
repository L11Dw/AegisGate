#include "aegisgate/resilience/CircuitBreaker.h"

#include <gtest/gtest.h>

#include <chrono>

namespace aegisgate::resilience {

namespace {

using Clock = CircuitBreaker::Clock;
using State = CircuitBreaker::State;
using Selection = CircuitBreaker::Selection;

constexpr std::chrono::milliseconds kWindow{100};
constexpr std::chrono::milliseconds kOpen{50};
CircuitBreakerConfig Config(std::uint32_t min_requests = 5,
                            std::uint32_t threshold_permille = 500,
                            std::uint32_t probes = 1) {
  return {kWindow, min_requests, threshold_permille, kOpen, probes};
}

} // namespace

TEST(CircuitBreakerTest, StaysClosedBelowThreshold) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(), now);
  for (int i = 0; i < 11; ++i) {
    breaker.RecordSuccess(now + std::chrono::milliseconds(i));
  }
  for (int i = 0; i < 9; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i));
  }
  // 9 failures out of 20 samples: just below the 50% threshold.
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(20)), Selection::kAllowed);
  EXPECT_EQ(breaker.StateNow(), State::kClosed);
}

TEST(CircuitBreakerTest, OpensWhenThresholdAndMinimumSamplesReached) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i));
  }
  EXPECT_EQ(breaker.StateNow(), State::kOpen);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(10)), Selection::kRejectedOpen);
}

TEST(CircuitBreakerTest, ExpiredWindowSamplesDoNotCount) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(), now);
  // Below min_requests, so the breaker stays closed while the failures are
  // still in the window.
  for (int i = 0; i < 4; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i));
  }
  EXPECT_EQ(breaker.StateNow(), State::kClosed);
  // The failures aged out of the window; a fresh success no longer opens.
  breaker.RecordSuccess(now + std::chrono::milliseconds(150));
  EXPECT_EQ(breaker.StateNow(), State::kClosed);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(151)), Selection::kAllowed);
}

TEST(CircuitBreakerTest, RejectsSelectionWhileOpen) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i));
  }
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(10)), Selection::kRejectedOpen);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(40)), Selection::kRejectedOpen);
}

TEST(CircuitBreakerTest, AllowsExactHalfOpenProbeQuotaAfterOpenWindow) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(5, 500, 2), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i));
  }
  // Open window elapsed: exactly two probes allowed.
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(60)), Selection::kProbe);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(61)), Selection::kProbe);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(62)), Selection::kRejectedHalfOpenQuota);
}

TEST(CircuitBreakerTest, HalfOpenSuccessClosesAndResetsWindow) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(5, 500, 2), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i));
  }
  (void)breaker.Select(now + std::chrono::milliseconds(60));
  (void)breaker.Select(now + std::chrono::milliseconds(61));
  breaker.RecordSuccess(now + std::chrono::milliseconds(70));
  // First probe success does not close yet (quota is two).
  EXPECT_EQ(breaker.StateNow(), State::kHalfOpen);
  breaker.RecordSuccess(now + std::chrono::milliseconds(71));
  EXPECT_EQ(breaker.StateNow(), State::kClosed);
  // The window was reset: old failures must not reopen immediately.
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(72)), Selection::kAllowed);
}

TEST(CircuitBreakerTest, HalfOpenFailureReopensImmediately) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i));
  }
  (void)breaker.Select(now + std::chrono::milliseconds(60));
  breaker.RecordFailure(now + std::chrono::milliseconds(70));
  EXPECT_EQ(breaker.StateNow(), State::kOpen);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(71)), Selection::kRejectedOpen);
}

TEST(CircuitBreakerTest, StaleProbeResultCannotMutateNewGeneration) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(5, 500, 2), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i));
  }
  (void)breaker.Select(now + std::chrono::milliseconds(60));  // probe 1
  (void)breaker.Select(now + std::chrono::milliseconds(61));  // probe 2
  breaker.RecordFailure(now + std::chrono::milliseconds(70)); // probe 1 fails -> reopens
  EXPECT_EQ(breaker.StateNow(), State::kOpen);
  // The in-flight probe 2 result arrives after the reopen: it must be ignored,
  // not treated as a success that closes the breaker.
  breaker.RecordSuccess(now + std::chrono::milliseconds(71));
  EXPECT_EQ(breaker.StateNow(), State::kOpen);
}

TEST(CircuitBreakerTest, RejectsInvalidConfiguration) {
  const auto now = Clock::now();
  EXPECT_THROW(CircuitBreaker(CircuitBreakerConfig{}, now), std::invalid_argument);
  EXPECT_THROW(CircuitBreaker(Config(0, 500, 1), now), std::invalid_argument);
  EXPECT_THROW(CircuitBreaker(Config(5, 0, 1), now), std::invalid_argument);
  EXPECT_THROW(CircuitBreaker(Config(5, 1000, 1), now), std::invalid_argument);
  EXPECT_THROW(CircuitBreaker(Config(5, 500, 0), now), std::invalid_argument);
}

} // namespace aegisgate::resilience
