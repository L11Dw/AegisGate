#include "aegisgate/resilience/CircuitBreaker.h"

#include <gtest/gtest.h>

#include <chrono>
#include <stdexcept>

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
    breaker.RecordSuccess(now + std::chrono::milliseconds(i),
                          breaker.Select(now + std::chrono::milliseconds(i)));
  }
  for (int i = 0; i < 9; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i),
                          breaker.Select(now + std::chrono::milliseconds(i)));
  }
  // 9 failures out of 20 samples: just below the 50% threshold.
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(20)).selection,
            Selection::kAllowed);
  EXPECT_EQ(breaker.StateNow(), State::kClosed);
}

TEST(CircuitBreakerTest, OpensWhenThresholdAndMinimumSamplesReached) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i),
                          breaker.Select(now + std::chrono::milliseconds(i)));
  }
  EXPECT_EQ(breaker.StateNow(), State::kOpen);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(10)).selection,
            Selection::kRejectedOpen);
}

TEST(CircuitBreakerTest, ExpiredWindowSamplesDoNotCount) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(), now);
  for (int i = 0; i < 4; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i),
                          breaker.Select(now + std::chrono::milliseconds(i)));
  }
  EXPECT_EQ(breaker.StateNow(), State::kClosed);
  breaker.RecordSuccess(now + std::chrono::milliseconds(150),
                        breaker.Select(now + std::chrono::milliseconds(150)));
  EXPECT_EQ(breaker.StateNow(), State::kClosed);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(151)).selection,
            Selection::kAllowed);
}

TEST(CircuitBreakerTest, RejectsSelectionWhileOpen) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i),
                          breaker.Select(now + std::chrono::milliseconds(i)));
  }
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(10)).selection,
            Selection::kRejectedOpen);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(40)).selection,
            Selection::kRejectedOpen);
}

TEST(CircuitBreakerTest, AllowsExactHalfOpenProbeQuotaAfterOpenWindow) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(5, 500, 2), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i),
                          breaker.Select(now + std::chrono::milliseconds(i)));
  }
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(60)).selection,
            Selection::kProbe);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(61)).selection,
            Selection::kProbe);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(62)).selection,
            Selection::kRejectedHalfOpenQuota);
}

TEST(CircuitBreakerTest, HalfOpenSuccessClosesAndResetsWindow) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(5, 500, 2), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i),
                          breaker.Select(now + std::chrono::milliseconds(i)));
  }
  const auto first = breaker.Select(now + std::chrono::milliseconds(60));
  const auto second = breaker.Select(now + std::chrono::milliseconds(61));
  ASSERT_EQ(first.selection, Selection::kProbe);
  ASSERT_EQ(second.selection, Selection::kProbe);
  breaker.RecordSuccess(now + std::chrono::milliseconds(70), first);
  EXPECT_EQ(breaker.StateNow(), State::kHalfOpen);
  breaker.RecordSuccess(now + std::chrono::milliseconds(71), second);
  EXPECT_EQ(breaker.StateNow(), State::kClosed);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(72)).selection,
            Selection::kAllowed);
}

TEST(CircuitBreakerTest, HalfOpenFailureReopensImmediately) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i),
                          breaker.Select(now + std::chrono::milliseconds(i)));
  }
  const auto probe = breaker.Select(now + std::chrono::milliseconds(60));
  ASSERT_EQ(probe.selection, Selection::kProbe);
  breaker.RecordFailure(now + std::chrono::milliseconds(70), probe);
  EXPECT_EQ(breaker.StateNow(), State::kOpen);
  EXPECT_EQ(breaker.Select(now + std::chrono::milliseconds(71)).selection,
            Selection::kRejectedOpen);
}

TEST(CircuitBreakerTest, StaleProbeResultCannotMutateNewGeneration) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(5, 500, 2), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i),
                          breaker.Select(now + std::chrono::milliseconds(i)));
  }
  const auto first = breaker.Select(now + std::chrono::milliseconds(60));
  const auto second = breaker.Select(now + std::chrono::milliseconds(61));
  breaker.RecordFailure(now + std::chrono::milliseconds(70), first);
  EXPECT_EQ(breaker.StateNow(), State::kOpen);
  // The in-flight probe 2 result arrives after the reopen: its permit belongs
  // to the old generation and must be ignored.
  breaker.RecordSuccess(now + std::chrono::milliseconds(71), second);
  EXPECT_EQ(breaker.StateNow(), State::kOpen);
}

TEST(CircuitBreakerTest, LateOrdinarySuccessCannotCloseHalfOpen) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(), now);
  // An ordinary request is admitted while Closed.
  const auto late = breaker.Select(now + std::chrono::milliseconds(0));
  ASSERT_EQ(late.selection, Selection::kAllowed);
  // Other failures open the breaker.
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i),
                          breaker.Select(now + std::chrono::milliseconds(i)));
  }
  EXPECT_EQ(breaker.StateNow(), State::kOpen);
  // The open window elapses and a genuine probe is issued.
  const auto probe = breaker.Select(now + std::chrono::milliseconds(60));
  ASSERT_EQ(probe.selection, Selection::kProbe);
  // The late ordinary success arrives before the probe result: it must not
  // be mistaken for the probe (which would close the breaker).
  breaker.RecordSuccess(now + std::chrono::milliseconds(61), late);
  EXPECT_EQ(breaker.StateNow(), State::kHalfOpen);
  breaker.RecordSuccess(now + std::chrono::milliseconds(62), probe);
  EXPECT_EQ(breaker.StateNow(), State::kClosed);
}

TEST(CircuitBreakerTest, DuplicateProbeResultIsIgnored) {
  const auto now = Clock::now();
  CircuitBreaker breaker(Config(5, 500, 2), now);
  for (int i = 0; i < 5; ++i) {
    breaker.RecordFailure(now + std::chrono::milliseconds(i),
                          breaker.Select(now + std::chrono::milliseconds(i)));
  }
  const auto first = breaker.Select(now + std::chrono::milliseconds(60));
  const auto second = breaker.Select(now + std::chrono::milliseconds(61));
  breaker.RecordSuccess(now + std::chrono::milliseconds(70), first);
  // The same probe result delivered twice must not complete twice.
  breaker.RecordSuccess(now + std::chrono::milliseconds(71), first);
  EXPECT_EQ(breaker.StateNow(), State::kHalfOpen);
  breaker.RecordSuccess(now + std::chrono::milliseconds(72), second);
  EXPECT_EQ(breaker.StateNow(), State::kClosed);
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
