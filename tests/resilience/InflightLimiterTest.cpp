#include "aegisgate/resilience/InflightLimiter.h"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

namespace aegisgate::resilience {
namespace {

TEST(InflightLimiterTest, RejectsWhenFullAndReleasesAtReservationDestruction) {
  InflightLimiter limiter(2);

  auto first = limiter.Acquire();
  auto second = limiter.Acquire();
  EXPECT_TRUE(first);
  EXPECT_TRUE(second);
  EXPECT_EQ(limiter.inflight(), 2U);
  EXPECT_FALSE(limiter.Acquire());

  first.Release();
  EXPECT_EQ(limiter.inflight(), 1U);
  EXPECT_TRUE(limiter.Acquire());
}

TEST(InflightLimiterTest, MoveTransfersExactlyOneReleaseResponsibility) {
  InflightLimiter limiter(1);

  auto reservation = limiter.Acquire();
  ASSERT_TRUE(reservation);
  auto moved = std::move(reservation);
  EXPECT_FALSE(reservation);
  EXPECT_TRUE(moved);
  EXPECT_EQ(limiter.inflight(), 1U);

  moved.Release();
  EXPECT_EQ(limiter.inflight(), 0U);
}

TEST(InflightLimiterTest, ReleasingTwiceDoesNotUnderflowOrFreeExtraCapacity) {
  InflightLimiter limiter(1);

  auto first = limiter.Acquire();
  ASSERT_TRUE(first);
  first.Release();
  first.Release();
  EXPECT_EQ(limiter.inflight(), 0U);

  auto second = limiter.Acquire();
  ASSERT_TRUE(second);
  EXPECT_FALSE(limiter.Acquire());
}

TEST(InflightLimiterTest, ReservationRemainsSafeAfterLimiterOwnerIsDestroyed) {
  std::optional<InflightLimiter::Reservation> reservation;
  {
    auto limiter = std::make_unique<InflightLimiter>(1);
    reservation.emplace(limiter->Acquire());
    ASSERT_TRUE(*reservation);
  }

  reservation->Release();
  reservation.reset();
}

TEST(InflightLimiterTest, RejectsZeroMaximum) {
  EXPECT_THROW((InflightLimiter{0}), std::invalid_argument);
}

TEST(InflightLimiterTest, MoveAssignmentReleasesThePreviousReservationExactlyOnce) {
  InflightLimiter limiter(2);
  auto first = limiter.Acquire();
  auto second = limiter.Acquire();
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);

  second = std::move(first);
  EXPECT_FALSE(first);
  EXPECT_TRUE(second);
  EXPECT_EQ(limiter.inflight(), 1U);
}

} // namespace
} // namespace aegisgate::resilience
