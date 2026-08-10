#include "aegisgate/resilience/TokenBucket.h"

#include <gtest/gtest.h>

#include <chrono>
#include <limits>
#include <stdexcept>

namespace aegisgate::resilience {
namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

TEST(TokenBucketTest, StartsWithItsEntireBurstAvailable) {
  const auto now = Clock::time_point{};
  TokenBucket bucket(10, 3, now);

  EXPECT_TRUE(bucket.TryAcquire(now));
  EXPECT_TRUE(bucket.TryAcquire(now));
  EXPECT_TRUE(bucket.TryAcquire(now));
  EXPECT_FALSE(bucket.TryAcquire(now));
}

TEST(TokenBucketTest, RefillsAtTheConfiguredRateWithoutExceedingBurst) {
  const auto now = Clock::time_point{};
  TokenBucket bucket(2, 3, now);

  EXPECT_TRUE(bucket.TryAcquire(now));
  EXPECT_TRUE(bucket.TryAcquire(now));
  EXPECT_TRUE(bucket.TryAcquire(now));
  EXPECT_FALSE(bucket.TryAcquire(now));

  EXPECT_TRUE(bucket.TryAcquire(now + 500ms));
  EXPECT_FALSE(bucket.TryAcquire(now + 500ms));
  EXPECT_TRUE(bucket.TryAcquire(now + 5s));
  EXPECT_TRUE(bucket.TryAcquire(now + 5s));
  EXPECT_TRUE(bucket.TryAcquire(now + 5s));
  EXPECT_FALSE(bucket.TryAcquire(now + 5s));
}

TEST(TokenBucketTest, RejectionDoesNotConsumeFutureRefillCredit) {
  const auto now = Clock::time_point{};
  TokenBucket bucket(1, 1, now);

  ASSERT_TRUE(bucket.TryAcquire(now));
  EXPECT_FALSE(bucket.TryAcquire(now + 250ms));
  EXPECT_TRUE(bucket.TryAcquire(now + 1s));
  EXPECT_FALSE(bucket.TryAcquire(now + 1s));
}

TEST(TokenBucketTest, RequiresACompleteTokenWithoutRoundingUpRefill) {
  const auto now = Clock::time_point{};
  TokenBucket bucket(3, 1, now);

  ASSERT_TRUE(bucket.TryAcquire(now));
  EXPECT_FALSE(bucket.TryAcquire(now + 333333333ns));
  EXPECT_TRUE(bucket.TryAcquire(now + 333333334ns));
}

TEST(TokenBucketTest, BackwardTimeDoesNotMintTokens) {
  const auto now = Clock::time_point{};
  TokenBucket bucket(1, 1, now);

  ASSERT_TRUE(bucket.TryAcquire(now));
  EXPECT_FALSE(bucket.TryAcquire(now + 500ms));
  EXPECT_FALSE(bucket.TryAcquire(now + 250ms));
  EXPECT_TRUE(bucket.TryAcquire(now + 1s));
}

TEST(TokenBucketTest, RejectsZeroRateOrBurst) {
  const auto now = Clock::time_point{};
  EXPECT_THROW((TokenBucket{0, 1, now}), std::invalid_argument);
  EXPECT_THROW((TokenBucket{1, 0, now}), std::invalid_argument);
}

TEST(TokenBucketTest, AcceptsLargestLegalRateAndBurstWithoutOverflow) {
  const auto now = Clock::time_point{};
  TokenBucket bucket(std::numeric_limits<std::uint32_t>::max(),
                     std::numeric_limits<std::uint32_t>::max(), now);

  EXPECT_TRUE(bucket.TryAcquire(now));
  EXPECT_TRUE(bucket.TryAcquire(now + 1ns));
}

} // namespace
} // namespace aegisgate::resilience
