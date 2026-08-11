#include <chrono>
#include <cstdint>
#include <optional>

#include <gtest/gtest.h>

#include "aegisgate/config/Config.h"
#include "aegisgate/resilience/GlobalAdmission.h"

namespace aegisgate::resilience {
namespace {

using Clock = GlobalAdmission::Clock;
using namespace std::chrono_literals;

config::Route Route(std::uint32_t rate, std::uint32_t burst, std::uint32_t max_inflight) {
  return config::Route{"api", "gateway.test", "/", {config::Endpoint{"127.0.0.1", {127, 0, 0, 1}, 9001, 1}},
                       rate, burst, max_inflight};
}

TEST(GlobalAdmissionTest, StartsWithFullBurstCredit) {
  GlobalAdmission admission(Route(10, 5, 8), Clock::now());
  EXPECT_EQ(admission.credit(), 5LL * 1'000'000'000);
  EXPECT_EQ(admission.inflight(), 0U);
  EXPECT_EQ(admission.MaxInflight(), 8U);
  EXPECT_EQ(admission.rate(), 10U);
  EXPECT_EQ(admission.burst(), 5U);
}

TEST(GlobalAdmissionTest, DrawsUpToRequestedTokensFromGlobalCredit) {
  GlobalAdmission admission(Route(10, 5, 8), Clock::now());
  EXPECT_EQ(admission.Draw(3), 3U);
  EXPECT_EQ(admission.Draw(3), 2U);  // only two tokens remain
  EXPECT_EQ(admission.Draw(1), 0U);
  EXPECT_EQ(admission.credit(), 0LL);
}

TEST(GlobalAdmissionTest, RefillAccruesRateAndCapsAtBurst) {
  const auto now = Clock::now();
  GlobalAdmission admission(Route(10, 5, 8), now);
  (void)admission.Draw(5);  // empty the bucket
  admission.Refill(now + 1s);
  EXPECT_EQ(admission.Draw(10), 5U);  // refilled to capacity, capped at burst
  EXPECT_EQ(admission.Draw(1), 0U);
  admission.Refill(now + 2s);
  EXPECT_EQ(admission.Draw(10), 5U);  // still capped at burst
}

TEST(GlobalAdmissionTest, FractionalRefillYieldsWholeTokensOnly) {
  const auto now = Clock::now();
  GlobalAdmission admission(Route(3, 10, 8), now);
  (void)admission.Draw(10);  // empty the bucket
  // 333,333,333ns at rate 3/s is 0.999 tokens: no whole token may appear.
  admission.Refill(now + 333333333ns);
  EXPECT_EQ(admission.Draw(1), 0U);
  admission.Refill(now + 334ms);
  EXPECT_EQ(admission.Draw(1), 1U);  // 1.002 tokens accrued -> one whole token
}

TEST(GlobalAdmissionTest, ReturnRefillsCreditCappedAtBurst) {
  const auto now = Clock::now();
  GlobalAdmission admission(Route(10, 5, 8), now);
  (void)admission.Draw(5);
  admission.Return(3);
  EXPECT_EQ(admission.credit(), 3LL * 1'000'000'000);
  admission.Return(10);  // returning more than capacity clamps at burst
  EXPECT_EQ(admission.credit(), 5LL * 1'000'000'000);
}

TEST(GlobalAdmissionTest, InflightAdmissionEnforcesMaximumExactlyOnce) {
  GlobalAdmission admission(Route(10, 5, 2), Clock::now());
  auto first = admission.TryAcquireInflight();
  auto second = admission.TryAcquireInflight();
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  EXPECT_FALSE(admission.TryAcquireInflight().has_value());
  EXPECT_EQ(admission.inflight(), 2U);
  first->Release();
  first->Release();  // idempotent: no double release, no underflow
  EXPECT_EQ(admission.inflight(), 1U);
  auto third = admission.TryAcquireInflight();
  ASSERT_TRUE(third);
  EXPECT_EQ(admission.inflight(), 2U);
}

TEST(GlobalAdmissionTest, LeaseBatchClampsByWorkersAndBurst) {
  EXPECT_EQ(GlobalAdmission::LeaseBatch(100, 3, 200), 34U);   // ceil(100/3)
  EXPECT_EQ(GlobalAdmission::LeaseBatch(1, 4, 1), 1U);        // clamp floor
  EXPECT_EQ(GlobalAdmission::LeaseBatch(1000, 3, 5), 5U);     // burst cap
  EXPECT_EQ(GlobalAdmission::LeaseBatch(10, 1, 10), 10U);     // single worker
}

} // namespace
} // namespace aegisgate::resilience
