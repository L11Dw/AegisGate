#include <atomic>
#include <memory>

#include <gtest/gtest.h>

#include "aegisgate/runtime/GenerationMailbox.h"
#include "aegisgate/runtime/RuntimeGeneration.h"

namespace aegisgate::runtime {
namespace {

TEST(RuntimeGenerationTest, RetiringGenerationNotifiesExactlyOnceAfterLastRequestLease) {
  auto generation = std::make_shared<RuntimeGeneration>(/*version=*/7);
  auto first = generation->TryAcquireRequestLease();
  auto second = generation->TryAcquireRequestLease();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(generation->active_request_leases(), 2U);

  std::atomic_uint notifications{0};
  EXPECT_TRUE(generation->BeginRetirement([&] { ++notifications; }));
  EXPECT_EQ(notifications.load(), 0U);

  first.reset();
  EXPECT_EQ(generation->active_request_leases(), 1U);
  EXPECT_EQ(notifications.load(), 0U);

  second.reset();
  EXPECT_EQ(generation->active_request_leases(), 0U);
  EXPECT_EQ(notifications.load(), 1U);
  EXPECT_FALSE(generation->BeginRetirement([&] { ++notifications; }));
  EXPECT_EQ(notifications.load(), 1U);
}

TEST(RuntimeGenerationTest, RetiringGenerationRejectsNewRequestLeases) {
  auto generation = std::make_shared<RuntimeGeneration>(/*version=*/8);
  EXPECT_TRUE(generation->BeginRetirement([] {}));
  EXPECT_FALSE(generation->TryAcquireRequestLease().has_value());
}

TEST(RuntimeGenerationTest, LastLeasePostsAValueEventForControlLoopRetirement) {
  auto mailbox = std::make_shared<GenerationMailbox>();
  auto generation = std::make_shared<RuntimeGeneration>(/*version=*/9);
  auto lease = generation->TryAcquireRequestLease();
  ASSERT_TRUE(lease.has_value());

  ASSERT_TRUE(generation->BeginRetirement([mailbox, generation] {
    if (!mailbox->Post({GenerationMailbox::Kind::kLastRequestLeaseReleased, generation})) {
      std::terminate();
    }
  }));
  EXPECT_TRUE(mailbox->Drain().empty());

  lease.reset();
  const auto events = mailbox->Drain();
  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events.front().kind, GenerationMailbox::Kind::kLastRequestLeaseReleased);
  EXPECT_EQ(events.front().generation, generation);
}

} // namespace
} // namespace aegisgate::runtime
