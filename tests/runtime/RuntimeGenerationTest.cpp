#include <atomic>
#include <memory>

#include <gtest/gtest.h>

#include "aegisgate/config/Config.h"
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

TEST(RuntimeGenerationTest, RetirementTransitionsToOutcomeDrainingOnlyOnce) {
  auto generation = std::make_shared<RuntimeGeneration>(/*version=*/81);
  ASSERT_TRUE(generation->BeginRetirement([] {}));
  EXPECT_EQ(generation->retirement_state(), RuntimeGeneration::RetirementState::kRetiring);
  // NotifyCheckersStopped advances to kCheckersStopped -> kWaitingForLeases
  // (no leases held).
  EXPECT_TRUE(generation->NotifyCheckersStopped());
  EXPECT_EQ(generation->retirement_state(), RuntimeGeneration::RetirementState::kWaitingForLeases);
  // BeginReaping transitions to kOutcomeDraining.
  EXPECT_TRUE(generation->BeginReaping());
  EXPECT_EQ(generation->retirement_state(), RuntimeGeneration::RetirementState::kOutcomeDraining);
  EXPECT_FALSE(generation->BeginReaping()); // already in kOutcomeDraining
  generation->MarkRetired();
  EXPECT_EQ(generation->retirement_state(), RuntimeGeneration::RetirementState::kDone);
}

TEST(RuntimeGenerationTest, SixStateRetirementOrdering) {
  auto generation = std::make_shared<RuntimeGeneration>(/*version=*/100);
  auto lease = generation->TryAcquireRequestLease();
  ASSERT_TRUE(lease.has_value());

  std::vector<RuntimeGeneration::RetirementState> observed;
  generation->SetStateChangeCallback([&observed](RuntimeGeneration::RetirementState s) {
    observed.push_back(s);
  });

  EXPECT_TRUE(generation->BeginRetirement([] {}));
  EXPECT_EQ(generation->retirement_state(), RuntimeGeneration::RetirementState::kRetiring);

  // Stop checkers — lease still held → kCheckersStopped.
  EXPECT_TRUE(generation->NotifyCheckersStopped());
  EXPECT_EQ(generation->retirement_state(), RuntimeGeneration::RetirementState::kCheckersStopped);

  // Release lease → auto-advance to kWaitingForLeases.
  lease = RuntimeGeneration::RequestLease{};
  EXPECT_EQ(generation->retirement_state(), RuntimeGeneration::RetirementState::kWaitingForLeases);

  // BeginReaping → kOutcomeDraining.
  EXPECT_TRUE(generation->BeginReaping());
  EXPECT_EQ(generation->retirement_state(), RuntimeGeneration::RetirementState::kOutcomeDraining);

  // MarkRetired → kDone.
  generation->MarkRetired();
  EXPECT_EQ(generation->retirement_state(), RuntimeGeneration::RetirementState::kDone);

  // Verify observed ordering.
  ASSERT_EQ(observed.size(), 5u);
  EXPECT_EQ(observed[0], RuntimeGeneration::RetirementState::kRetiring);
  EXPECT_EQ(observed[1], RuntimeGeneration::RetirementState::kCheckersStopped);
  EXPECT_EQ(observed[2], RuntimeGeneration::RetirementState::kWaitingForLeases);
  EXPECT_EQ(observed[3], RuntimeGeneration::RetirementState::kOutcomeDraining);
  EXPECT_EQ(observed[4], RuntimeGeneration::RetirementState::kDone);
}

TEST(RuntimeGenerationTest, ZeroLeasesStillStopsCheckersFirst) {
  auto generation = std::make_shared<RuntimeGeneration>(/*version=*/101);
  EXPECT_EQ(generation->active_request_leases(), 0u);

  EXPECT_TRUE(generation->BeginRetirement([] {}));
  EXPECT_EQ(generation->retirement_state(), RuntimeGeneration::RetirementState::kRetiring);

  // Cannot advance past kRetiring without stopping checkers.
  EXPECT_FALSE(generation->BeginReaping());

  // Stop checkers → kCheckersStopped → kWaitingForLeases (leases zero).
  EXPECT_TRUE(generation->NotifyCheckersStopped());
  EXPECT_EQ(generation->retirement_state(), RuntimeGeneration::RetirementState::kWaitingForLeases);
}

TEST(RuntimeGenerationTest, CoordinatorStoppedFlagIsAtomic) {
  auto generation = std::make_shared<RuntimeGeneration>(/*version=*/102);
  EXPECT_FALSE(generation->coordinator_stopped());
  generation->MarkCoordinatorStopped();
  EXPECT_TRUE(generation->coordinator_stopped());
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

TEST(RuntimeGenerationTest, OwnsOneImmutableRuntimeBundlePerGeneration) {
  config::Endpoint endpoint{"127.0.0.1", {127, 0, 0, 1}, 18080, 1};
  config::Route route{"api", "api.demo.local", "/", {endpoint},
                      10,    10,               8,   1000,  1000, 5000, 0};
  config::Config config{{route}, /*workers=*/2};
  auto generation = std::make_shared<RuntimeGeneration>(/*version=*/10, std::move(config));

  ASSERT_NE(generation->snapshot(), nullptr);
  EXPECT_EQ(generation->snapshot()->version, 10U);
  ASSERT_EQ(generation->snapshot()->config.routes.size(), 1U);
  EXPECT_EQ(generation->snapshot()->config.routes.front().endpoints.front().port, 18080);
  EXPECT_EQ(generation->admissions().size(), 1U);
  ASSERT_NE(generation->coordinator(), nullptr);
  ASSERT_EQ(generation->selection_states().size(), 2U);
  EXPECT_EQ(generation->selection_states()[0]->Version(), 10U);
  EXPECT_NE(generation->selection_states()[0], generation->selection_states()[1]);
}

} // namespace
} // namespace aegisgate::runtime
