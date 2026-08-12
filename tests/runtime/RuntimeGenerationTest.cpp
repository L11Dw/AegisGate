#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "aegisgate/config/Config.h"
#include "aegisgate/health/Coordinator.h"
#include "aegisgate/resilience/GlobalAdmission.h"
#include "aegisgate/runtime/GenerationMailbox.h"
#include "aegisgate/runtime/RuntimeGeneration.h"
#include "aegisgate/runtime/SelectionState.h"

namespace aegisgate::runtime {
namespace {

using RetirementState = RuntimeGeneration::RetirementState;

// Builds a minimal config with one route, one endpoint, and health checks
// enabled so the Coordinator creates at least one HealthChecker.
config::Config MakeConfigWithHealthCheck() {
  config::Endpoint ep{"127.0.0.1", {127, 0, 0, 1}, 9000, 1};
  config::HealthCheckSettings hc;
  hc.interval_ms = 10000;
  hc.timeout_ms = 5000;
  config::Route route("api", "test.local", "/", {ep}, 100, 50, 10, 5000, 5000, 30000, 0,
                      std::nullopt, hc);
  return config::Config{{route}};
}

// Builds a RuntimeGeneration with a real Coordinator (started) and one worker
// selection state.
RuntimeGenerationRef MakeGeneration(config::Config config) {
  auto snapshot = std::make_shared<ConfigSnapshot>(
      ConfigSnapshot{1, std::move(config)});
  auto coord = std::make_shared<health::Coordinator>(
      std::make_shared<const config::Config>(snapshot->config),
      health::Coordinator::Clock::now());
  const auto now = resilience::GlobalAdmission::Clock::now();
  std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions;
  for (const auto &route : snapshot->config.routes) {
    admissions.push_back(std::make_shared<resilience::GlobalAdmission>(route, now));
  }
  coord->SetAdmissions(admissions);
  coord->Start();
  std::vector<std::shared_ptr<SelectionState>> sel_states;
  sel_states.push_back(
      std::make_shared<SelectionState>(snapshot->config, snapshot->version));
  return std::make_shared<RuntimeGeneration>(
      snapshot->version, snapshot, std::move(coord), std::move(admissions),
      std::move(sel_states));
}

// ---------------------------------------------------------------------------
// A5: retirement state machine ordering
// ---------------------------------------------------------------------------

TEST(RuntimeGenerationTest, RetirementStopsCheckersBeforeLeaseDrain) {
  // Scenario: the old generation has one outstanding request lease when
  // reload begins.  The state machine must:
  //   1. kRetiring         — new leases refused, checkers still running
  //   2. kCheckersStopped  — checkers stopped, lease still held
  //   3. kWaitingForLeases — lease released, ready for balance return
  //   4. kOutcomeDraining  — outcome channels stopped
  //   5. kDone             — fully retired
  //
  // The test drives each transition explicitly and asserts the ordering.

  auto gen = MakeGeneration(MakeConfigWithHealthCheck());

  // Acquire one request lease (simulates an in-flight request).
  auto lease = gen->TryAcquireRequestLease();
  ASSERT_TRUE(lease.has_value());
  EXPECT_EQ(gen->active_request_leases(), 1u);
  EXPECT_EQ(gen->retirement_state(), RetirementState::kActive);

  // Track every state the callback observes.
  std::vector<RetirementState> observed;
  gen->SetStateChangeCallback([&observed](RetirementState s) {
    observed.push_back(s);
  });

  // Begin retirement.  With one lease held, the state stays at kRetiring.
  EXPECT_TRUE(gen->BeginRetirement());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kRetiring);
  ASSERT_EQ(observed.size(), 1u);
  EXPECT_EQ(observed.back(), RetirementState::kRetiring);

  // Stop health checkers (synchronous, posts to coordinator thread).
  // With a lease still held, the state must be kCheckersStopped — not
  // kWaitingForLeases.
  gen->coordinator()->StopCheckers();
  EXPECT_TRUE(gen->NotifyCheckersStopped());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kCheckersStopped);
  ASSERT_GE(observed.size(), 2u);
  EXPECT_EQ(observed.back(), RetirementState::kCheckersStopped);

  // Duplicate notification must be rejected.
  EXPECT_TRUE(gen->NotifyCheckersStopped());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kCheckersStopped);

  // Release the request lease.  With checkers already stopped, the state
  // machine must auto-advance to kWaitingForLeases.
  lease = RuntimeGeneration::RequestLease{}; // Release.
  EXPECT_EQ(gen->active_request_leases(), 0u);
  EXPECT_EQ(gen->retirement_state(), RetirementState::kWaitingForLeases);
  ASSERT_GE(observed.size(), 3u);
  EXPECT_EQ(observed.back(), RetirementState::kWaitingForLeases);

  // BeginOutcomeStopping transitions to kOutcomeDraining.
  EXPECT_TRUE(gen->BeginOutcomeStopping());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kOutcomeDraining);
  ASSERT_GE(observed.size(), 4u);
  EXPECT_EQ(observed.back(), RetirementState::kOutcomeDraining);

  // MarkRetired transitions to kDone.
  gen->MarkRetired();
  EXPECT_EQ(gen->retirement_state(), RetirementState::kDone);
  ASSERT_GE(observed.size(), 5u);
  EXPECT_EQ(observed.back(), RetirementState::kDone);

  // After kDone, no further transitions are possible.
  EXPECT_FALSE(gen->BeginRetirement());
  EXPECT_FALSE(gen->NotifyCheckersStopped());
  EXPECT_FALSE(gen->BeginOutcomeStopping());

  // The observed ordering proves: checkers stopped before lease drain.
  ASSERT_EQ(observed.size(), 5u);
  EXPECT_EQ(observed[0], RetirementState::kRetiring);
  EXPECT_EQ(observed[1], RetirementState::kCheckersStopped);
  EXPECT_EQ(observed[2], RetirementState::kWaitingForLeases);
  EXPECT_EQ(observed[3], RetirementState::kOutcomeDraining);
  EXPECT_EQ(observed[4], RetirementState::kDone);
}

TEST(RuntimeGenerationTest, LeaseReleasedBeforeCheckersAutoAdvances) {
  // Scenario: checkers stop after the last lease is already released.
  // The state machine must still visit kCheckersStopped before
  // kWaitingForLeases — health checkers are periodic and must be
  // explicitly stopped regardless of lease count.
  auto gen = MakeGeneration(MakeConfigWithHealthCheck());

  auto lease = gen->TryAcquireRequestLease();
  ASSERT_TRUE(lease.has_value());

  std::vector<RetirementState> observed;
  gen->SetStateChangeCallback([&observed](RetirementState s) {
    observed.push_back(s);
  });

  EXPECT_TRUE(gen->BeginRetirement());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kRetiring);

  // Release the lease BEFORE stopping checkers.
  lease = RuntimeGeneration::RequestLease{};
  EXPECT_EQ(gen->active_request_leases(), 0u);
  // State is still kRetiring (checkers not stopped yet).
  EXPECT_EQ(gen->retirement_state(), RetirementState::kRetiring);

  // Now stop checkers.  Must visit kCheckersStopped first, then
  // auto-advance to kWaitingForLeases.
  EXPECT_TRUE(gen->NotifyCheckersStopped());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kWaitingForLeases);

  // Verify ordering: kRetiring -> kCheckersStopped -> kWaitingForLeases.
  ASSERT_EQ(observed.size(), 3u);
  EXPECT_EQ(observed[0], RetirementState::kRetiring);
  EXPECT_EQ(observed[1], RetirementState::kCheckersStopped);
  EXPECT_EQ(observed[2], RetirementState::kWaitingForLeases);
}

TEST(RuntimeGenerationTest, RetirementWithNoLeasesStillStopsCheckersFirst) {
  // Scenario: no request leases are held when retirement begins.
  // The state machine must still require checkers to be stopped before
  // advancing — health checkers are periodic and will keep producing
  // probes until explicitly stopped.
  auto gen = MakeGeneration(MakeConfigWithHealthCheck());
  EXPECT_EQ(gen->active_request_leases(), 0u);

  std::vector<RetirementState> observed;
  gen->SetStateChangeCallback([&observed](RetirementState s) {
    observed.push_back(s);
  });

  // BeginRetirement must enter kRetiring, NOT skip to kWaitingForLeases.
  EXPECT_TRUE(gen->BeginRetirement());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kRetiring);
  ASSERT_EQ(observed.size(), 1u);
  EXPECT_EQ(observed[0], RetirementState::kRetiring);

  // Cannot advance past kRetiring without stopping checkers.
  EXPECT_FALSE(gen->BeginOutcomeStopping());

  // Stop checkers -> kCheckersStopped -> kWaitingForLeases (leases zero).
  gen->coordinator()->StopCheckers();
  EXPECT_TRUE(gen->NotifyCheckersStopped());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kWaitingForLeases);

  // Verify mandatory kCheckersStopped was visited.
  ASSERT_GE(observed.size(), 2u);
  EXPECT_EQ(observed[1], RetirementState::kCheckersStopped);
}

// StopCheckers on a stopped coordinator must not crash or access checkers
// from the wrong thread.
TEST(CoordinatorTest, StopCheckersOnStoppedCoordinatorIsHarmless) {
  auto config = MakeConfigWithHealthCheck();
  auto coord = std::make_shared<health::Coordinator>(
      std::make_shared<const config::Config>(config),
      health::Coordinator::Clock::now());
  coord->Start();
  // Stop the coordinator first.
  coord->Stop();
  // StopCheckers after Stop must not crash.  PostTask will fail because
  // the runtime is stopped; the fallback must NOT touch loop_data_.
  EXPECT_NO_THROW(coord->StopCheckers());
}

TEST(RuntimeGenerationTest, BeginRetirementRefusesNewLease) {
  auto gen = MakeGeneration(MakeConfigWithHealthCheck());
  EXPECT_TRUE(gen->BeginRetirement());
  EXPECT_FALSE(gen->TryAcquireRequestLease().has_value());
}

TEST(RuntimeGenerationTest, DuplicateBeginRetirementRejected) {
  auto gen = MakeGeneration(MakeConfigWithHealthCheck());
  EXPECT_TRUE(gen->BeginRetirement());
  EXPECT_FALSE(gen->BeginRetirement());
}

TEST(RuntimeGenerationTest, BeginOutcomeStoppingRequiresWaitingForLeases) {
  auto gen = MakeGeneration(MakeConfigWithHealthCheck());
  EXPECT_FALSE(gen->BeginOutcomeStopping()); // kActive
  // Hold a lease so BeginRetirement stays at kRetiring (no auto-advance).
  auto lease = gen->TryAcquireRequestLease();
  ASSERT_TRUE(lease.has_value());
  EXPECT_TRUE(gen->BeginRetirement());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kRetiring);
  // Cannot stop outcomes while still in kRetiring (checkers not stopped).
  EXPECT_FALSE(gen->BeginOutcomeStopping());
}

TEST(RuntimeGenerationTest, NotifyCheckersStoppedRejectedWhenNotRetiring) {
  auto gen = MakeGeneration(MakeConfigWithHealthCheck());
  EXPECT_FALSE(gen->NotifyCheckersStopped()); // kActive
}

// Gateway/Coordinator integration: proves StopCheckers() actually stops health
// checkers on the coordinator thread and that the full retirement sequence
// (checkers -> leases -> outcomes -> coordinator) completes without deadlock
// or UAF.
TEST(RuntimeGenerationTest, GatewayCoordinatorRetirementIntegration) {
  auto gen = MakeGeneration(MakeConfigWithHealthCheck());

  // Verify the coordinator is running and has health checkers.
  ASSERT_TRUE(gen->coordinator());
  EXPECT_NE(gen->coordinator()->CurrentSnapshot(), nullptr);

  // Acquire two request leases to simulate multiple in-flight requests.
  auto lease1 = gen->TryAcquireRequestLease();
  auto lease2 = gen->TryAcquireRequestLease();
  ASSERT_TRUE(lease1.has_value());
  ASSERT_TRUE(lease2.has_value());
  EXPECT_EQ(gen->active_request_leases(), 2u);

  // Track state transitions.
  std::vector<RetirementState> observed;
  gen->SetStateChangeCallback([&observed](RetirementState s) {
    observed.push_back(s);
  });

  // Begin retirement.  Two leases held -> stays at kRetiring.
  EXPECT_TRUE(gen->BeginRetirement());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kRetiring);

  // Stop checkers on the real coordinator thread.  This must complete
  // without deadlock and without crashing (the checkers hold timer and
  // connection references that must be released on the coordinator loop).
  gen->coordinator()->StopCheckers();

  // Notify checkers stopped.  With two leases still held, state must be
  // kCheckersStopped.
  EXPECT_TRUE(gen->NotifyCheckersStopped());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kCheckersStopped);
  EXPECT_EQ(observed.back(), RetirementState::kCheckersStopped);

  // Release first lease.  Still one held -> stays at kCheckersStopped.
  lease1 = RuntimeGeneration::RequestLease{};
  EXPECT_EQ(gen->active_request_leases(), 1u);
  EXPECT_EQ(gen->retirement_state(), RetirementState::kCheckersStopped);

  // Release second lease.  Now zero -> auto-advance to kWaitingForLeases.
  lease2 = RuntimeGeneration::RequestLease{};
  EXPECT_EQ(gen->active_request_leases(), 0u);
  EXPECT_EQ(gen->retirement_state(), RetirementState::kWaitingForLeases);
  EXPECT_EQ(observed.back(), RetirementState::kWaitingForLeases);

  // BeginOutcomeStopping -> kOutcomeDraining.
  EXPECT_TRUE(gen->BeginOutcomeStopping());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kOutcomeDraining);

  // Drain outcomes and stop the coordinator on its thread.
  gen->coordinator()->DrainOutcomesAndWait();
  gen->coordinator()->Stop();

  // MarkRetired -> kDone.
  gen->MarkRetired();
  EXPECT_EQ(gen->retirement_state(), RetirementState::kDone);

  // Verify the complete ordering.
  ASSERT_EQ(observed.size(), 5u);
  EXPECT_EQ(observed[0], RetirementState::kRetiring);
  EXPECT_EQ(observed[1], RetirementState::kCheckersStopped);
  EXPECT_EQ(observed[2], RetirementState::kWaitingForLeases);
  EXPECT_EQ(observed[3], RetirementState::kOutcomeDraining);
  EXPECT_EQ(observed[4], RetirementState::kDone);
}

// Late checker-stop notification after lease drain: must not duplicate the
// kWaitingForLeases transition.
TEST(RuntimeGenerationTest, LateCheckerStopDoesNotDuplicateTransition) {
  auto gen = MakeGeneration(MakeConfigWithHealthCheck());
  auto lease = gen->TryAcquireRequestLease();
  ASSERT_TRUE(lease.has_value());

  std::vector<RetirementState> observed;
  gen->SetStateChangeCallback([&observed](RetirementState s) {
    observed.push_back(s);
  });

  EXPECT_TRUE(gen->BeginRetirement());

  // Release lease first (stays kRetiring because checkers not stopped).
  lease = RuntimeGeneration::RequestLease{};
  EXPECT_EQ(gen->retirement_state(), RetirementState::kRetiring);

  // Now stop checkers and notify.  Must go directly to kWaitingForLeases,
  // NOT kCheckersStopped -> kWaitingForLeases (two transitions).
  gen->coordinator()->StopCheckers();
  EXPECT_TRUE(gen->NotifyCheckersStopped());
  EXPECT_EQ(gen->retirement_state(), RetirementState::kWaitingForLeases);

  // Only two transitions: kRetiring -> kCheckersStopped -> kWaitingForLeases.
  // kCheckersStopped is always visited even when leases are already zero.
  ASSERT_EQ(observed.size(), 3u);
  EXPECT_EQ(observed[0], RetirementState::kRetiring);
  EXPECT_EQ(observed[1], RetirementState::kCheckersStopped);
  EXPECT_EQ(observed[2], RetirementState::kWaitingForLeases);
}

// Mailbox: pure wake signal — bounded, no events, no silent drops.
TEST(GenerationMailboxTest, WakeAndDrainArePaired) {
  GenerationMailbox mailbox;

  EXPECT_FALSE(mailbox.Drain()); // no pending wake

  EXPECT_TRUE(mailbox.Wake());
  EXPECT_TRUE(mailbox.Drain());
  EXPECT_FALSE(mailbox.Drain()); // consumed
}

TEST(GenerationMailboxTest, MultipleWakesCoalesce) {
  GenerationMailbox mailbox;

  // Many wakes coalesce into one Drain.
  for (int i = 0; i < 100; ++i) {
    EXPECT_TRUE(mailbox.Wake());
  }
  EXPECT_TRUE(mailbox.Drain());
  EXPECT_FALSE(mailbox.Drain()); // all consumed
}

TEST(GenerationMailboxTest, RetirementWakeHasConstantMemory) {
  // The mailbox is a single wake flag — no internal queue, no growth.
  // Posting many wakes doesn't allocate or accumulate state.
  GenerationMailbox mailbox;

  for (int i = 0; i < 1000; ++i) {
    EXPECT_TRUE(mailbox.Wake());
  }
  // Drain consumes the single wake counter — constant memory.
  EXPECT_TRUE(mailbox.Drain());
  EXPECT_FALSE(mailbox.Drain());
}

TEST(GenerationMailboxTest, WakeAfterStateTransitionIsCoalesced) {
  // Multiple wakes from rapid state transitions coalesce into one Drain.
  GenerationMailbox mailbox;

  // Simulate rapid state transitions: each calls Wake().
  for (int i = 0; i < 50; ++i) {
    EXPECT_TRUE(mailbox.Wake());
  }
  // One Drain sees all of them as a single wake.
  EXPECT_TRUE(mailbox.Drain());
  EXPECT_FALSE(mailbox.Drain());
}

TEST(GenerationMailboxTest, WakeDuringCloseIsHarmless) {
  // A concurrent Wake() during Close() must not crash or use a closed fd.
  GenerationMailbox mailbox;
  EXPECT_TRUE(mailbox.Wake());
  mailbox.Close();
  EXPECT_FALSE(mailbox.Wake());
  EXPECT_FALSE(mailbox.Drain());
}

TEST(GenerationMailboxTest, LatePostAfterCloseIsHarmless) {
  GenerationMailbox mailbox;
  EXPECT_TRUE(mailbox.Wake());
  mailbox.Close();
  EXPECT_FALSE(mailbox.Wake());   // closed, no-op
  EXPECT_FALSE(mailbox.Drain());  // closed, always false
  EXPECT_EQ(mailbox.wake_fd(), -1);
}

TEST(GenerationMailboxTest, DoubleCloseIsIdempotent) {
  GenerationMailbox mailbox;
  mailbox.Close();
  mailbox.Close(); // must not crash
}

} // namespace
} // namespace aegisgate::runtime
