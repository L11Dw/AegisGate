#include "aegisgate/resilience/CircuitBreaker.h"

#include <gtest/gtest.h>

#include <chrono>

namespace aegisgate::resilience {
namespace {

using Clock = CircuitBreaker::Clock;

CircuitBreakerConfig MigrationConfig(std::uint32_t probes = 2) {
  return {std::chrono::milliseconds(100), 1, 500, std::chrono::milliseconds(50), probes};
}

} // namespace

TEST(CircuitBreakerMigrationTest, ImportedBreakerRejectsPermitFromSourceGeneration) {
  const auto now = Clock::now();
  CircuitBreaker source(MigrationConfig(), now);
  const auto old_permit = source.Select(now);
  const auto snapshot = source.ExportSnapshot(now);

  CircuitBreaker imported(MigrationConfig(), now);
  const auto cycle = imported.ImportSnapshot(snapshot, now);

  EXPECT_FALSE(cycle.has_value());
  EXPECT_NE(imported.Generation(), old_permit.generation);
  imported.RecordFailure(now, old_permit);
  EXPECT_EQ(imported.StateNow(), CircuitBreaker::State::kClosed);
}

TEST(CircuitBreakerMigrationTest, HalfOpenImportPublishesOneExactFreshProbeCycle) {
  const auto now = Clock::now();
  CircuitBreaker source(MigrationConfig(2), now);
  const auto ordinary = source.Select(now);
  source.RecordFailure(now, ordinary);
  const auto original_probe = source.Select(now + std::chrono::milliseconds(60));
  ASSERT_EQ(original_probe.selection, CircuitBreaker::Selection::kProbe);
  const auto snapshot = source.ExportSnapshot(now + std::chrono::milliseconds(61));
  ASSERT_EQ(snapshot.state, CircuitBreakerState::kHalfOpen);

  CircuitBreaker imported(MigrationConfig(2), now);
  const auto cycle = imported.ImportSnapshot(snapshot, now + std::chrono::milliseconds(62));

  ASSERT_TRUE(cycle.has_value());
  EXPECT_EQ(cycle->quota, 2U);
  EXPECT_EQ(cycle->generation, imported.Generation());
  EXPECT_EQ(imported.Select(now + std::chrono::milliseconds(62)).selection,
            CircuitBreaker::Selection::kRejectedHalfOpenQuota);

  imported.RecordSuccess(now + std::chrono::milliseconds(63),
                         {CircuitBreaker::Selection::kProbe, cycle->generation,
                          cycle->probe_base});
  EXPECT_EQ(imported.StateNow(), CircuitBreaker::State::kHalfOpen);
  imported.RecordSuccess(now + std::chrono::milliseconds(64),
                         {CircuitBreaker::Selection::kProbe, cycle->generation,
                          cycle->probe_base + 1});
  EXPECT_EQ(imported.StateNow(), CircuitBreaker::State::kClosed);
}

} // namespace aegisgate::resilience
