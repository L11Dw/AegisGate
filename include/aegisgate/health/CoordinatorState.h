#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "aegisgate/config/Config.h"
#include "aegisgate/health/EndpointHealth.h"
#include "aegisgate/health/ProtectionSnapshot.h"
#include "aegisgate/resilience/CircuitBreaker.h"

namespace aegisgate::health {

// One HalfOpen cycle's immutable probe slot object (R-058).  Workers claim
// from the object their snapshot holds, so a permit is always consistent with
// the snapshot it was requested from; the coordinator replaces the object on
// the next cycle.  The counters are atomic (claimable from any worker), the
// base/generation/quota are fixed at arm time.
struct ProbeSlotState {
  // mutable: workers claim through a shared_ptr<const ProbeSlotState>; the
  // counters are the only mutating part of an otherwise immutable cycle.
  mutable std::atomic<std::uint32_t> remaining{0};
  mutable std::atomic<std::uint64_t> issued{0};
  std::uint64_t probe_base = 0;
  std::uint64_t generation = 0;
  std::uint32_t quota = 0;
};

// One route x endpoint decision as published to every worker.  breaker_state
// is the CircuitBreaker::State enum value (0 closed / 1 open / 2 half_open);
// probe_slots is the per-cycle claim object this snapshot was built with (null
// when the endpoint is not half-open).
struct EndpointDecision {
  bool healthy = true;
  std::uint8_t breaker_state = 0;
  std::uint64_t generation = 0;
  std::uint64_t probe_base = 0;
  std::uint32_t probe_quota = 0;
  std::shared_ptr<const ProbeSlotState> probe_slots;
};

// Immutable, versioned coordination snapshot.  Published with
// std::atomic_store on a shared_ptr; readers never hold a plain copy racing
// a concurrent publish.
struct HealthCircuitSnapshot {
  std::uint64_t version = 0;
  std::vector<std::vector<EndpointDecision>> endpoints;
};

// The license a worker hands back with an attempt outcome.  probe is set when
// the attempt was admitted as a half-open probe; probe_id is valid then only.
struct AttemptPermit {
  bool probe = false;
  std::uint64_t generation = 0;
  std::uint64_t probe_id = 0;
};

struct AttemptResult {
  std::size_t route_index = 0;
  std::size_t endpoint_index = 0;
  AttemptPermit permit;
  bool success = false;
};

// C1' single-writer health/breaker state machine.  Every state transition
// (bucket accounting, Open/HalfOpen/Close, health commits, probe pre-issue)
// happens on the coordinator loop; workers only submit results, claim probe
// slots atomically and read published snapshots.  The underlying
// CircuitBreaker objects are never shared with workers.
class CoordinatorState {
public:
  using Clock = std::chrono::steady_clock;

  explicit CoordinatorState(std::shared_ptr<const config::Config> config, Clock::time_point now);

  CoordinatorState(const CoordinatorState &) = delete;
  CoordinatorState &operator=(const CoordinatorState &) = delete;

  // --- coordinator-loop (single writer) ---
  void RecordHealth(std::size_t route, std::size_t endpoint, bool healthy);
  // Import a 4-state HealthState (P1 #4).
  void ImportHealthState(std::size_t route, std::size_t endpoint, HealthState state);
  void RecordResult(const AttemptResult &result, Clock::time_point now);
  // Transitions an elapsed Open window to HalfOpen and pre-issues the full
  // probe quota so worker probe ids can be validated exactly once.
  void ArmHalfOpen(std::size_t route, std::size_t endpoint, Clock::time_point now);
  // Import a breaker snapshot (P1 #2).  Identity/policy must already match.
  // For Open/HalfOpen, the caller must subsequently call
  // CreateFreshHalfOpenCycle if the import results in HalfOpen state.
  void ImportBreakerSnapshot(std::size_t route, std::size_t endpoint,
                             const resilience::CircuitBreakerSnapshot &snap,
                             Clock::time_point now);
  // Create a fresh HalfOpen cycle for the given endpoint.  This is the
  // single place that transitions a breaker to HalfOpen, pre-issues the
  // full probe quota, and creates the corresponding ProbeSlotState.
  // The slot's probe_base and probe ids are in 1:1 correspondence with
  // the breaker's pending_probes_.
  void CreateFreshHalfOpenCycle(std::size_t route, std::size_t endpoint,
                                Clock::time_point now);
  // Export the full protection snapshot (P1 #2).
  [[nodiscard]] ProtectionSnapshot ExportProtectionSnapshot();

  // --- any thread (atomics only) ---
  [[nodiscard]] std::shared_ptr<const HealthCircuitSnapshot> BuildSnapshot();
  [[nodiscard]] std::optional<AttemptPermit>
  ClaimProbe(std::size_t route, std::size_t endpoint,
             const HealthCircuitSnapshot &snapshot) noexcept;
  [[nodiscard]] bool ProbeAvailable(std::size_t route, std::size_t endpoint) const noexcept;

  // --- coordinator-loop observation for the runtime's arm scheduling ---
  [[nodiscard]] bool IsOpen(std::size_t route, std::size_t endpoint) const noexcept;
  [[nodiscard]] Clock::time_point OpenUntil(std::size_t route, std::size_t endpoint) const noexcept;

  // --- observation (tests; single-threaded use) ---
  [[nodiscard]] std::size_t RouteCount() const noexcept;
  [[nodiscard]] std::size_t EndpointCount(std::size_t route) const noexcept;
  [[nodiscard]] bool Healthy(std::size_t route, std::size_t endpoint) const noexcept;
  [[nodiscard]] resilience::CircuitBreaker::State
  BreakerState(std::size_t route, std::size_t endpoint) const noexcept;
  [[nodiscard]] std::uint64_t Generation(std::size_t route, std::size_t endpoint) const noexcept;

private:
  struct EndpointState {
    health::EndpointHealth health;
    std::unique_ptr<resilience::CircuitBreaker> breaker;
    // The current HalfOpen cycle's probe slot object; published by the
    // coordinator loop, read by workers (atomics only).
    std::atomic<std::shared_ptr<const ProbeSlotState>> probe_slots{nullptr};
  };

  std::shared_ptr<const config::Config> config_;
  std::vector<std::vector<EndpointState>> endpoints_;
  std::uint64_t version_ = 0;
};

} // namespace aegisgate::health
