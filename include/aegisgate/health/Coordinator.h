#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "aegisgate/config/Config.h"
#include "aegisgate/health/CoordinatorState.h"
#include "aegisgate/health/HealthChecker.h"
#include "aegisgate/health/OutcomeChannel.h"
#include "aegisgate/health/ProtectionSnapshot.h"
#include "aegisgate/resilience/GlobalAdmission.h"
#include "aegisgate/runtime/WorkerRuntime.h"

namespace aegisgate::net {
class Channel;
class EventLoop;
class TimerQueue;
} // namespace aegisgate::net

namespace aegisgate::health {

// C1' global coordination runtime.  One coordinator thread owns every
// health/breaker transition and the rate-limit refill ticks; workers submit
// attempt results as value tasks, claim half-open probe slots atomically and
// read the published immutable snapshot.  The worker's request path never
// shares a CircuitBreaker/EndpointHealth object.
class Coordinator {
public:
  using Clock = std::chrono::steady_clock;

  Coordinator(std::shared_ptr<const config::Config> config, Clock::time_point now);
  ~Coordinator();

  Coordinator(const Coordinator &) = delete;
  Coordinator &operator=(const Coordinator &) = delete;

  // Admissions whose credit the coordinator loop refills every tick.  Must be
  // set before Start(); may be empty (no rate-limit refill).
  void SetAdmissions(std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions);
  // Spawns the coordinator thread and blocks until the loop-attached objects
  // (timer queue, health checkers, refill tick) are initialized.
  void Start();
  void StartPrepared();
  [[nodiscard]] bool ImportProtectionSnapshotAndWait(
      const ProtectionSnapshot &snapshot, std::chrono::milliseconds timeout);
  [[nodiscard]] bool Activate();
  [[nodiscard]] std::optional<ProtectionSnapshot>
  ExportProtectionSnapshotAndWait(std::chrono::milliseconds timeout);
  // Destroys loop-attached objects on the coordinator thread, then stops the
  // runtime and joins.  Idempotent.
  void Stop() noexcept;

  // --- worker side (any thread) ---
  [[nodiscard]] std::shared_ptr<const HealthCircuitSnapshot> CurrentSnapshot() const noexcept;
  // Fire-and-forget result submission; false when the queue is full or the
  // coordinator is stopping (the result is dropped, never double-applied).
  // Kept for the test seams below; production breaker accounting goes through
  // the per-route OutcomeChannel (see ReserveOutcome).
  [[nodiscard]] bool PostResult(const AttemptResult &result) noexcept;
  // Worker-side outcome reservation (R-053): claims one result slot on the
  // route's OutcomeChannel before an accounted attempt may connect.  nullopt
  // when the route has no breaker, the channel is stopping or its credit is
  // exhausted — the route cannot safely start another breaker-accounted
  // attempt (caller answers 503/terminates the retry without connecting).
  [[nodiscard]] std::optional<OutcomeChannel::Reservation>
  ReserveOutcome(std::size_t route_index) noexcept;
  [[nodiscard]] bool ProbeAvailable(std::size_t route, std::size_t endpoint) const noexcept;
  [[nodiscard]] std::optional<AttemptPermit>
  ClaimProbe(std::size_t route, std::size_t endpoint,
             const HealthCircuitSnapshot &snapshot) noexcept;

  // --- shutdown orchestration (R-062) ---
  // Rejects every future reservation; already-published outcomes stay
  // drainable.  Called before workers tear down their clients.
  void BeginOutcomeStopping() noexcept;
  // Drains every route's outcome ring on the coordinator loop and blocks until
  // done; called after all workers have joined, before Stop().  Throws
  // std::logic_error when the coordinator loop cannot accept the drain task
  // (it is stuck) so the failure is explicit, never silently skipped.
  void DrainOutcomesAndWait();
  // Sum of outcome_reservation_rejected_total across routes (R-061).
  [[nodiscard]] std::uint64_t OutcomeRejectedTotal() const noexcept;

  // --- test seams (block until the coordinator processed the value) ---
  void SubmitResultAndWait(const AttemptResult &result);
  void RecordHealthAndWait(std::size_t route, std::size_t endpoint, bool healthy);

  [[nodiscard]] std::size_t RouteCount() const noexcept;
  [[nodiscard]] std::size_t EndpointCount(std::size_t route) const noexcept;

private:
  struct LoopData {
    std::unique_ptr<net::TimerQueue> timers;
    std::vector<std::unique_ptr<health::HealthChecker>> checkers;
    std::vector<std::vector<net::TimerQueue::TimerId>> arm_timers;
    // One Channel per outcome channel's wake descriptor; the callback drains
    // that channel and republishes the snapshot.
    std::vector<std::unique_ptr<net::Channel>> outcome_wake_channels;
  };

  void OnLoopInit(net::EventLoop &loop);
  void OnPreparedLoopInit(net::EventLoop &loop);
  void OnActivateLoopInit(net::EventLoop &loop);
  void RecordResultTask(const AttemptResult &result);
  void RecordResultDirect(const AttemptResult &result);
  void RecordHealthTask(std::size_t route, std::size_t endpoint, bool healthy);
  void ScheduleArm(std::size_t route, std::size_t endpoint);
  void ScheduleRefillTick(net::TimerQueue &timers);
  void DrainOneOutcomeChannel(OutcomeChannel &channel);
  void DrainAllOutcomeChannels();
  void Publish() noexcept;
  [[nodiscard]] bool PostTask(std::function<void()> task) noexcept;

  std::shared_ptr<const config::Config> config_;
  std::unique_ptr<CoordinatorState> state_;
  std::unique_ptr<runtime::WorkerRuntime> runtime_;
  std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions_;
  // One OutcomeChannel per breaker-configured route (nullptr otherwise).  The
  // reservation accounting bounds the ring to the route's in-flight attempts.
  std::vector<std::unique_ptr<OutcomeChannel>> outcome_channels_;
  std::atomic<std::shared_ptr<const HealthCircuitSnapshot>> snapshot_{nullptr};
  std::unique_ptr<LoopData> loop_data_;
  enum class Lifecycle : std::uint8_t { kNew, kPrepared, kActive, kStopping, kStopped };
  // Preserve the M3-D construction-time test/API behavior: a coordinator
  // without a runtime loop may still reserve value-owned outcome credits.
  // StartPrepared() changes this to kPrepared before a candidate is exposed.
  std::atomic<Lifecycle> lifecycle_{Lifecycle::kActive};
  bool prepared_only_ = false;
};

} // namespace aegisgate::health
