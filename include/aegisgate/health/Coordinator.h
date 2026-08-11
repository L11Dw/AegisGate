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
#include "aegisgate/resilience/GlobalAdmission.h"
#include "aegisgate/runtime/WorkerRuntime.h"

namespace aegisgate::net {
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
  // Destroys loop-attached objects on the coordinator thread, then stops the
  // runtime and joins.  Idempotent.
  void Stop() noexcept;

  // --- worker side (any thread) ---
  [[nodiscard]] std::shared_ptr<const HealthCircuitSnapshot> CurrentSnapshot() const noexcept;
  // Fire-and-forget result submission; false when the queue is full or the
  // coordinator is stopping (the result is dropped, never double-applied).
  [[nodiscard]] bool PostResult(const AttemptResult &result) noexcept;
  [[nodiscard]] bool ProbeAvailable(std::size_t route, std::size_t endpoint) const noexcept;
  [[nodiscard]] std::optional<AttemptPermit>
  ClaimProbe(std::size_t route, std::size_t endpoint,
             const HealthCircuitSnapshot &snapshot) noexcept;

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
  };

  void OnLoopInit(net::EventLoop &loop);
  void RecordResultTask(const AttemptResult &result);
  void RecordHealthTask(std::size_t route, std::size_t endpoint, bool healthy);
  void ScheduleArm(std::size_t route, std::size_t endpoint);
  void ScheduleRefillTick(net::TimerQueue &timers);
  void Publish() noexcept;
  [[nodiscard]] bool PostTask(std::function<void()> task) noexcept;

  std::shared_ptr<const config::Config> config_;
  std::unique_ptr<CoordinatorState> state_;
  std::unique_ptr<runtime::WorkerRuntime> runtime_;
  std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions_;
  std::atomic<std::shared_ptr<const HealthCircuitSnapshot>> snapshot_{nullptr};
  std::unique_ptr<LoopData> loop_data_;
};

} // namespace aegisgate::health
