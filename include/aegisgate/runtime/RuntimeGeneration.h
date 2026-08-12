#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "aegisgate/health/Coordinator.h"
#include "aegisgate/resilience/GlobalAdmission.h"
#include "aegisgate/runtime/ConfigSnapshot.h"
#include "aegisgate/runtime/SelectionState.h"

namespace aegisgate::runtime {

// One complete reload generation: immutable config snapshot, dedicated
// coordinator, per-generation admissions and per-worker selection states.
// The retirement state machine enforces the teardown ordering:
//   kRetiring -> kCheckersStopped -> kWaitingForLeases ->
//   kOutcomeDraining -> kDone
// Each transition is observable through the on_state_change callback so
// tests can assert the ordering without polling or sleeping.
class RuntimeGeneration : public std::enable_shared_from_this<RuntimeGeneration> {
public:
  enum class RetirementState : std::uint8_t {
    kActive,
    kRetiring,
    kCheckersStopped,
    kWaitingForLeases,
    kOutcomeDraining,
    kDone
  };

  // RAII request lease.  The generation cannot advance past
  // kWaitingForLeases while any lease is outstanding.
  class RequestLease {
  public:
    RequestLease() = default;
    RequestLease(const RequestLease &) = delete;
    RequestLease &operator=(const RequestLease &) = delete;
    RequestLease(RequestLease &&other) noexcept;
    RequestLease &operator=(RequestLease &&other) noexcept;
    ~RequestLease();

    void Release() noexcept;
    explicit operator bool() const noexcept { return generation_ != nullptr; }

  private:
    friend class RuntimeGeneration;
    explicit RequestLease(std::shared_ptr<RuntimeGeneration> generation) noexcept
        : generation_(std::move(generation)) {}
    std::shared_ptr<RuntimeGeneration> generation_;
  };

  // State change callback: fires under the retirement lock immediately
  // after every transition.  Receives the new state.
  using StateChangeCallback = std::function<void(RetirementState)>;

  // Builds the generation bundle.  The coordinator must already have its
  // admissions configured before construction.
  RuntimeGeneration(std::uint64_t version,
                    std::shared_ptr<const ConfigSnapshot> snapshot,
                    std::shared_ptr<health::Coordinator> coordinator,
                    std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions,
                    std::vector<std::shared_ptr<SelectionState>> selection_states);

  [[nodiscard]] std::uint64_t version() const noexcept { return version_; }
  [[nodiscard]] const std::shared_ptr<const ConfigSnapshot> &snapshot() const noexcept {
    return snapshot_;
  }
  [[nodiscard]] const std::shared_ptr<health::Coordinator> &coordinator() const noexcept {
    return coordinator_;
  }
  [[nodiscard]] const std::vector<std::shared_ptr<resilience::GlobalAdmission>> &
  admissions() const noexcept {
    return admissions_;
  }
  [[nodiscard]] const std::vector<std::shared_ptr<SelectionState>> &
  selection_states() const noexcept {
    return selection_states_;
  }

  // --- request lease (any thread) ---
  [[nodiscard]] std::optional<RequestLease> TryAcquireRequestLease();

  // --- retirement state machine (control loop only) ---
  [[nodiscard]] RetirementState retirement_state() const noexcept {
    return retirement_state_.load(std::memory_order_acquire);
  }
  [[nodiscard]] std::uint64_t active_request_leases() const noexcept {
    return active_request_leases_.load(std::memory_order_acquire);
  }

  void SetStateChangeCallback(StateChangeCallback cb);

  // Transition kActive -> kRetiring.  Returns false if already retiring.
  [[nodiscard]] bool BeginRetirement();
  // Transition kRetiring -> kCheckersStopped.  Returns false if not in
  // kRetiring.  Advances automatically to kWaitingForLeases or
  // kOutcomeDraining when the other precondition is also met.
  [[nodiscard]] bool NotifyCheckersStopped();
  // Transition kWaitingForLeases -> kOutcomeDraining.  Returns false if
  // not in kWaitingForLeases or leases are still outstanding.
  [[nodiscard]] bool BeginOutcomeStopping();
  // Transition kOutcomeDraining -> kDone.
  void MarkRetired() noexcept;

  // Set by the reaper thread after DrainOutcomesAndWait + Stop complete.
  // The control loop checks this to know when it's safe to call MarkRetired.
  // Thread-safe (atomic).
  void MarkCoordinatorStopped() noexcept;
  [[nodiscard]] bool coordinator_stopped() const noexcept {
    return coordinator_stopped_.load(std::memory_order_acquire);
  }

private:
  void ReleaseRequestLease() noexcept;
  using Transition = std::pair<StateChangeCallback, RetirementState>;
  void AdvanceIfReady(std::vector<Transition> &transitions) noexcept;

  std::uint64_t version_;
  std::shared_ptr<const ConfigSnapshot> snapshot_;
  std::shared_ptr<health::Coordinator> coordinator_;
  std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions_;
  std::vector<std::shared_ptr<SelectionState>> selection_states_;

  mutable std::mutex retirement_mu_;
  std::atomic<std::uint64_t> active_request_leases_{0};
  std::atomic<RetirementState> retirement_state_{RetirementState::kActive};
  std::atomic<bool> coordinator_stopped_{false};
  StateChangeCallback on_state_change_;
  bool checkers_stopped_ = false;
};

using RuntimeGenerationRef = std::shared_ptr<RuntimeGeneration>;

} // namespace aegisgate::runtime
