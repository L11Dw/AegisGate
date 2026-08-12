#include "aegisgate/runtime/RuntimeGeneration.h"

#include <utility>

namespace aegisgate::runtime {

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

RuntimeGeneration::RuntimeGeneration(
    std::uint64_t version,
    std::shared_ptr<const ConfigSnapshot> snapshot,
    std::shared_ptr<health::Coordinator> coordinator,
    std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions,
    std::vector<std::shared_ptr<SelectionState>> selection_states)
    : version_(version),
      snapshot_(std::move(snapshot)),
      coordinator_(std::move(coordinator)),
      admissions_(std::move(admissions)),
      selection_states_(std::move(selection_states)) {}

// ---------------------------------------------------------------------------
// RequestLease
// ---------------------------------------------------------------------------

RuntimeGeneration::RequestLease::RequestLease(RequestLease &&other) noexcept
    : generation_(std::move(other.generation_)) {}

RuntimeGeneration::RequestLease &
RuntimeGeneration::RequestLease::operator=(RequestLease &&other) noexcept {
  if (this == &other) return *this;
  Release();
  generation_ = std::move(other.generation_);
  return *this;
}

RuntimeGeneration::RequestLease::~RequestLease() { Release(); }

void RuntimeGeneration::RequestLease::Release() noexcept {
  auto gen = std::move(generation_);
  if (gen) gen->ReleaseRequestLease();
}

// ---------------------------------------------------------------------------
// Lease acquisition
// ---------------------------------------------------------------------------

std::optional<RuntimeGeneration::RequestLease>
RuntimeGeneration::TryAcquireRequestLease() {
  std::lock_guard<std::mutex> guard(retirement_mu_);
  if (retirement_state_.load(std::memory_order_acquire) != RetirementState::kActive) {
    return std::nullopt;
  }
  active_request_leases_.fetch_add(1, std::memory_order_release);
  return RequestLease(shared_from_this());
}

// ---------------------------------------------------------------------------
// Retirement state machine
//
// Every public method follows the same pattern:
//   1. Lock retirement_mu_.
//   2. Perform the state transition.
//   3. Copy on_state_change_ and the new state to locals.
//   4. Unlock.
//   5. Fire the callback (if set).
//
// This guarantees the callback never runs under the lock, preventing
// lock inversion if the callback calls back into the mailbox or gateway.
// ---------------------------------------------------------------------------

void RuntimeGeneration::SetStateChangeCallback(StateChangeCallback cb) {
  std::lock_guard<std::mutex> guard(retirement_mu_);
  on_state_change_ = std::move(cb);
}

bool RuntimeGeneration::BeginRetirement() {
  StateChangeCallback cb;
  RetirementState new_state;
  {
    std::lock_guard<std::mutex> guard(retirement_mu_);
    if (retirement_state_.load(std::memory_order_acquire) != RetirementState::kActive) {
      return false;
    }
    // Always enter kRetiring.  Even when leases are already zero, the caller
    // must still stop checkers before the generation can advance — health
    // checkers are periodic and will keep producing probes until stopped.
    retirement_state_.store(RetirementState::kRetiring, std::memory_order_release);
    cb = on_state_change_;
    new_state = RetirementState::kRetiring;
  }
  if (cb) cb(new_state);
  return true;
}

bool RuntimeGeneration::NotifyCheckersStopped() {
  std::vector<std::pair<StateChangeCallback, RetirementState>> transitions;
  {
    std::lock_guard<std::mutex> guard(retirement_mu_);
    const auto state = retirement_state_.load(std::memory_order_acquire);
    if (state != RetirementState::kRetiring && state != RetirementState::kCheckersStopped) {
      return false;
    }
    checkers_stopped_ = true;
    AdvanceIfReady(transitions);
  }
  for (const auto &[cb, s] : transitions) {
    if (cb) cb(s);
  }
  return true;
}

bool RuntimeGeneration::BeginOutcomeStopping() {
  StateChangeCallback cb;
  RetirementState new_state;
  {
    std::lock_guard<std::mutex> guard(retirement_mu_);
    const auto state = retirement_state_.load(std::memory_order_acquire);
    if (state != RetirementState::kWaitingForLeases) {
      return false;
    }
    if (active_request_leases_.load(std::memory_order_acquire) != 0) {
      return false;
    }
    retirement_state_.store(RetirementState::kOutcomeDraining, std::memory_order_release);
    cb = on_state_change_;
    new_state = RetirementState::kOutcomeDraining;
  }
  if (cb) cb(new_state);
  return true;
}

void RuntimeGeneration::MarkRetired() noexcept {
  StateChangeCallback cb;
  RetirementState new_state;
  {
    std::lock_guard<std::mutex> guard(retirement_mu_);
    if (retirement_state_.load(std::memory_order_acquire) == RetirementState::kOutcomeDraining) {
      retirement_state_.store(RetirementState::kDone, std::memory_order_release);
      cb = on_state_change_;
      new_state = RetirementState::kDone;
    }
  }
  if (cb) cb(new_state);
}

// ---------------------------------------------------------------------------
// Coordinator stopped flag (set by reaper thread)
// ---------------------------------------------------------------------------

void RuntimeGeneration::MarkCoordinatorStopped() noexcept {
  coordinator_stopped_.store(true, std::memory_order_release);
}

// ---------------------------------------------------------------------------
// Lease release
// ---------------------------------------------------------------------------

void RuntimeGeneration::ReleaseRequestLease() noexcept {
  std::vector<std::pair<StateChangeCallback, RetirementState>> transitions;
  {
    std::lock_guard<std::mutex> guard(retirement_mu_);
    const std::uint64_t previous =
        active_request_leases_.fetch_sub(1, std::memory_order_acq_rel);
    if (previous == 0) std::terminate();
    // When the last lease is released while in kCheckersStopped, auto-advance.
    if (previous == 1) {
      AdvanceIfReady(transitions);
    }
  }
  for (const auto &[cb, s] : transitions) {
    if (cb) cb(s);
  }
}

// ---------------------------------------------------------------------------
// Internal
// ---------------------------------------------------------------------------

// Must be called under retirement_mu_.  Collects transitions to fire
// after the lock is released.
// Advances through the mandatory ordering:
//   kRetiring -> kCheckersStopped -> kWaitingForLeases
// kCheckersStopped is always visited, even when leases are already zero.
void RuntimeGeneration::AdvanceIfReady(
    std::vector<std::pair<StateChangeCallback, RetirementState>> &transitions) noexcept {
  auto state = retirement_state_.load(std::memory_order_acquire);

  // From kRetiring: checkers stopped -> always kCheckersStopped first.
  if (state == RetirementState::kRetiring && checkers_stopped_) {
    retirement_state_.store(RetirementState::kCheckersStopped,
                            std::memory_order_release);
    transitions.push_back({on_state_change_, RetirementState::kCheckersStopped});
    state = RetirementState::kCheckersStopped;
    // Fall through to check lease drain.
  }

  // From kCheckersStopped: when the last lease drains -> kWaitingForLeases.
  if (state == RetirementState::kCheckersStopped &&
      active_request_leases_.load(std::memory_order_acquire) == 0) {
    retirement_state_.store(RetirementState::kWaitingForLeases,
                            std::memory_order_release);
    transitions.push_back({on_state_change_, RetirementState::kWaitingForLeases});
  }
}

} // namespace aegisgate::runtime
