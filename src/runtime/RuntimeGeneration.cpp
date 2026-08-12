#include "aegisgate/runtime/RuntimeGeneration.h"

#include <utility>

namespace aegisgate::runtime {

RuntimeGeneration::RuntimeGeneration(std::uint64_t version, config::Config config)
    : version_(version), snapshot_(std::make_shared<ConfigSnapshot>(
                            ConfigSnapshot{version, std::move(config)})),
      coordinator_(std::make_shared<health::Coordinator>(
          std::make_shared<const config::Config>(snapshot_->config),
          health::Coordinator::Clock::now())) {
  const auto now = resilience::GlobalAdmission::Clock::now();
  admissions_.reserve(snapshot_->config.routes.size());
  for (const config::Route &route : snapshot_->config.routes) {
    admissions_.push_back(std::make_shared<resilience::GlobalAdmission>(route, now));
  }
  coordinator_->SetAdmissions(admissions_);
  selection_states_.reserve(snapshot_->config.workers);
  for (std::uint32_t worker = 0; worker < snapshot_->config.workers; ++worker) {
    selection_states_.push_back(
        std::make_shared<SelectionState>(snapshot_->config, snapshot_->version));
  }
}

RuntimeGeneration::RequestLease::RequestLease(RequestLease &&other) noexcept
    : generation_(std::move(other.generation_)) {}

RuntimeGeneration::RequestLease &RuntimeGeneration::RequestLease::operator=(RequestLease &&other) noexcept {
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

std::optional<RuntimeGeneration::RequestLease> RuntimeGeneration::TryAcquireRequestLease() {
  std::lock_guard<std::mutex> guard(retirement_mutex_);
  if (retirement_state_.load(std::memory_order_acquire) != RetirementState::kActive) {
    return std::nullopt;
  }
  active_request_leases_.fetch_add(1, std::memory_order_release);
  return RequestLease(shared_from_this());
}

// ---------------------------------------------------------------------------
// 6-state retirement machine.
// Callbacks fire outside the lock to prevent lock inversion.
// ---------------------------------------------------------------------------

void RuntimeGeneration::SetStateChangeCallback(StateChangeCallback cb) {
  std::lock_guard<std::mutex> guard(retirement_mutex_);
  on_state_change_ = std::move(cb);
}

bool RuntimeGeneration::BeginRetirement(std::function<void()> on_last_lease) {
  std::function<void()> notify;
  StateChangeCallback cb;
  {
    std::lock_guard<std::mutex> guard(retirement_mutex_);
    if (retirement_state_.load(std::memory_order_acquire) != RetirementState::kActive) {
      return false;
    }
    retirement_state_.store(RetirementState::kRetiring, std::memory_order_release);
    on_last_lease_ = std::move(on_last_lease);
    cb = on_state_change_;
    if (active_request_leases_.load(std::memory_order_acquire) == 0 && !retirement_notified_) {
      retirement_notified_ = true;
      notify = on_last_lease_;
    }
  }
  if (cb) cb(RetirementState::kRetiring);
  if (notify) notify();
  return true;
}

bool RuntimeGeneration::NotifyCheckersStopped() {
  std::vector<Transition> transitions;
  {
    std::lock_guard<std::mutex> guard(retirement_mutex_);
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
  {
    std::lock_guard<std::mutex> guard(retirement_mutex_);
    if (retirement_state_.load(std::memory_order_acquire) != RetirementState::kWaitingForLeases) {
      return false;
    }
    if (active_request_leases_.load(std::memory_order_acquire) != 0) {
      return false;
    }
    retirement_state_.store(RetirementState::kOutcomeDraining, std::memory_order_release);
    cb = on_state_change_;
  }
  if (cb) cb(RetirementState::kOutcomeDraining);
  return true;
}

bool RuntimeGeneration::BeginReaping() noexcept {
  StateChangeCallback cb;
  {
    std::lock_guard<std::mutex> guard(retirement_mutex_);
    const auto state = retirement_state_.load(std::memory_order_acquire);
    // Accept from kCheckersStopped (leases already zero) or kWaitingForLeases.
    if (state != RetirementState::kCheckersStopped &&
        state != RetirementState::kWaitingForLeases) {
      return false;
    }
    // If still in kCheckersStopped, advance to kWaitingForLeases first.
    if (state == RetirementState::kCheckersStopped) {
      retirement_state_.store(RetirementState::kWaitingForLeases, std::memory_order_release);
    }
    retirement_state_.store(RetirementState::kOutcomeDraining, std::memory_order_release);
    cb = on_state_change_;
  }
  if (cb) cb(RetirementState::kOutcomeDraining);
  return true;
}

void RuntimeGeneration::MarkRetired() noexcept {
  StateChangeCallback cb;
  {
    std::lock_guard<std::mutex> guard(retirement_mutex_);
    if (retirement_state_.load(std::memory_order_acquire) == RetirementState::kOutcomeDraining) {
      retirement_state_.store(RetirementState::kDone, std::memory_order_release);
      cb = on_state_change_;
    }
  }
  if (cb) cb(RetirementState::kDone);
}

void RuntimeGeneration::MarkCoordinatorStopped() noexcept {
  coordinator_stopped_.store(true, std::memory_order_release);
}

void RuntimeGeneration::ReleaseRequestLease() noexcept {
  std::vector<Transition> transitions;
  std::function<void()> notify;
  {
    std::lock_guard<std::mutex> guard(retirement_mutex_);
    const std::uint64_t previous = active_request_leases_.fetch_sub(1, std::memory_order_acq_rel);
    if (previous == 0) std::terminate();
    if (previous == 1) {
      AdvanceIfReady(transitions);
      if (retirement_state_.load(std::memory_order_acquire) == RetirementState::kRetiring &&
          !retirement_notified_) {
        retirement_notified_ = true;
        notify = on_last_lease_;
      }
    }
  }
  for (const auto &[cb, s] : transitions) {
    if (cb) cb(s);
  }
  if (notify) notify();
}

// Must be called under retirement_mutex_.  Advances through:
//   kRetiring -> kCheckersStopped -> kWaitingForLeases
void RuntimeGeneration::AdvanceIfReady(std::vector<Transition> &transitions) noexcept {
  auto state = retirement_state_.load(std::memory_order_acquire);

  if (state == RetirementState::kRetiring && checkers_stopped_) {
    retirement_state_.store(RetirementState::kCheckersStopped, std::memory_order_release);
    transitions.push_back({on_state_change_, RetirementState::kCheckersStopped});
    state = RetirementState::kCheckersStopped;
  }

  if (state == RetirementState::kCheckersStopped &&
      active_request_leases_.load(std::memory_order_acquire) == 0) {
    retirement_state_.store(RetirementState::kWaitingForLeases, std::memory_order_release);
    transitions.push_back({on_state_change_, RetirementState::kWaitingForLeases});
  }
}

} // namespace aegisgate::runtime
