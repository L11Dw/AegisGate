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
  const auto generation = std::move(generation_);
  if (generation) generation->ReleaseRequestLease();
}

std::optional<RuntimeGeneration::RequestLease> RuntimeGeneration::TryAcquireRequestLease() {
  std::lock_guard<std::mutex> guard(retirement_mutex_);
  if (retirement_state_.load(std::memory_order_acquire) != RetirementState::kActive) {
    return std::nullopt;
  }
  active_request_leases_.fetch_add(1, std::memory_order_release);
  return RequestLease(shared_from_this());
}

bool RuntimeGeneration::BeginRetirement(std::function<void()> on_last_lease) {
  std::function<void()> notify;
  {
    std::lock_guard<std::mutex> guard(retirement_mutex_);
    if (retirement_state_.load(std::memory_order_acquire) != RetirementState::kActive) {
      return false;
    }
    retirement_state_.store(RetirementState::kRetiring, std::memory_order_release);
    on_last_lease_ = std::move(on_last_lease);
    if (active_request_leases_.load(std::memory_order_acquire) == 0 && !retirement_notified_) {
      retirement_notified_ = true;
      notify = on_last_lease_;
    }
  }
  if (notify) notify();
  return true;
}

bool RuntimeGeneration::BeginReaping() noexcept {
  std::lock_guard<std::mutex> guard(retirement_mutex_);
  if (retirement_state_.load(std::memory_order_acquire) != RetirementState::kRetiring) {
    return false;
  }
  retirement_state_.store(RetirementState::kReaping, std::memory_order_release);
  return true;
}

void RuntimeGeneration::MarkRetired() noexcept {
  std::lock_guard<std::mutex> guard(retirement_mutex_);
  if (retirement_state_.load(std::memory_order_acquire) == RetirementState::kReaping) {
    retirement_state_.store(RetirementState::kDone, std::memory_order_release);
  }
}

void RuntimeGeneration::ReleaseRequestLease() noexcept {
  std::function<void()> notify;
  {
    std::lock_guard<std::mutex> guard(retirement_mutex_);
    const std::uint64_t previous = active_request_leases_.fetch_sub(1, std::memory_order_acq_rel);
    if (previous == 0) std::terminate();
    if (previous == 1 &&
        retirement_state_.load(std::memory_order_acquire) == RetirementState::kRetiring &&
        !retirement_notified_) {
      retirement_notified_ = true;
      notify = on_last_lease_;
    }
  }
  if (notify) notify();
}

} // namespace aegisgate::runtime
