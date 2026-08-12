#include "aegisgate/runtime/RuntimeGeneration.h"

#include <utility>

namespace aegisgate::runtime {

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
