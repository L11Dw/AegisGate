#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "aegisgate/health/Coordinator.h"
#include "aegisgate/resilience/GlobalAdmission.h"
#include "aegisgate/runtime/ConfigSnapshot.h"
#include "aegisgate/runtime/SelectionState.h"

namespace aegisgate::runtime {

// Immutable runtime resources for one reload generation will be attached here
// in subsequent M4-A steps.  This base establishes the ownership boundary
// first: every ProxyTransaction holds one request lease for its entire
// lifetime, so a retiring generation is never torn down between attempts.
class RuntimeGeneration : public std::enable_shared_from_this<RuntimeGeneration> {
public:
  enum class RetirementState : std::uint8_t { kActive, kRetiring, kReaping, kDone };

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

  explicit RuntimeGeneration(std::uint64_t version) : version_(version) {}
  // Builds the complete immutable/control-plane bundle for one generation.
  // The Coordinator is deliberately constructed but not started here: the
  // Gateway control loop owns the prepare/publish/rollback transaction and
  // starts it only after every worker-local selection state is ready.
  RuntimeGeneration(std::uint64_t version, config::Config config);

  [[nodiscard]] std::uint64_t version() const noexcept { return version_; }
  [[nodiscard]] const ConfigSnapshotRef &snapshot() const noexcept { return snapshot_; }
  [[nodiscard]] const std::shared_ptr<health::Coordinator> &coordinator() const noexcept {
    return coordinator_;
  }
  [[nodiscard]] const std::vector<std::shared_ptr<resilience::GlobalAdmission>> &
  admissions() const noexcept {
    return admissions_;
  }
  // One SelectionState exists for each fixed I/O worker.  It is only ever
  // touched by that worker after publication; the shared pointer exists so
  // an in-flight request can retain its old state across reload.
  [[nodiscard]] const std::vector<std::shared_ptr<SelectionState>> &selection_states() const noexcept {
    return selection_states_;
  }
  [[nodiscard]] std::optional<RequestLease> TryAcquireRequestLease();
  // Called only by the Gateway control loop.  The callback must merely notify
  // that same loop through its control mailbox; it must not tear down owner
  // resources on the releasing worker thread.
  [[nodiscard]] bool BeginRetirement(std::function<void()> on_last_lease);
  [[nodiscard]] std::uint64_t active_request_leases() const noexcept {
    return active_request_leases_.load(std::memory_order_acquire);
  }
  [[nodiscard]] RetirementState retirement_state() const noexcept {
    return retirement_state_.load(std::memory_order_acquire);
  }

private:
  void ReleaseRequestLease() noexcept;

  std::uint64_t version_;
  ConfigSnapshotRef snapshot_;
  std::shared_ptr<health::Coordinator> coordinator_;
  std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions_;
  std::vector<std::shared_ptr<SelectionState>> selection_states_;
  std::atomic<std::uint64_t> active_request_leases_{0};
  std::atomic<RetirementState> retirement_state_{RetirementState::kActive};
  std::mutex retirement_mutex_;
  std::function<void()> on_last_lease_;
  bool retirement_notified_ = false;
};

using RuntimeGenerationRef = std::shared_ptr<RuntimeGeneration>;

} // namespace aegisgate::runtime
