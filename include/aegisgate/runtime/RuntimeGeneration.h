#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

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

  [[nodiscard]] std::uint64_t version() const noexcept { return version_; }
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
  std::atomic<std::uint64_t> active_request_leases_{0};
  std::atomic<RetirementState> retirement_state_{RetirementState::kActive};
  std::mutex retirement_mutex_;
  std::function<void()> on_last_lease_;
  bool retirement_notified_ = false;
};

using RuntimeGenerationRef = std::shared_ptr<RuntimeGeneration>;

} // namespace aegisgate::runtime
