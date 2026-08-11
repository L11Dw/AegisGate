#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <stdexcept>

namespace aegisgate::resilience {

// Startup-time value object for one route x endpoint breaker.  The window is
// the fixed statistical horizon, min_requests the sample floor before the
// failure rate may open the breaker, failure_threshold_permille the rate
// (1..999) that trips it, open_duration how long ordinary selection is
// refused, and half_open_probes how many probe requests may pass once the
// open window elapses.
struct CircuitBreakerConfig {
  std::chrono::milliseconds window{};
  std::uint32_t min_requests{};
  std::uint32_t failure_threshold_permille{};
  std::chrono::milliseconds open_duration{};
  std::uint32_t half_open_probes{};
};

// Single-EventLoop circuit breaker scoped to one route x endpoint.
// Closed -> Open -> HalfOpen -> Closed; ordinary successes never close an
// Open breaker, only successful probes reaching the full probe quota do.
// All methods take an injectable steady-clock time point so tests never
// sleep.
class CircuitBreaker {
public:
  using Clock = std::chrono::steady_clock;
  enum class State { kClosed, kOpen, kHalfOpen };
  enum class Selection { kAllowed, kRejectedOpen, kRejectedHalfOpenQuota, kProbe };

  explicit CircuitBreaker(CircuitBreakerConfig config, Clock::time_point now);

  // Passive request outcomes.  Callers decide what counts: 429/404 and
  // client-closed requests must not be reported as failures (and therefore
  // never reach these methods).
  void RecordSuccess(Clock::time_point now);
  void RecordFailure(Clock::time_point now);

  // Selection decision for a normal request.  A probe result is completed
  // through the Record methods; the total deadline guarantees every probe
  // eventually produces a result, so the half-open quota cannot stall.
  [[nodiscard]] Selection Select(Clock::time_point now);
  [[nodiscard]] State StateNow() const noexcept { return state_; }

private:
  struct Bucket {
    Clock::time_point start{};
    std::uint32_t success = 0;
    std::uint32_t failure = 0;
  };
  static constexpr std::size_t kBucketCount = 10;

  void AdvanceTo(Clock::time_point now);
  void Evaluate(Clock::time_point now);
  [[nodiscard]] std::pair<std::uint64_t, std::uint64_t> Aggregate(
      Clock::time_point now) const noexcept;
  void Open(Clock::time_point now) noexcept;
  void Close() noexcept;

  CircuitBreakerConfig config_;
  State state_ = State::kClosed;
  Clock::time_point epoch_;
  Clock::time_point active_start_{};
  std::size_t active_index_ = 0;
  bool initialized_ = false;
  std::array<Bucket, kBucketCount> buckets_{};
  Clock::time_point open_until_{};
  std::uint32_t half_open_issued_ = 0;
  std::uint32_t half_open_completed_ = 0;
};

} // namespace aegisgate::resilience
