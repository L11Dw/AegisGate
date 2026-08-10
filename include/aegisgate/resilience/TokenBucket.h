#pragma once

#include <chrono>
#include <cstdint>

namespace aegisgate::resilience {

// A single-event-loop token bucket.  Callers supply the clock value so tests
// and event-loop code never depend on wall-clock scheduling.
class TokenBucket {
public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;

  TokenBucket(std::uint32_t rate_per_second, std::uint32_t burst,
              TimePoint now);

  // Returns false without consuming any accrued credit when fewer than one
  // complete token is available.  A time point earlier than the last observed
  // point never refills the bucket.
  [[nodiscard]] bool TryAcquire(TimePoint now) noexcept;

private:
  static constexpr std::int64_t kCreditScale = 1'000'000'000;

  std::uint32_t rate_per_second_;
  std::int64_t capacity_credit_;
  std::int64_t credit_;
  TimePoint last_refill_;
};

} // namespace aegisgate::resilience
