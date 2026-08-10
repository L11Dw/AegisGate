#include "aegisgate/resilience/TokenBucket.h"

#include <stdexcept>

namespace aegisgate::resilience {

TokenBucket::TokenBucket(const std::uint32_t rate_per_second,
                         const std::uint32_t burst, const TimePoint now)
    : rate_per_second_(rate_per_second),
      capacity_credit_(static_cast<std::int64_t>(burst) * kCreditScale),
      credit_(capacity_credit_), last_refill_(now) {
  if (rate_per_second == 0 || burst == 0) {
    throw std::invalid_argument("token bucket rate and burst must be positive");
  }
}

bool TokenBucket::TryAcquire(const TimePoint now) noexcept {
  if (now > last_refill_) {
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        now - last_refill_);
    const auto missing_credit = capacity_credit_ - credit_;
    const auto rate = static_cast<std::int64_t>(rate_per_second_);
    // A fractional credit is not a token: refill to capacity only after the
    // first nanosecond that supplies all missing credit.
    const auto needed_nanoseconds = (missing_credit + rate - 1) / rate;
    if (elapsed.count() >= needed_nanoseconds) {
      credit_ = capacity_credit_;
    } else {
      credit_ += rate * elapsed.count();
    }
    last_refill_ = now;
  }

  if (credit_ < kCreditScale) return false;
  credit_ -= kCreditScale;
  return true;
}

} // namespace aegisgate::resilience
