#pragma once

#include <chrono>
#include <memory>
#include <optional>

#include "aegisgate/config/Config.h"
#include "aegisgate/resilience/InflightLimiter.h"
#include "aegisgate/resilience/TokenBucket.h"

namespace aegisgate::resilience {

// Long-lived, route-scoped admission state.  It is deliberately owned by the
// route table rather than reconstructed for every Match().
class RouteAdmission {
public:
  using TimePoint = TokenBucket::TimePoint;

  explicit RouteAdmission(const config::Route &route, TimePoint now);

  // Single EventLoop-thread operation: reserve concurrency first, then spend
  // one rate token.  A rate rejection releases the temporary reservation.
  [[nodiscard]] std::optional<InflightLimiter::Reservation> TryAcquire(TimePoint now) noexcept;

private:
  TokenBucket bucket_;
  InflightLimiter limiter_;
};

} // namespace aegisgate::resilience
