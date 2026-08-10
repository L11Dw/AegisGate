#include "aegisgate/resilience/RouteAdmission.h"

namespace aegisgate::resilience {

RouteAdmission::RouteAdmission(const config::Route &route, const TimePoint now)
    : bucket_(route.rate_limit, route.burst, now), limiter_(route.max_inflight) {}

std::optional<InflightLimiter::Reservation>
RouteAdmission::TryAcquire(const TimePoint now) noexcept {
  auto reservation = limiter_.Acquire();
  if (!reservation) return std::nullopt;
  if (!bucket_.TryAcquire(now)) return std::nullopt;
  return std::optional<InflightLimiter::Reservation>(std::move(reservation));
}

} // namespace aegisgate::resilience
