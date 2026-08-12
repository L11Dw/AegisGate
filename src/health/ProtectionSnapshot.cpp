#include "aegisgate/health/ProtectionSnapshot.h"

namespace aegisgate::health {

RouteIdentity RouteKey(const config::Route &route) {
  return {route.name, route.host, route.path_prefix};
}

EndpointIdentity EndpointKey(const config::Endpoint &endpoint) {
  return {endpoint.host, endpoint.address, endpoint.port};
}

bool SameRouteIdentity(const RouteIdentity &key, const config::Route &route) noexcept {
  return key.name == route.name && key.host == route.host && key.path_prefix == route.path_prefix;
}

bool SameEndpointIdentity(const EndpointIdentity &key, const config::Endpoint &endpoint) noexcept {
  return key.host == endpoint.host && key.address == endpoint.address && key.port == endpoint.port;
}

bool SameHealthPolicy(const std::optional<config::HealthCheckSettings> &left,
                      const std::optional<config::HealthCheckSettings> &right) noexcept {
  return left.has_value() == right.has_value() &&
         (!left || (left->interval_ms == right->interval_ms && left->timeout_ms == right->timeout_ms));
}

bool SameBreakerPolicy(const std::optional<config::CircuitBreakerSettings> &left,
                       const std::optional<config::CircuitBreakerSettings> &right) noexcept {
  return left.has_value() == right.has_value() &&
         (!left || (left->window_seconds == right->window_seconds &&
                    left->min_requests == right->min_requests &&
                    left->failure_threshold_permille == right->failure_threshold_permille &&
                    left->open_seconds == right->open_seconds &&
                    left->half_open_probes == right->half_open_probes));
}

} // namespace aegisgate::health
