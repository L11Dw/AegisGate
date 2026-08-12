#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "aegisgate/config/Config.h"
#include "aegisgate/health/EndpointHealth.h"
#include "aegisgate/resilience/CircuitBreakerSnapshot.h"

namespace aegisgate::health {

// Identity types for route/endpoint matching across generations.
struct RouteIdentity {
  std::string name;
  std::string host;
  std::string path_prefix;
};

struct EndpointIdentity {
  std::string host;
  std::array<std::uint8_t, 4> address{};
  std::uint16_t port = 0;
};

// Pure-value health snapshot for migration.
struct EndpointHealthSnapshot {
  HealthState state = HealthState::kImplicitHealthy;
};

// Complete protection snapshot for one endpoint.
struct EndpointProtectionSnapshot {
  RouteIdentity route;
  EndpointIdentity endpoint;
  EndpointHealthSnapshot health;
  std::optional<resilience::CircuitBreakerSnapshot> breaker;  // nullopt = no breaker config
  // Source policy for equivalence comparison during import.
  std::optional<config::HealthCheckSettings> source_health_policy;
  std::optional<config::CircuitBreakerSettings> source_breaker_policy;
};

// Full protection snapshot for migration.
struct ProtectionSnapshot {
  std::vector<EndpointProtectionSnapshot> endpoints;
};

// Identity comparison functions (semantic, not byte-level).
[[nodiscard]] inline bool SameRouteIdentity(const RouteIdentity &a, const RouteIdentity &b) {
  return a.name == b.name && a.host == b.host && a.path_prefix == b.path_prefix;
}

[[nodiscard]] inline bool SameEndpointIdentity(const EndpointIdentity &a,
                                                const EndpointIdentity &b) {
  return a.host == b.host && a.address == b.address && a.port == b.port;
}

[[nodiscard]] inline bool SameHealthPolicy(
    const std::optional<config::HealthCheckSettings> &a,
    const std::optional<config::HealthCheckSettings> &b) {
  if (a.has_value() != b.has_value()) return false;
  if (!a.has_value()) return true;  // both absent
  return a->interval_ms == b->interval_ms && a->timeout_ms == b->timeout_ms;
}

[[nodiscard]] inline bool SameBreakerPolicy(
    const std::optional<config::CircuitBreakerSettings> &a,
    const std::optional<config::CircuitBreakerSettings> &b) {
  if (a.has_value() != b.has_value()) return false;
  if (!a.has_value()) return true;  // both absent
  return a->window_seconds == b->window_seconds && a->min_requests == b->min_requests &&
         a->failure_threshold_permille == b->failure_threshold_permille &&
         a->open_seconds == b->open_seconds && a->half_open_probes == b->half_open_probes;
}

} // namespace aegisgate::health
