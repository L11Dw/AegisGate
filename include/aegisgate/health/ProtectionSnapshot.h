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

struct EndpointProtectionSnapshot {
  RouteIdentity route;
  EndpointIdentity endpoint;
  HealthState health_state = HealthState::kImplicitHealthy;
  std::optional<config::HealthCheckSettings> source_health_policy;
  std::optional<config::CircuitBreakerSettings> source_breaker_policy;
  std::optional<resilience::CircuitBreakerSnapshot> breaker;
};

struct ProtectionSnapshot {
  std::vector<EndpointProtectionSnapshot> endpoints;
};

[[nodiscard]] RouteIdentity RouteKey(const config::Route &route);
[[nodiscard]] EndpointIdentity EndpointKey(const config::Endpoint &endpoint);
[[nodiscard]] bool SameRouteIdentity(const RouteIdentity &key, const config::Route &route) noexcept;
[[nodiscard]] bool SameEndpointIdentity(const EndpointIdentity &key,
                                         const config::Endpoint &endpoint) noexcept;
[[nodiscard]] bool SameHealthPolicy(const std::optional<config::HealthCheckSettings> &left,
                                    const std::optional<config::HealthCheckSettings> &right) noexcept;
[[nodiscard]] bool SameBreakerPolicy(const std::optional<config::CircuitBreakerSettings> &left,
                                     const std::optional<config::CircuitBreakerSettings> &right) noexcept;

} // namespace aegisgate::health
