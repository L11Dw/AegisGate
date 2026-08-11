#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace aegisgate::config {

struct Endpoint {
  std::string host;
  // Parsed once from host.  B2 must connect directly to this literal address;
  // it must not fall back to DNS or the loopback-only connector.
  std::array<std::uint8_t, 4> address{};
  std::uint16_t port{};
  std::uint32_t weight{};
};

// Per-route circuit breaker value object, applied to every endpoint of the
// route.  failure_threshold_permille is the YAML fraction (0,1) scaled to
// integer permille at parse time so the breaker never compares floats.
struct CircuitBreakerSettings {
  std::uint32_t window_seconds{};
  std::uint32_t min_requests{};
  std::uint32_t failure_threshold_permille{};
  std::uint32_t open_seconds{};
  std::uint32_t half_open_probes{};
};

// Per-route active health check value object; the checker probes every
// endpoint of the route.
struct HealthCheckSettings {
  std::uint32_t interval_ms{};
  std::uint32_t timeout_ms{};
};

struct Route {
  Route(std::string name_, std::string host_, std::string path_prefix_,
        std::vector<Endpoint> endpoints_ = {}, std::uint32_t rate_limit_ = 0,
        std::uint32_t burst_ = 0, std::uint32_t max_inflight_ = 0,
        std::uint32_t connect_timeout_ms_ = 5000,
        std::uint32_t first_byte_timeout_ms_ = 5000,
        std::uint32_t total_timeout_ms_ = 30000, std::uint32_t retry_budget_ = 1,
        std::optional<CircuitBreakerSettings> circuit_breaker_ = std::nullopt,
        std::optional<HealthCheckSettings> health_check_ = std::nullopt)
      : name(std::move(name_)), host(std::move(host_)),
        path_prefix(std::move(path_prefix_)), endpoints(std::move(endpoints_)),
        rate_limit(rate_limit_), burst(burst_), max_inflight(max_inflight_),
        connect_timeout_ms(connect_timeout_ms_), first_byte_timeout_ms(first_byte_timeout_ms_),
        total_timeout_ms(total_timeout_ms_), retry_budget(retry_budget_),
        circuit_breaker(std::move(circuit_breaker_)), health_check(std::move(health_check_)) {}

  std::string name;
  std::string host;
  std::string path_prefix;
  std::vector<Endpoint> endpoints;
  std::uint32_t rate_limit{};
  std::uint32_t burst{};
  std::uint32_t max_inflight{};
  std::uint32_t connect_timeout_ms = 5000;
  std::uint32_t first_byte_timeout_ms = 5000;
  std::uint32_t total_timeout_ms = 30000;
  std::uint32_t retry_budget = 1;
  // Absent means the route keeps the M1 behavior: no breaker, no health
  // checks, every endpoint always eligible for selection.
  std::optional<CircuitBreakerSettings> circuit_breaker;
  std::optional<HealthCheckSettings> health_check;
};

struct Config {
  std::vector<Route> routes;
};

// Parses the fixed startup configuration schema.  Any malformed or unsupported
// configuration is reported as std::invalid_argument.
[[nodiscard]] Config LoadFromYaml(std::string_view yaml);

} // namespace aegisgate::config
