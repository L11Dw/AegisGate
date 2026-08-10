#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
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

struct Route {
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
};

struct Config {
  std::vector<Route> routes;
};

// Parses the fixed startup configuration schema.  Any malformed or unsupported
// configuration is reported as std::invalid_argument.
[[nodiscard]] Config LoadFromYaml(std::string_view yaml);

} // namespace aegisgate::config
