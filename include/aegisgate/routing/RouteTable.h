#pragma once

#include "aegisgate/config/Config.h"

#include <string_view>

namespace aegisgate::routing {

// Immutable route matching over one configuration.  Runtime state (selectors,
// active counts, health, breaker, admission) is no longer table-owned: the
// M3-D worker data plane keeps selection state per worker, the coordinator
// owns health/breaker, and admission is global.  A configuration snapshot can
// therefore be shared by every worker and swapped atomically (M4) without
// touching in-flight requests.
class RouteTable {
public:
  explicit RouteTable(config::Config config);
  RouteTable(const RouteTable &) = delete;
  RouteTable &operator=(const RouteTable &) = delete;
  RouteTable(RouteTable &&) noexcept = default;
  RouteTable &operator=(RouteTable &&) noexcept = default;

  // The immutable configuration this table was built from.
  [[nodiscard]] const config::Config &Config() const noexcept { return config_; }

  // Returns a pointer owned by the given immutable config, or nullptr when no
  // route matches an origin-form request target and Host field.
  [[nodiscard]] static const config::Route *
  Match(const config::Config &config, std::string_view host, std::string_view target) noexcept;

  // Instance convenience over the table's own config.
  [[nodiscard]] const config::Route *Match(std::string_view host,
                                           std::string_view target) const noexcept;

private:
  config::Config config_;
};

} // namespace aegisgate::routing
