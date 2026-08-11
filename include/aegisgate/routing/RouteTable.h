#pragma once

#include "aegisgate/config/Config.h"

#include <optional>
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

  // Returns the index of the longest-prefix matching route in `config`, or
  // nullopt when no route matches an origin-form request target and Host
  // field.  Callers index config.routes with the result; no caller keeps a
  // bare Route* across a snapshot boundary (R-071).
  [[nodiscard]] static std::optional<std::size_t>
  Match(const config::Config &config, std::string_view host, std::string_view target) noexcept;

  // Instance convenience over the table's own config.
  [[nodiscard]] std::optional<std::size_t> Match(std::string_view host,
                                                 std::string_view target) const noexcept;

private:
  config::Config config_;
};

} // namespace aegisgate::routing
