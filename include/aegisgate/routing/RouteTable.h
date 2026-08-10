#pragma once

#include "aegisgate/config/Config.h"

#include <string_view>

namespace aegisgate::routing {

class RouteTable {
public:
  explicit RouteTable(config::Config config);

  // Returns a pointer owned by this immutable table, or nullptr when no route
  // matches an origin-form request target and Host field.
  [[nodiscard]] const config::Route *Match(std::string_view host,
                                            std::string_view target) const noexcept;

private:
  config::Config config_;
};

} // namespace aegisgate::routing
