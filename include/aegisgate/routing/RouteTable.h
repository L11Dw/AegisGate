#pragma once

#include "aegisgate/config/Config.h"
#include "aegisgate/resilience/RouteAdmission.h"

#include <memory>
#include <string_view>
#include <vector>

namespace aegisgate::routing {

class RouteTable {
public:
  explicit RouteTable(config::Config config);
  RouteTable(const RouteTable &) = delete;
  RouteTable &operator=(const RouteTable &) = delete;
  RouteTable(RouteTable &&) noexcept = default;
  RouteTable &operator=(RouteTable &&) noexcept = default;

  // Returns a pointer owned by this immutable table, or nullptr when no route
  // matches an origin-form request target and Host field.
  [[nodiscard]] const config::Route *Match(std::string_view host,
                                            std::string_view target) const noexcept;

  // The returned state is shared by every request matched to this table-owned
  // Route. Passing a Route from another table returns nullptr.
  [[nodiscard]] std::shared_ptr<resilience::RouteAdmission>
  AdmissionFor(const config::Route &route) const noexcept;

private:
  config::Config config_;
  std::vector<std::shared_ptr<resilience::RouteAdmission>> admissions_;
};

} // namespace aegisgate::routing
