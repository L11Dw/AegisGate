#pragma once

#include "aegisgate/config/Config.h"
#include "aegisgate/health/EndpointHealth.h"
#include "aegisgate/resilience/CircuitBreaker.h"
#include "aegisgate/resilience/RouteAdmission.h"
#include "aegisgate/routing/ActiveReservation.h"
#include "aegisgate/routing/WeightedRoundRobin.h"

#include <memory>
#include <optional>
#include <set>
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

  // The immutable configuration this table was built from.
  [[nodiscard]] const config::Config &Config() const noexcept { return config_; }

  // Returns a pointer owned by this immutable table, or nullptr when no route
  // matches an origin-form request target and Host field.
  [[nodiscard]] const config::Route *Match(std::string_view host,
                                            std::string_view target) const noexcept;

  // The returned state is shared by every request matched to this table-owned
  // Route. Passing a Route from another table returns nullptr.
  [[nodiscard]] std::shared_ptr<resilience::RouteAdmission>
  AdmissionFor(const config::Route &route) const noexcept;

  // Advances the route-owned weighted selector.  Passing a Route from another
  // table returns nullptr; this table is single-EventLoop-thread confined.
  [[nodiscard]] const config::Endpoint *NextEndpoint(const config::Route &route) const noexcept;

  // Advances the route's weighted rotation cursor exactly once and returns the
  // table-owned index of the selected endpoint (content-matched back into the
  // config list), so callers never depend on the selector's internal copies
  // (R-041).  Returns nullopt for a Route from another table.
  [[nodiscard]] std::optional<std::size_t> NextWeightedIndex(const config::Route &route) const noexcept;

  // route x endpoint runtime state.  Both identify the endpoint by its
  // address + port content (the logical identity; the weighted selector
  // returns copies of the configured endpoints), so any Endpoint value with
  // the same address/port works.  Returns nullptr for routes from another
  // table or routes that did not enable the feature.
  [[nodiscard]] health::EndpointHealth *HealthFor(const config::Route &route,
                                                  const config::Endpoint &endpoint) const noexcept;
  [[nodiscard]] resilience::CircuitBreaker *BreakerFor(const config::Route &route,
                                                       const config::Endpoint &endpoint) const noexcept;

  // Selection filter: false when the endpoint is unhealthy or the breaker
  // currently refuses selection.  Read-only; the caller performs the actual
  // Select() (with its half-open side effects) only for the chosen endpoint.
  [[nodiscard]] bool Eligible(const config::Route &route,
                              const config::Endpoint &endpoint) const noexcept;

  // Least-active selection: advances the route's weighted rotation cursor
  // exactly once, then two-pass scans its endpoints in cyclic order from the
  // cursor owner (eligible, untried only), returning the table-owned index of
  // the endpoint with the fewest active attempts.  Active values outrank
  // weights; weights resolve ties only.  Returns nullopt when no candidate
  // remains.
  [[nodiscard]] std::optional<std::size_t>
  NextLeastActiveIndex(const config::Route &route,
                       const std::set<std::size_t> &tried) const noexcept;

  // Active-attempt slot state for a table-owned route x endpoint.  Identity is
  // the endpoint's address + port content; a Route from another table or an
  // unknown endpoint yields 0 / an empty reservation.
  [[nodiscard]] std::uint32_t ActiveFor(const config::Route &route,
                                        const config::Endpoint &endpoint) const noexcept;
  [[nodiscard]] ActiveReservation AcquireActive(const config::Route &route,
                                                const config::Endpoint &endpoint) noexcept;

private:
  struct EndpointState {
    std::unique_ptr<health::EndpointHealth> health;
    std::unique_ptr<resilience::CircuitBreaker> breaker;
    std::shared_ptr<ActiveReservation::State> active;
  };
  [[nodiscard]] std::size_t RouteIndex(const config::Route &route) const noexcept;
  [[nodiscard]] std::size_t EndpointIndex(std::size_t route_index,
                                          const config::Endpoint &endpoint) const noexcept;

  config::Config config_;
  std::vector<std::shared_ptr<resilience::RouteAdmission>> admissions_;
  mutable std::vector<WeightedRoundRobin> selectors_;
  std::vector<std::vector<EndpointState>> endpoint_states_;
};

} // namespace aegisgate::routing
