#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <vector>

#include "aegisgate/config/Config.h"
#include "aegisgate/routing/ActiveReservation.h"
#include "aegisgate/routing/WeightedRoundRobin.h"

namespace aegisgate::health {
struct HealthCircuitSnapshot;
} // namespace aegisgate::health

namespace aegisgate::runtime {

// Worker-thread-confined endpoint selection state over one immutable config
// snapshot: the weighted rotation cursors and the per-endpoint active-attempt
// counters are strictly local to the owning worker and never represent global
// concurrency.  Eligibility (health + breaker) comes from the coordinator
// snapshot and is supplied by the caller as a predicate, so this class stays
// free of coordination concerns.
class SelectionState {
public:
  // Binds the selection cursors to one immutable config snapshot (R-072): the
  // version records which snapshot this state belongs to, so a snapshot
  // reload (M4) rebuilds a fresh worker-local state instead of reusing cursors
  // and route indices across versions.  In-flight transactions keep the state
  // they started with.
  explicit SelectionState(const config::Config &config, std::uint64_t version = 0);

  // The snapshot version this state was built for.
  [[nodiscard]] std::uint64_t Version() const noexcept { return version_; }

  // Advances the route's weighted cursor exactly once and returns the
  // table-owned index of the selected endpoint (content-matched back into the
  // config list), so callers never depend on the selector's internal copies.
  [[nodiscard]] std::optional<std::size_t> NextWeightedIndex(std::size_t route_index) noexcept;

  // Least-active selection: two-pass scan over the route's endpoints in the
  // weighted rotation order, restricted to endpoints the caller's predicate
  // marks eligible and that are not in `tried`.  Returns the index with the
  // fewest active attempts; active values outrank weights.  nullopt when no
  // candidate remains.
  [[nodiscard]] std::optional<std::size_t>
  NextLeastActiveIndex(std::size_t route_index, const std::set<std::size_t> &tried,
                       const std::function<bool(std::size_t)> &eligible) noexcept;

  // Active-attempt slot state for a route x endpoint.
  [[nodiscard]] std::uint32_t ActiveFor(std::size_t route_index,
                                        std::size_t endpoint_index) const noexcept;
  [[nodiscard]] routing::ActiveReservation AcquireActive(std::size_t route_index,
                                                         std::size_t endpoint_index) noexcept;

private:
  const config::Config &config_;
  std::uint64_t version_;
  std::vector<routing::WeightedRoundRobin> selectors_;
  std::vector<std::vector<std::shared_ptr<routing::ActiveReservation::State>>> active_counts_;
};

} // namespace aegisgate::runtime
