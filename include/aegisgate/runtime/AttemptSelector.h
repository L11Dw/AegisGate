#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <set>

#include "aegisgate/proxy/ProxyTransaction.h"
#include "aegisgate/runtime/SelectionState.h"
#include "aegisgate/runtime/WorkerShared.h"

namespace aegisgate::health {
class Coordinator;
struct HealthCircuitSnapshot;
} // namespace aegisgate::health

namespace aegisgate::runtime {

// Per-attempt endpoint selection shared by the initial attempt and every
// retry (R-036): worker-local index selection, coordinator snapshot
// eligibility, half-open probe claims, and the per-attempt active slot.  The
// config snapshot is held by shared_ptr inside the closure that owns the
// selector, so no hot-replaceable raw Route* is ever captured.
class AttemptSelector {
public:
  AttemptSelector(SelectionState &selection, std::shared_ptr<WorkerShared> shared,
                  std::size_t route_index);

  // Picks one attempt selection for the route's balance policy.  nullopt
  // means no eligible candidate remains (initial -> unique 503, retry ->
  // terminal failure); the caller never connects on nullopt.
  [[nodiscard]] std::optional<proxy::ProxyTransaction::AttemptSelection>
  Select(bool least_active);

private:
  [[nodiscard]] bool Eligible(std::size_t endpoint_index,
                              const health::HealthCircuitSnapshot &snapshot) const noexcept;
  [[nodiscard]] std::optional<proxy::ProxyTransaction::BreakerLink>
  MakeLink(std::size_t endpoint_index, const health::HealthCircuitSnapshot &snapshot) noexcept;
  [[nodiscard]] std::optional<proxy::ProxyTransaction::AttemptSelection>
  MakeSelection(std::size_t endpoint_index, const health::HealthCircuitSnapshot &snapshot) noexcept;

  SelectionState &selection_;
  std::shared_ptr<WorkerShared> shared_;
  std::size_t route_index_;
  std::set<std::size_t> tried_;
};

} // namespace aegisgate::runtime
