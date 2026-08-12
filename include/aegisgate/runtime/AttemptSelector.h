#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <set>

#include "aegisgate/proxy/ProxyTransaction.h"
#include "aegisgate/runtime/SelectionState.h"
#include "aegisgate/runtime/RuntimeGeneration.h"

namespace aegisgate::health {
class Coordinator;
struct HealthCircuitSnapshot;
} // namespace aegisgate::health

namespace aegisgate::runtime {

// Per-attempt endpoint selection shared by the initial attempt and every
// retry (R-036), bound to the request's own config snapshot (R-054): the
// initial attempt and every retry read route/endpoint data from the same
// snapshot_ the request started with, never the current global snapshot.  The
// selector lives inside the provider closure the transaction owns, so the
// snapshot outlives every attempt.  Eligibility (health + breaker) still comes
// from the live coordinator snapshot.
class AttemptSelector {
public:
  AttemptSelector(SelectionState &selection, RuntimeGenerationRef generation,
                  std::size_t route_index);

  // Picks one attempt selection for the route's balance policy.  A nullopt
  // selection means no eligible candidate remains (initial -> unique 503,
  // retry -> terminal failure); the decision's coordinator_overloaded flag
  // reports whether that was because the route's outcome capacity is exhausted
  // rather than no healthy candidate (R-053).  The caller never connects on a
  // nullopt selection.
  [[nodiscard]] proxy::ProxyTransaction::AttemptDecision Select(bool least_active);

private:
  [[nodiscard]] bool Eligible(std::size_t endpoint_index,
                              const health::HealthCircuitSnapshot &snapshot) const noexcept;
  [[nodiscard]] std::optional<proxy::ProxyTransaction::BreakerLink>
  MakeLink(std::size_t endpoint_index, const health::HealthCircuitSnapshot &snapshot) noexcept;
  [[nodiscard]] std::optional<proxy::ProxyTransaction::AttemptSelection>
  MakeSelection(std::size_t endpoint_index, const health::HealthCircuitSnapshot &snapshot) noexcept;

  SelectionState &selection_;
  RuntimeGenerationRef generation_;
  std::shared_ptr<health::Coordinator> coordinator_;
  std::size_t route_index_;
  // The request-bound configuration; never re-read from the global snapshot.
  ConfigSnapshotRef snapshot_;
  // Set when the last Select saw an outcome reservation refusal; reported in
  // the returned decision so the caller can distinguish overload from a plain
  // no-healthy-endpoint.
  bool overload_ = false;
  std::set<std::size_t> tried_;
};

} // namespace aegisgate::runtime
