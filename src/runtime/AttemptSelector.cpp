#include "aegisgate/runtime/AttemptSelector.h"

#include "aegisgate/health/CoordinatorState.h"
#include "aegisgate/resilience/CircuitBreaker.h"

namespace aegisgate::runtime {

AttemptSelector::AttemptSelector(SelectionState &selection, std::shared_ptr<WorkerShared> shared,
                                 std::size_t route_index, ConfigSnapshotRef snapshot)
    : selection_(selection), shared_(std::move(shared)), route_index_(route_index),
      snapshot_(std::move(snapshot)) {}

std::optional<proxy::ProxyTransaction::AttemptSelection>
AttemptSelector::Select(bool least_active) {
  const auto health_snapshot = shared_->coordinator->CurrentSnapshot();
  if (!health_snapshot) return std::nullopt;
  if (least_active) {
    for (;;) {
      const auto index = selection_.NextLeastActiveIndex(
          route_index_, tried_, [this, &health_snapshot](std::size_t endpoint_index) {
            return Eligible(endpoint_index, *health_snapshot);
          });
      if (!index) return std::nullopt;
      tried_.insert(*index);
      auto selection = MakeSelection(*index, *health_snapshot);
      if (selection) return selection;
      // A failed probe claim is not a candidate: re-scan (the index stays in
      // tried so the scan cannot loop on it).
    }
  }
  // The request-bound snapshot: the initial attempt and every retry select
  // from the same endpoint set (R-054).
  const std::vector<config::Endpoint> &endpoints = snapshot_->config.routes[route_index_].endpoints;
  for (std::size_t attempts = 0; attempts < endpoints.size(); ++attempts) {
    const auto index = selection_.NextWeightedIndex(route_index_);
    if (!index || !tried_.insert(*index).second) continue;
    if (!Eligible(*index, *health_snapshot)) continue;
    auto selection = MakeSelection(*index, *health_snapshot);
    if (selection) return selection;
  }
  return std::nullopt;
}

bool AttemptSelector::Eligible(std::size_t endpoint_index,
                               const health::HealthCircuitSnapshot &snapshot) const noexcept {
  if (route_index_ >= snapshot.endpoints.size() ||
      endpoint_index >= snapshot.endpoints[route_index_].size()) {
    return false;
  }
  const health::EndpointDecision &decision = snapshot.endpoints[route_index_][endpoint_index];
  if (!decision.healthy) return false;
  if (decision.breaker_state ==
      static_cast<std::uint8_t>(resilience::CircuitBreaker::State::kOpen)) {
    return false;
  }
  if (decision.breaker_state ==
          static_cast<std::uint8_t>(resilience::CircuitBreaker::State::kHalfOpen) &&
      !shared_->coordinator->ProbeAvailable(route_index_, endpoint_index)) {
    return false;
  }
  return true;
}

std::optional<proxy::ProxyTransaction::AttemptSelection>
AttemptSelector::MakeSelection(std::size_t endpoint_index,
                               const health::HealthCircuitSnapshot &snapshot) noexcept {
  const health::EndpointDecision &decision = snapshot.endpoints[route_index_][endpoint_index];
  std::optional<proxy::ProxyTransaction::BreakerLink> link;
  if (decision.generation != 0) {  // a breaker is configured for this endpoint
    health::AttemptPermit permit;
    if (decision.breaker_state ==
        static_cast<std::uint8_t>(resilience::CircuitBreaker::State::kHalfOpen)) {
      const auto claimed = shared_->coordinator->ClaimProbe(route_index_, endpoint_index, snapshot);
      if (!claimed) return std::nullopt;  // slot race: not a candidate
      permit = *claimed;
    } else {
      permit = health::AttemptPermit{false, decision.generation, 0};
    }
    // R-053: claim a result slot before the attempt may connect.  A nullopt
    // means the route's outcome capacity is exhausted or the coordinator is
    // stopping: this candidate cannot start a breaker-accounted attempt, so it
    // is not selectable (the caller terminates with 503 / ends the retry).
    auto reservation = shared_->coordinator->ReserveOutcome(route_index_);
    if (!reservation) return std::nullopt;
    link = proxy::ProxyTransaction::BreakerLink{
        shared_->coordinator, route_index_, endpoint_index, permit, std::move(*reservation)};
  }
  // Value-copied endpoint from the request-bound snapshot: the transaction
  // never holds a pointer into snapshot internals (R-054).
  const config::Endpoint endpoint = snapshot_->config.routes[route_index_].endpoints[endpoint_index];
  return proxy::ProxyTransaction::AttemptSelection{
      endpoint, std::move(link), selection_.AcquireActive(route_index_, endpoint_index),
      snapshot_};
}

} // namespace aegisgate::runtime
