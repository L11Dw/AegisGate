#include "aegisgate/runtime/AttemptSelector.h"

#include "aegisgate/health/CoordinatorState.h"
#include "aegisgate/resilience/CircuitBreaker.h"

namespace aegisgate::runtime {

AttemptSelector::AttemptSelector(SelectionState &selection, std::shared_ptr<WorkerShared> shared,
                                 std::size_t route_index)
    : selection_(selection), shared_(std::move(shared)), route_index_(route_index) {}

std::optional<proxy::ProxyTransaction::AttemptSelection>
AttemptSelector::Select(bool least_active) {
  const auto snapshot = shared_->coordinator->CurrentSnapshot();
  if (!snapshot) return std::nullopt;
  if (least_active) {
    for (;;) {
      const auto index = selection_.NextLeastActiveIndex(
          route_index_, tried_, [this, &snapshot](std::size_t endpoint_index) {
            return Eligible(endpoint_index, *snapshot);
          });
      if (!index) return std::nullopt;
      tried_.insert(*index);
      auto selection = MakeSelection(*index, *snapshot);
      if (selection) return selection;
      // A failed probe claim is not a candidate: re-scan (the index stays in
      // tried so the scan cannot loop on it).
    }
  }
  const std::vector<config::Endpoint> &endpoints =
      shared_->config_snapshot.load(std::memory_order_acquire)->config.routes[route_index_].endpoints;
  for (std::size_t attempts = 0; attempts < endpoints.size(); ++attempts) {
    const auto index = selection_.NextWeightedIndex(route_index_);
    if (!index || !tried_.insert(*index).second) continue;
    if (!Eligible(*index, *snapshot)) continue;
    auto selection = MakeSelection(*index, *snapshot);
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
    link = proxy::ProxyTransaction::BreakerLink{shared_->coordinator, route_index_,
                                                endpoint_index, permit};
  }
  const config::Endpoint &endpoint =
      shared_->config_snapshot.load(std::memory_order_acquire)->config.routes[route_index_].endpoints[endpoint_index];
  return proxy::ProxyTransaction::AttemptSelection{
      &endpoint, std::move(link), selection_.AcquireActive(route_index_, endpoint_index)};
}

} // namespace aegisgate::runtime
