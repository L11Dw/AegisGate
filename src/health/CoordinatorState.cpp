#include "aegisgate/health/CoordinatorState.h"

#include <stdexcept>
#include <utility>

namespace aegisgate::health {

CoordinatorState::CoordinatorState(std::shared_ptr<const config::Config> config,
                                   Clock::time_point now)
    : config_(std::move(config)) {
  if (!config_) throw std::invalid_argument("coordinator requires a config snapshot");
  endpoints_.reserve(config_->routes.size());
  for (const config::Route &route : config_->routes) {
    // EndpointState holds an atomic claim counter, so it is not movable:
    // default-construct in place and assign the per-endpoint pieces.
    std::vector<EndpointState> states(route.endpoints.size());
    if (route.circuit_breaker.has_value()) {
      const auto &breaker = *route.circuit_breaker;
      const resilience::CircuitBreakerConfig breaker_config{
          std::chrono::seconds(breaker.window_seconds), breaker.min_requests,
          breaker.failure_threshold_permille, std::chrono::seconds(breaker.open_seconds),
          breaker.half_open_probes};
      for (EndpointState &state : states) {
        state.breaker = std::make_unique<resilience::CircuitBreaker>(breaker_config, now);
      }
    }
    endpoints_.push_back(std::move(states));
  }
}

void CoordinatorState::RecordHealth(std::size_t route, std::size_t endpoint, bool healthy) {
  if (route >= endpoints_.size() || endpoint >= endpoints_[route].size()) return;
  endpoints_[route][endpoint].health.RecordCheckResult(healthy);
}

void CoordinatorState::RecordResult(const AttemptResult &result, Clock::time_point now) {
  if (result.route_index >= endpoints_.size()) return;
  auto &states = endpoints_[result.route_index];
  if (result.endpoint_index >= states.size()) return;
  resilience::CircuitBreaker *breaker = states[result.endpoint_index].breaker.get();
  if (breaker == nullptr) return;  // route without breaker config: inert
  resilience::CircuitBreaker::RequestPermit permit{
      result.permit.probe ? resilience::CircuitBreaker::Selection::kProbe
                          : resilience::CircuitBreaker::Selection::kAllowed,
      result.permit.generation, result.permit.probe_id};
  if (result.success) {
    breaker->RecordSuccess(now, permit);
  } else {
    breaker->RecordFailure(now, permit);
  }
}

void CoordinatorState::ArmHalfOpen(std::size_t route, std::size_t endpoint, Clock::time_point now) {
  if (route >= endpoints_.size() || endpoint >= endpoints_[route].size()) return;
  EndpointState &state = endpoints_[route][endpoint];
  resilience::CircuitBreaker *breaker = state.breaker.get();
  if (breaker == nullptr || breaker->StateNow() != resilience::CircuitBreaker::State::kOpen) {
    return;
  }
  if (now < breaker->OpenUntil()) return;
  const config::CircuitBreakerSettings &settings = *config_->routes[route].circuit_breaker;
  // The first Select() transitions Open -> HalfOpen; the remaining calls
  // pre-issue the rest of the quota so pending_probes_ can validate every
  // worker probe id exactly once (ids are consecutive from the first issue).
  std::uint64_t base = 0;
  for (std::uint32_t index = 0; index != settings.half_open_probes; ++index) {
    const auto permit = breaker->Select(now);
    if (index == 0) base = permit.probe_id;
  }
  state.probe_base = base;
  state.slots.available.store(settings.half_open_probes, std::memory_order_release);
  state.slots.claims.store(0, std::memory_order_release);
}

std::shared_ptr<const HealthCircuitSnapshot> CoordinatorState::BuildSnapshot() {
  auto snapshot = std::make_shared<HealthCircuitSnapshot>();
  snapshot->version = ++version_;
  snapshot->endpoints.resize(endpoints_.size());
  for (std::size_t route = 0; route < endpoints_.size(); ++route) {
    snapshot->endpoints[route].resize(endpoints_[route].size());
    for (std::size_t endpoint = 0; endpoint < endpoints_[route].size(); ++endpoint) {
      const EndpointState &state = endpoints_[route][endpoint];
      EndpointDecision decision;
      decision.healthy = state.health.Healthy();
      if (state.breaker) {
        decision.breaker_state = static_cast<std::uint8_t>(state.breaker->StateNow());
        decision.generation = state.breaker->Generation();
        decision.probe_base = state.probe_base;
        decision.probe_quota = config_->routes[route].circuit_breaker->half_open_probes;
      }
      snapshot->endpoints[route][endpoint] = decision;
    }
  }
  return snapshot;
}

std::optional<AttemptPermit>
CoordinatorState::ClaimProbe(std::size_t route, std::size_t endpoint,
                             const HealthCircuitSnapshot &snapshot) noexcept {
  if (route >= snapshot.endpoints.size() || endpoint >= snapshot.endpoints[route].size()) {
    return std::nullopt;
  }
  const EndpointDecision &decision = snapshot.endpoints[route][endpoint];
  if (decision.breaker_state !=
      static_cast<std::uint8_t>(resilience::CircuitBreaker::State::kHalfOpen)) {
    return std::nullopt;
  }
  if (route >= endpoints_.size() || endpoint >= endpoints_[route].size()) {
    return std::nullopt;
  }
  ProbeClaimSlots &slots = endpoints_[route][endpoint].slots;
  std::uint32_t available = slots.available.load(std::memory_order_acquire);
  while (available > 0) {
    if (slots.available.compare_exchange_weak(available, available - 1,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
      const std::uint64_t index = slots.claims.fetch_add(1, std::memory_order_relaxed);
      return AttemptPermit{true, decision.generation, decision.probe_base + index};
    }
  }
  return std::nullopt;
}

bool CoordinatorState::ProbeAvailable(std::size_t route, std::size_t endpoint) const noexcept {
  if (route >= endpoints_.size() || endpoint >= endpoints_[route].size()) return false;
  return endpoints_[route][endpoint].slots.available.load(std::memory_order_acquire) > 0;
}

bool CoordinatorState::IsOpen(std::size_t route, std::size_t endpoint) const noexcept {
  return BreakerState(route, endpoint) == resilience::CircuitBreaker::State::kOpen;
}

CoordinatorState::Clock::time_point
CoordinatorState::OpenUntil(std::size_t route, std::size_t endpoint) const noexcept {
  if (route >= endpoints_.size() || endpoint >= endpoints_[route].size()) {
    return Clock::time_point::max();
  }
  const auto &breaker = endpoints_[route][endpoint].breaker;
  return breaker ? breaker->OpenUntil() : Clock::time_point::max();
}

std::size_t CoordinatorState::RouteCount() const noexcept { return endpoints_.size(); }

std::size_t CoordinatorState::EndpointCount(std::size_t route) const noexcept {
  return route < endpoints_.size() ? endpoints_[route].size() : 0U;
}

bool CoordinatorState::Healthy(std::size_t route, std::size_t endpoint) const noexcept {
  if (route >= endpoints_.size() || endpoint >= endpoints_[route].size()) return true;
  return endpoints_[route][endpoint].health.Healthy();
}

resilience::CircuitBreaker::State
CoordinatorState::BreakerState(std::size_t route, std::size_t endpoint) const noexcept {
  if (route >= endpoints_.size() || endpoint >= endpoints_[route].size()) {
    return resilience::CircuitBreaker::State::kClosed;
  }
  const auto &breaker = endpoints_[route][endpoint].breaker;
  return breaker ? breaker->StateNow() : resilience::CircuitBreaker::State::kClosed;
}

std::uint64_t CoordinatorState::Generation(std::size_t route, std::size_t endpoint) const noexcept {
  if (route >= endpoints_.size() || endpoint >= endpoints_[route].size()) return 0;
  const auto &breaker = endpoints_[route][endpoint].breaker;
  return breaker ? breaker->Generation() : 0;
}

} // namespace aegisgate::health
