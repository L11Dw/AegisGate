#include "aegisgate/health/CoordinatorState.h"

#include <stdexcept>
#include <utility>
#include <algorithm>

namespace aegisgate::health {

namespace {
bool SameHealth(const std::optional<config::HealthCheckSettings> &a,
                const std::optional<config::HealthCheckSettings> &b) {
  if (a.has_value() != b.has_value()) return false;
  return !a || (a->interval_ms == b->interval_ms && a->timeout_ms == b->timeout_ms);
}
bool SameBreaker(const std::optional<config::CircuitBreakerSettings> &a,
                 const std::optional<config::CircuitBreakerSettings> &b) {
  if (a.has_value() != b.has_value()) return false;
  return !a || (a->window_seconds == b->window_seconds &&
                a->min_requests == b->min_requests &&
                a->failure_threshold_permille == b->failure_threshold_permille &&
                a->open_seconds == b->open_seconds &&
                a->half_open_probes == b->half_open_probes);
}
bool SameIdentity(const config::Route &route, const config::Endpoint &ep,
                  const EndpointDecision &old) {
  return route.name == old.route_name && route.host == old.route_host &&
         route.path_prefix == old.route_path_prefix && ep.host == old.endpoint_host &&
         ep.port == old.endpoint_port;
}
} // namespace

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
  // Publish a fresh per-cycle slot object: workers claim from the object their
  // snapshot holds, so a permit is always consistent with that snapshot.
  auto slots = std::make_shared<ProbeSlotState>();
  slots->remaining.store(settings.half_open_probes, std::memory_order_release);
  slots->issued.store(0, std::memory_order_relaxed);
  slots->probe_base = base;
  slots->generation = breaker->Generation();
  slots->quota = settings.half_open_probes;
  state.probe_slots.store(std::move(slots), std::memory_order_release);
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
      const auto &route_config = config_->routes[route];
      const auto &endpoint_config = route_config.endpoints[endpoint];
      decision.route_name = route_config.name;
      decision.route_host = route_config.host;
      decision.route_path_prefix = route_config.path_prefix;
      decision.endpoint_host = endpoint_config.host;
      decision.endpoint_port = endpoint_config.port;
      decision.health_policy = route_config.health_check;
      decision.breaker_policy = route_config.circuit_breaker;
      decision.healthy = state.health.Healthy();
      decision.health_state = state.health.state();
      if (state.breaker) {
        decision.breaker_state = static_cast<std::uint8_t>(state.breaker->StateNow());
        decision.generation = state.breaker->Generation();
        const auto slots = state.probe_slots.load(std::memory_order_acquire);
        if (slots) {
          decision.probe_base = slots->probe_base;
          decision.probe_quota = slots->quota;
        }
        decision.probe_slots = slots;
        decision.breaker_snapshot = state.breaker->ExportSnapshot(Clock::now());
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
  // Claim from the snapshot's own per-cycle object: the permit is always
  // consistent with the snapshot it was requested from (R-058).
  const auto slots = decision.probe_slots;
  if (!slots) return std::nullopt;
  std::uint32_t remaining = slots->remaining.load(std::memory_order_acquire);
  while (remaining > 0) {
    if (slots->remaining.compare_exchange_weak(remaining, remaining - 1,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
      const std::uint64_t index = slots->issued.fetch_add(1, std::memory_order_relaxed);
      // Post-claim currency check: the permit is void if the coordinator has
      // already moved to a new HalfOpen cycle.  No counters are rolled back —
      // the old slot is no longer active, so the lost quota cannot pollute the
      // new cycle, and had the permit been handed out its result would be
      // rejected by the generation/probe_id validation.
      if (route >= endpoints_.size() || endpoint >= endpoints_[route].size() ||
          endpoints_[route][endpoint].probe_slots.load(std::memory_order_acquire) != slots) {
        return std::nullopt;
      }
      return AttemptPermit{true, slots->generation, slots->probe_base + index};
    }
  }
  return std::nullopt;
}

bool CoordinatorState::ProbeAvailable(std::size_t route, std::size_t endpoint) const noexcept {
  if (route >= endpoints_.size() || endpoint >= endpoints_[route].size()) return false;
  const auto slots = endpoints_[route][endpoint].probe_slots.load(std::memory_order_acquire);
  return slots && slots->remaining.load(std::memory_order_acquire) > 0;
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

void CoordinatorState::ImportFromSnapshot(const HealthCircuitSnapshot &snapshot) {
  for (std::size_t r = 0; r < endpoints_.size(); ++r) {
    const auto &route = config_->routes[r];
    for (std::size_t e = 0; e < endpoints_[r].size(); ++e) {
      const auto &endpoint = route.endpoints[e];
      const EndpointDecision *match = nullptr;
      for (const auto &old_route : snapshot.endpoints) {
        for (const auto &old : old_route) {
          if (SameIdentity(route, endpoint, old)) { match = &old; break; }
        }
        if (match) break;
      }
      if (!match || !SameHealth(route.health_check, match->health_policy)) continue;
      endpoints_[r][e].health.SetState(match->health_state);
      if (endpoints_[r][e].breaker && SameBreaker(route.circuit_breaker, match->breaker_policy) &&
          match->breaker_snapshot.has_value()) {
        auto *breaker = endpoints_[r][e].breaker.get();
        breaker->ImportSnapshot(*match->breaker_snapshot, Clock::now());
        if (breaker->StateNow() == resilience::CircuitBreaker::State::kHalfOpen) {
          const auto quota = route.circuit_breaker->half_open_probes;
          std::uint64_t base = 0;
          for (std::uint32_t i = 0; i < quota; ++i) {
            const auto permit = breaker->Select(Clock::now());
            if (i == 0) base = permit.probe_id;
          }
          auto slots = std::make_shared<ProbeSlotState>();
          slots->remaining.store(quota, std::memory_order_release);
          slots->probe_base = base;
          slots->generation = breaker->Generation();
          slots->quota = quota;
          endpoints_[r][e].probe_slots.store(std::move(slots), std::memory_order_release);
        }
      }
    }
  }
}

} // namespace aegisgate::health
