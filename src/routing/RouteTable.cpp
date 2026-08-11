#include "aegisgate/routing/RouteTable.h"

#include <chrono>
#include <string_view>
#include <utility>

namespace aegisgate::routing {
namespace {

bool EqualsIgnoreCase(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) return false;
  for (std::size_t index = 0; index < left.size(); ++index) {
    const char normalized_left = left[index] >= 'A' && left[index] <= 'Z'
                                     ? static_cast<char>(left[index] - 'A' + 'a')
                                     : left[index];
    const char normalized_right = right[index] >= 'A' && right[index] <= 'Z'
                                       ? static_cast<char>(right[index] - 'A' + 'a')
                                       : right[index];
    if (normalized_left != normalized_right) {
      return false;
    }
  }
  return true;
}

bool IsOriginForm(std::string_view target) {
  if (target.empty() || target.front() != '/') return false;
  for (const unsigned char character : target) {
    if (character <= 0x20 || character == 0x7f || character == '#') return false;
  }
  return true;
}

std::string_view PathOnly(std::string_view target) {
  const std::size_t query = target.find('?');
  return target.substr(0, query);
}

bool PrefixMatches(std::string_view path, std::string_view prefix) {
  if (prefix == "/") return true;
  if (!path.starts_with(prefix)) return false;
  return path.size() == prefix.size() || prefix.back() == '/' || path[prefix.size()] == '/';
}

} // namespace

RouteTable::RouteTable(config::Config config) : config_(std::move(config)) {
  admissions_.reserve(config_.routes.size());
  endpoint_states_.reserve(config_.routes.size());
  const auto now = resilience::TokenBucket::Clock::now();
  for (const config::Route &route : config_.routes) {
    admissions_.push_back(std::make_shared<resilience::RouteAdmission>(route, now));
    selectors_.emplace_back(route.endpoints);
    std::vector<EndpointState> states;
    states.reserve(route.endpoints.size());
    for (std::size_t index = 0; index < route.endpoints.size(); ++index) {
      EndpointState state;
      if (route.health_check.has_value()) {
        state.health = std::make_unique<health::EndpointHealth>();
      }
      if (route.circuit_breaker.has_value()) {
        const auto &breaker = *route.circuit_breaker;
        state.breaker = std::make_unique<resilience::CircuitBreaker>(
            resilience::CircuitBreakerConfig{
                std::chrono::seconds(breaker.window_seconds), breaker.min_requests,
                breaker.failure_threshold_permille, std::chrono::seconds(breaker.open_seconds),
                breaker.half_open_probes},
            now);
      }
      states.push_back(std::move(state));
    }
    endpoint_states_.push_back(std::move(states));
  }
}

std::size_t RouteTable::RouteIndex(const config::Route &route) const noexcept {
  for (std::size_t index = 0; index < config_.routes.size(); ++index) {
    if (&config_.routes[index] == &route) return index;
  }
  return config_.routes.size();
}

health::EndpointHealth *RouteTable::HealthFor(const config::Route &route,
                                              const config::Endpoint &endpoint) const noexcept {
  const std::size_t route_index = RouteIndex(route);
  if (route_index == config_.routes.size()) return nullptr;
  // The weighted selector returns copies, so identify the endpoint by its
  // address + port (its logical health identity) rather than by pointer.
  for (std::size_t index = 0; index < config_.routes[route_index].endpoints.size(); ++index) {
    const config::Endpoint &candidate = config_.routes[route_index].endpoints[index];
    if (candidate.address == endpoint.address && candidate.port == endpoint.port) {
      return endpoint_states_[route_index][index].health.get();
    }
  }
  return nullptr;
}

resilience::CircuitBreaker *RouteTable::BreakerFor(const config::Route &route,
                                                   const config::Endpoint &endpoint) const noexcept {
  const std::size_t route_index = RouteIndex(route);
  if (route_index == config_.routes.size()) return nullptr;
  for (std::size_t index = 0; index < config_.routes[route_index].endpoints.size(); ++index) {
    const config::Endpoint &candidate = config_.routes[route_index].endpoints[index];
    if (candidate.address == endpoint.address && candidate.port == endpoint.port) {
      return endpoint_states_[route_index][index].breaker.get();
    }
  }
  return nullptr;
}

bool RouteTable::Eligible(const config::Route &route,
                          const config::Endpoint &endpoint) const noexcept {
  const health::EndpointHealth *health = HealthFor(route, endpoint);
  if (health != nullptr && !health->Healthy()) return false;
  const resilience::CircuitBreaker *breaker = BreakerFor(route, endpoint);
  if (breaker != nullptr &&
      breaker->RefusesSelection(resilience::CircuitBreaker::Clock::now())) {
    return false;
  }
  return true;
}

const config::Route *RouteTable::Match(std::string_view host, std::string_view target) const noexcept {
  if (!IsOriginForm(target)) return nullptr;
  const std::string_view path = PathOnly(target);
  const config::Route *best = nullptr;
  for (const config::Route &route : config_.routes) {
    if (!EqualsIgnoreCase(route.host, host) || !PrefixMatches(path, route.path_prefix)) continue;
    if (best == nullptr || route.path_prefix.size() > best->path_prefix.size()) best = &route;
  }
  return best;
}

std::shared_ptr<resilience::RouteAdmission>
RouteTable::AdmissionFor(const config::Route &route) const noexcept {
  for (std::size_t index = 0; index < config_.routes.size(); ++index) {
    if (&config_.routes[index] == &route) return admissions_[index];
  }
  return nullptr;
}

const config::Endpoint *RouteTable::NextEndpoint(const config::Route &route) const noexcept {
  for (std::size_t index = 0; index < config_.routes.size(); ++index) {
    if (&config_.routes[index] == &route) return &selectors_[index].Next();
  }
  return nullptr;
}

} // namespace aegisgate::routing
