#include "aegisgate/gateway/Gateway.h"

#include <chrono>
#include <set>
#include <stdexcept>
#include <utility>

#include <unistd.h>

#include "aegisgate/http/HttpResponse.h"
#include "aegisgate/net/Acceptor.h"
#include "aegisgate/net/ClientConnection.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/TimerQueue.h"
#include "aegisgate/observability/Metrics.h"
#include "aegisgate/proxy/ProxyTransaction.h"
#include "aegisgate/proxy/UpstreamPool.h"

namespace aegisgate::gateway {

Gateway::Gateway(net::EventLoop &loop, config::Config config, std::string_view listen_address,
                 std::uint16_t listen_port)
    : lifetime_token_(std::make_shared<int>(0)), loop_(loop),
      state_(std::make_shared<State>()), routes_(std::move(config)),
      metrics_(std::make_shared<observability::Metrics>()),
      upstream_pool_(std::make_shared<proxy::UpstreamPool>(loop)),
      timers_(std::make_unique<net::TimerQueue>(loop)),
      acceptor_(std::make_unique<net::Acceptor>(loop, listen_address, listen_port)) {
  state_->owner = this;
  acceptor_->SetNewConnectionCallback([this](int fd) { Accept(fd); });
  for (const config::Route &route : routes_.Config().routes) {
    if (!route.health_check.has_value()) continue;
    const auto &settings = *route.health_check;
    for (const config::Endpoint &endpoint : route.endpoints) {
      health_checkers_.push_back(std::make_unique<health::HealthChecker>(
          loop_, *timers_, endpoint,
          health::HealthCheckConfig{std::chrono::milliseconds(settings.interval_ms),
                                    std::chrono::milliseconds(settings.timeout_ms)},
          [this, &route, &endpoint](bool healthy) {
            // The table owns the state for the lifetime of this gateway; the
            // checker's generation guards stale callbacks after Stop().
            if (health::EndpointHealth *state = routes_.HealthFor(route, endpoint)) {
              state->RecordCheckResult(healthy);
            }
          }));
      health_checkers_.back()->Start();
    }
  }
}

Gateway::~Gateway() {
  // Fixed shutdown order (R-040): invalidate the lifetime token first so any
  // late callback sees the gateway as down, stop the health checkers (their
  // timer cancellations still find a live TimerQueue), then terminate every
  // in-flight upstream exchange so transactions release via RAII instead of
  // waiting for an upstream EOF or timeout, then drop the clients.
  lifetime_token_.reset();
  state_->owner = nullptr;
  health_checkers_.clear();
  upstream_pool_->CancelAll();
  clients_.clear();
}

void Gateway::Start() { acceptor_->Listen(); }

std::uint16_t Gateway::port() const { return acceptor_->port(); }

std::size_t Gateway::ClientCount() const noexcept { return clients_.size(); }

std::string Gateway::MetricsText() {
  // Refresh the route x endpoint protection state, then render everything
  // through Metrics so label escaping and exposition stay centralized.
  // State refresh is best-effort: allocation failure in observability must
  // never break serving /metrics.
  try {
    for (const config::Route &route : routes_.Config().routes) {
    for (const config::Endpoint &endpoint : route.endpoints) {
      const std::string upstream = endpoint.host + ":" + std::to_string(endpoint.port);
      if (const resilience::CircuitBreaker *breaker = routes_.BreakerFor(route, endpoint)) {
        const char *state = "closed";
        switch (breaker->StateNow()) {
        case resilience::CircuitBreaker::State::kOpen: state = "open"; break;
        case resilience::CircuitBreaker::State::kHalfOpen: state = "half_open"; break;
        case resilience::CircuitBreaker::State::kClosed: break;
        }
        metrics_->SetCircuitState(route.name, upstream, state);
      }
      if (const health::EndpointHealth *state = routes_.HealthFor(route, endpoint)) {
        metrics_->SetUpstreamHealth(route.name, upstream, state->Healthy());
      }
    }
  }
  } catch (...) {
  }
  return metrics_->RenderPrometheus();
}

void Gateway::Accept(int fd) {
  // Acceptor closes fd if its callback throws.  Once a ClientConnection owns
  // that fd, this method must instead consume every setup failure itself: map
  // erasure destroys the sole owner and this normally-returning callback keeps
  // Acceptor from closing a potentially reused descriptor a second time.
  std::uint64_t identifier = 0;
  bool inserted = false;
  if (next_client_identifier_ == 0) {
    (void)::close(fd);
    return;
  }
  try {
    identifier = next_client_identifier_++;
    auto client = std::make_unique<net::ClientConnection>(
        loop_, fd, [this](net::ClientConnection &connection, const http::HttpRequest &request) {
          HandleRequest(connection, request);
        });
    client->SetCloseCallback([&loop = loop_, state = std::weak_ptr<State>(state_), identifier] {
      NotifyClientClosed(loop, state, identifier);
    });
    const auto result = clients_.emplace(identifier, std::move(client));
    if (!result.second) throw std::logic_error("duplicate accepted client identifier");
    inserted = true;
    metrics_->SetActiveConnections(clients_.size());
    result.first->second->Start();
  } catch (...) {
    if (inserted) clients_.erase(identifier);
    metrics_->SetActiveConnections(clients_.size());
  }
}

void Gateway::HandleRequest(net::ClientConnection &client, const http::HttpRequest &request) {
  if (request.method == "GET" && request.target == "/metrics") {
    client.SendResponse(http::HttpResponse{200, "OK", {{"Content-Type", "text/plain; version=0.0.4; charset=utf-8"}},
                                           MetricsText()});
    return;
  }
  const config::Route *route = routes_.Match(request.Header("host"), request.target);
  if (route == nullptr) {
    try { metrics_->RecordImmediate("_unmatched", 404); } catch (...) {}
    client.SendResponse(http::HttpResponse{404, "Not Found", {}, ""});
    return;
  }
  proxy::UpstreamPolicy policy;
  policy.connect_timeout = std::chrono::milliseconds(route->connect_timeout_ms);
  policy.first_byte_timeout = std::chrono::milliseconds(route->first_byte_timeout_ms);
  policy.total_timeout = std::chrono::milliseconds(route->total_timeout_ms);
  policy.retry_budget = route->retry_budget;
  // The provider chooses the endpoint for the initial attempt and every retry
  // (eligible means healthy and not refused by its breaker) and issues the
  // attempt permit plus its active slot; no candidate ever connects.
  proxy::ProxyTransaction::AttemptProvider provider;
  if (route->balance == config::BalancePolicy::kLeastActive) {
    // Two-pass minimum-active scan in the route's weighted rotation order;
    // tried holds table-owned endpoint indexes.  A defensively rejected
    // permit joins tried and the selection is recomputed (unreachable after
    // Eligible(), kept for future wiring changes).
    provider = [this, route, tried = std::set<std::size_t>{}]() mutable
        -> std::optional<proxy::ProxyTransaction::AttemptSelection> {
      for (;;) {
        const auto index = routes_.NextLeastActiveIndex(*route, tried);
        if (!index) return std::nullopt;
        tried.insert(*index);
        const config::Endpoint &candidate = route->endpoints[*index];
        std::optional<proxy::ProxyTransaction::BreakerLink> link;
        if (resilience::CircuitBreaker *breaker = routes_.BreakerFor(*route, candidate)) {
          link = proxy::ProxyTransaction::BreakerLink{
              breaker, breaker->Select(resilience::CircuitBreaker::Clock::now())};
          if (link->permit.selection ==
                  resilience::CircuitBreaker::Selection::kRejectedOpen ||
              link->permit.selection ==
                  resilience::CircuitBreaker::Selection::kRejectedHalfOpenQuota) {
            continue;
          }
        }
        return proxy::ProxyTransaction::AttemptSelection{
            &candidate, std::move(link), routes_.AcquireActive(*route, candidate)};
      }
    };
  } else {
    // Weighted rotation scan; tried tracks table-owned endpoint indexes so a
    // repeated weighted choice cannot loop and the identity never depends on
    // the selector's internal copies (R-041).
    provider = [this, route, tried = std::set<std::size_t>{}]() mutable
        -> std::optional<proxy::ProxyTransaction::AttemptSelection> {
      for (std::size_t attempts = 0; attempts < route->endpoints.size(); ++attempts) {
        const auto index = routes_.NextWeightedIndex(*route);
        if (!index || !tried.insert(*index).second) continue;
        const config::Endpoint &candidate = route->endpoints[*index];
        if (!routes_.Eligible(*route, candidate)) continue;
        std::optional<proxy::ProxyTransaction::BreakerLink> link;
        if (resilience::CircuitBreaker *breaker = routes_.BreakerFor(*route, candidate)) {
          link = proxy::ProxyTransaction::BreakerLink{
              breaker, breaker->Select(resilience::CircuitBreaker::Clock::now())};
          // Defensive: a rejected permit never starts an attempt, so it also
          // never takes an active slot.  The Eligible() pre-filter makes
          // this unreachable in the single-loop design.
          if (link->permit.selection == resilience::CircuitBreaker::Selection::kRejectedOpen ||
              link->permit.selection ==
                  resilience::CircuitBreaker::Selection::kRejectedHalfOpenQuota) {
            continue;
          }
        }
        return proxy::ProxyTransaction::AttemptSelection{
            &candidate, std::move(link), routes_.AcquireActive(*route, candidate)};
      }
      return std::nullopt;
    };
  }
  (void)proxy::ProxyTransaction::Start(
      loop_, client, route->endpoints.front(), request, upstream_pool_,
      routes_.AdmissionFor(*route), timers_.get(), std::move(policy), metrics_, route->name,
      std::move(provider), std::weak_ptr<void>(lifetime_token_));
}

void Gateway::NotifyClientClosed(net::EventLoop &loop, std::weak_ptr<State> weak_state,
                                 std::uint64_t identifier) {
  const auto state = weak_state.lock();
  if (!state || state->owner == nullptr) return;
  state->closed_clients.push_back(identifier);
  if (state->cleanup_scheduled) return;
  loop.QueueAfterCurrentBatch([state] {
    state->cleanup_scheduled = false;
    if (state->owner == nullptr) return;
    auto identifiers = std::move(state->closed_clients);
    state->closed_clients.clear();
    state->owner->ReapClosedClients(std::move(identifiers));
  });
  state->cleanup_scheduled = true;
}

void Gateway::ReapClosedClients(std::vector<std::uint64_t> identifiers) {
  for (const std::uint64_t identifier : identifiers) clients_.erase(identifier);
  metrics_->SetActiveConnections(clients_.size());
}

} // namespace aegisgate::gateway
