#include "aegisgate/gateway/Gateway.h"

#include <chrono>
#include <stdexcept>
#include <utility>

#include <unistd.h>

#include "aegisgate/http/HttpResponse.h"
#include "aegisgate/net/Acceptor.h"
#include "aegisgate/net/ClientConnection.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/TimerQueue.h"
#include "aegisgate/proxy/ProxyTransaction.h"
#include "aegisgate/proxy/UpstreamPool.h"

namespace aegisgate::gateway {

Gateway::Gateway(net::EventLoop &loop, config::Config config, std::string_view listen_address,
                 std::uint16_t listen_port)
    : loop_(loop), state_(std::make_shared<State>()), routes_(std::move(config)),
      upstream_pool_(std::make_shared<proxy::UpstreamPool>(loop)),
      timers_(std::make_unique<net::TimerQueue>(loop)),
      acceptor_(std::make_unique<net::Acceptor>(loop, listen_address, listen_port)) {
  state_->owner = this;
  acceptor_->SetNewConnectionCallback([this](int fd) { Accept(fd); });
}

Gateway::~Gateway() {
  state_->owner = nullptr;
  clients_.clear();
}

void Gateway::Start() { acceptor_->Listen(); }

std::uint16_t Gateway::port() const { return acceptor_->port(); }

std::size_t Gateway::ClientCount() const noexcept { return clients_.size(); }

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
    result.first->second->Start();
  } catch (...) {
    if (inserted) clients_.erase(identifier);
  }
}

void Gateway::HandleRequest(net::ClientConnection &client, const http::HttpRequest &request) {
  const config::Route *route = routes_.Match(request.Header("host"), request.target);
  if (route == nullptr) {
    client.SendResponse(http::HttpResponse{404, "Not Found", {}, ""});
    return;
  }
  const config::Endpoint *endpoint = routes_.NextEndpoint(*route);
  if (endpoint == nullptr) {
    client.SendResponse(http::HttpResponse{502, "Bad Gateway", {}, ""});
    return;
  }
  proxy::UpstreamPolicy policy;
  policy.connect_timeout = std::chrono::milliseconds(route->connect_timeout_ms);
  policy.first_byte_timeout = std::chrono::milliseconds(route->first_byte_timeout_ms);
  policy.total_timeout = std::chrono::milliseconds(route->total_timeout_ms);
  policy.retry_budget = route->retry_budget;
  policy.retry_endpoints = route->endpoints;
  (void)proxy::ProxyTransaction::Start(loop_, client, *endpoint, request, upstream_pool_,
                                       routes_.AdmissionFor(*route), timers_.get(), std::move(policy));
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
}

} // namespace aegisgate::gateway
