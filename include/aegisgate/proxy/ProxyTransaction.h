#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/net/UpstreamConnection.h"
#include "aegisgate/resilience/InflightLimiter.h"

namespace aegisgate::net {
class ClientConnection;
class EventLoop;
} // namespace aegisgate::net

namespace aegisgate::resilience {
class RouteAdmission;
} // namespace aegisgate::resilience

namespace aegisgate::proxy {

// Owns the one upstream exchange for one request while its client is paused.
class ProxyTransaction : public std::enable_shared_from_this<ProxyTransaction> {
public:
  [[nodiscard]] static std::shared_ptr<ProxyTransaction>
  Start(net::EventLoop &loop, net::ClientConnection &client, std::uint16_t upstream_port,
        http::HttpRequest request,
        std::shared_ptr<resilience::RouteAdmission> admission = nullptr);

private:
  ProxyTransaction(net::EventLoop &loop, net::ClientConnection &client,
                   std::uint16_t upstream_port, http::HttpRequest request,
                   std::shared_ptr<resilience::RouteAdmission> admission);

  void Begin();
  void StartUpstream();
  void HandleAdmissionRejected();
  void HandleUpstream(net::UpstreamResult result, http::HttpResponse response);

  net::EventLoop &loop_;
  net::ClientConnection *client_;
  std::weak_ptr<void> client_lifetime_;
  std::uint16_t upstream_port_;
  http::HttpRequest request_;
  std::shared_ptr<resilience::RouteAdmission> admission_;
  std::optional<resilience::InflightLimiter::Reservation> reservation_;
  std::unique_ptr<net::UpstreamConnection> upstream_;
  bool starting_upstream_ = false;
  bool finished_ = false;
};

} // namespace aegisgate::proxy
