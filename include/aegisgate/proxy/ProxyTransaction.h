#pragma once

#include <cstdint>
#include <memory>

#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/net/UpstreamConnection.h"

namespace aegisgate::net {
class ClientConnection;
class EventLoop;
} // namespace aegisgate::net

namespace aegisgate::proxy {

// Owns the one upstream exchange for one request while its client is paused.
class ProxyTransaction : public std::enable_shared_from_this<ProxyTransaction> {
public:
  [[nodiscard]] static std::shared_ptr<ProxyTransaction>
  Start(net::EventLoop &loop, net::ClientConnection &client, std::uint16_t upstream_port,
        http::HttpRequest request);

private:
  ProxyTransaction(net::EventLoop &loop, net::ClientConnection &client,
                   std::uint16_t upstream_port, http::HttpRequest request);

  void StartUpstream();
  void HandleUpstream(net::UpstreamResult result, http::HttpResponse response);

  net::EventLoop &loop_;
  net::ClientConnection *client_;
  std::weak_ptr<void> client_lifetime_;
  std::uint16_t upstream_port_;
  http::HttpRequest request_;
  std::unique_ptr<net::UpstreamConnection> upstream_;
  bool starting_upstream_ = false;
  bool finished_ = false;
};

} // namespace aegisgate::proxy
