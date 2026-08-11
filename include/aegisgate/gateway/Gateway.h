#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "aegisgate/config/Config.h"
#include "aegisgate/health/HealthChecker.h"
#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/routing/RouteTable.h"

namespace aegisgate::net {
class Acceptor;
class ClientConnection;
class EventLoop;
class TimerQueue;
} // namespace aegisgate::net

namespace aegisgate::proxy {
class UpstreamPool;
} // namespace aegisgate::proxy

namespace aegisgate::observability {
class Metrics;
} // namespace aegisgate::observability

namespace aegisgate::gateway {

// Single-threaded application assembly: one listener, immutable route table,
// route-local admission, weighted endpoint choice, and a reusable upstream pool.
class Gateway {
public:
  Gateway(net::EventLoop &loop, config::Config config, std::string_view listen_address,
          std::uint16_t listen_port);
  ~Gateway();

  Gateway(const Gateway &) = delete;
  Gateway &operator=(const Gateway &) = delete;

  void Start();
  [[nodiscard]] std::uint16_t port() const;
  [[nodiscard]] std::size_t ClientCount() const noexcept;
  [[nodiscard]] std::string MetricsText() const;
  // Test access to the immutable route table and its route x endpoint state.
  [[nodiscard]] routing::RouteTable &Routes() noexcept { return routes_; }

private:
  struct State {
    Gateway *owner = nullptr;
    bool cleanup_scheduled = false;
    std::vector<std::uint64_t> closed_clients;
  };

  void Accept(int fd);
  void HandleRequest(net::ClientConnection &client, const http::HttpRequest &request);
  void ReapClosedClients(std::vector<std::uint64_t> identifiers);
  static void NotifyClientClosed(net::EventLoop &loop, std::weak_ptr<State> state,
                                 std::uint64_t identifier);

  net::EventLoop &loop_;
  std::shared_ptr<State> state_;
  routing::RouteTable routes_;
  std::shared_ptr<observability::Metrics> metrics_;
  std::shared_ptr<proxy::UpstreamPool> upstream_pool_;
  std::unique_ptr<net::TimerQueue> timers_;
  std::unique_ptr<net::Acceptor> acceptor_;
  std::vector<std::unique_ptr<health::HealthChecker>> health_checkers_;
  std::unordered_map<std::uint64_t, std::unique_ptr<net::ClientConnection>> clients_;
  std::uint64_t next_client_identifier_ = 1;
};

} // namespace aegisgate::gateway
