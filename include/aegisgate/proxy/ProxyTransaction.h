#pragma once

#include <cstdint>
#include <chrono>
#include <functional>
#include <memory>
#include <functional>
#include <optional>
#include <vector>

#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/config/Config.h"
#include "aegisgate/net/UpstreamConnection.h"
#include "aegisgate/net/TimerQueue.h"
#include "aegisgate/observability/Metrics.h"
#include "aegisgate/resilience/CircuitBreaker.h"
#include "aegisgate/resilience/InflightLimiter.h"

namespace aegisgate::net {
class ClientConnection;
class EventLoop;
} // namespace aegisgate::net

namespace aegisgate::resilience {
class RouteAdmission;
} // namespace aegisgate::resilience

namespace aegisgate::proxy {

class UpstreamPool;

struct UpstreamPolicy {
  std::chrono::milliseconds connect_timeout{5000};
  std::chrono::milliseconds first_byte_timeout{5000};
  std::chrono::milliseconds total_timeout{30000};
  std::uint32_t retry_budget = 1;
  std::vector<config::Endpoint> retry_endpoints;
};

// Owns the one upstream exchange for one request while its client is paused.
class ProxyTransaction : public std::enable_shared_from_this<ProxyTransaction> {
public:
  [[nodiscard]] static std::shared_ptr<ProxyTransaction>
  Start(net::EventLoop &loop, net::ClientConnection &client, std::uint16_t upstream_port,
        http::HttpRequest request,
        std::shared_ptr<resilience::RouteAdmission> admission = nullptr);
  // One breaker link per upstream attempt: the non-owning breaker owned by
  // the route table (lives at least as long as the gateway) plus the permit
  // this attempt was admitted with.  A retry to another endpoint obtains a
  // fresh link.  nullopt means the route has no breaker and outcomes are not
  // accounted.
  struct BreakerLink {
    resilience::CircuitBreaker *breaker;
    resilience::CircuitBreaker::RequestPermit permit;
  };
  using BreakerProvider =
      std::function<std::optional<BreakerLink>(const config::Endpoint &)>;

  [[nodiscard]] static std::shared_ptr<ProxyTransaction>
  Start(net::EventLoop &loop, net::ClientConnection &client, config::Endpoint endpoint,
        http::HttpRequest request, std::shared_ptr<UpstreamPool> pool,
                        std::shared_ptr<resilience::RouteAdmission> admission = nullptr,
        net::TimerQueue *timers = nullptr, UpstreamPolicy policy = {},
        std::shared_ptr<observability::Metrics> metrics = nullptr, std::string route_name = {},
        BreakerProvider breaker_provider = {});

private:
  ProxyTransaction(net::EventLoop &loop, net::ClientConnection &client,
                   std::uint16_t upstream_port, http::HttpRequest request,
                   std::shared_ptr<resilience::RouteAdmission> admission);
  ProxyTransaction(net::EventLoop &loop, net::ClientConnection &client,
                   config::Endpoint endpoint, http::HttpRequest request,
                   std::shared_ptr<UpstreamPool> pool,
                   std::shared_ptr<resilience::RouteAdmission> admission,
                   net::TimerQueue *timers, UpstreamPolicy policy,
                   std::shared_ptr<observability::Metrics> metrics, std::string route_name,
                   BreakerProvider breaker_provider);

  void Begin();
  void StartUpstream();
  void HandleProgress(net::UpstreamProgress progress);
  void ArmConnectDeadline();
  void ArmFirstByteDeadline();
  void ArmTotalDeadline();
  void CancelAttemptDeadlines();
  void CancelDeadlines();
  void HandleDeadline(std::uint64_t generation);
  [[nodiscard]] bool RetryableFailure(net::UpstreamResult result) const noexcept;
  [[nodiscard]] bool HasRetryAlternative() const noexcept;
  [[nodiscard]] bool StartRetry();
  void FinishFailure();
  void FinishGatewayTimeout();
  void AccountSuccess() noexcept;
  void AccountFailure() noexcept;
  void HandleAdmissionRejected();
  void HandleUpstream(net::UpstreamResult result, http::HttpResponse response);
  void CompleteMetric(int status, bool rate_limited = false) noexcept;
  [[nodiscard]] std::string UpstreamLabel() const;

  net::EventLoop &loop_;
  net::ClientConnection *client_;
  std::weak_ptr<void> client_lifetime_;
  std::uint16_t upstream_port_;
  std::optional<config::Endpoint> endpoint_;
  http::HttpRequest request_;
  std::shared_ptr<resilience::RouteAdmission> admission_;
  std::optional<resilience::InflightLimiter::Reservation> reservation_;
  std::shared_ptr<observability::Metrics> metrics_;
  std::string route_name_;
  observability::Metrics::RequestHandle metric_request_;
  std::optional<BreakerLink> breaker_link_;
  std::unique_ptr<net::UpstreamConnection> upstream_;
  std::shared_ptr<UpstreamPool> pool_;
  net::TimerQueue *timers_ = nullptr;
  BreakerProvider breaker_provider_;
  UpstreamPolicy policy_;
  net::UpstreamConnection *active_connection_ = nullptr;
  net::TimerQueue::TimerId connect_timer_ = 0;
  net::TimerQueue::TimerId first_byte_timer_ = 0;
  net::TimerQueue::TimerId total_timer_ = 0;
  std::uint64_t generation_ = 0;
  std::uint32_t retries_ = 0;
  bool connected_ = false;
  bool response_header_received_ = false;
  bool starting_upstream_ = false;
  bool finished_ = false;
};

} // namespace aegisgate::proxy
