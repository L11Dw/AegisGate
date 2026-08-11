#pragma once

#include <cstdint>
#include <chrono>
#include <functional>
#include <string_view>
#include <memory>
#include <functional>
#include <string_view>
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
  // this attempt was admitted with.  nullopt means the route has no breaker
  // and outcomes are not accounted.
  struct BreakerLink {
    resilience::CircuitBreaker *breaker;
    resilience::CircuitBreaker::RequestPermit permit;
  };
  // The outcome of choosing one upstream attempt: an eligible endpoint plus
  // its breaker link (absent when the route has no breaker).
  struct AttemptSelection {
    const config::Endpoint *endpoint;
    std::optional<BreakerLink> link;
  };
  // Chooses the endpoint for the initial attempt and for every retry, so
  // unhealthy or open candidates are never connected to.  nullopt means no
  // eligible candidate remains; the initial call terminates with a unique
  // 503 and a retry call terminates the transaction.
  using AttemptProvider = std::function<std::optional<AttemptSelection>()>;

  [[nodiscard]] static std::shared_ptr<ProxyTransaction>
  Start(net::EventLoop &loop, net::ClientConnection &client, config::Endpoint endpoint,
        http::HttpRequest request, std::shared_ptr<UpstreamPool> pool,
                        std::shared_ptr<resilience::RouteAdmission> admission = nullptr,
        net::TimerQueue *timers = nullptr, UpstreamPolicy policy = {},
        std::shared_ptr<observability::Metrics> metrics = nullptr, std::string route_name = {},
        AttemptProvider attempt_provider = {});

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
                   AttemptProvider attempt_provider);

  void Begin();
  [[nodiscard]] bool StartUpstream();
  void FinishNoEndpoint();
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
  void CompleteMetric(int status, bool rate_limited = false,
                     std::string_view reason = {}) noexcept;
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
  // One-shot guard: each upstream attempt may account its outcome at most
  // once, even when the retry fallback terminates the same attempt.
  bool attempt_accounted_ = false;
  std::unique_ptr<net::UpstreamConnection> upstream_;
  std::shared_ptr<UpstreamPool> pool_;
  net::TimerQueue *timers_ = nullptr;
  AttemptProvider attempt_provider_;
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
