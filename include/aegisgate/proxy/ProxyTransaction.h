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
#include "aegisgate/health/CoordinatorState.h"
#include "aegisgate/resilience/GlobalAdmission.h"
#include "aegisgate/routing/ActiveReservation.h"
#include "aegisgate/runtime/ConfigSnapshot.h"

namespace aegisgate::net {
class ClientConnection;
class EventLoop;
} // namespace aegisgate::net

namespace aegisgate::health {
class Coordinator;
} // namespace aegisgate::health

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
        std::optional<resilience::GlobalAdmission::Reservation> reservation = std::nullopt);
  // One breaker link per upstream attempt: the C1' coordinator handle plus
  // the permit this attempt was admitted with.  The coordinator validates the
  // permit (generation, probe id) on its own loop; nullopt means the route
  // has no breaker and outcomes are not accounted.
  struct BreakerLink {
    std::shared_ptr<health::Coordinator> coordinator;
    std::size_t route_index;
    std::size_t endpoint_index;
    health::AttemptPermit permit;
  };
  // The outcome of choosing one upstream attempt: a value-copied endpoint from
  // the request-bound config snapshot plus its breaker link (absent when the
  // route has no breaker) plus the active slot it holds plus that same
  // snapshot.  No pointer into snapshot internals is ever kept (R-054); the
  // transaction stores the snapshot so a later retry stays on the request's
  // configuration.
  struct AttemptSelection {
    config::Endpoint endpoint;
    std::optional<BreakerLink> link;
    routing::ActiveReservation active;
    runtime::ConfigSnapshotRef snapshot;
  };
  // Chooses the endpoint for the initial attempt and for every retry, so
  // unhealthy or open candidates are never connected to.  nullopt means no
  // eligible candidate remains; the initial call terminates with a unique
  // 503 and a retry call terminates the transaction.
  using AttemptProvider = std::function<std::optional<AttemptSelection>()>;

  [[nodiscard]] static std::shared_ptr<ProxyTransaction>
  Start(net::EventLoop &loop, net::ClientConnection &client, config::Endpoint endpoint,
        http::HttpRequest request, std::shared_ptr<UpstreamPool> pool,
        std::optional<resilience::GlobalAdmission::Reservation> reservation = std::nullopt,
        net::TimerQueue *timers = nullptr, UpstreamPolicy policy = {},
        std::shared_ptr<observability::Metrics> metrics = nullptr, std::string route_name = {},
        AttemptProvider attempt_provider = {},
        std::optional<std::weak_ptr<void>> gateway_lifetime = std::nullopt);

private:
  ProxyTransaction(net::EventLoop &loop, net::ClientConnection &client,
                   std::uint16_t upstream_port, http::HttpRequest request,
                   std::optional<resilience::GlobalAdmission::Reservation> reservation);
  ProxyTransaction(net::EventLoop &loop, net::ClientConnection &client,
                   config::Endpoint endpoint, http::HttpRequest request,
                   std::shared_ptr<UpstreamPool> pool,
                   std::optional<resilience::GlobalAdmission::Reservation> reservation,
                   net::TimerQueue *timers, UpstreamPolicy policy,
                   std::shared_ptr<observability::Metrics> metrics, std::string route_name,
                   AttemptProvider attempt_provider,
                   std::optional<std::weak_ptr<void>> gateway_lifetime);

  void Begin();
  // True when the owning gateway has been destroyed.  nullopt (no gateway)
  // means the caller guarantees the timers/provider lifetime.
  [[nodiscard]] bool GatewayDown() const noexcept {
    return gateway_lifetime_.has_value() && gateway_lifetime_->expired();
  }
  [[nodiscard]] bool StartUpstream();
  void FinishNoEndpoint();
  void HandleProgress(net::UpstreamProgress progress);
  // Streaming response delivery: the head commits the downstream response
  // (closing the retry window), body chunks are forwarded via
  // WriteResponseBody, and the peer's death cancels the upstream exchange.
  void HandleResponseHead(const http::HttpResponseHead &head);
  [[nodiscard]] bool HandleResponseBody(std::string_view bytes);
  void HandleClientAbort();
  void PauseUpstreamReading() noexcept;
  void ResumeUpstreamReading() noexcept;
  void ClearClientStreamCallbacks() noexcept;
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
  void HandleUpstream(net::UpstreamResult result, http::HttpResponse response);
  void CompleteMetric(int status, bool rate_limited = false,
                     std::string_view reason = {}) noexcept;
  [[nodiscard]] std::string UpstreamLabel() const;

  net::EventLoop &loop_;
  net::ClientConnection *client_;
  std::weak_ptr<void> client_lifetime_;
  std::optional<std::weak_ptr<void>> gateway_lifetime_;
  std::uint16_t upstream_port_;
  std::optional<config::Endpoint> endpoint_;
  // The request-bound configuration snapshot selected by the provider; held for
  // the whole transaction so no retry re-reads the current global snapshot
  // (R-054).  Null when the provider is absent (caller-owned lifetime).
  runtime::ConfigSnapshotRef request_snapshot_;
  http::HttpRequest request_;
  // Pre-acquired by the caller (worker data plane) from the global
  // admission; released exactly once at the terminal path or by RAII.
  std::optional<resilience::GlobalAdmission::Reservation> reservation_;
  std::shared_ptr<observability::Metrics> metrics_;
  std::string route_name_;
  observability::Metrics::RequestHandle metric_request_;
  std::optional<BreakerLink> breaker_link_;
  // One active-attempt slot per upstream attempt: acquired by the provider,
  // released at the attempt's terminal point (see the design lifecycle
  // matrix).  Release is idempotent; an empty guard is a safe no-op.
  routing::ActiveReservation active_reservation_;
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
  // Set the moment the validated response head is handed to the downstream
  // connection: the conservative retry boundary (the kernel may not have
  // written any byte yet, but the output can no longer be replaced).
  bool downstream_response_committed_ = false;
  bool starting_upstream_ = false;
  bool finished_ = false;
  http::HttpResponseHead response_head_;
};

} // namespace aegisgate::proxy
