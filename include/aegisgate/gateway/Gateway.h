#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "aegisgate/config/Config.h"
#include "aegisgate/net/StreamFlowControl.h"
#include "aegisgate/observability/Metrics.h"
#include "aegisgate/resilience/CircuitBreaker.h"
#include "aegisgate/routing/RouteTable.h"
#include "aegisgate/runtime/ConfigSnapshot.h"
#include "aegisgate/runtime/WorkerData.h"
#include "aegisgate/runtime/WorkerSet.h"
#include "aegisgate/runtime/WorkerShared.h"

namespace aegisgate::net {
class Acceptor;
class EventLoop;
} // namespace aegisgate::net

namespace aegisgate::health {
class Coordinator;
} // namespace aegisgate::health

namespace aegisgate::resilience {
class GlobalAdmission;
} // namespace aegisgate::resilience

namespace aegisgate::gateway {

// Multi-worker application assembly.  The control loop (the loop passed in,
// owned by the caller) runs the acceptor only; a fixed set of I/O workers
// each run their own EventLoop with client connections, the upstream pool,
// timers, selection state and worker-local metrics; the coordinator loop owns
// every health/breaker transition and the admission refill ticks.  The
// gateway is the main-thread aggregate: it starts/stops the workers and the
// coordinator, hands accepted fds to workers round-robin, and renders the
// aggregated /metrics.  It never touches worker-owned objects directly.
class Gateway {
public:
  Gateway(net::EventLoop &loop, config::Config config, std::string_view listen_address,
          std::uint16_t listen_port,
          net::StreamFlowControl flow_control = net::StreamFlowControl{});
  ~Gateway();

  Gateway(const Gateway &) = delete;
  Gateway &operator=(const Gateway &) = delete;

  void Start();
  [[nodiscard]] std::uint16_t port() const;
  [[nodiscard]] std::size_t ClientCount() const noexcept;
  [[nodiscard]] std::string MetricsText();
  // Test access to the immutable config snapshot's matcher.
  [[nodiscard]] routing::RouteTable &Routes() noexcept { return routes_; }
  // M3-D test views over the live coordinator snapshot.  No worker or
  // coordinator object is shared; the snapshot is the only contract.
  [[nodiscard]] bool EndpointHealthy(const config::Route &route,
                                     const config::Endpoint &endpoint) const noexcept;
  [[nodiscard]] resilience::CircuitBreaker::State
  BreakerState(const config::Route &route, const config::Endpoint &endpoint) const noexcept;
  // Test seam: submits one attempt outcome to the coordinator and blocks
  // until it was processed and the snapshot republished.
  void SubmitResultAndWait(const config::Route &route, const config::Endpoint &endpoint,
                           bool success);

private:
  void Accept(int fd);
  [[nodiscard]] std::size_t RouteIndexOf(const config::Route &route) const noexcept;
  [[nodiscard]] std::size_t EndpointIndexOf(std::size_t route_index,
                                            const config::Endpoint &endpoint) const noexcept;
  [[nodiscard]] std::string RenderMetrics() const;

  net::EventLoop &loop_;
  // Lifecycle token observed (weakly) by in-flight transactions.  Reset on
  // the first line of the destructor so any late callback sees the gateway
  // as down before its members are torn down (R-040).
  std::shared_ptr<void> lifetime_token_;
  runtime::ConfigSnapshotRef config_snapshot_;
  routing::RouteTable routes_;
  std::shared_ptr<runtime::WorkerShared> worker_shared_;
  std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions_;
  std::vector<std::shared_ptr<observability::Metrics>> worker_metrics_;
  std::vector<std::shared_ptr<std::atomic<std::uint64_t>>> client_counts_;
  std::vector<std::shared_ptr<runtime::WorkerData>> worker_datas_;
  std::unique_ptr<runtime::WorkerSet> workers_;
  std::shared_ptr<health::Coordinator> coordinator_;
  std::unique_ptr<net::Acceptor> acceptor_;
  net::StreamFlowControl flow_control_;
};

} // namespace aegisgate::gateway
