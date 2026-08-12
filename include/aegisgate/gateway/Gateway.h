#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "aegisgate/config/Config.h"
#include "aegisgate/net/StreamFlowControl.h"
#include "aegisgate/observability/Metrics.h"
#include "aegisgate/resilience/CircuitBreaker.h"
#include "aegisgate/routing/RouteTable.h"
#include "aegisgate/runtime/ConfigSnapshot.h"
#include "aegisgate/runtime/GenerationMailbox.h"
#include "aegisgate/runtime/ReloadController.h"
#include "aegisgate/runtime/WorkerData.h"
#include "aegisgate/runtime/WorkerSet.h"
#include "aegisgate/runtime/WorkerShared.h"

namespace aegisgate::net {
class Acceptor;
class Channel;
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
          net::StreamFlowControl flow_control = net::StreamFlowControl{},
          std::string config_path = {});
  ~Gateway();

  Gateway(const Gateway &) = delete;
  Gateway &operator=(const Gateway &) = delete;

  // Explicit lifecycle state (R-067 low-risk): Start() rejects a second start
  // and the destructor records the stopped state; a partial Start relies on
  // the destructor to roll back whatever was already created.
  enum class Lifecycle : std::uint8_t { kNotStarted, kStarting, kRunning, kStopped };

  void Start();
  // Control-loop-owner only.  Publishes a fully prepared generation atomically
  // when its worker count is compatible with the fixed WorkerSet.  Existing
  // requests retain their old generation; new requests bind the replacement.
  // A later ReloadController is responsible for parsing YAML off-thread and
  // calls this method only with a validated candidate.
  [[nodiscard]] bool RequestReload(config::Config candidate);
  // Thread-safe trigger for the configured on-disk YAML.  Parsing happens on
  // ReloadController's background thread; publication happens later from the
  // control-loop callback.  False when this Gateway has no config path or is
  // already stopping.
  [[nodiscard]] bool RequestReload();
  [[nodiscard]] std::uint16_t port() const;
  [[nodiscard]] std::uint64_t CurrentGenerationVersion() const noexcept {
    const auto generation = CurrentGeneration();
    return generation ? generation->version() : 0;
  }
  [[nodiscard]] std::size_t ClientCount() const noexcept;
  [[nodiscard]] std::string MetricsText();
  [[nodiscard]] Lifecycle lifecycle() const noexcept { return lifecycle_; }
  // Test access to the immutable config snapshot's matcher.
  [[nodiscard]] routing::RouteTable &Routes() noexcept { return routes_; }
  // M3-D test views over the live coordinator snapshot, addressed by route and
  // endpoint index into the config snapshot (obtainable via Routes().Match).
  // No worker or coordinator object is shared; the snapshot is the only
  // contract.  No bare Route* / Endpoint* is ever matched (R-060).
  [[nodiscard]] bool EndpointHealthy(std::size_t route_index,
                                     std::size_t endpoint_index) const noexcept;
  [[nodiscard]] resilience::CircuitBreaker::State
  BreakerState(std::size_t route_index, std::size_t endpoint_index) const noexcept;
  // Test seam: submits one attempt outcome to the coordinator and blocks
  // until it was processed and the snapshot republished.
  void SubmitResultAndWait(std::size_t route_index, std::size_t endpoint_index, bool success);

private:
  struct RetiringGeneration {
    runtime::RuntimeGenerationRef generation;
    std::size_t returned_worker_balances = 0;
  };

  void Accept(int fd);
  // Control-loop-only retirement pipeline.  A WorkerData notification never
  // calls these directly; the cross-thread boundary is GenerationMailbox.
  void RetireGeneration(runtime::RuntimeGenerationRef generation);
  void HandleGenerationEvents();
  void RequestWorkerBalanceReturn(const runtime::RuntimeGenerationRef &generation);
  void StartRetirementReaper(const runtime::RuntimeGenerationRef &generation);
  void HandleReloadResults();
  [[nodiscard]] runtime::RuntimeGenerationRef CurrentGeneration() const noexcept {
    return current_generation_.load(std::memory_order_acquire);
  }
  [[nodiscard]] std::string RenderMetrics() const;

  net::EventLoop &loop_;
  Lifecycle lifecycle_ = Lifecycle::kNotStarted;
  // Lifecycle token observed (weakly) by in-flight transactions.  Reset on
  // the first line of the destructor so any late callback sees the gateway
  // as down before its members are torn down (R-040).
  std::shared_ptr<void> lifetime_token_;
  // The one published generation used by every newly accepted request.  M4-A
  // reload will atomically replace this pointer; no duplicate gateway-level
  // coordinator/admission aliases are permitted.
  std::atomic<runtime::RuntimeGenerationRef> current_generation_;
  routing::RouteTable routes_;
  std::shared_ptr<runtime::WorkerShared> worker_shared_;
  std::vector<std::shared_ptr<observability::Metrics>> worker_metrics_;
  std::vector<std::shared_ptr<std::atomic<std::uint64_t>>> client_counts_;
  std::vector<std::shared_ptr<runtime::WorkerData>> worker_datas_;
  std::unique_ptr<runtime::WorkerSet> workers_;
  std::unique_ptr<net::Acceptor> acceptor_;
  std::shared_ptr<runtime::GenerationMailbox> generation_mailbox_;
  std::unique_ptr<net::Channel> generation_mailbox_channel_;
  std::unique_ptr<runtime::ReloadController> reload_controller_;
  std::unique_ptr<net::Channel> reload_channel_;
  std::unordered_map<std::uint64_t, RetiringGeneration> retiring_generations_;
  std::vector<std::thread> retirement_reapers_;
  bool workers_stopped_ = false;
  net::StreamFlowControl flow_control_;
};

} // namespace aegisgate::gateway
