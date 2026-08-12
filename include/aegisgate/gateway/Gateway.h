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
#include "aegisgate/runtime/ReloadWatcher.h"
#include "aegisgate/runtime/RuntimeGeneration.h"
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
  // M4-A: control-loop owner only.  Attempts to publish a new generation
  // from the given candidate config.  Returns true if the candidate was
  // accepted, prepared on all workers, and published atomically.  Returns
  // false if the candidate is invalid, workers mismatch, prepare fails,
  // or the gateway is not running.  On failure the current generation
  // and all runtime state are unchanged.
  [[nodiscard]] bool RequestReload(config::Config candidate);
  // M4-A: triggers a background config file reload.  Thread-safe.
  // Returns false if no config path was provided or the gateway is
  // stopping.  The result is delivered to the control loop via the
  // ReloadController's eventfd.
  [[nodiscard]] bool RequestReload();
  [[nodiscard]] std::uint16_t port() const;
  [[nodiscard]] std::size_t ClientCount() const noexcept;
  [[nodiscard]] std::string MetricsText();
  [[nodiscard]] Lifecycle lifecycle() const noexcept { return lifecycle_; }
  // M4-A: control-loop owner only.  Returns the current generation version.
  [[nodiscard]] std::uint64_t CurrentGenerationVersion() const noexcept;
  // M4-A: control-loop owner only.  Returns the number of generations in
  // the retirement pipeline.  Used for test observation.
  [[nodiscard]] std::size_t RetiringGenerationCount() const noexcept {
    return retiring_generations_.size();
  }
  // M4-A: returns the monotonic sequence of the last reload result consumed
  // by the control loop.  Tests use this as a completion barrier.
  [[nodiscard]] std::uint64_t LastReloadResultSequence() const noexcept {
    return last_reload_result_sequence_;
  }
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
    bool reaper_started = false;
  };

  struct PendingPrepare {
    runtime::RuntimeGenerationRef generation;
    std::vector<std::shared_ptr<runtime::SelectionState>> sel_states;
    std::atomic<std::size_t> pending{0};
    std::atomic<bool> failed{false};
    std::atomic<bool> cancelled{false};
  };

  void Accept(int fd);
  void RetireGeneration(runtime::RuntimeGenerationRef generation);
  void HandleGenerationEvents();
  void HandlePrepareCompletion();
  void HandleReloadResults();
  [[nodiscard]] std::string RenderMetrics() const;

  net::EventLoop &loop_;
  Lifecycle lifecycle_ = Lifecycle::kNotStarted;
  // Lifecycle token observed (weakly) by in-flight transactions.  Reset on
  // the first line of the destructor so any late callback sees the gateway
  // as down before its members are torn down (R-040).
  std::shared_ptr<void> lifetime_token_;
  runtime::ConfigSnapshotRef config_snapshot_;
  // M4-A: the one published generation used by every newly accepted request.
  std::atomic<runtime::RuntimeGenerationRef> current_generation_;
  routing::RouteTable routes_;
  std::shared_ptr<runtime::WorkerShared> worker_shared_;
  std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions_;
  std::vector<std::shared_ptr<observability::Metrics>> worker_metrics_;
  std::vector<std::shared_ptr<std::atomic<std::uint64_t>>> client_counts_;
  std::vector<std::shared_ptr<runtime::WorkerData>> worker_datas_;
  std::unique_ptr<runtime::WorkerSet> workers_;
  std::shared_ptr<health::Coordinator> coordinator_;
  std::unique_ptr<net::Acceptor> acceptor_;
  std::unique_ptr<runtime::ReloadController> reload_controller_;
  std::unique_ptr<net::Channel> reload_channel_;
  std::unique_ptr<runtime::ReloadWatcher> reload_watcher_;
  std::unique_ptr<net::Channel> sighup_channel_;
  std::unique_ptr<net::Channel> inotify_channel_;
  std::unique_ptr<net::Channel> watcher_timer_channel_;
  std::shared_ptr<runtime::GenerationMailbox> generation_mailbox_;
  std::shared_ptr<PendingPrepare> pending_prepare_;
  std::unique_ptr<net::Channel> generation_mailbox_channel_;
  std::unordered_map<std::uint64_t, RetiringGeneration> retiring_generations_;
  std::vector<std::thread> retirement_reapers_;
  net::StreamFlowControl flow_control_;
  std::string config_path_;
  std::uint64_t last_reload_result_sequence_ = 0;
};

} // namespace aegisgate::gateway
