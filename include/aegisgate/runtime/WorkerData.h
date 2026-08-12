#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/net/ClientConnection.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/Fd.h"
#include "aegisgate/net/StreamFlowControl.h"
#include "aegisgate/net/TimerQueue.h"
#include "aegisgate/observability/Metrics.h"
#include "aegisgate/proxy/ProxyTransaction.h"
#include "aegisgate/proxy/UpstreamPool.h"
#include "aegisgate/runtime/AttemptSelector.h"
#include "aegisgate/runtime/SelectionState.h"
#include "aegisgate/runtime/WorkerShared.h"

namespace aegisgate::runtime {

// One worker's data plane: the client connections, upstream pool, timer
// queue, metrics, selection state and lease balances, all confined to the
// worker thread.  Constructed and destroyed only on that thread (via
// PostWithLoop init/destroy tasks); the gateway never touches these objects
// directly, only their atomics and the shared Metrics instances.
class WorkerData {
public:
  WorkerData(net::EventLoop &loop, std::shared_ptr<WorkerShared> shared,
             std::uint32_t worker_index, std::shared_ptr<observability::Metrics> metrics,
             std::shared_ptr<std::atomic<std::uint64_t>> client_count);
  ~WorkerData();

  WorkerData(const WorkerData &) = delete;
  WorkerData &operator=(const WorkerData &) = delete;

  // Runs on the worker thread: takes ownership of the accepted descriptor
  // (already nonblocking) and registers a client connection.
  void Accept(net::FdOwner fd);
  // Runs on the worker thread during shutdown: terminates every exchange
  // (pool CancelAll) and drops the clients so transactions release via RAII.
  void Shutdown() noexcept;
  // Owner-worker thread only.  Before a retired generation's coordinator is
  // stopped, return this worker's unused token lease for that generation.
  // It is idempotent and intentionally does not touch live transactions: their
  // GlobalAdmission reservations remain owned by ProxyTransaction RAII.
  void ReturnGenerationLeaseBalance(std::uint64_t generation_version) noexcept;
  [[nodiscard]] std::shared_ptr<observability::Metrics> Metrics() const noexcept {
    return metrics_;
  }

private:
  struct State {
    WorkerData *owner = nullptr;
    bool cleanup_scheduled = false;
    std::vector<std::uint64_t> closed_clients;
  };

  void HandleRequest(net::ClientConnection &client, const http::HttpRequest &request);
  void ReapClosedClients(std::vector<std::uint64_t> identifiers);
  static void NotifyClientClosed(net::EventLoop &loop, std::weak_ptr<State> state,
                                 std::uint64_t identifier);
  [[nodiscard]] bool TryAdmit(const RuntimeGenerationRef &generation, std::size_t route_index,
                              std::optional<resilience::GlobalAdmission::Reservation> &reservation);
  struct LeaseBalance {
    std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions;
    std::vector<std::uint32_t> balances;
  };

  net::EventLoop &loop_;
  std::shared_ptr<WorkerShared> shared_;
  std::uint32_t worker_index_;
  std::shared_ptr<State> state_;
  std::shared_ptr<proxy::UpstreamPool> pool_;
  std::unique_ptr<net::TimerQueue> timers_;
  std::shared_ptr<observability::Metrics> metrics_;
  std::shared_ptr<std::atomic<std::uint64_t>> client_count_;
  std::unordered_map<std::uint64_t, std::unique_ptr<net::ClientConnection>> clients_;
  std::uint64_t next_client_identifier_ = 1;
  // A request selects exclusively from the state owned by its generation.
  // Local admission leases are keyed by generation version so tokens drawn
  // before a reload are returned to the matching GlobalAdmission, never to a
  // newly published route with the same index.
  std::unordered_map<std::uint64_t, LeaseBalance> lease_balances_;
};

} // namespace aegisgate::runtime
