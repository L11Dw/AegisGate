#include "aegisgate/runtime/WorkerData.h"

#include <set>
#include <stdexcept>
#include <utility>

#include "aegisgate/http/HttpResponse.h"
#include "aegisgate/routing/RouteTable.h"

namespace aegisgate::runtime {
namespace {

// R-065: the accepted descriptor is owned by RAII until ClientConnection takes
// ownership, so an allocation failure in the construction path can never leak
// the fd.  release() transfers ownership; the destructor closes otherwise.
class FdOwner {
public:
  explicit FdOwner(int fd) : fd_(fd) {}
  ~FdOwner() { if (fd_ >= 0) (void)::close(fd_); }
  FdOwner(const FdOwner &) = delete;
  FdOwner &operator=(const FdOwner &) = delete;
  [[nodiscard]] int get() const noexcept { return fd_; }
  [[nodiscard]] int release() noexcept {
    const int fd = fd_;
    fd_ = -1;
    return fd;
  }

private:
  int fd_;
};

} // namespace

WorkerData::WorkerData(net::EventLoop &loop, std::shared_ptr<WorkerShared> shared,
                       std::uint32_t worker_index,
                       std::shared_ptr<observability::Metrics> metrics,
                       std::shared_ptr<std::atomic<std::uint64_t>> client_count)
    : loop_(loop), shared_(std::move(shared)), worker_index_(worker_index),
      state_(std::make_shared<State>()), pool_(std::make_shared<proxy::UpstreamPool>(loop)),
      timers_(std::make_unique<net::TimerQueue>(loop)), metrics_(std::move(metrics)),
      client_count_(std::move(client_count)),
      // Bind the worker-local selection state to the request snapshot version
      // it was built from (R-072); a reload rebuilds it for the new version.
      selection_(shared_->config_snapshot.load(std::memory_order_acquire)->config,
                 shared_->config_snapshot.load(std::memory_order_acquire)->version),
      lease_balances_(shared_->config_snapshot.load(std::memory_order_acquire)->config.routes.size(), 0) {
  state_->owner = this;
}

WorkerData::~WorkerData() {
  // Invalidate the owner first: a deferred reap task holds State alive and
  // must never reach the destroyed clients map (R-024/R-040 pattern).
  state_->owner = nullptr;
  // Runs on the worker thread: every connection, pooled descriptor and timer
  // is destroyed here (their Channels unregister before the loop dies).
  Shutdown();
}

void WorkerData::Accept(int fd) {
  // R-065: the descriptor is RAII-owned here and released only once
  // ClientConnection has been constructed and taken ownership; an allocation
  // failure in the construction path closes it instead of leaking.  The
  // handoff task (PostFd) owns the fd until Accept runs.
  FdOwner owned(fd);
  std::uint64_t identifier = 0;
  bool inserted = false;
  if (next_client_identifier_ == 0) return;  // FdOwner closes
  try {
    identifier = next_client_identifier_++;
    auto client = std::make_unique<net::ClientConnection>(
        loop_, owned.get(),
        [this](net::ClientConnection &connection, const http::HttpRequest &request) {
          // R-066: an unexpected request-handling exception must not leave
          // the connection paused with the request half-processed; answer with
          // a controlled 500, or abort the connection if its state no longer
          // permits a new response.
          try {
            HandleRequest(connection, request);
          } catch (...) {
            try {
              metrics_->RecordImmediate("_internal_error", 500);
            } catch (...) {
            }
            try {
              connection.SendResponse(http::HttpResponse{500, "Internal Server Error", {}, ""});
            } catch (const std::logic_error &) {
              connection.AbortResponse();
            } catch (const std::system_error &) {
              connection.AbortResponse();
            }
          }
        },
        shared_->flow_control);
    (void)owned.release();  // ClientConnection owns the descriptor from here
    client->SetCloseCallback([&loop = loop_, state = std::weak_ptr<State>(state_),
                              identifier] { NotifyClientClosed(loop, state, identifier); });
    const auto result = clients_.emplace(identifier, std::move(client));
    if (!result.second) throw std::logic_error("duplicate accepted client identifier");
    inserted = true;
    metrics_->SetActiveConnections(clients_.size());
    result.first->second->Start();
  } catch (...) {
    if (inserted) clients_.erase(identifier);
    metrics_->SetActiveConnections(clients_.size());
  }
  client_count_->store(clients_.size(), std::memory_order_release);
}

void WorkerData::HandleRequest(net::ClientConnection &client, const http::HttpRequest &request) {
  if (request.method == "GET" && request.target == "/metrics") {
    try {
      client.SendResponse(http::HttpResponse{
          200, "OK", {{"Content-Type", "text/plain; version=0.0.4; charset=utf-8"}},
          shared_->metrics_renderer()});
    } catch (const std::logic_error &) {
    } catch (const std::system_error &) {
    }
    return;
  }
  const ConfigSnapshotRef snapshot = shared_->config_snapshot.load(std::memory_order_acquire);
  const std::optional<std::size_t> route_index =
      routing::RouteTable::Match(snapshot->config, request.Header("host"), request.target);
  if (!route_index.has_value()) {
    try {
      metrics_->RecordImmediate("_unmatched", 404);
    } catch (...) {
    }
    try {
      client.SendResponse(http::HttpResponse{404, "Not Found", {}, ""});
    } catch (const std::logic_error &) {
    } catch (const std::system_error &) {
    }
    return;
  }
  const config::Route &route = snapshot->config.routes[*route_index];

  // Global admission: the in-flight slot first, then one lease token from the
  // worker-local balance (drawing a new lease when it runs low).  A rejection
  // answers 429 immediately and never starts a transaction.
  std::optional<resilience::GlobalAdmission::Reservation> reservation;
  if (!TryAdmit(*route_index, reservation)) {
    try {
      metrics_->RecordImmediate(route.name, 429, {}, true);
    } catch (...) {
    }
    try {
      client.SendResponse(http::HttpResponse{429, "Too Many Requests", {}, ""});
    } catch (const std::logic_error &) {
    } catch (const std::system_error &) {
    }
    return;
  }

  proxy::UpstreamPolicy policy;
  policy.connect_timeout = std::chrono::milliseconds(route.connect_timeout_ms);
  policy.first_byte_timeout = std::chrono::milliseconds(route.first_byte_timeout_ms);
  policy.total_timeout = std::chrono::milliseconds(route.total_timeout_ms);
  policy.retry_budget = route.retry_budget;
  // The provider chooses the endpoint for the initial attempt and every
  // retry through the same eligibility rules (R-036), bound to the request's
  // own snapshot (R-054); no candidate ever connects.
  const bool least_active = route.balance == config::BalancePolicy::kLeastActive;
  AttemptSelector selector(selection_, shared_, *route_index, snapshot);
  proxy::ProxyTransaction::AttemptProvider provider =
      [selector = std::move(selector), least_active]() mutable {
        return selector.Select(least_active);
      };
  (void)proxy::ProxyTransaction::Start(
      loop_, client, route.endpoints.front(), request, pool_, std::move(reservation),
      timers_.get(), std::move(policy), metrics_, route.name, std::move(provider),
      std::weak_ptr<void>(shared_->lifetime_token));
}

bool WorkerData::TryAdmit(std::size_t route_index,
                          std::optional<resilience::GlobalAdmission::Reservation> &reservation) {
  const auto &admission = shared_->admissions[route_index];
  reservation = admission->TryAcquireInflight();
  if (!reservation) return false;
  // Lease: draw a fresh batch when the local balance runs low (an empty
  // balance must always trigger a draw, hence balance * 2 < batch rather
  // than a half-open comparison that never fires for batch == 1); refresh
  // first returns the remainder so the global credit stays exact.
  std::uint32_t &balance = lease_balances_[route_index];
  const std::uint32_t batch = resilience::GlobalAdmission::LeaseBatch(
      admission->rate(), shared_->worker_count, admission->burst());
  if (balance * 2 < batch) {
    admission->Return(balance);
    balance = 0;
    balance = admission->Draw(batch);
  }
  if (balance == 0) {
    // Token rejection: the reservation is released by its RAII guard.
    reservation.reset();
    return false;
  }
  --balance;
  return true;
}

void WorkerData::Shutdown() noexcept {
  // Return every unspent lease before the balances are destroyed: a worker
  // that stops holding a lease must not leak global tokens (R-055).  Balances
  // are zeroed on return, so repeated shutdown returns nothing.  This runs
  // before any worker-local selection state is destroyed (the destructor body
  // precedes member destruction) and touches admissions only while they are
  // still alive (held via shared_).
  for (std::size_t route = 0; route < lease_balances_.size() &&
                              route < shared_->admissions.size();
       ++route) {
    shared_->admissions[route]->Return(lease_balances_[route]);
    lease_balances_[route] = 0;
  }
  pool_->CancelAll();
  clients_.clear();
  metrics_->SetActiveConnections(0);
  client_count_->store(0, std::memory_order_release);
}

void WorkerData::NotifyClientClosed(net::EventLoop &loop, std::weak_ptr<State> weak_state,
                                    std::uint64_t identifier) {
  const auto state = weak_state.lock();
  if (!state || state->owner == nullptr) return;
  state->closed_clients.push_back(identifier);
  if (state->cleanup_scheduled) return;
  loop.QueueAfterCurrentBatch([state] {
    state->cleanup_scheduled = false;
    if (state->owner == nullptr) return;
    auto identifiers = std::move(state->closed_clients);
    state->closed_clients.clear();
    state->owner->ReapClosedClients(std::move(identifiers));
  });
  state->cleanup_scheduled = true;
}

void WorkerData::ReapClosedClients(std::vector<std::uint64_t> identifiers) {
  for (const std::uint64_t identifier : identifiers) clients_.erase(identifier);
  metrics_->SetActiveConnections(clients_.size());
  client_count_->store(clients_.size(), std::memory_order_release);
}

} // namespace aegisgate::runtime
