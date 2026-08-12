#include "aegisgate/runtime/WorkerData.h"

#include <set>
#include <stdexcept>
#include <utility>

#include "aegisgate/http/HttpResponse.h"
#include "aegisgate/routing/RouteTable.h"

namespace aegisgate::runtime {

WorkerData::WorkerData(net::EventLoop &loop, std::shared_ptr<WorkerShared> shared,
                       std::uint32_t worker_index,
                       std::shared_ptr<observability::Metrics> metrics,
                       std::shared_ptr<std::atomic<std::uint64_t>> client_count)
    : loop_(loop), shared_(std::move(shared)), worker_index_(worker_index),
      state_(std::make_shared<State>()), pool_(std::make_shared<proxy::UpstreamPool>(loop)),
      timers_(std::make_unique<net::TimerQueue>(loop)), metrics_(std::move(metrics)),
      client_count_(std::move(client_count)) {
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

void WorkerData::Accept(net::FdOwner fd) {
  // R-065: the descriptor is RAII-owned and released only once ClientConnection
  // has been constructed and taken ownership; an allocation failure in the
  // construction path closes it (via the FdOwner) instead of leaking.
  std::uint64_t identifier = 0;
  bool inserted = false;
  if (next_client_identifier_ == 0) return;  // FdOwner closes
  try {
    identifier = next_client_identifier_++;
    auto client = std::make_unique<net::ClientConnection>(
        loop_, fd.get(),
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
    (void)fd.release();  // ClientConnection owns the descriptor from here
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
  const RuntimeGenerationRef generation =
      shared_->current_generation.load(std::memory_order_acquire);
  if (!generation || !generation->snapshot() || worker_index_ >= generation->selection_states().size()) {
    try {
      metrics_->RecordImmediate("_generation_unavailable", 503);
      client.SendResponse(http::HttpResponse{503, "Service Unavailable", {}, ""});
    } catch (...) {
    }
    return;
  }
  const ConfigSnapshotRef snapshot = generation->snapshot();
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

  // A generation lease is acquired once for the entire request, before any
  // admission/attempt work.  The ProxyTransaction retains it through retries,
  // streaming writes and all terminal cleanup, so a reload cannot retire this
  // generation underneath an in-flight request.
  auto generation_lease = generation->TryAcquireRequestLease();
  if (!generation_lease) {
    try {
      metrics_->RecordImmediate(route.name, 503, {}, false, "generation_retiring");
      client.SendResponse(http::HttpResponse{503, "Service Unavailable", {}, ""});
    } catch (...) {
    }
    return;
  }

  // Global admission: the in-flight slot first, then one lease token from the
  // worker-local balance (drawing a new lease when it runs low).  A rejection
  // answers 429 immediately and never starts a transaction.
  std::optional<resilience::GlobalAdmission::Reservation> reservation;
  if (!TryAdmit(generation, *route_index, reservation)) {
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
  AttemptSelector selector(*generation->selection_states()[worker_index_], generation, *route_index);
  proxy::ProxyTransaction::AttemptProvider provider =
      [selector = std::move(selector), least_active]() mutable {
        return selector.Select(least_active);
      };
  (void)proxy::ProxyTransaction::Start(
      loop_, client, route.endpoints.front(), request, pool_, std::move(reservation),
      timers_.get(), std::move(policy), metrics_, route.name, std::move(provider),
      std::weak_ptr<void>(shared_->lifetime_token), std::move(generation_lease));
}

bool WorkerData::TryAdmit(const RuntimeGenerationRef &generation, std::size_t route_index,
                          std::optional<resilience::GlobalAdmission::Reservation> &reservation) {
  if (!generation || route_index >= generation->admissions().size()) return false;
  const auto &admission = generation->admissions()[route_index];
  reservation = admission->TryAcquireInflight();
  if (!reservation) return false;
  // Lease: draw a fresh batch when the local balance runs low (an empty
  // balance must always trigger a draw, hence balance * 2 < batch rather
  // than a half-open comparison that never fires for batch == 1); refresh
  // first returns the remainder so the global credit stays exact.
  auto [balances, inserted] = lease_balances_.try_emplace(
      generation->version(), LeaseBalance{generation->admissions(),
                                           std::vector<std::uint32_t>(generation->admissions().size(), 0)});
  (void)inserted;
  std::uint32_t &balance = balances->second.balances[route_index];
  const std::uint32_t batch = resilience::GlobalAdmission::LeaseBatch(
      admission->rate(), generation->snapshot()->config.workers, admission->burst());
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
  for (auto &[version, balances] : lease_balances_) {
    (void)version;
    for (std::size_t route = 0; route < balances.balances.size() &&
                                route < balances.admissions.size();
         ++route) {
      balances.admissions[route]->Return(balances.balances[route]);
      balances.balances[route] = 0;
    }
  }
  pool_->CancelAll();
  clients_.clear();
  metrics_->SetActiveConnections(0);
  client_count_->store(0, std::memory_order_release);
}

void WorkerData::ReturnGenerationLeaseBalance(std::uint64_t generation_version) noexcept {
  const auto found = lease_balances_.find(generation_version);
  if (found == lease_balances_.end()) return;
  LeaseBalance &balances = found->second;
  for (std::size_t route = 0; route < balances.balances.size() &&
                              route < balances.admissions.size();
       ++route) {
    balances.admissions[route]->Return(balances.balances[route]);
    balances.balances[route] = 0;
  }
  lease_balances_.erase(found);
}

std::shared_ptr<SelectionState> WorkerData::PrepareSelectionState(
    const config::Config &config, std::uint64_t version) {
  try {
    return std::make_shared<SelectionState>(config, version);
  } catch (...) {
    return nullptr;
  }
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
