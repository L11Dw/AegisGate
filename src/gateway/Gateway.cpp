#include "aegisgate/gateway/Gateway.h"

#include <chrono>
#include <cstdio>
#include <future>
#include <stdexcept>
#include <thread>
#include <utility>

#include <unistd.h>

#include "aegisgate/health/Coordinator.h"
#include "aegisgate/net/Acceptor.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/resilience/GlobalAdmission.h"

namespace aegisgate::gateway {

Gateway::Gateway(net::EventLoop &loop, config::Config config, std::string_view listen_address,
                 std::uint16_t listen_port, net::StreamFlowControl flow_control)
    : loop_(loop), lifetime_token_(std::make_shared<int>(0)),
      current_generation_(std::make_shared<runtime::RuntimeGeneration>(1, std::move(config))),
      routes_(CurrentGeneration()->snapshot()->config),
      worker_shared_(std::make_shared<runtime::WorkerShared>()),
      acceptor_(std::make_unique<net::Acceptor>(loop, listen_address, listen_port)),
      generation_mailbox_(std::make_shared<runtime::GenerationMailbox>()),
      flow_control_(flow_control) {
  if (!loop_.IsOwnerThread()) {
    throw std::logic_error("gateway must be constructed on its control EventLoop thread");
  }
  const auto generation = CurrentGeneration();
  worker_shared_->current_generation.store(generation, std::memory_order_release);
  worker_shared_->flow_control = flow_control_;
  worker_shared_->lifetime_token = lifetime_token_;
  worker_shared_->metrics_renderer = [this] { return RenderMetrics(); };
  worker_metrics_.resize(generation->snapshot()->config.workers);
  client_counts_.resize(generation->snapshot()->config.workers);
  for (std::size_t index = 0; index < generation->snapshot()->config.workers; ++index) {
    worker_metrics_[index] = std::make_shared<observability::Metrics>();
    client_counts_[index] = std::make_shared<std::atomic<std::uint64_t>>(0);
  }
  generation_mailbox_channel_ =
      std::make_unique<net::Channel>(loop_, generation_mailbox_->wake_fd());
  generation_mailbox_channel_->SetReadCallback([this] { HandleGenerationEvents(); });
  generation_mailbox_channel_->EnableReading();
  acceptor_->SetNewConnectionCallback([this](int fd) { Accept(fd); });
}

Gateway::~Gateway() {
  // Fixed shutdown order (R-040 extended, R-062): invalidate the lifetime
  // token first so any late callback sees the gateway as down; stop accepting;
  // refuse every new outcome reservation (no attempt may start against a
  // stopping channel); tear down each worker on its own thread (CancelAll and
  // connection teardown release their outcome reservations via RAII); join
  // them; only then drain the remaining published outcomes on the coordinator
  // loop and stop the coordinator — a worker still publishing to a stopped
  // coordinator would lose breaker accounting.
  if (!loop_.IsOwnerThread()) std::terminate();
  lifecycle_ = Lifecycle::kStopped;
  lifetime_token_.reset();
  acceptor_.reset();
  const auto generation = CurrentGeneration();
  if (generation) generation->coordinator()->BeginOutcomeStopping();
  // Give already-retired generations a chance to schedule their worker-local
  // lease returns while workers still own their EventLoops.
  HandleGenerationEvents();
  // Original-index teardown (R-057): worker_datas_[i] always corresponds to
  // workers_->At(i), so a partially initialized set cannot misalign.
  for (std::size_t index = 0; index < worker_datas_.size(); ++index) {
    if (!worker_datas_[index]) continue;
    auto &data = worker_datas_[index];
    auto &worker = workers_->At(index);
    // The destroy task must run on the worker thread (loop-attached objects);
    // it captures `data` (a reference into this member vector, alive until the
    // join below) and resets it when it runs.  R-063/R-067: the reserved
    // shutdown slot is always accepted while the worker runs, so the destroy
    // task is guaranteed to execute on its owner thread — a rejection means the
    // worker is already gone, which is an explicit shutdown failure (abort),
    // never a silent leak.
    if (!worker.PostShutdown([&data](net::EventLoop &) mutable { data.reset(); })) {
      (void)std::fprintf(stderr, "fatal: worker %zu destroy task not accepted\n", index);
      std::terminate();
    }
  }
  if (workers_) workers_->StopAll();
  workers_stopped_ = true;
  // Worker shutdown can release the final request lease.  At this point it is
  // safe to start any pending old-generation reaper directly: balances were
  // returned by WorkerData::Shutdown and no worker owner APIs remain needed.
  HandleGenerationEvents();
  // The coordinator drains every published-but-undrained outcome on its own
  // loop before stopping.
  if (generation) {
    generation->coordinator()->DrainOutcomesAndWait();
    generation->coordinator()->Stop();
  }
  for (std::thread &reaper : retirement_reapers_) {
    if (reaper.joinable()) reaper.join();
  }
  generation_mailbox_channel_.reset();
  generation_mailbox_->Close();
  // worker_datas_ entries are all empty: the destroy tasks ran on their workers.
}

bool Gateway::RequestReload(config::Config candidate) {
  if (!loop_.IsOwnerThread() || lifecycle_ != Lifecycle::kRunning) return false;
  const auto previous = CurrentGeneration();
  if (!previous || candidate.workers != previous->snapshot()->config.workers) return false;

  runtime::RuntimeGenerationRef replacement;
  try {
    replacement = std::make_shared<runtime::RuntimeGeneration>(previous->version() + 1,
                                                                std::move(candidate));
    replacement->coordinator()->Start();
  } catch (...) {
    // Nothing was published, so every old runtime object remains untouched.
    return false;
  }

  // Publish only after every resource belonging to the candidate exists and
  // its coordinator loop is running.  All worker request paths load this one
  // pointer once; retries retain the old pointer through ProxyTransaction.
  routes_ = routing::RouteTable(replacement->snapshot()->config);
  current_generation_.store(replacement, std::memory_order_release);
  worker_shared_->current_generation.store(replacement, std::memory_order_release);
  RetireGeneration(previous);
  return true;
}

void Gateway::RetireGeneration(runtime::RuntimeGenerationRef generation) {
  if (!generation || !loop_.IsOwnerThread()) return;
  const auto [entry, inserted] = retiring_generations_.try_emplace(
      generation->version(), RetiringGeneration{generation, 0});
  if (!inserted) return;
  const std::weak_ptr<runtime::GenerationMailbox> mailbox = generation_mailbox_;
  if (!generation->BeginRetirement([mailbox, generation] {
        const auto alive = mailbox.lock();
        if (!alive ||
            !alive->Post({runtime::GenerationMailbox::Kind::kLastRequestLeaseReleased,
                          generation})) {
          std::terminate();
        }
      })) {
    retiring_generations_.erase(entry);
  }
}

void Gateway::HandleGenerationEvents() {
  if (!loop_.IsOwnerThread()) std::terminate();
  for (runtime::GenerationMailbox::Event event : generation_mailbox_->Drain()) {
    if (!event.generation) continue;
    const auto found = retiring_generations_.find(event.generation->version());
    if (found == retiring_generations_.end()) continue;
    if (event.kind == runtime::GenerationMailbox::Kind::kLastRequestLeaseReleased) {
      RequestWorkerBalanceReturn(event.generation);
      continue;
    }
    if (event.kind == runtime::GenerationMailbox::Kind::kWorkerBalancesReturned) {
      ++found->second.returned_worker_balances;
      if (found->second.returned_worker_balances == worker_datas_.size()) {
        StartRetirementReaper(event.generation);
      }
      continue;
    }
    if (event.kind == runtime::GenerationMailbox::Kind::kReaperFinished) {
      event.generation->MarkRetired();
      retiring_generations_.erase(found);
    }
  }
}

void Gateway::RequestWorkerBalanceReturn(const runtime::RuntimeGenerationRef &generation) {
  if (workers_stopped_ || worker_datas_.empty()) {
    StartRetirementReaper(generation);
    return;
  }
  for (std::size_t index = 0; index < worker_datas_.size(); ++index) {
    const auto data = worker_datas_[index];
    if (!data) {
      (void)generation_mailbox_->Post(
          {runtime::GenerationMailbox::Kind::kWorkerBalancesReturned, generation});
      continue;
    }
    if (!workers_->At(index).PostWithLoop(
            [data, generation, mailbox = generation_mailbox_](net::EventLoop &) {
              data->ReturnGenerationLeaseBalance(generation->version());
              if (!mailbox->Post(
                      {runtime::GenerationMailbox::Kind::kWorkerBalancesReturned, generation})) {
                std::terminate();
              }
            })) {
      std::terminate();
    }
  }
}

void Gateway::StartRetirementReaper(const runtime::RuntimeGenerationRef &generation) {
  if (!generation || !generation->BeginReaping()) return;
  const auto mailbox = generation_mailbox_;
  retirement_reapers_.emplace_back([generation, mailbox] {
    try {
      generation->coordinator()->BeginOutcomeStopping();
      generation->coordinator()->DrainOutcomesAndWait();
      generation->coordinator()->Stop();
      if (!mailbox->Post({runtime::GenerationMailbox::Kind::kReaperFinished, generation})) {
        std::terminate();
      }
    } catch (...) {
      std::terminate();
    }
  });
}

void Gateway::Start() {
  if (lifecycle_ != Lifecycle::kNotStarted) {
    throw std::logic_error("gateway cannot be started twice");
  }
  lifecycle_ = Lifecycle::kStarting;
  const auto generation = CurrentGeneration();
  if (!generation) throw std::logic_error("gateway has no runtime generation");
  generation->coordinator()->Start();
  workers_ = std::make_unique<runtime::WorkerSet>(generation->snapshot()->config.workers);
  workers_->Start();
  worker_datas_.resize(workers_->size());
  for (std::size_t index = 0; index < workers_->size(); ++index) {
    std::promise<void> ready;
    auto future = ready.get_future();
    auto &slot = worker_datas_[index];
    auto &worker = workers_->At(index);
    if (!worker.PostWithLoop([this, index, &slot, &ready](net::EventLoop &loop) {
          try {
            slot = std::make_shared<runtime::WorkerData>(
                loop, worker_shared_, static_cast<std::uint32_t>(index), worker_metrics_[index],
                client_counts_[index]);
            ready.set_value();
          } catch (...) {
            ready.set_exception(std::current_exception());
          }
        })) {
      throw std::logic_error("worker init task was not accepted");
    }
    future.get();
  }
  acceptor_->Listen();
  lifecycle_ = Lifecycle::kRunning;
}

std::uint16_t Gateway::port() const { return acceptor_->port(); }

std::size_t Gateway::ClientCount() const noexcept {
  std::size_t total = 0;
  for (const auto &count : client_counts_) {
    total += count->load(std::memory_order_acquire);
  }
  return total;
}

std::string Gateway::MetricsText() { return RenderMetrics(); }

bool Gateway::EndpointHealthy(std::size_t route_index,
                              std::size_t endpoint_index) const noexcept {
  const auto generation = CurrentGeneration();
  if (!generation) return true;
  const auto snapshot = generation->coordinator()->CurrentSnapshot();
  if (!snapshot) return true;
  if (route_index >= snapshot->endpoints.size() ||
      endpoint_index >= snapshot->endpoints[route_index].size()) {
    return true;
  }
  return snapshot->endpoints[route_index][endpoint_index].healthy;
}

resilience::CircuitBreaker::State
Gateway::BreakerState(std::size_t route_index, std::size_t endpoint_index) const noexcept {
  const auto generation = CurrentGeneration();
  if (!generation) return resilience::CircuitBreaker::State::kClosed;
  const auto snapshot = generation->coordinator()->CurrentSnapshot();
  if (!snapshot) return resilience::CircuitBreaker::State::kClosed;
  if (route_index >= snapshot->endpoints.size() ||
      endpoint_index >= snapshot->endpoints[route_index].size()) {
    return resilience::CircuitBreaker::State::kClosed;
  }
  return static_cast<resilience::CircuitBreaker::State>(
      snapshot->endpoints[route_index][endpoint_index].breaker_state);
}

void Gateway::SubmitResultAndWait(std::size_t route_index, std::size_t endpoint_index,
                                  bool success) {
  const auto generation_ref = CurrentGeneration();
  if (!generation_ref) return;
  const auto snapshot = generation_ref->coordinator()->CurrentSnapshot();
  if (!snapshot || route_index >= snapshot->endpoints.size() ||
      endpoint_index >= snapshot->endpoints[route_index].size()) {
    return;
  }
  const std::uint64_t generation = snapshot->endpoints[route_index][endpoint_index].generation;
  generation_ref->coordinator()->SubmitResultAndWait(
      {route_index, endpoint_index, {false, generation, 0}, success});
}

void Gateway::Accept(int fd) {
  // The handoff task owns the fd: once accepted by a worker it is registered
  // there and never touched by this thread; on a rejected post the fd is
  // closed here (the submitting side owns the failure).  The task runs on
  // the target worker, which owns the WorkerData slot it reads.
  if (!workers_ || worker_datas_.empty()) {
    (void)::close(fd);
    return;
  }
  const runtime::WorkerSet::WorkerHandle handle = workers_->Next();
  // R-056: on success the handler adopts the move-only FdOwner; on rejection
  // PostFd closes it, so the gateway never closes it twice.
  (void)handle.worker.PostFd(fd, [this, worker_index = handle.index](net::FdOwner fd) {
    auto &slot = worker_datas_[worker_index];
    if (slot) {
      slot->Accept(std::move(fd));
    }
    // If the slot is gone (worker tearing down), the FdOwner closes the
    // descriptor on destruction.
  });
}

std::string Gateway::RenderMetrics() const {
  // Take the coordinator protection snapshot before the worker counters so the
  // protection gauges and the counters are observed in one consistent order
  // (R-059).
  const auto generation = CurrentGeneration();
  if (!generation) return observability::Metrics::RenderPrometheus({}, {});
  const auto snapshot = generation->coordinator()->CurrentSnapshot();
  observability::Metrics::Data aggregate;
  for (const auto &metrics : worker_metrics_) {
    observability::Metrics::MergeInto(aggregate, metrics->Snapshot());
  }
  std::vector<observability::Metrics::ProtectionSample> protection;
  if (snapshot) {
    // Iterate the config snapshot (the same boundary workers and the
    // coordinator use), never the table's own copy (R-060).
    const std::vector<config::Route> &routes = generation->snapshot()->config.routes;
    for (std::size_t route_index = 0; route_index < routes.size(); ++route_index) {
      if (!routes[route_index].circuit_breaker.has_value() &&
          !routes[route_index].health_check.has_value()) {
        continue;
      }
      if (route_index >= snapshot->endpoints.size()) continue;
      for (std::size_t endpoint_index = 0; endpoint_index < routes[route_index].endpoints.size();
           ++endpoint_index) {
        if (endpoint_index >= snapshot->endpoints[route_index].size()) continue;
        const health::EndpointDecision &decision = snapshot->endpoints[route_index][endpoint_index];
        observability::Metrics::ProtectionSample sample;
        sample.route = routes[route_index].name;
        const config::Endpoint &endpoint = routes[route_index].endpoints[endpoint_index];
        sample.upstream = endpoint.host + ":" + std::to_string(endpoint.port);
        switch (decision.breaker_state) {
        case 1: sample.state = "open"; break;
        case 2: sample.state = "half_open"; break;
        default: sample.state = "closed"; break;
        }
        sample.healthy = decision.healthy;
        protection.push_back(std::move(sample));
      }
    }
  }
  return observability::Metrics::RenderPrometheus(aggregate, protection);
}

} // namespace aegisgate::gateway
