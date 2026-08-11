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
#include "aegisgate/resilience/GlobalAdmission.h"

namespace aegisgate::gateway {
namespace {
// Bounded shutdown-destroy retries: a live, draining worker always accepts the
// destroy task on the first tries, so exhausting this bound means the worker
// thread is gone and retrying would spin forever (R-063).
constexpr int kShutdownPostAttempts = 256;

// Fatal-crash leak holder: when a worker thread died without accepting its
// destroy task, the WorkerData is kept alive here rather than destroyed
// off-thread (its loop-attached objects can only be torn down on the worker
// thread).  Reached only when the worker has already crashed.
std::vector<std::shared_ptr<runtime::WorkerData>> &WorkerDataLeaks() {
  static std::vector<std::shared_ptr<runtime::WorkerData>> leaks;
  return leaks;
}
} // namespace

Gateway::Gateway(net::EventLoop &loop, config::Config config, std::string_view listen_address,
                 std::uint16_t listen_port, net::StreamFlowControl flow_control)
    : loop_(loop), lifetime_token_(std::make_shared<int>(0)),
      config_snapshot_(std::make_shared<runtime::ConfigSnapshot>(
          runtime::ConfigSnapshot{1, std::move(config)})),
      routes_(config_snapshot_->config), worker_shared_(std::make_shared<runtime::WorkerShared>()),
      coordinator_(std::make_shared<health::Coordinator>(
          std::make_shared<config::Config>(config_snapshot_->config),
          health::Coordinator::Clock::now())),
      acceptor_(std::make_unique<net::Acceptor>(loop, listen_address, listen_port)),
      flow_control_(flow_control) {
  worker_shared_->config_snapshot.store(config_snapshot_, std::memory_order_release);
  worker_shared_->coordinator = coordinator_;
  worker_shared_->worker_count = config_snapshot_->config.workers;
  worker_shared_->flow_control = flow_control_;
  worker_shared_->lifetime_token = lifetime_token_;
  const auto now = resilience::GlobalAdmission::Clock::now();
  for (const config::Route &route : config_snapshot_->config.routes) {
    admissions_.push_back(std::make_shared<resilience::GlobalAdmission>(route, now));
  }
  worker_shared_->admissions = admissions_;
  coordinator_->SetAdmissions(admissions_);
  worker_shared_->metrics_renderer = [this] { return RenderMetrics(); };
  worker_metrics_.resize(worker_shared_->worker_count);
  client_counts_.resize(worker_shared_->worker_count);
  for (std::size_t index = 0; index < worker_shared_->worker_count; ++index) {
    worker_metrics_[index] = std::make_shared<observability::Metrics>();
    client_counts_[index] = std::make_shared<std::atomic<std::uint64_t>>(0);
  }
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
  lifetime_token_.reset();
  acceptor_.reset();
  coordinator_->BeginOutcomeStopping();
  // Original-index teardown (R-057): worker_datas_[i] always corresponds to
  // workers_->At(i), so a partially initialized set cannot misalign.
  for (std::size_t index = 0; index < worker_datas_.size(); ++index) {
    if (!worker_datas_[index]) continue;
    auto &data = worker_datas_[index];
    auto &worker = workers_->At(index);
    // The destroy task must run on the worker thread (loop-attached objects);
    // it captures `data` (a reference into this member vector, alive until the
    // join below) and resets it when it runs.  R-063: bounded retry, never an
    // infinite spin — a live draining worker always accepts, so a rejection
    // means the worker thread is gone; abandon the worker data rather than
    // burn a core (it cannot be safely destroyed off-thread).
    bool accepted = false;
    for (int attempt = 0; attempt < kShutdownPostAttempts; ++attempt) {
      if (worker.PostWithLoop([&data](net::EventLoop &) mutable { data.reset(); })) {
        accepted = true;
        break;
      }
      std::this_thread::yield();
    }
    if (!accepted && data) {
      (void)std::fprintf(stderr,
                         "fatal: worker destroy task not accepted; worker data leaked\n");
      // Keep the WorkerData alive instead of destroying it off-thread (its
      // loop-attached objects can only be torn down on the worker thread).
      WorkerDataLeaks().push_back(std::move(data));
    }
  }
  if (workers_) workers_->StopAll();
  // The coordinator drains every published-but-undrained outcome on its own
  // loop before stopping.
  coordinator_->DrainOutcomesAndWait();
  coordinator_->Stop();
  // worker_datas_ entries are all empty: the destroy tasks ran on their
  // workers (or were intentionally abandoned after the bounded retry gave up).
}

void Gateway::Start() {
  coordinator_->Start();
  workers_ = std::make_unique<runtime::WorkerSet>(config_snapshot_->config.workers);
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

bool Gateway::EndpointHealthy(const config::Route &route,
                              const config::Endpoint &endpoint) const noexcept {
  const auto snapshot = coordinator_->CurrentSnapshot();
  if (!snapshot) return true;
  const std::size_t route_index = RouteIndexOf(route);
  if (route_index >= snapshot->endpoints.size()) return true;
  const std::size_t endpoint_index = EndpointIndexOf(route_index, endpoint);
  if (endpoint_index >= snapshot->endpoints[route_index].size()) return true;
  return snapshot->endpoints[route_index][endpoint_index].healthy;
}

resilience::CircuitBreaker::State
Gateway::BreakerState(const config::Route &route, const config::Endpoint &endpoint) const noexcept {
  const auto snapshot = coordinator_->CurrentSnapshot();
  if (!snapshot) return resilience::CircuitBreaker::State::kClosed;
  const std::size_t route_index = RouteIndexOf(route);
  if (route_index >= snapshot->endpoints.size()) return resilience::CircuitBreaker::State::kClosed;
  const std::size_t endpoint_index = EndpointIndexOf(route_index, endpoint);
  if (endpoint_index >= snapshot->endpoints[route_index].size()) {
    return resilience::CircuitBreaker::State::kClosed;
  }
  return static_cast<resilience::CircuitBreaker::State>(
      snapshot->endpoints[route_index][endpoint_index].breaker_state);
}

void Gateway::SubmitResultAndWait(const config::Route &route, const config::Endpoint &endpoint,
                                  bool success) {
  const auto snapshot = coordinator_->CurrentSnapshot();
  const std::size_t route_index = RouteIndexOf(route);
  const std::size_t endpoint_index = EndpointIndexOf(route_index, endpoint);
  if (!snapshot || route_index >= snapshot->endpoints.size() ||
      endpoint_index >= snapshot->endpoints[route_index].size()) {
    return;
  }
  const std::uint64_t generation = snapshot->endpoints[route_index][endpoint_index].generation;
  coordinator_->SubmitResultAndWait(
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
  runtime::WorkerRuntime &worker = workers_->Next();
  const std::size_t worker_index = [&worker, this] {
    for (std::size_t index = 0; index < workers_->size(); ++index) {
      if (&worker == &workers_->At(index)) return index;
    }
    return std::size_t(0);
  }();
  if (!worker.Post([this, worker_index, fd] {
        auto &slot = worker_datas_[worker_index];
        if (slot) {
          slot->Accept(fd);
        } else {
          (void)::close(fd);
        }
      })) {
    (void)::close(fd);
  }
}

std::size_t Gateway::RouteIndexOf(const config::Route &route) const noexcept {
  const std::vector<config::Route> &routes = routes_.Config().routes;
  for (std::size_t index = 0; index < routes.size(); ++index) {
    if (&routes[index] == &route) return index;
  }
  return routes.size();
}

std::size_t Gateway::EndpointIndexOf(std::size_t route_index,
                                     const config::Endpoint &endpoint) const noexcept {
  const std::vector<config::Route> &routes = routes_.Config().routes;
  if (route_index >= routes.size()) return routes.size();
  const std::vector<config::Endpoint> &endpoints = routes[route_index].endpoints;
  for (std::size_t index = 0; index < endpoints.size(); ++index) {
    if (endpoints[index].address == endpoint.address && endpoints[index].port == endpoint.port) {
      return index;
    }
  }
  return endpoints.size();
}

std::string Gateway::RenderMetrics() const {
  observability::Metrics::Data aggregate;
  for (const auto &metrics : worker_metrics_) {
    observability::Metrics::MergeInto(aggregate, metrics->Snapshot());
  }
  std::vector<observability::Metrics::ProtectionSample> protection;
  const auto snapshot = coordinator_->CurrentSnapshot();
  if (snapshot) {
    const std::vector<config::Route> &routes = routes_.Config().routes;
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
