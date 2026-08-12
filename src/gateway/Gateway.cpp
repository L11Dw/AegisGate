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
#include "aegisgate/runtime/SelectionState.h"
#include "aegisgate/runtime/SelectionState.h"

namespace aegisgate::gateway {

Gateway::Gateway(net::EventLoop &loop, config::Config config, std::string_view listen_address,
                 std::uint16_t listen_port, net::StreamFlowControl flow_control,
                 std::string config_path)
    : loop_(loop), lifetime_token_(std::make_shared<int>(0)),
      config_snapshot_(std::make_shared<runtime::ConfigSnapshot>(
          runtime::ConfigSnapshot{1, std::move(config)})),
      routes_(config_snapshot_->config),
      worker_shared_(std::make_shared<runtime::WorkerShared>()),
      coordinator_(std::make_shared<health::Coordinator>(
          std::make_shared<config::Config>(config_snapshot_->config),
          health::Coordinator::Clock::now())),
      acceptor_(std::make_unique<net::Acceptor>(loop, listen_address, listen_port)),
      generation_mailbox_(std::make_shared<runtime::GenerationMailbox>()),
      flow_control_(flow_control) {
  if (!loop_.IsOwnerThread()) {
    throw std::logic_error("gateway must be constructed on its control EventLoop thread");
  }
  // Set up admissions before creating the generation.
  const auto now = resilience::GlobalAdmission::Clock::now();
  for (const config::Route &route : config_snapshot_->config.routes) {
    admissions_.push_back(std::make_shared<resilience::GlobalAdmission>(route, now));
  }
  coordinator_->SetAdmissions(admissions_);
  // Build per-worker selection states.
  std::vector<std::shared_ptr<runtime::SelectionState>> sel_states;
  sel_states.reserve(config_snapshot_->config.workers);
  for (std::uint32_t i = 0; i < config_snapshot_->config.workers; ++i) {
    sel_states.push_back(std::make_shared<runtime::SelectionState>(
        config_snapshot_->config, config_snapshot_->version));
  }
  // Create the initial RuntimeGeneration.
  auto generation = std::make_shared<runtime::RuntimeGeneration>(
      config_snapshot_->version, config_snapshot_, coordinator_, admissions_,
      std::move(sel_states));
  current_generation_.store(generation, std::memory_order_release);
  // Wire WorkerShared.
  worker_shared_->config_snapshot.store(config_snapshot_, std::memory_order_release);
  worker_shared_->coordinator = coordinator_;
  worker_shared_->admissions = admissions_;
  worker_shared_->current_generation.store(generation, std::memory_order_release);
  worker_shared_->worker_count = config_snapshot_->config.workers;
  worker_shared_->flow_control = flow_control_;
  worker_shared_->lifetime_token = lifetime_token_;
  worker_shared_->metrics_renderer = [this] { return RenderMetrics(); };
  worker_metrics_.resize(config_snapshot_->config.workers);
  client_counts_.resize(config_snapshot_->config.workers);
  for (std::size_t index = 0; index < config_snapshot_->config.workers; ++index) {
    worker_metrics_[index] = std::make_shared<observability::Metrics>();
    client_counts_[index] = std::make_shared<std::atomic<std::uint64_t>>(0);
  }
  // Generation mailbox channel: wakes the control loop on lifecycle events.
  generation_mailbox_channel_ =
      std::make_unique<net::Channel>(loop_, generation_mailbox_->wake_fd());
  generation_mailbox_channel_->SetReadCallback([this] { HandleGenerationEvents(); });
  generation_mailbox_channel_->EnableReading();
  // ReloadController: background YAML parse with eventfd wake.
  if (!config_path.empty()) {
    config_path_ = std::move(config_path);
    reload_controller_ = std::make_unique<runtime::ReloadController>(config_path_);
    reload_channel_ = std::make_unique<net::Channel>(loop_, reload_controller_->wake_fd());
    reload_channel_->SetReadCallback([this] { HandleReloadResults(); });
    reload_channel_->EnableReading();
    // ReloadWatcher: SIGHUP + inotify + debounce.
    reload_watcher_ = std::make_unique<runtime::ReloadWatcher>(
        config_path_, [this] { (void)RequestReload(); });
    if (reload_watcher_->sighup_fd() >= 0) {
      sighup_channel_ = std::make_unique<net::Channel>(loop_, reload_watcher_->sighup_fd());
      sighup_channel_->SetReadCallback([this] { reload_watcher_->HandleSighup(); });
      sighup_channel_->EnableReading();
    }
    if (reload_watcher_->inotify_fd() >= 0) {
      inotify_channel_ = std::make_unique<net::Channel>(loop_, reload_watcher_->inotify_fd());
      inotify_channel_->SetReadCallback([this] { reload_watcher_->HandleInotify(); });
      inotify_channel_->EnableReading();
    }
    if (reload_watcher_->timer_fd() >= 0) {
      watcher_timer_channel_ = std::make_unique<net::Channel>(loop_, reload_watcher_->timer_fd());
      watcher_timer_channel_->SetReadCallback([this] { reload_watcher_->HandleTimer(); });
      watcher_timer_channel_->EnableReading();
    }
  }
  acceptor_->SetNewConnectionCallback([this](int fd) { Accept(fd); });
}

Gateway::~Gateway() {
  // R-040/R-062: invalidate the lifetime token first; stop accepting; refuse
  // every new outcome reservation; tear down workers; drain outcomes; stop
  // the coordinator.
  if (!loop_.IsOwnerThread()) std::terminate();
  lifecycle_ = Lifecycle::kStopped;
  lifetime_token_.reset();
  acceptor_.reset();
  if (reload_watcher_) reload_watcher_->Stop();
  sighup_channel_.reset();
  inotify_channel_.reset();
  watcher_timer_channel_.reset();
  if (reload_controller_) reload_controller_->Stop();
  reload_channel_.reset();
  // Stop the generation mailbox channel before draining events.
  generation_mailbox_channel_.reset();
  // Drain any remaining generation events (e.g., pending balance returns).
  HandleGenerationEvents();
  // Stop health checkers on the current generation's coordinator.
  if (coordinator_) coordinator_->StopCheckers();
  // Stop the current generation's outcome channel.
  const auto generation = current_generation_.load(std::memory_order_acquire);
  if (generation) generation->coordinator()->BeginOutcomeStopping();
  // Original-index teardown (R-057).
  for (std::size_t index = 0; index < worker_datas_.size(); ++index) {
    if (!worker_datas_[index]) continue;
    auto &data = worker_datas_[index];
    auto &worker = workers_->At(index);
    if (!worker.PostShutdown([&data](net::EventLoop &) mutable { data.reset(); })) {
      (void)std::fprintf(stderr, "fatal: worker %zu destroy task not accepted\n", index);
      std::terminate();
    }
  }
  if (workers_) workers_->StopAll();
  // Drain any final generation events after workers stopped.
  HandleGenerationEvents();
  if (generation) {
    generation->coordinator()->DrainOutcomesAndWait();
    generation->coordinator()->Stop();
  }
  // Join reaper threads first — they may still be draining outcomes.
  for (auto &reaper : retirement_reapers_) {
    if (reaper.joinable()) reaper.join();
  }
  retirement_reapers_.clear();
  // Stop any retiring generation coordinators that the reaper didn't reach.
  for (auto &[version, entry] : retiring_generations_) {
    if (!entry.generation) continue;
    if (!entry.generation->coordinator_stopped()) {
      entry.generation->coordinator()->BeginOutcomeStopping();
      entry.generation->coordinator()->DrainOutcomesAndWait();
      entry.generation->coordinator()->Stop();
    }
  }
  retiring_generations_.clear();
  generation_mailbox_->Close();
}

void Gateway::Start() {
  if (lifecycle_ != Lifecycle::kNotStarted) {
    throw std::logic_error("gateway cannot be started twice");
  }
  lifecycle_ = Lifecycle::kStarting;
  const auto generation = current_generation_.load(std::memory_order_acquire);
  if (!generation) throw std::logic_error("gateway has no runtime generation");
  generation->coordinator()->Start();
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
  lifecycle_ = Lifecycle::kRunning;
}

bool Gateway::RequestReload(config::Config candidate) {
  if (!loop_.IsOwnerThread() || lifecycle_ != Lifecycle::kRunning) return false;
  const auto current = current_generation_.load(std::memory_order_acquire);
  if (!current) return false;

  // 1. Validate workers count matches.
  if (candidate.workers != current->snapshot()->config.workers) return false;

  // 2. Create candidate RuntimeGeneration (without selection states yet).
  runtime::RuntimeGenerationRef replacement;
  try {
    auto snapshot = std::make_shared<runtime::ConfigSnapshot>(
        runtime::ConfigSnapshot{current->version() + 1, std::move(candidate)});
    auto coord = std::make_shared<health::Coordinator>(
        std::make_shared<const config::Config>(snapshot->config),
        health::Coordinator::Clock::now());
    const auto now = resilience::GlobalAdmission::Clock::now();
    std::vector<std::shared_ptr<resilience::GlobalAdmission>> new_admissions;
    for (const config::Route &route : snapshot->config.routes) {
      new_admissions.push_back(std::make_shared<resilience::GlobalAdmission>(route, now));
    }
    coord->SetAdmissions(new_admissions);
    // Empty selection states — will be filled by worker prepare tasks.
    std::vector<std::shared_ptr<runtime::SelectionState>> empty_sel;
    replacement = std::make_shared<runtime::RuntimeGeneration>(
        snapshot->version, snapshot, std::move(coord), std::move(new_admissions),
        std::move(empty_sel));
  } catch (...) {
    return false;
  }

  // 3. Start coordinator in prepared mode (no health checkers yet).
  try {
    replacement->coordinator()->StartPrepared();
  } catch (...) {
    return false;
  }

  // 4. Migrate protection state from old coordinator.
  try {
    const auto old_snapshot = current->coordinator()->CurrentSnapshot();
    if (old_snapshot) {
      replacement->coordinator()->ImportProtectionSnapshot(*old_snapshot);
    }
  } catch (...) {
    replacement->coordinator()->Stop();
    return false;
  }

  // 5. Prepare per-worker selection states on worker owner threads.
  auto sel_states = std::make_shared<std::vector<std::shared_ptr<runtime::SelectionState>>>(
      worker_datas_.size());
  auto pending = std::make_shared<std::atomic<std::size_t>>(worker_datas_.size());
  auto failed = std::make_shared<std::atomic<bool>>(false);
  auto config_copy = replacement->snapshot()->config;
  auto version = replacement->version();

  for (std::size_t i = 0; i < worker_datas_.size(); ++i) {
    if (!worker_datas_[i]) {
      // Worker not initialized — mark as done.
      if (pending->fetch_sub(1, std::memory_order_acq_rel) == 1) {
        // Last worker — all done.
      }
      continue;
    }
    auto &worker = workers_->At(i);
    auto wd = worker_datas_[i];
    auto sel = sel_states;
    auto p = pending;
    auto f = failed;
    auto coord = replacement->coordinator();
    if (!worker.PostWithLoop(
            [wd, i, config_copy, version, sel, p, f, coord](net::EventLoop &) {
              auto state = wd->PrepareSelectionState(config_copy, version);
              if (!state) {
                f->store(true, std::memory_order_release);
              } else {
                (*sel)[i] = std::move(state);
              }
              if (p->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                // Last worker — wake the control loop.
                // The control loop will check failed and proceed or rollback.
              }
            })) {
      // Worker stopped — count as done, but mark as failed.
      failed->store(true, std::memory_order_release);
      if (pending->fetch_sub(1, std::memory_order_acq_rel) == 1) {
        // Last worker.
      }
    }
  }

  // Wait for all prepares to complete (synchronous barrier).
  // In a real async design this would be event-driven, but for simplicity
  // we spin-wait with a short sleep.
  while (pending->load(std::memory_order_acquire) > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // 6. Check if any prepare failed.
  if (failed->load(std::memory_order_acquire)) {
    // Rollback: stop the candidate coordinator, don't publish.
    replacement->coordinator()->Stop();
    return false;
  }

  // 7. Set the prepared selection states on the replacement generation.
  replacement->SetSelectionStates(std::move(*sel_states));

  // 8. Activate the coordinator (starts health checkers).
  try {
    replacement->coordinator()->Activate();
  } catch (...) {
    replacement->coordinator()->Stop();
    return false;
  }

  // 9. Publish atomically.
  auto old_gen = current;
  routes_ = routing::RouteTable(replacement->snapshot()->config);
  current_generation_.store(replacement, std::memory_order_release);
  worker_shared_->config_snapshot.store(replacement->snapshot(), std::memory_order_release);
  worker_shared_->current_generation.store(replacement, std::memory_order_release);
  worker_shared_->coordinator = replacement->coordinator();
  worker_shared_->admissions = replacement->admissions();

  // 10. Retire old generation via A5 pipeline.
  RetireGeneration(old_gen);
  return true;
}

bool Gateway::RequestReload() {
  if (lifecycle_ != Lifecycle::kRunning || !reload_controller_) return false;
  return reload_controller_->Request();
}

void Gateway::HandleReloadResults() {
  if (!loop_.IsOwnerThread()) std::terminate();
  auto results = reload_controller_->Drain();
  if (results.empty()) return;
  // Record the highest sequence so tests can wait for consumption.
  for (const auto &r : results) {
    if (r.sequence > last_reload_result_sequence_) {
      last_reload_result_sequence_ = r.sequence;
    }
  }
  // A burst may contain an obsolete completed parse followed by the coalesced
  // latest file image.  Publishing only the newest result prevents an
  // unnecessary transient generation; a failed newest parse leaves the live
  // generation exactly as it was.
  auto latest = std::move(results.back());
  if (!latest.candidate.has_value()) return;
  (void)RequestReload(std::move(*latest.candidate));
}

std::uint16_t Gateway::port() const { return acceptor_->port(); }

std::uint64_t Gateway::CurrentGenerationVersion() const noexcept {
  const auto gen = current_generation_.load(std::memory_order_acquire);
  return gen ? gen->version() : 0;
}

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
  const auto snapshot = coordinator_->CurrentSnapshot();
  if (!snapshot) return true;
  if (route_index >= snapshot->endpoints.size() ||
      endpoint_index >= snapshot->endpoints[route_index].size()) {
    return true;
  }
  return snapshot->endpoints[route_index][endpoint_index].healthy;
}

resilience::CircuitBreaker::State
Gateway::BreakerState(std::size_t route_index, std::size_t endpoint_index) const noexcept {
  const auto snapshot = coordinator_->CurrentSnapshot();
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
  const auto snapshot = coordinator_->CurrentSnapshot();
  if (!snapshot || route_index >= snapshot->endpoints.size() ||
      endpoint_index >= snapshot->endpoints[route_index].size()) {
    return;
  }
  const std::uint64_t generation = snapshot->endpoints[route_index][endpoint_index].generation;
  coordinator_->SubmitResultAndWait(
      {route_index, endpoint_index, {false, generation, 0}, success});
}

void Gateway::Accept(int fd) {
  if (!workers_ || worker_datas_.empty()) {
    (void)::close(fd);
    return;
  }
  const runtime::WorkerSet::WorkerHandle handle = workers_->Next();
  (void)handle.worker.PostFd(fd, [this, worker_index = handle.index](net::FdOwner fd) {
    auto &slot = worker_datas_[worker_index];
    if (slot) {
      slot->Accept(std::move(fd));
    }
  });
}

// ---------------------------------------------------------------------------
// Retirement pipeline
//
// The mailbox is a pure wake signal — no event payload.  After every wake
// the control loop scans every retiring generation's actual
// retirement_state() and drives the next action.  This guarantees the
// loop always sees the true state, not a stale or lost event.
// ---------------------------------------------------------------------------

void Gateway::RetireGeneration(runtime::RuntimeGenerationRef generation) {
  if (!generation || !loop_.IsOwnerThread()) return;
  const auto [entry, inserted] = retiring_generations_.try_emplace(
      generation->version(), RetiringGeneration{generation, 0});
  if (!inserted) return;

  const std::weak_ptr<runtime::GenerationMailbox> mailbox = generation_mailbox_;

  generation->SetStateChangeCallback([mailbox](runtime::RuntimeGeneration::RetirementState) {
    if (auto alive = mailbox.lock()) {
      (void)alive->Wake();
    }
  });

  if (!generation->BeginRetirement()) {
    retiring_generations_.erase(entry);
  }
}

void Gateway::HandleGenerationEvents() {
  if (!loop_.IsOwnerThread()) std::terminate();
  // Drain the wake counter (always returns bool).
  (void)generation_mailbox_->Drain();

  // Scan every retiring generation and drive the state machine.
  for (auto it = retiring_generations_.begin(); it != retiring_generations_.end();) {
    auto &entry = it->second;
    const auto state = entry.generation->retirement_state();

    if (state == runtime::RuntimeGeneration::RetirementState::kCheckersStopped &&
        entry.generation->active_request_leases() == 0) {
      // Checkers stopped and leases drained — notify to advance to
      // kWaitingForLeases.  The NotifyCheckersStopped call is idempotent
      // (the generation is already in kCheckersStopped).
      (void)entry.generation->NotifyCheckersStopped();
      // Fall through to kWaitingForLeases handling below.
    }

    if (entry.generation->retirement_state() ==
        runtime::RuntimeGeneration::RetirementState::kWaitingForLeases) {
      // Ready for balance return.  Request each worker to return its
      // lease tokens to the old generation's admissions.
      if (entry.returned_worker_balances == 0 && !worker_datas_.empty()) {
        for (std::size_t i = 0; i < worker_datas_.size(); ++i) {
          if (!worker_datas_[i]) {
            // Worker already destroyed — its Shutdown() returned balances.
            ++entry.returned_worker_balances;
            continue;
          }
          auto &worker = workers_->At(i);
          auto wd = worker_datas_[i];
          auto admissions = entry.generation->admissions();
          auto mb = generation_mailbox_;
          // Use PostShutdown (reserved slot, always accepted while running).
          // If it fails the worker is stopping/stopped: WorkerData::Shutdown
          // has been or will be called before the thread joins, returning
          // lease balances.  Count as completed to avoid hanging.
          if (!worker.PostShutdown(
                  [wd, admissions, mb](net::EventLoop &) {
                    wd->ReturnGenerationLeaseBalance(admissions);
                    (void)mb->Wake();
                  })) {
            ++entry.returned_worker_balances;
          }
        }
      }
      // Wait for all workers to report back.
      if (entry.returned_worker_balances < worker_datas_.size()) {
        ++it;
        continue;
      }
      // All balances returned.  Move to outcome draining.
      (void)entry.generation->BeginOutcomeStopping();
      if (worker_datas_.empty()) {
        // No workers — drain and retire immediately.
        entry.generation->coordinator()->DrainOutcomesAndWait();
        entry.generation->coordinator()->Stop();
        entry.generation->MarkRetired();
        it = retiring_generations_.erase(it);
        continue;
      }
      // Start the reaper thread for the coordinator shutdown sequence.
      // The reaper sets coordinator_stopped_ (atomic) on the generation
      // itself — no dangling pointer into the unordered_map entry.
      entry.reaper_started = true;
      auto gen = entry.generation;
      auto mb = generation_mailbox_;
      retirement_reapers_.emplace_back([gen, mb] {
        gen->coordinator()->DrainOutcomesAndWait();
        gen->coordinator()->Stop();
        gen->MarkCoordinatorStopped();
        (void)mb->Wake();
      });
      ++it;
      continue;
    }

    if (entry.generation->retirement_state() ==
        runtime::RuntimeGeneration::RetirementState::kOutcomeDraining) {
      // Reaper is running.  When it finishes, it sets
      // coordinator_stopped_ (atomic) and wakes us.
      if (entry.generation->coordinator_stopped()) {
        entry.generation->MarkRetired();
        it = retiring_generations_.erase(it);
        continue;
      }
      ++it;
      continue;
    }

    if (entry.generation->retirement_state() ==
        runtime::RuntimeGeneration::RetirementState::kDone) {
      it = retiring_generations_.erase(it);
      continue;
    }

    ++it;
  }
}

std::string Gateway::RenderMetrics() const {
  const auto snapshot = coordinator_->CurrentSnapshot();
  observability::Metrics::Data aggregate;
  for (const auto &metrics : worker_metrics_) {
    observability::Metrics::MergeInto(aggregate, metrics->Snapshot());
  }
  std::vector<observability::Metrics::ProtectionSample> protection;
  if (snapshot) {
    const std::vector<config::Route> &routes = config_snapshot_->config.routes;
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
