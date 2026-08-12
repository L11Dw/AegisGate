#include "aegisgate/health/Coordinator.h"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <future>
#include <stdexcept>
#include <thread>
#include <utility>

#include <unistd.h>

#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/TimerQueue.h"

namespace aegisgate::health {
namespace {

constexpr std::chrono::milliseconds kRefillTick = std::chrono::milliseconds(250);

} // namespace

Coordinator::Coordinator(std::shared_ptr<const config::Config> config, Clock::time_point now)
    : config_(std::move(config)), state_(std::make_unique<CoordinatorState>(config_, now)),
      runtime_(std::make_unique<runtime::WorkerRuntime>()),
      lifecycle_(Lifecycle::kActive) {
  if (!config_) throw std::invalid_argument("coordinator requires a config snapshot");
  // One OutcomeChannel per breaker route: every accounted attempt reserves a
  // slot before it connects, so its terminal result always has room (capacity
  // = max_inflight x (1 + retry_budget)).  Config-level limits are enforced by
  // CapacityForRoute (overflow / safety ceiling).
  outcome_channels_.resize(config_->routes.size());
  for (std::size_t route = 0; route < config_->routes.size(); ++route) {
    const config::Route &route_config = config_->routes[route];
    if (!route_config.circuit_breaker.has_value()) continue;
    outcome_channels_[route] = std::make_unique<OutcomeChannel>(
        OutcomeChannel::CapacityForRoute(route_config.max_inflight, route_config.retry_budget));
  }
  // Publish the initial decision set before any worker reads a snapshot.
  Publish();
}

Coordinator::~Coordinator() { Stop(); }

void Coordinator::SetAdmissions(
    std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions) {
  admissions_ = std::move(admissions);
}

void Coordinator::Start() {
  runtime_->Start();
  std::promise<void> initialized;
  auto future = initialized.get_future();
  if (!runtime_->PostWithLoop([this, &initialized](net::EventLoop &loop) {
    try {
      OnLoopInit(loop);
      lifecycle_.store(Lifecycle::kActive, std::memory_order_release);
      initialized.set_value();
    } catch (...) {
      initialized.set_exception(std::current_exception());
    }
  })) {
    throw std::logic_error("coordinator init task was not accepted");
  }
  future.get();
}

void Coordinator::StartPrepared() {
  lifecycle_.store(Lifecycle::kPrepared, std::memory_order_release);
  runtime_->Start();
  std::promise<void> initialized;
  auto future = initialized.get_future();
  if (!runtime_->PostWithLoop([this, &initialized](net::EventLoop &loop) {
    try {
      OnPreparedLoopInit(loop);
      initialized.set_value();
    } catch (...) {
      initialized.set_exception(std::current_exception());
    }
  })) {
    throw std::logic_error("coordinator prepared init task was not accepted");
  }
  future.get();
}

void Coordinator::OnPreparedLoopInit(net::EventLoop &loop) {
  auto data = std::make_unique<LoopData>();
  data->timers = std::make_unique<net::TimerQueue>(loop);
  data->arm_timers.resize(config_->routes.size());
  // Register outcome channel wake descriptors only; no health checkers,
  // no admission refill started.
  for (std::size_t route = 0; route < outcome_channels_.size(); ++route) {
    OutcomeChannel *channel = outcome_channels_[route].get();
    if (!channel) continue;
    data->outcome_wake_channels.push_back(
        std::make_unique<net::Channel>(loop, channel->WakeFd()));
    data->outcome_wake_channels.back()->SetReadCallback([this, channel] {
      std::uint64_t counter = 0;
      for (;;) {
        const ssize_t count = ::read(channel->WakeFd(), &counter, sizeof(counter));
        if (count == static_cast<ssize_t>(sizeof(counter))) break;
        if (count < 0 && errno == EINTR) continue;
        break;
      }
      DrainOneOutcomeChannel(*channel);
    });
    data->outcome_wake_channels.back()->EnableReading();
  }
  loop_data_ = std::move(data);
  Publish();
}

bool Coordinator::ImportProtectionSnapshotAndWait(
    const ProtectionSnapshot &snapshot, std::chrono::milliseconds timeout) {
  if (lifecycle_.load(std::memory_order_acquire) != Lifecycle::kPrepared) return false;
  struct Completion {
    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    bool cancelled = false;
    bool ok = false;
  };
  auto completion = std::make_shared<Completion>();
  // Value-capture snapshot to avoid UAF after timeout (P1 #1).
  ProtectionSnapshot snap_copy = snapshot;
  if (!PostTask([this, snap = std::move(snap_copy), completion] {
        std::lock_guard<std::mutex> guard(completion->mu);
        if (completion->cancelled) return;
        try {
          const auto now = Clock::now();
          // Import by identity matching.
          for (const auto &ep : snap.endpoints) {
            for (std::size_t r = 0; r < config_->routes.size(); ++r) {
              const auto &route = config_->routes[r];
              RouteIdentity rid{route.name, route.host, route.path_prefix};
              if (!SameRouteIdentity(rid, ep.route)) continue;
              // Check policy equivalence.
              if (!SameHealthPolicy(route.health_check,
                                    config_->routes[r].health_check)) continue;
              if (!SameBreakerPolicy(route.circuit_breaker,
                                     config_->routes[r].circuit_breaker)) continue;
              for (std::size_t e = 0; e < route.endpoints.size(); ++e) {
                const auto &endpoint = route.endpoints[e];
                EndpointIdentity eid{endpoint.host, endpoint.address, endpoint.port};
                if (!SameEndpointIdentity(eid, ep.endpoint)) continue;
                // Import health state (P1 #4: preserve 4-state enum).
                state_->ImportHealthState(r, e, ep.health.state);
                // Import breaker state (P1 #2: real breaker migration).
                if (ep.breaker.has_value()) {
                  state_->ImportBreakerSnapshot(r, e, *ep.breaker, now);
                }
              }
            }
          }
          Publish();
          completion->ok = true;
        } catch (...) {
          completion->ok = false;
        }
        completion->done = true;
        completion->cv.notify_all();
      })) {
    return false;
  }
  {
    std::unique_lock<std::mutex> lock(completion->mu);
    if (!completion->cv.wait_for(lock, timeout, [&] { return completion->done; })) {
      completion->cancelled = true;
      return false;
    }
  }
  return completion->ok;
}

bool Coordinator::Activate() {
  if (lifecycle_.load(std::memory_order_acquire) != Lifecycle::kPrepared) return false;
  try {
    std::promise<void> activated;
    auto future = activated.get_future();
    if (!runtime_->PostWithLoop([this, &activated](net::EventLoop &loop) {
      try {
        OnActivateLoopInit(loop);
        lifecycle_.store(Lifecycle::kActive, std::memory_order_release);
        activated.set_value();
      } catch (...) {
        activated.set_exception(std::current_exception());
      }
    })) {
      return false;
    }
    future.get();
    return true;
  } catch (...) {
    return false;
  }
}

void Coordinator::OnActivateLoopInit(net::EventLoop &loop) {
  // Start health checkers.
  for (std::size_t route = 0; route < config_->routes.size(); ++route) {
    const config::Route &route_config = config_->routes[route];
    loop_data_->arm_timers[route].resize(route_config.endpoints.size(), 0);
    if (!route_config.health_check.has_value()) continue;
    const auto &settings = *route_config.health_check;
    for (std::size_t endpoint = 0; endpoint < route_config.endpoints.size(); ++endpoint) {
      const config::Endpoint &endpoint_config = route_config.endpoints[endpoint];
      loop_data_->checkers.push_back(std::make_unique<health::HealthChecker>(
          loop, *loop_data_->timers, endpoint_config,
          HealthCheckConfig{std::chrono::milliseconds(settings.interval_ms),
                            std::chrono::milliseconds(settings.timeout_ms)},
          [this, route, endpoint](bool healthy) {
            state_->RecordHealth(route, endpoint, healthy);
            Publish();
          }));
      loop_data_->checkers.back()->Start();
    }
  }
  // Start admission refill.
  if (!admissions_.empty()) ScheduleRefillTick(*loop_data_->timers);
}

std::optional<ProtectionSnapshot> Coordinator::ExportProtectionSnapshotAndWait(
    std::chrono::milliseconds timeout) {
  if (lifecycle_.load(std::memory_order_acquire) != Lifecycle::kActive) return std::nullopt;
  struct Completion {
    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    bool cancelled = false;
    std::optional<ProtectionSnapshot> result;
  };
  auto completion = std::make_shared<Completion>();
  if (!PostTask([this, completion] {
        std::lock_guard<std::mutex> guard(completion->mu);
        if (completion->cancelled) return;
        try {
          completion->result = state_->ExportProtectionSnapshot();
        } catch (...) {}
        completion->done = true;
        completion->cv.notify_all();
      })) {
    return std::nullopt;
  }
  {
    std::unique_lock<std::mutex> lock(completion->mu);
    if (!completion->cv.wait_for(lock, timeout, [&] { return completion->done; })) {
      completion->cancelled = true;
      return std::nullopt;
    }
  }
  return std::move(completion->result);
}

void Coordinator::Stop() noexcept {
  lifecycle_.store(Lifecycle::kStopping, std::memory_order_release);
  std::unique_ptr<LoopData> data;
  if (loop_data_) {
    data = std::move(loop_data_);
    if (!runtime_->PostShutdown([&data](net::EventLoop &) mutable { data.reset(); })) {
      (void)std::fprintf(stderr, "fatal: coordinator destroy task not accepted\n");
      std::terminate();
    }
  }
  runtime_->Stop();
  lifecycle_.store(Lifecycle::kStopped, std::memory_order_release);
}

std::shared_ptr<const HealthCircuitSnapshot> Coordinator::CurrentSnapshot() const noexcept {
  return std::atomic_load(&snapshot_);
}

bool Coordinator::PostResult(const AttemptResult &result) noexcept {
  if (lifecycle_.load(std::memory_order_acquire) != Lifecycle::kActive) return false;
  return PostTask([this, result] { RecordResultTask(result); });
}

std::optional<OutcomeChannel::Reservation>
Coordinator::ReserveOutcome(std::size_t route_index) noexcept {
  if (lifecycle_.load(std::memory_order_acquire) != Lifecycle::kActive) return std::nullopt;
  if (route_index >= outcome_channels_.size() || !outcome_channels_[route_index]) {
    return std::nullopt;
  }
  return outcome_channels_[route_index]->TryReserve();
}

void Coordinator::BeginOutcomeStopping() noexcept {
  for (const auto &channel : outcome_channels_) {
    if (channel) channel->BeginStopping();
  }
}

void Coordinator::DrainOutcomesAndWait() {
  std::promise<void> drained;
  auto future = drained.get_future();
  // The coordinator loop is running during gateway shutdown (this runs before
  // Stop()), so the drain task is accepted on the first try; a rejection means
  // the loop is stuck, which must fail the shutdown loudly rather than skip
  // draining published outcomes silently.
  if (!PostTask([this, &drained] {
        DrainAllOutcomeChannels();
        drained.set_value();
      })) {
    throw std::logic_error("coordinator outcome drain task was not accepted");
  }
  future.wait();
}

std::uint64_t Coordinator::OutcomeRejectedTotal() const noexcept {
  std::uint64_t total = 0;
  for (const auto &channel : outcome_channels_) {
    if (channel) total += channel->rejected();
  }
  return total;
}

bool Coordinator::ProbeAvailable(std::size_t route, std::size_t endpoint) const noexcept {
  if (lifecycle_.load(std::memory_order_acquire) != Lifecycle::kActive) return false;
  return state_->ProbeAvailable(route, endpoint);
}

std::optional<AttemptPermit> Coordinator::ClaimProbe(
    std::size_t route, std::size_t endpoint, const HealthCircuitSnapshot &snapshot) noexcept {
  if (lifecycle_.load(std::memory_order_acquire) != Lifecycle::kActive) return std::nullopt;
  return state_->ClaimProbe(route, endpoint, snapshot);
}

void Coordinator::SubmitResultAndWait(const AttemptResult &result) {
  std::promise<void> processed;
  auto future = processed.get_future();
  // Test seam: a rejection means the coordinator is stopping or its queue is
  // broken; fail loudly instead of busy-looping on the caller's core.
  if (!PostTask([this, result, &processed] {
        try {
          RecordResultTask(result);
          processed.set_value();
        } catch (...) {
          processed.set_exception(std::current_exception());
        }
      })) {
    throw std::logic_error("coordinator result submission was not accepted");
  }
  future.get();
}

void Coordinator::RecordHealthAndWait(std::size_t route, std::size_t endpoint, bool healthy) {
  std::promise<void> processed;
  auto future = processed.get_future();
  if (!PostTask([this, route, endpoint, healthy, &processed] {
        RecordHealthTask(route, endpoint, healthy);
        processed.set_value();
      })) {
    throw std::logic_error("coordinator health submission was not accepted");
  }
  future.get();
}

std::size_t Coordinator::RouteCount() const noexcept { return state_->RouteCount(); }

std::size_t Coordinator::EndpointCount(std::size_t route) const noexcept {
  return state_->EndpointCount(route);
}

void Coordinator::OnLoopInit(net::EventLoop &loop) {
  auto data = std::make_unique<LoopData>();
  data->timers = std::make_unique<net::TimerQueue>(loop);
  data->arm_timers.resize(config_->routes.size());
  // Health checkers run here, on the single-writer loop; their results are
  // committed in-thread and published.
  for (std::size_t route = 0; route < config_->routes.size(); ++route) {
    const config::Route &route_config = config_->routes[route];
    data->arm_timers[route].resize(route_config.endpoints.size(), 0);
    if (!route_config.health_check.has_value()) continue;
    const auto &settings = *route_config.health_check;
    for (std::size_t endpoint = 0; endpoint < route_config.endpoints.size(); ++endpoint) {
      const config::Endpoint &endpoint_config = route_config.endpoints[endpoint];
      data->checkers.push_back(std::make_unique<health::HealthChecker>(
          loop, *data->timers, endpoint_config,
          HealthCheckConfig{std::chrono::milliseconds(settings.interval_ms),
                            std::chrono::milliseconds(settings.timeout_ms)},
          [this, route, endpoint](bool healthy) {
            state_->RecordHealth(route, endpoint, healthy);
            Publish();
          }));
      data->checkers.back()->Start();
    }
  }
  // Register each outcome channel's wake descriptor: a Publish signals that a
  // result is waiting; drain it and republish the snapshot.  The eventfd is a
  // control wake only — the result payload travels in the channel's ring.
  for (std::size_t route = 0; route < outcome_channels_.size(); ++route) {
    OutcomeChannel *channel = outcome_channels_[route].get();
    if (!channel) continue;
    data->outcome_wake_channels.push_back(
        std::make_unique<net::Channel>(loop, channel->WakeFd()));
    data->outcome_wake_channels.back()->SetReadCallback([this, channel] {
      // Drain the eventfd counter once; the ring drain below consumes every
      // queued result regardless of how many publishes coalesced.
      std::uint64_t counter = 0;
      for (;;) {
        const ssize_t count = ::read(channel->WakeFd(), &counter, sizeof(counter));
        if (count == static_cast<ssize_t>(sizeof(counter))) break;
        if (count < 0 && errno == EINTR) continue;
        break;  // EAGAIN: no pending wake
      }
      DrainOneOutcomeChannel(*channel);
    });
    data->outcome_wake_channels.back()->EnableReading();
  }
  if (!admissions_.empty()) ScheduleRefillTick(*data->timers);
  loop_data_ = std::move(data);
}

void Coordinator::RecordResultTask(const AttemptResult &result) {
  RecordResultDirect(result);
  Publish();
}

void Coordinator::RecordResultDirect(const AttemptResult &result) {
  state_->RecordResult(result, Clock::now());
  if (state_->IsOpen(result.route_index, result.endpoint_index)) {
    ScheduleArm(result.route_index, result.endpoint_index);
  }
}

void Coordinator::DrainOneOutcomeChannel(OutcomeChannel &channel) {
  const std::size_t drained = channel.DrainOnCoordinatorLoop([this](const AttemptResult &result) {
    RecordResultDirect(result);
  });
  if (drained > 0) Publish();
}

void Coordinator::DrainAllOutcomeChannels() {
  bool any = false;
  for (const auto &channel : outcome_channels_) {
    if (!channel) continue;
    const std::size_t drained = channel->DrainOnCoordinatorLoop(
        [this](const AttemptResult &result) { RecordResultDirect(result); });
    any = any || drained > 0;
  }
  if (any) Publish();
}

void Coordinator::RecordHealthTask(std::size_t route, std::size_t endpoint, bool healthy) {
  state_->RecordHealth(route, endpoint, healthy);
  Publish();
}

void Coordinator::ScheduleArm(std::size_t route, std::size_t endpoint) {
  if (!loop_data_ || route >= loop_data_->arm_timers.size() ||
      endpoint >= loop_data_->arm_timers[route].size()) {
    return;
  }
  auto &timer_id = loop_data_->arm_timers[route][endpoint];
  if (timer_id != 0) (void)loop_data_->timers->Cancel(timer_id);
  timer_id = 0;
  const auto now = Clock::now();
  const auto until = state_->OpenUntil(route, endpoint);
  if (until == Clock::time_point::max() || until <= now) return;
  timer_id = loop_data_->timers->ScheduleAfter(
      until - now, [this, route, endpoint] {
        loop_data_->arm_timers[route][endpoint] = 0;
        const auto fired_at = Clock::now();
        if (state_->IsOpen(route, endpoint)) {
          if (fired_at < state_->OpenUntil(route, endpoint)) {
            // The timer can fire a few microseconds before the open window
            // elapses (ScheduleArm computes until - now with a now captured
            // slightly after Open()): re-arm instead of dropping the
            // transition, or the breaker would stay open forever.
            ScheduleArm(route, endpoint);
            return;
          }
          state_->ArmHalfOpen(route, endpoint, fired_at);
        }
        Publish();
      });
}

void Coordinator::ScheduleRefillTick(net::TimerQueue &timers) {
  // Fixed-delay repetition: the next tick is scheduled after the current one
  // finishes, mirroring the health-checker scheduling semantics.
  (void)timers.ScheduleAfter(kRefillTick, [this, &timers] {
    const auto now = Clock::now();
    for (const auto &admission : admissions_) admission->Refill(now);
    ScheduleRefillTick(timers);
  });
}

void Coordinator::Publish() noexcept {
  std::atomic_store(&snapshot_, state_->BuildSnapshot());
}

bool Coordinator::PostTask(std::function<void()> task) noexcept {
  return runtime_->Post(std::move(task));
}

} // namespace aegisgate::health
