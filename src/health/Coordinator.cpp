#include "aegisgate/health/Coordinator.h"

#include <chrono>
#include <future>
#include <stdexcept>
#include <utility>

#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/TimerQueue.h"

namespace aegisgate::health {
namespace {

constexpr std::chrono::milliseconds kRefillTick = std::chrono::milliseconds(250);

} // namespace

Coordinator::Coordinator(std::shared_ptr<const config::Config> config, Clock::time_point now)
    : config_(std::move(config)), state_(std::make_unique<CoordinatorState>(config_, now)),
      runtime_(std::make_unique<runtime::WorkerRuntime>()) {
  if (!config_) throw std::invalid_argument("coordinator requires a config snapshot");
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
      initialized.set_value();
    } catch (...) {
      initialized.set_exception(std::current_exception());
    }
  })) {
    throw std::logic_error("coordinator init task was not accepted");
  }
  future.get();
}

void Coordinator::Stop() noexcept {
  // The destroy task must run on the coordinator thread (TimerQueue and
  // checkers hold loop registrations).  It captures the Stop-frame owner by
  // reference and resets it when it runs, destroying LoopData on the
  // coordinator thread.  `data` stays in scope until Stop returns (the join
  // guarantees the accepted destroy task ran during the drain), so the
  // reference can never dangle.  Retry until accepted: the coordinator loop
  // keeps draining, so acceptance is guaranteed.
  std::unique_ptr<LoopData> data;
  if (loop_data_) {
    data = std::move(loop_data_);
    while (!runtime_->PostWithLoop([&data](net::EventLoop &) mutable { data.reset(); })) {
    }
  }
  runtime_->Stop();
  // data is empty here: the destroy task ran on the coordinator thread.
}

std::shared_ptr<const HealthCircuitSnapshot> Coordinator::CurrentSnapshot() const noexcept {
  return std::atomic_load(&snapshot_);
}

bool Coordinator::PostResult(const AttemptResult &result) noexcept {
  return PostTask([this, result] { RecordResultTask(result); });
}

bool Coordinator::ProbeAvailable(std::size_t route, std::size_t endpoint) const noexcept {
  return state_->ProbeAvailable(route, endpoint);
}

std::optional<AttemptPermit> Coordinator::ClaimProbe(
    std::size_t route, std::size_t endpoint, const HealthCircuitSnapshot &snapshot) noexcept {
  return state_->ClaimProbe(route, endpoint, snapshot);
}

void Coordinator::SubmitResultAndWait(const AttemptResult &result) {
  std::promise<void> processed;
  auto future = processed.get_future();
  while (!PostTask([this, result, &processed] {
    try {
      RecordResultTask(result);
      processed.set_value();
    } catch (...) {
      processed.set_exception(std::current_exception());
    }
  })) {
  }
  future.get();
}

void Coordinator::RecordHealthAndWait(std::size_t route, std::size_t endpoint, bool healthy) {
  std::promise<void> processed;
  auto future = processed.get_future();
  while (!PostTask([this, route, endpoint, healthy, &processed] {
    RecordHealthTask(route, endpoint, healthy);
    processed.set_value();
  })) {
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
  if (!admissions_.empty()) ScheduleRefillTick(*data->timers);
  loop_data_ = std::move(data);
}

void Coordinator::RecordResultTask(const AttemptResult &result) {
  state_->RecordResult(result, Clock::now());
  if (state_->IsOpen(result.route_index, result.endpoint_index)) {
    ScheduleArm(result.route_index, result.endpoint_index);
  }
  Publish();
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
        state_->ArmHalfOpen(route, endpoint, Clock::now());
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
