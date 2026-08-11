#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

#include "aegisgate/config/Config.h"
#include "aegisgate/net/UpstreamConnection.h"
#include "aegisgate/net/TimerQueue.h"

namespace aegisgate::net {
class EventLoop;
} // namespace aegisgate::net

namespace aegisgate::health {

struct HealthCheckConfig {
  std::chrono::milliseconds interval{};
  std::chrono::milliseconds timeout{};
};

// Periodically probes one endpoint with a fixed bodyless GET /healthz over an
// independent, one-shot upstream connection (never the client connection
// pool) and reports healthy/unhealthy.  A check is healthy only for a
// complete, valid 2xx Content-Length response; failure, EOF, timeout,
// protocol error and non-2xx all mark it unhealthy.  Stale timers and late
// results are no-ops after Stop()/destruction or a newer generation, and
// health checks never produce a downstream response.
class HealthChecker {
public:
  using Callback = std::function<void(bool healthy)>;

  HealthChecker(net::EventLoop &loop, net::TimerQueue &timers, config::Endpoint endpoint,
                HealthCheckConfig config, Callback callback);
  ~HealthChecker();

  HealthChecker(const HealthChecker &) = delete;
  HealthChecker &operator=(const HealthChecker &) = delete;

  void Start();
  void Stop() noexcept;

private:
  struct State {
    HealthChecker *owner = nullptr;
  };

  void RunCheck(std::uint64_t generation);
  void HandleResult(std::uint64_t generation, net::UpstreamResult result,
                    http::HttpResponse response);
  void HandleTimeout(std::uint64_t generation);
  void ScheduleNext(std::uint64_t generation);
  void FinishCheck(std::uint64_t generation, bool healthy);

  net::EventLoop &loop_;
  net::TimerQueue &timers_;
  config::Endpoint endpoint_;
  HealthCheckConfig config_;
  Callback callback_;
  std::shared_ptr<State> state_;
  std::unique_ptr<net::UpstreamConnection> connection_;
  net::TimerQueue::TimerId timeout_timer_ = 0;
  std::uint64_t generation_ = 0;
  bool running_ = false;
};

} // namespace aegisgate::health
