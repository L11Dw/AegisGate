#include "aegisgate/health/HealthChecker.h"

#include <stdexcept>
#include <string_view>
#include <utility>

#include "aegisgate/net/EventLoop.h"

namespace aegisgate::health {
namespace {

bool EqualsIgnoreCase(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) return false;
  for (std::size_t index = 0; index < left.size(); ++index) {
    const auto lower = [](char character) {
      return character >= 'A' && character <= 'Z'
                 ? static_cast<char>(character - 'A' + 'a')
                 : character;
    };
    if (lower(left[index]) != lower(right[index])) return false;
  }
  return true;
}

// The health contract requires a Content-Length declaration: a bodyless 204
// without one must not be mistaken for a healthy probe result.
bool HasContentLength(const http::HttpResponse &response) {
  for (const auto &[name, value] : response.headers) {
    if (EqualsIgnoreCase(name, "content-length")) return true;
  }
  return false;
}

} // namespace

HealthChecker::HealthChecker(net::EventLoop &loop, net::TimerQueue &timers,
                             config::Endpoint endpoint, HealthCheckConfig config,
                             Callback callback)
    : loop_(loop), timers_(timers), endpoint_(std::move(endpoint)), config_(config),
      callback_(std::move(callback)) {
  if (config_.interval <= std::chrono::milliseconds::zero() ||
      config_.timeout <= std::chrono::milliseconds::zero()) {
    throw std::invalid_argument("invalid health check configuration");
  }
  state_ = std::make_shared<State>();
  state_->owner = this;
}

HealthChecker::~HealthChecker() { Stop(); }

void HealthChecker::Start() {
  if (running_) {
    throw std::logic_error("health checker already started");
  }
  running_ = true;
  RunCheck(++generation_);
}

void HealthChecker::Stop() noexcept {
  running_ = false;
  ++generation_;
  if (timeout_timer_ != 0) (void)timers_.Cancel(timeout_timer_);
  timeout_timer_ = 0;
  if (connection_) connection_->Close();
  connection_.reset();
}

void HealthChecker::RunCheck(std::uint64_t generation) {
  if (!running_ || generation != generation_) return;
  const std::weak_ptr<State> weak = state_;
  timeout_timer_ = timers_.ScheduleAfter(config_.timeout, [weak, generation] {
    if (const auto state = weak.lock(); state && state->owner) {
      state->owner->HandleTimeout(generation);
    }
  });
  connection_ = std::make_unique<net::UpstreamConnection>(
      loop_, endpoint_,
      [weak, generation](net::UpstreamResult result, http::HttpResponse response) {
        if (const auto state = weak.lock(); state && state->owner) {
          state->owner->HandleResult(generation, result, std::move(response));
        }
      });
  http::HttpRequest request{"GET", "/healthz", "HTTP/1.1", "", {{"Host", endpoint_.host}}};
  try {
    connection_->Start(request);
  } catch (...) {
    connection_.reset();
    FinishCheck(generation, false);
  }
}

void HealthChecker::HandleResult(std::uint64_t generation, net::UpstreamResult result,
                                 http::HttpResponse response) {
  if (!running_ || generation != generation_) return;
  if (timeout_timer_ != 0) (void)timers_.Cancel(timeout_timer_);
  timeout_timer_ = 0;
  const bool healthy = result == net::UpstreamResult::kSuccess &&
                       response.status >= 200 && response.status <= 299 &&
                       HasContentLength(response);
  connection_.reset();
  FinishCheck(generation, healthy);
}

void HealthChecker::HandleTimeout(std::uint64_t generation) {
  if (!running_ || generation != generation_) return;
  timeout_timer_ = 0;
  if (connection_) connection_->Close();
  connection_.reset();
  FinishCheck(generation, false);
}

void HealthChecker::ScheduleNext(std::uint64_t generation) {
  // Only the current generation may arm its successor.  Each check is its own
  // generation: advance before arming so the callback matches the member
  // value and stale results stay stale.
  if (generation != generation_) return;
  ++generation_;
  const std::weak_ptr<State> weak = state_;
  (void)timers_.ScheduleAfter(config_.interval, [weak, next = generation_] {
    if (const auto state = weak.lock(); state && state->owner) {
      state->owner->RunCheck(next);
    }
  });
}

void HealthChecker::FinishCheck(std::uint64_t generation, bool healthy) {
  // Copy the callback first: it may destroy this checker, after which no
  // member may be touched.
  Callback callback = callback_;
  if (running_ && generation == generation_) {
    ScheduleNext(generation);
  }
  if (callback) callback(healthy);
}

} // namespace aegisgate::health
