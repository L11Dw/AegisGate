#pragma once

namespace aegisgate::health {

// Per-endpoint availability as decided only by active health checks.  This is
// deliberately separate from circuit-breaker state (route x endpoint runtime
// protection): a failed check marks the endpoint unhealthy, and only a later
// successful check restores it.  Selection refuses an endpoint when either
// input rejects it.
class EndpointHealth {
public:
  enum class State : unsigned char { kImplicitHealthy, kUnknown, kHealthy, kUnhealthy };
  [[nodiscard]] bool Healthy() const noexcept { return healthy_; }
  [[nodiscard]] State state() const noexcept { return state_; }
  void RecordCheckResult(bool ok) noexcept {
    state_ = ok ? State::kHealthy : State::kUnhealthy;
    healthy_ = ok;
  }
  void SetState(State state) noexcept {
    state_ = state;
    healthy_ = state == State::kHealthy || state == State::kImplicitHealthy;
  }

private:
  bool healthy_ = true;
  State state_ = State::kImplicitHealthy;
};

} // namespace aegisgate::health
