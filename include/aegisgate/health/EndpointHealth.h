#pragma once

#include <cstdint>

namespace aegisgate::health {

enum class HealthState : std::uint8_t {
  kImplicitHealthy,
  kUnknown,
  kHealthy,
  kUnhealthy,
};

// Per-endpoint availability as decided only by active health checks.  This is
// deliberately separate from circuit-breaker state (route x endpoint runtime
// protection): a failed check marks the endpoint unhealthy, and only a later
// successful check restores it.  Selection refuses an endpoint when either
// input rejects it.
class EndpointHealth {
public:
  explicit EndpointHealth(HealthState initial = HealthState::kImplicitHealthy) : state_(initial) {}

  [[nodiscard]] HealthState State() const noexcept { return state_; }
  [[nodiscard]] bool Healthy() const noexcept {
    return state_ == HealthState::kImplicitHealthy || state_ == HealthState::kHealthy;
  }
  void RecordCheckResult(bool ok) noexcept {
    state_ = ok ? HealthState::kHealthy : HealthState::kUnhealthy;
  }
  void ImportState(HealthState state) noexcept { state_ = state; }

private:
  HealthState state_;
};

} // namespace aegisgate::health
