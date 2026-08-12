#pragma once

#include <cstdint>

namespace aegisgate::health {

// Endpoint health state for migration-aware selection.
enum class HealthState : std::uint8_t {
  kImplicitHealthy,  // 无 health_check 配置，始终可选
  kUnknown,          // 有 health_check 但尚无证据，选择时拒绝
  kHealthy,          // 已检查健康，可选
  kUnhealthy,        // 已检查不健康，选择时拒绝
};

// Per-endpoint availability as decided only by active health checks.
class EndpointHealth {
public:
  // Default: implicit healthy (no health check configured).
  EndpointHealth() = default;
  explicit EndpointHealth(HealthState initial) : state_(initial) {}

  [[nodiscard]] HealthState State() const noexcept { return state_; }

  // Whether the endpoint is eligible for selection.
  [[nodiscard]] bool Healthy() const noexcept {
    return state_ == HealthState::kImplicitHealthy || state_ == HealthState::kHealthy;
  }

  void RecordCheckResult(bool ok) noexcept {
    state_ = ok ? HealthState::kHealthy : HealthState::kUnhealthy;
  }

  // Import a migrated state (must be called on coordinator owner loop).
  void ImportState(HealthState state) noexcept { state_ = state; }

private:
  HealthState state_ = HealthState::kImplicitHealthy;
};

} // namespace aegisgate::health
