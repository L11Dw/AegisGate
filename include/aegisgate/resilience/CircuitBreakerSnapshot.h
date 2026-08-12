#pragma once

#include <chrono>
#include <cstdint>
#include <vector>

namespace aegisgate::resilience {

// Forward-declare to avoid circular include with CircuitBreaker.h.
enum class CircuitBreakerState : std::uint8_t { kClosed, kOpen, kHalfOpen };

// Pure-value bucket snapshot with relative age (not absolute time_point).
struct BucketSnapshot {
  std::chrono::steady_clock::duration age_from_export{};
  std::uint32_t success = 0;
  std::uint32_t failure = 0;
};

// Pure-value circuit breaker snapshot for migration.
struct CircuitBreakerSnapshot {
  CircuitBreakerState state = CircuitBreakerState::kClosed;
  std::vector<BucketSnapshot> buckets;
  std::chrono::steady_clock::duration open_remaining{};
  std::uint32_t half_open_quota = 0;
  // Not exported: generation, probe_id, issued/completed
};

} // namespace aegisgate::resilience
