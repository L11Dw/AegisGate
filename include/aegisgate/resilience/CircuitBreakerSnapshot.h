#pragma once

#include <chrono>
#include <cstdint>
#include <vector>

namespace aegisgate::resilience {

enum class CircuitBreakerState : std::uint8_t { kClosed, kOpen, kHalfOpen };

struct BucketSnapshot {
  std::chrono::steady_clock::duration age_from_export{};
  std::uint32_t success = 0;
  std::uint32_t failure = 0;
};

// Pure-value state exported from one coordinator generation.  source_generation
// is an invalidation token, not state to restore: import always creates a
// distinct generation so an in-flight old permit cannot mutate the new breaker.
struct CircuitBreakerSnapshot {
  CircuitBreakerState state = CircuitBreakerState::kClosed;
  std::vector<BucketSnapshot> buckets;
  std::chrono::steady_clock::duration open_remaining{};
  std::uint32_t half_open_quota = 0;
  std::uint64_t source_generation = 0;
};

} // namespace aegisgate::resilience
