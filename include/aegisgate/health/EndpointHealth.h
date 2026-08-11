#pragma once

namespace aegisgate::health {

// Per-endpoint availability as decided only by active health checks.  This is
// deliberately separate from circuit-breaker state (route x endpoint runtime
// protection): a failed check marks the endpoint unhealthy, and only a later
// successful check restores it.  Selection refuses an endpoint when either
// input rejects it.
class EndpointHealth {
public:
  [[nodiscard]] bool Healthy() const noexcept { return healthy_; }
  void RecordCheckResult(bool ok) noexcept { healthy_ = ok; }

private:
  bool healthy_ = true;
};

} // namespace aegisgate::health
