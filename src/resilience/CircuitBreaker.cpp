#include "aegisgate/resilience/CircuitBreaker.h"

#include <algorithm>

namespace aegisgate::resilience {

CircuitBreaker::CircuitBreaker(CircuitBreakerConfig config, Clock::time_point now)
    : config_(config), epoch_(now) {
  if (config_.window <= std::chrono::milliseconds::zero() ||
      config_.min_requests == 0 || config_.failure_threshold_permille == 0 ||
      config_.failure_threshold_permille >= 1000 ||
      config_.open_duration <= std::chrono::milliseconds::zero() ||
      config_.half_open_probes == 0) {
    throw std::invalid_argument("invalid circuit breaker configuration");
  }
}

void CircuitBreaker::RecordSuccess(Clock::time_point now) {
  AdvanceTo(now);
  if (state_ == State::kOpen) {
    // A result from a previous generation must not mutate the open state.
    return;
  }
  if (state_ == State::kHalfOpen) {
    ++half_open_completed_;
    if (half_open_completed_ == half_open_issued_) {
      Close();
    }
    return;
  }
  ++buckets_[active_index_].success;
  Evaluate(now);
}

void CircuitBreaker::RecordFailure(Clock::time_point now) {
  AdvanceTo(now);
  if (state_ == State::kOpen) {
    return;
  }
  if (state_ == State::kHalfOpen) {
    // A failed probe reopens immediately and restarts the open window.
    Open(now);
    return;
  }
  ++buckets_[active_index_].failure;
  Evaluate(now);
}

CircuitBreaker::Selection CircuitBreaker::Select(Clock::time_point now) {
  AdvanceTo(now);
  switch (state_) {
  case State::kClosed:
    return Selection::kAllowed;
  case State::kOpen:
    if (now < open_until_) {
      return Selection::kRejectedOpen;
    }
    // Open window elapsed: transition to half-open and issue the first probe.
    state_ = State::kHalfOpen;
    half_open_issued_ = 1;
    half_open_completed_ = 0;
    return Selection::kProbe;
  case State::kHalfOpen:
    if (half_open_issued_ < config_.half_open_probes) {
      ++half_open_issued_;
      return Selection::kProbe;
    }
    return Selection::kRejectedHalfOpenQuota;
  }
  return Selection::kRejectedOpen;
}

void CircuitBreaker::AdvanceTo(Clock::time_point now) {
  if (now < epoch_) return;
  if (!initialized_) {
    active_start_ = epoch_ - (epoch_ - epoch_) % config_.window / 10;
    active_start_ = epoch_;
    active_index_ = 0;
    buckets_[0] = Bucket{epoch_, 0, 0};
    initialized_ = true;
  }
  const auto bucket_duration = config_.window / static_cast<int>(kBucketCount);
  while (active_start_ + bucket_duration <= now) {
    active_start_ += bucket_duration;
    active_index_ = (active_index_ + 1) % kBucketCount;
    buckets_[active_index_] = Bucket{active_start_, 0, 0};
  }
}

void CircuitBreaker::Evaluate(Clock::time_point now) {
  const auto [success, failure] = Aggregate(now);
  const std::uint64_t total = success + failure;
  if (total >= config_.min_requests &&
      failure * 1000ULL >=
          static_cast<std::uint64_t>(config_.failure_threshold_permille) * total) {
    Open(now);
  }
}

std::pair<std::uint64_t, std::uint64_t>
CircuitBreaker::Aggregate(Clock::time_point now) const noexcept {
  std::uint64_t success = 0;
  std::uint64_t failure = 0;
  for (const Bucket &bucket : buckets_) {
    if (bucket.start > now - config_.window) {
      success += bucket.success;
      failure += bucket.failure;
    }
  }
  return {success, failure};
}

void CircuitBreaker::Open(Clock::time_point now) noexcept {
  state_ = State::kOpen;
  open_until_ = now + config_.open_duration;
  half_open_issued_ = 0;
  half_open_completed_ = 0;
}

void CircuitBreaker::Close() noexcept {
  state_ = State::kClosed;
  half_open_issued_ = 0;
  half_open_completed_ = 0;
  // Reset the statistical window: old failures must not reopen it.
  for (Bucket &bucket : buckets_) {
    bucket = Bucket{};
  }
  initialized_ = false;
  active_index_ = 0;
}

} // namespace aegisgate::resilience
