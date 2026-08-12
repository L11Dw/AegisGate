#include "aegisgate/resilience/CircuitBreaker.h"

#include <algorithm>
#include <limits>

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

void CircuitBreaker::RecordSuccess(Clock::time_point now, const RequestPermit &permit) {
  AdvanceTo(now);
  if (permit.generation != generation_) {
    // A result from an older epoch must not mutate the current state.
    return;
  }
  switch (state_) {
  case State::kClosed:
    if (permit.selection != Selection::kAllowed) return;
    ++buckets_[active_index_].success;
    Evaluate(now);
    return;
  case State::kHalfOpen:
    if (permit.selection != Selection::kProbe || !ConsumeProbe(permit.probe_id)) {
      return;
    }
    ++half_open_completed_;
    // Only the full configured probe quota closes the breaker: a probe that
    // succeeds before the remaining probes are even issued must not close it.
    if (half_open_completed_ == config_.half_open_probes) {
      Close();
    }
    return;
  case State::kOpen:
    return;
  }
}

void CircuitBreaker::RecordFailure(Clock::time_point now, const RequestPermit &permit) {
  AdvanceTo(now);
  if (permit.generation != generation_) {
    return;
  }
  switch (state_) {
  case State::kClosed:
    if (permit.selection != Selection::kAllowed) return;
    ++buckets_[active_index_].failure;
    Evaluate(now);
    return;
  case State::kHalfOpen:
    if (permit.selection != Selection::kProbe || !ConsumeProbe(permit.probe_id)) {
      return;
    }
    // A failed probe reopens immediately and restarts the open window.
    Open(now);
    return;
  case State::kOpen:
    return;
  }
}

CircuitBreaker::RequestPermit CircuitBreaker::Select(Clock::time_point now) {
  AdvanceTo(now);
  switch (state_) {
  case State::kClosed:
    return {Selection::kAllowed, generation_, 0};
  case State::kOpen:
    if (now < open_until_) {
      return {Selection::kRejectedOpen, generation_, 0};
    }
    // Open window elapsed: transition to half-open and issue the first probe.
    BeginHalfOpen();
    return {Selection::kProbe, generation_, pending_probes_.back()};
  case State::kHalfOpen:
    if (half_open_issued_ < config_.half_open_probes) {
      ++half_open_issued_;
      pending_probes_.push_back(next_probe_id_++);
      return {Selection::kProbe, generation_, pending_probes_.back()};
    }
    return {Selection::kRejectedHalfOpenQuota, generation_, 0};
  }
  return {Selection::kRejectedOpen, generation_, 0};
}

void CircuitBreaker::AdvanceTo(Clock::time_point now) {
  if (now < epoch_) return;
  const auto bucket_duration =
      std::chrono::duration_cast<Clock::duration>(config_.window) /
      static_cast<int>(kBucketCount);
  if (!initialized_) {
    active_start_ = epoch_;
    active_index_ = 0;
    buckets_[0] = Bucket{epoch_, 0, 0};
    initialized_ = true;
  }
  if (now < active_start_ + bucket_duration) return;
  // Compute how many buckets the clock skipped over and clear in one step
  // instead of looping per bucket; a jump past the whole window voids the
  // statistics entirely.
  const auto skipped = (now - active_start_) / bucket_duration;
  if (static_cast<std::size_t>(skipped) >= kBucketCount) {
    for (Bucket &bucket : buckets_) bucket = Bucket{};
    const auto offset = (now - epoch_) % bucket_duration;
    active_start_ = now - offset;
    active_index_ = static_cast<std::size_t>((now - epoch_) / bucket_duration) % kBucketCount;
    buckets_[active_index_] = Bucket{active_start_, 0, 0};
    return;
  }
  for (auto count = static_cast<std::size_t>(skipped); count > 0; --count) {
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
  ++generation_;
  pending_probes_.clear();
  half_open_issued_ = 0;
  half_open_completed_ = 0;
}

void CircuitBreaker::BeginHalfOpen() noexcept {
  state_ = State::kHalfOpen;
  ++generation_;
  pending_probes_.clear();
  half_open_issued_ = 1;
  half_open_completed_ = 0;
  pending_probes_.push_back(next_probe_id_++);
}

void CircuitBreaker::Close() noexcept {
  state_ = State::kClosed;
  ++generation_;
  pending_probes_.clear();
  half_open_issued_ = 0;
  half_open_completed_ = 0;
  for (Bucket &bucket : buckets_) {
    bucket = Bucket{};
  }
  initialized_ = false;
  active_index_ = 0;
}

bool CircuitBreaker::ConsumeProbe(std::uint64_t probe_id) noexcept {
  const auto position = std::find(pending_probes_.begin(), pending_probes_.end(), probe_id);
  if (position == pending_probes_.end()) return false;
  pending_probes_.erase(position);
  return true;
}

CircuitBreakerSnapshot CircuitBreaker::ExportSnapshot(Clock::time_point now) const {
  CircuitBreakerSnapshot snapshot;
  snapshot.state = static_cast<std::uint8_t>(state_);
  snapshot.generation = generation_;
  if (state_ == State::kOpen && open_until_ > now) {
    snapshot.open_remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(open_until_ - now);
  }
  snapshot.half_open_issued = half_open_issued_;
  snapshot.half_open_completed = half_open_completed_;
  snapshot.buckets.reserve(kBucketCount);
  for (const Bucket &bucket : buckets_) {
    CircuitBreakerBucketSnapshot out;
    if (bucket.start < now) {
      out.age = std::chrono::duration_cast<std::chrono::milliseconds>(now - bucket.start);
    }
    out.success = bucket.success;
    out.failure = bucket.failure;
    snapshot.buckets.push_back(out);
  }
  return snapshot;
}

void CircuitBreaker::ImportSnapshot(const CircuitBreakerSnapshot &snapshot,
                                    Clock::time_point now) noexcept {
  for (Bucket &bucket : buckets_) bucket = Bucket{};
  initialized_ = true;
  active_start_ = now;
  active_index_ = 0;
  for (std::size_t i = 0; i < snapshot.buckets.size() && i < kBucketCount; ++i) {
    const auto &in = snapshot.buckets[i];
    const auto age = std::min(in.age, config_.window);
    buckets_[i] = Bucket{now - age, in.success, in.failure};
  }
  state_ = State::kClosed;
  open_until_ = {};
  pending_probes_.clear();
  half_open_issued_ = 0;
  half_open_completed_ = 0;
  // Never reuse an old permit epoch or probe id after a reload.
  generation_ = std::max(generation_ + 1, snapshot.generation + 1);
  next_probe_id_ = 1;
  const auto imported = static_cast<State>(snapshot.state);
  if (imported == State::kOpen) {
    if (snapshot.open_remaining > std::chrono::milliseconds::zero()) {
      state_ = State::kOpen;
      open_until_ = now + snapshot.open_remaining;
      return;
    }
    // An Open window that elapsed while the candidate was being prepared
    // resumes directly in a fresh HalfOpen cycle.
    state_ = State::kHalfOpen;
  } else if (imported == State::kHalfOpen) {
    state_ = State::kHalfOpen;
    // CoordinatorState will issue a fresh cycle, so no old probe is retained.
  }
}

} // namespace aegisgate::resilience
