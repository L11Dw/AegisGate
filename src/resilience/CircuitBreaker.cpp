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
  AdvanceGeneration();
  pending_probes_.clear();
  half_open_issued_ = 0;
  half_open_completed_ = 0;
}

void CircuitBreaker::BeginHalfOpen() noexcept {
  state_ = State::kHalfOpen;
  AdvanceGeneration();
  pending_probes_.clear();
  half_open_issued_ = 1;
  half_open_completed_ = 0;
  pending_probes_.push_back(next_probe_id_++);
}

void CircuitBreaker::Close() noexcept {
  state_ = State::kClosed;
  AdvanceGeneration();
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

void CircuitBreaker::AdvanceGeneration() noexcept {
  ++generation_;
  // Zero is reserved for an unbound permit.  Skipping it also gives import a
  // stable way to create a generation distinct from every normal source epoch.
  if (generation_ == 0) ++generation_;
}

CircuitBreakerSnapshot CircuitBreaker::ExportSnapshot(Clock::time_point now) const {
  CircuitBreakerSnapshot snapshot;
  snapshot.source_generation = generation_;
  switch (state_) {
  case State::kClosed:
    snapshot.state = CircuitBreakerState::kClosed;
    break;
  case State::kOpen:
    snapshot.state = CircuitBreakerState::kOpen;
    snapshot.open_remaining = now < open_until_ ? open_until_ - now : Clock::duration::zero();
    break;
  case State::kHalfOpen:
    snapshot.state = CircuitBreakerState::kHalfOpen;
    snapshot.half_open_quota = config_.half_open_probes;
    break;
  }
  if (state_ != State::kClosed) return snapshot;
  for (const Bucket &bucket : buckets_) {
    if (bucket.start == Clock::time_point{} || bucket.start > now ||
        now - bucket.start >= config_.window) {
      continue;
    }
    snapshot.buckets.push_back({now - bucket.start, bucket.success, bucket.failure});
  }
  return snapshot;
}

std::optional<CircuitBreaker::HalfOpenCycle>
CircuitBreaker::ImportSnapshot(const CircuitBreakerSnapshot &snapshot, Clock::time_point now) {
  // Import is a migration boundary, not a continuation of old permits.  The
  // source epoch is deliberately advanced before any state is made visible.
  generation_ = snapshot.source_generation;
  AdvanceGeneration();
  next_probe_id_ = 1;
  pending_probes_.clear();
  half_open_issued_ = 0;
  half_open_completed_ = 0;
  open_until_ = Clock::time_point{};
  epoch_ = now;
  active_start_ = now;
  active_index_ = 0;
  initialized_ = true;
  for (Bucket &bucket : buckets_) bucket = Bucket{};
  buckets_[active_index_] = Bucket{active_start_, 0, 0};

  const auto bucket_duration =
      std::chrono::duration_cast<Clock::duration>(config_.window) /
      static_cast<int>(kBucketCount);
  if (snapshot.state == CircuitBreakerState::kClosed) {
    state_ = State::kClosed;
    for (const BucketSnapshot &source : snapshot.buckets) {
      if (source.age_from_export < Clock::duration::zero() ||
          source.age_from_export >= config_.window) {
        continue;
      }
      const auto steps_back = static_cast<std::size_t>(source.age_from_export / bucket_duration);
      if (steps_back >= kBucketCount) continue;
      const std::size_t index = (kBucketCount - steps_back) % kBucketCount;
      Bucket &target = buckets_[index];
      if (target.start == Clock::time_point{}) {
        target.start = now - static_cast<Clock::duration>(steps_back * bucket_duration);
      }
      target.success += source.success;
      target.failure += source.failure;
    }
    return std::nullopt;
  }

  if (snapshot.state == CircuitBreakerState::kOpen &&
      snapshot.open_remaining > Clock::duration::zero()) {
    state_ = State::kOpen;
    open_until_ = now + snapshot.open_remaining;
    return std::nullopt;
  }

  // HalfOpen has no transferable in-flight probe ownership, and an elapsed
  // Open window must not reuse an old timer cycle.  Both form one fresh,
  // entirely pre-issued cycle and hand its exact range to the coordinator.
  return BeginFreshHalfOpenCycle(now);
}

CircuitBreaker::HalfOpenCycle
CircuitBreaker::BeginFreshHalfOpenCycle(Clock::time_point /*now*/) noexcept {
  state_ = State::kHalfOpen;
  AdvanceGeneration();
  pending_probes_.clear();
  half_open_completed_ = 0;
  half_open_issued_ = config_.half_open_probes;
  const std::uint64_t base = next_probe_id_;
  for (std::uint32_t index = 0; index < config_.half_open_probes; ++index) {
    pending_probes_.push_back(next_probe_id_++);
  }
  return {generation_, base, config_.half_open_probes};
}

} // namespace aegisgate::resilience
