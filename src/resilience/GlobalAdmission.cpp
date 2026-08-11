#include "aegisgate/resilience/GlobalAdmission.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace aegisgate::resilience {

GlobalAdmission::GlobalAdmission(const config::Route &route, Clock::time_point now)
    : rate_(route.rate_limit), burst_(route.burst),
      capacity_credit_(static_cast<std::int64_t>(route.burst) * kCreditScale),
      credit_(capacity_credit_), last_refill_(now),
      state_(std::make_shared<State>(0, route.max_inflight)) {
  if (route.rate_limit == 0 || route.burst == 0 || route.max_inflight == 0) {
    throw std::invalid_argument("global admission limits must be positive");
  }
}

void GlobalAdmission::Refill(Clock::time_point now) noexcept {
  if (now <= last_refill_) return;
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(now - last_refill_);
  last_refill_ = now;
  if (elapsed.count() <= 0) return;
  // Mirror the token-bucket credit math: refill to capacity only after the
  // first nanosecond that supplies all missing credit, otherwise accrue a
  // fractional amount that never yields a whole token early.
  for (;;) {
    std::int64_t credit = credit_.load(std::memory_order_relaxed);
    const std::int64_t missing = capacity_credit_ - credit;
    if (missing <= 0) return;  // already at capacity
    const std::int64_t rate = static_cast<std::int64_t>(rate_);
    const std::int64_t needed = (missing + rate - 1) / rate;
    std::int64_t refilled = 0;
    if (elapsed.count() >= needed) {
      refilled = missing;
    } else {
      refilled = rate * elapsed.count();
    }
    if (credit_.compare_exchange_weak(credit, credit + refilled, std::memory_order_acq_rel,
                                      std::memory_order_relaxed)) {
      return;
    }
  }
}

std::uint32_t GlobalAdmission::Draw(std::uint32_t want) noexcept {
  for (;;) {
    std::int64_t credit = credit_.load(std::memory_order_relaxed);
    const std::int64_t whole = credit / kCreditScale;
    if (whole <= 0) return 0;
    const std::int64_t take = std::min<std::int64_t>(whole, static_cast<std::int64_t>(want));
    if (credit_.compare_exchange_weak(credit, credit - take * kCreditScale,
                                      std::memory_order_acq_rel, std::memory_order_relaxed)) {
      return static_cast<std::uint32_t>(take);
    }
  }
}

void GlobalAdmission::Return(std::uint32_t tokens) noexcept {
  if (tokens == 0) return;
  const std::int64_t value = static_cast<std::int64_t>(tokens) * kCreditScale;
  for (;;) {
    std::int64_t credit = credit_.load(std::memory_order_relaxed);
    const std::int64_t returned = std::min(capacity_credit_ - credit, value);
    if (returned <= 0) return;
    if (credit_.compare_exchange_weak(credit, credit + returned, std::memory_order_acq_rel,
                                      std::memory_order_relaxed)) {
      return;
    }
  }
}

std::optional<GlobalAdmission::Reservation> GlobalAdmission::TryAcquireInflight() noexcept {
  if (!state_->TryAcquire()) return std::nullopt;
  return std::optional<Reservation>(Reservation(state_));
}

std::uint32_t GlobalAdmission::inflight() const noexcept {
  return state_ ? state_->inflight.load(std::memory_order_acquire) : 0U;
}

std::int64_t GlobalAdmission::credit() const noexcept {
  return credit_.load(std::memory_order_acquire);
}

std::uint32_t GlobalAdmission::MaxInflight() const noexcept {
  return state_ ? state_->maximum : 0U;
}

std::uint32_t GlobalAdmission::rate() const noexcept { return rate_; }

std::uint32_t GlobalAdmission::burst() const noexcept { return burst_; }

std::uint32_t GlobalAdmission::LeaseBatch(std::uint32_t rate, std::uint32_t workers,
                                          std::uint32_t burst) noexcept {
  if (workers == 0) return 0;  // callers validate the worker count
  const std::uint64_t batch = (static_cast<std::uint64_t>(rate) + workers - 1) / workers;
  if (batch >= burst) return burst;
  return static_cast<std::uint32_t>(batch > 0 ? batch : 1);
}

bool GlobalAdmission::State::TryAcquire() noexcept {
  std::uint32_t current = inflight.load(std::memory_order_acquire);
  while (current < maximum) {
    if (inflight.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

void GlobalAdmission::State::ReleaseOne() noexcept {
  std::uint32_t current = inflight.load(std::memory_order_acquire);
  while (current != 0) {
    if (inflight.compare_exchange_weak(current, current - 1, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
      return;
    }
  }
}

GlobalAdmission::Reservation::Reservation(Reservation &&other) noexcept
    : state_(std::move(other.state_)) {}

GlobalAdmission::Reservation &GlobalAdmission::Reservation::operator=(Reservation &&other) noexcept {
  if (this == &other) return *this;
  Release();
  state_ = std::move(other.state_);
  return *this;
}

GlobalAdmission::Reservation::~Reservation() { Release(); }

void GlobalAdmission::Reservation::Release() noexcept {
  const auto state = state_.lock();
  state_.reset();
  if (state) state->ReleaseOne();
}

} // namespace aegisgate::resilience
