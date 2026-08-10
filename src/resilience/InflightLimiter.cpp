#include "aegisgate/resilience/InflightLimiter.h"

#include <stdexcept>
#include <utility>

namespace aegisgate::resilience {

struct InflightLimiter::State {
  explicit State(const std::size_t maximum_value) : maximum(maximum_value) {}

  void ReleaseOne() noexcept {
    if (inflight != 0) --inflight;
  }

  std::size_t maximum;
  std::size_t inflight = 0;
};

InflightLimiter::Reservation::Reservation(Reservation &&other) noexcept
    : state_(std::move(other.state_)) {}

InflightLimiter::Reservation &
InflightLimiter::Reservation::operator=(Reservation &&other) noexcept {
  if (this == &other) return *this;
  Release();
  state_ = std::move(other.state_);
  return *this;
}

InflightLimiter::Reservation::~Reservation() { Release(); }

void InflightLimiter::Reservation::Release() noexcept {
  const auto state = state_.lock();
  state_.reset();
  if (state) state->ReleaseOne();
}

InflightLimiter::InflightLimiter(const std::size_t maximum) {
  if (maximum == 0) {
    throw std::invalid_argument("inflight maximum must be positive");
  }
  state_ = std::make_shared<State>(maximum);
}

InflightLimiter::Reservation InflightLimiter::Acquire() noexcept {
  if (state_->inflight >= state_->maximum) return Reservation();
  ++state_->inflight;
  return Reservation(state_);
}

std::size_t InflightLimiter::inflight() const noexcept {
  return state_ ? state_->inflight : 0;
}

std::size_t InflightLimiter::maximum() const noexcept {
  return state_ ? state_->maximum : 0;
}

} // namespace aegisgate::resilience
