#include "aegisgate/routing/ActiveReservation.h"

#include <utility>

namespace aegisgate::routing {

ActiveReservation::ActiveReservation(ActiveReservation &&other) noexcept
    : state_(std::move(other.state_)) {}

ActiveReservation &
ActiveReservation::operator=(ActiveReservation &&other) noexcept {
  if (this == &other) return *this;
  Release();
  state_ = std::move(other.state_);
  return *this;
}

ActiveReservation::~ActiveReservation() { Release(); }

void ActiveReservation::Release() noexcept {
  const auto state = state_.lock();
  state_.reset();
  if (state) state->ReleaseOne();
}

} // namespace aegisgate::routing
