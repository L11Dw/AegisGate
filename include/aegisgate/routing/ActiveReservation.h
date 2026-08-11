#pragma once

#include <cstdint>
#include <memory>
#include <utility>

namespace aegisgate::routing {

// RAII guard over one route x endpoint active-attempt slot.  It is move-only
// and weak-state like resilience::InflightLimiter::Reservation: releasing after
// the owning RouteTable was destroyed is a safe no-op, and Release is
// idempotent (the counter can never underflow or double-release).
class ActiveReservation {
public:
  // Shared per-endpoint counter; owned by RouteTable::EndpointState so the
  // guard can observe the table's lifetime through a weak_ptr.
  struct State {
    void ReleaseOne() noexcept {
      if (count != 0) --count;
    }
    std::uint32_t count = 0;
  };

  ActiveReservation() = default;
  ActiveReservation(const ActiveReservation &) = delete;
  ActiveReservation &operator=(const ActiveReservation &) = delete;
  ActiveReservation(ActiveReservation &&other) noexcept;
  ActiveReservation &operator=(ActiveReservation &&other) noexcept;
  ~ActiveReservation();

  // True while the owning state is still alive and this guard holds a slot.
  explicit operator bool() const noexcept { return !state_.expired(); }
  void Release() noexcept;

  // Public so worker-local selection state (per-worker active counters) can
  // hand out reservations; the weak state keeps the release safe after the
  // owning worker state was destroyed.
  explicit ActiveReservation(std::weak_ptr<State> state) noexcept
      : state_(std::move(state)) {}

private:
  std::weak_ptr<State> state_;
};

} // namespace aegisgate::routing
