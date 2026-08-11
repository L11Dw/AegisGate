#include "aegisgate/runtime/SelectionState.h"

#include <gtest/gtest.h>

#include <optional>
#include <utility>

namespace aegisgate::routing {
namespace {

config::Endpoint Loopback(std::uint16_t port) {
  return {"127.0.0.1", {127, 0, 0, 1}, port, 1};
}

runtime::SelectionState MakeSelection() {
  config::Route route{"least", "la.test", "/", {Loopback(9001)}, 10, 10, 4};
  route.balance = config::BalancePolicy::kLeastActive;
  config::Config config{{std::move(route)}};
  return runtime::SelectionState(config);
}

TEST(ActiveReservationTest, ReleasesExactlyOnceAndMoves) {
  auto selection = MakeSelection();
  auto first = selection.AcquireActive(0, 0);
  auto second = selection.AcquireActive(0, 0);
  EXPECT_EQ(selection.ActiveFor(0, 0), 2U);

  auto moved = std::move(first);
  EXPECT_FALSE(first);
  EXPECT_TRUE(moved);
  EXPECT_EQ(selection.ActiveFor(0, 0), 2U);

  moved.Release();
  moved.Release();  // idempotent: a second release must not underflow
  EXPECT_EQ(selection.ActiveFor(0, 0), 1U);

  second.Release();
  EXPECT_EQ(selection.ActiveFor(0, 0), 0U);
}

TEST(ActiveReservationTest, ReleasesCountObservableWhileOwnerAlive) {
  auto selection = MakeSelection();
  auto held = selection.AcquireActive(0, 0);
  EXPECT_EQ(selection.ActiveFor(0, 0), 1U);
  held.Release();
  EXPECT_EQ(selection.ActiveFor(0, 0), 0U);
}

TEST(ActiveReservationTest, SafeNoOpWhenOwnerDestroyedFirst) {
  std::optional<ActiveReservation> reservation;
  {
    auto selection = MakeSelection();
    reservation.emplace(selection.AcquireActive(0, 0));
    ASSERT_TRUE(*reservation);
  }
  // The owning worker selection state is gone: the weak state expired and the
  // counter object no longer exists.  Releasing must be a safe no-op.
  reservation->Release();
  reservation.reset();
}

} // namespace
} // namespace aegisgate::routing
