#include "aegisgate/routing/RouteTable.h"

#include <gtest/gtest.h>

#include <optional>
#include <utility>

namespace aegisgate::routing {
namespace {

config::Endpoint Loopback(std::uint16_t port) {
  return {"127.0.0.1", {127, 0, 0, 1}, port, 1};
}

RouteTable MakeTable() {
  config::Route route{"least", "la.test", "/", {Loopback(9001)}, 10, 10, 4};
  route.balance = config::BalancePolicy::kLeastActive;
  return RouteTable(config::Config{{std::move(route)}});
}

TEST(ActiveReservationTest, ReleasesExactlyOnceAndMoves) {
  RouteTable table = MakeTable();
  const config::Route *matched = table.Match("la.test", "/x");
  ASSERT_NE(matched, nullptr);
  const config::Endpoint &endpoint = matched->endpoints.front();

  auto first = table.AcquireActive(*matched, endpoint);
  auto second = table.AcquireActive(*matched, endpoint);
  EXPECT_EQ(table.ActiveFor(*matched, endpoint), 2U);

  auto moved = std::move(first);
  EXPECT_FALSE(first);
  EXPECT_TRUE(moved);
  EXPECT_EQ(table.ActiveFor(*matched, endpoint), 2U);

  moved.Release();
  moved.Release();  // idempotent: a second release must not underflow
  EXPECT_EQ(table.ActiveFor(*matched, endpoint), 1U);

  second.Release();
  EXPECT_EQ(table.ActiveFor(*matched, endpoint), 0U);
}

TEST(ActiveReservationTest, ReleasesCountObservableWhileOwnerAlive) {
  RouteTable table = MakeTable();
  const config::Route *matched = table.Match("la.test", "/x");
  ASSERT_NE(matched, nullptr);
  const config::Endpoint &endpoint = matched->endpoints.front();

  auto held = table.AcquireActive(*matched, endpoint);
  EXPECT_EQ(table.ActiveFor(*matched, endpoint), 1U);
  held.Release();
  EXPECT_EQ(table.ActiveFor(*matched, endpoint), 0U);
}

TEST(ActiveReservationTest, SafeNoOpWhenOwnerDestroyedFirst) {
  std::optional<ActiveReservation> reservation;
  {
    RouteTable table = MakeTable();
    const config::Route *matched = table.Match("la.test", "/x");
    ASSERT_NE(matched, nullptr);
    reservation.emplace(table.AcquireActive(*matched, matched->endpoints.front()));
    ASSERT_TRUE(*reservation);
  }
  // The owning table is gone: the weak state expired and the counter object no
  // longer exists.  Releasing must be a safe no-op; no count is asserted.
  reservation->Release();
  reservation.reset();
}

} // namespace
} // namespace aegisgate::routing
