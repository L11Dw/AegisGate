#include "aegisgate/config/Config.h"
#include "aegisgate/routing/RouteTable.h"

#include <gtest/gtest.h>

#include <chrono>
#include <set>
#include <string_view>
#include <utility>
#include <vector>

namespace aegisgate::routing {
namespace {

constexpr std::string_view kRoutes = R"yaml(
routes:
  - name: root
    host: api.demo.local
    path_prefix: /
    endpoints: [{host: 127.0.0.1, port: 9001, weight: 1}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
  - name: api
    host: api.demo.local
    path_prefix: /api
    endpoints: [{host: 127.0.0.1, port: 9002, weight: 1}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
  - name: versioned
    host: api.demo.local
    path_prefix: /api/v1
    endpoints: [{host: 127.0.0.1, port: 9003, weight: 1}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
  - name: ported
    host: admin.demo.local:8080
    path_prefix: /
    endpoints: [{host: 127.0.0.1, port: 9004, weight: 1}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
)yaml";

RouteTable MakeTable() { return RouteTable(config::LoadFromYaml(kRoutes)); }

TEST(RouteTableTest, MatchesHostIgnoringAsciiCase) {
  const RouteTable table = MakeTable();
  const config::Route *route = table.Match("API.DEMO.LOCAL", "/anything");
  ASSERT_NE(route, nullptr);
  EXPECT_EQ(route->name, "root");
}

TEST(RouteTableTest, SelectsLongestMatchingPathPrefix) {
  const RouteTable table = MakeTable();
  const config::Route *route = table.Match("api.demo.local", "/api/v1/orders?full=1");
  ASSERT_NE(route, nullptr);
  EXPECT_EQ(route->name, "versioned");
}

TEST(RouteTableTest, RequiresPathBoundaryForNonRootPrefixes) {
  const RouteTable table = MakeTable();
  const config::Route *route = table.Match("api.demo.local", "/apix");
  ASSERT_NE(route, nullptr);
  EXPECT_EQ(route->name, "root");
}

TEST(RouteTableTest, RejectsNonOriginTargetAndUnknownHost) {
  const RouteTable table = MakeTable();
  EXPECT_EQ(table.Match("api.demo.local", "https://api.demo.local/api"), nullptr);
  EXPECT_EQ(table.Match("other.demo.local", "/api"), nullptr);
}

TEST(RouteTableTest, MatchesHostWithExplicitPortExactly) {
  const RouteTable table = MakeTable();
  const config::Route *route = table.Match("ADMIN.DEMO.LOCAL:8080", "/");
  ASSERT_NE(route, nullptr);
  EXPECT_EQ(route->name, "ported");
  EXPECT_EQ(table.Match("admin.demo.local", "/"), nullptr);
}

TEST(RouteTableTest, RejectsNonAsciiHostInput) {
  const RouteTable table = MakeTable();
  EXPECT_EQ(table.Match("api.demo.local\xC3\xA9", "/api"), nullptr);
}

TEST(RouteTableTest, KeepsOneAdmissionStateForEachMatchedRoute) {
  RouteTable table = MakeTable();
  const config::Route *route = table.Match("api.demo.local", "/api");
  ASSERT_NE(route, nullptr);

  const auto first_state = table.AdmissionFor(*route);
  const auto second_state = table.AdmissionFor(*route);
  ASSERT_NE(first_state, nullptr);
  EXPECT_EQ(first_state, second_state);

  const auto now = std::chrono::steady_clock::time_point{};
  EXPECT_TRUE(first_state->TryAcquire(now));
  EXPECT_FALSE(second_state->TryAcquire(now));
}

config::Endpoint Loopback(std::uint16_t port, std::uint32_t weight = 1) {
  return {"127.0.0.1", {127, 0, 0, 1}, port, weight};
}

RouteTable MakeLeastActiveTable(std::vector<config::Endpoint> endpoints) {
  config::Route route{"least", "la.test", "/", std::move(endpoints), 10, 10, 4};
  route.balance = config::BalancePolicy::kLeastActive;
  return RouteTable(config::Config{{std::move(route)}});
}

TEST(RouteTableTest, PicksFewerActiveEndpoint) {
  // The busy endpoint owns the current rotation position and the higher
  // weight; the less busy one must still win.
  RouteTable table = MakeLeastActiveTable({Loopback(9001, 10), Loopback(9002, 1)});
  const config::Route *route = table.Match("la.test", "/x");
  ASSERT_NE(route, nullptr);
  const config::Endpoint &busy = route->endpoints[0];
  const config::Endpoint &idle = route->endpoints[1];
  auto first = table.AcquireActive(*route, busy);
  auto second = table.AcquireActive(*route, busy);
  auto third = table.AcquireActive(*route, idle);

  const std::set<std::size_t> tried;
  EXPECT_EQ(table.NextLeastActiveIndex(*route, tried), 1U);
  EXPECT_EQ(table.ActiveFor(*route, busy), 2U);
  EXPECT_EQ(table.ActiveFor(*route, idle), 1U);

  first.Release();
  second.Release();
  third.Release();
  EXPECT_EQ(table.ActiveFor(*route, busy), 0U);
  EXPECT_EQ(table.ActiveFor(*route, idle), 0U);
}

TEST(RouteTableTest, AlternatesEqualWeightTies) {
  RouteTable table = MakeLeastActiveTable({Loopback(9001), Loopback(9002)});
  const config::Route *route = table.Match("la.test", "/x");
  ASSERT_NE(route, nullptr);
  const std::set<std::size_t> tried;
  std::vector<std::size_t> picks;
  for (int i = 0; i < 6; ++i) {
    picks.push_back(*table.NextLeastActiveIndex(*route, tried));
  }
  EXPECT_EQ(picks, (std::vector<std::size_t>{0, 1, 0, 1, 0, 1}));
}

TEST(RouteTableTest, WeightsResolveTiesProportionally) {
  RouteTable table = MakeLeastActiveTable({Loopback(9001, 2), Loopback(9002, 1)});
  const config::Route *route = table.Match("la.test", "/x");
  ASSERT_NE(route, nullptr);
  const std::set<std::size_t> tried;
  std::size_t first = 0;
  std::size_t second = 0;
  for (int i = 0; i < 9; ++i) {
    if (*table.NextLeastActiveIndex(*route, tried) == 0) {
      ++first;
    } else {
      ++second;
    }
  }
  EXPECT_EQ(first, 6U);
  EXPECT_EQ(second, 3U);
}

TEST(RouteTableTest, SkipsTriedAndIneligibleCandidates) {
  RouteTable table = MakeLeastActiveTable({Loopback(9001), Loopback(9002)});
  const config::Route *route = table.Match("la.test", "/x");
  ASSERT_NE(route, nullptr);
  // A tried index is skipped even when it is the rotation owner.
  EXPECT_EQ(table.NextLeastActiveIndex(*route, std::set<std::size_t>{0}), 1U);
  EXPECT_EQ(table.NextLeastActiveIndex(*route, std::set<std::size_t>{0, 1}),
            std::nullopt);
}

TEST(RouteTableTest, SkipsUnhealthyAndOpenCandidates) {
  config::Route route{"least", "la.test", "/", {Loopback(9001), Loopback(9002)},
                      10, 10, 4};
  route.balance = config::BalancePolicy::kLeastActive;
  route.health_check = config::HealthCheckSettings{1000, 200};
  route.circuit_breaker = config::CircuitBreakerSettings{10, 5, 500, 5, 1};
  RouteTable table{config::Config{{route}}};
  const config::Route *matched = table.Match("la.test", "/x");
  ASSERT_NE(matched, nullptr);

  // Unhealthy endpoint 0 must never be selected.
  health::EndpointHealth *health = table.HealthFor(*matched, matched->endpoints[0]);
  ASSERT_NE(health, nullptr);
  health->RecordCheckResult(false);
  EXPECT_EQ(table.NextLeastActiveIndex(*matched, std::set<std::size_t>{}), 1U);

  // Open breaker on endpoint 1: endpoint 0 becomes the only candidate again.
  health->RecordCheckResult(true);
  resilience::CircuitBreaker *breaker =
      table.BreakerFor(*matched, matched->endpoints[1]);
  ASSERT_NE(breaker, nullptr);
  const auto now = resilience::CircuitBreaker::Clock::now();
  for (int i = 0; i < 5; ++i) {
    const auto at = now + std::chrono::milliseconds(i);
    breaker->RecordFailure(at, breaker->Select(at));
  }
  EXPECT_EQ(table.NextLeastActiveIndex(*matched, std::set<std::size_t>{}), 0U);

  // Both unavailable: no candidate at all.
  health->RecordCheckResult(false);
  EXPECT_EQ(table.NextLeastActiveIndex(*matched, std::set<std::size_t>{}),
            std::nullopt);
}

TEST(RouteTableTest, ActiveCountsDoNotCrossRoutes) {
  config::Route first_route{"a", "a.test", "/", {Loopback(9001)}, 10, 10, 4};
  first_route.balance = config::BalancePolicy::kLeastActive;
  config::Route second_route{"b", "b.test", "/", {Loopback(9001)}, 10, 10, 4};
  second_route.balance = config::BalancePolicy::kLeastActive;
  RouteTable table{config::Config{{first_route, second_route}}};
  const config::Route *matched_a = table.Match("a.test", "/x");
  const config::Route *matched_b = table.Match("b.test", "/x");
  ASSERT_NE(matched_a, nullptr);
  ASSERT_NE(matched_b, nullptr);
  const config::Endpoint &shared = matched_a->endpoints.front();

  auto held = table.AcquireActive(*matched_a, shared);
  EXPECT_EQ(table.ActiveFor(*matched_a, shared), 1U);
  EXPECT_EQ(table.ActiveFor(*matched_b, shared), 0U);

  held.Release();
  EXPECT_EQ(table.ActiveFor(*matched_a, shared), 0U);
  EXPECT_EQ(table.ActiveFor(*matched_b, shared), 0U);
}

} // namespace
} // namespace aegisgate::routing
