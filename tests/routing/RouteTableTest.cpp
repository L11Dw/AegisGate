#include "aegisgate/config/Config.h"
#include "aegisgate/routing/RouteTable.h"

#include <gtest/gtest.h>

#include <string_view>

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

} // namespace
} // namespace aegisgate::routing
