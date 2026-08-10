#include "aegisgate/config/Config.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string_view>

namespace aegisgate::config {
namespace {

constexpr std::string_view kValidConfig = R"yaml(
routes:
  - name: orders
    host: api.demo.local
    path_prefix: /orders
    endpoints:
      - host: 127.0.0.1
        port: 9001
        weight: 2
      - host: 127.0.0.1
        port: 9002
        weight: 1
    rate_limit: 1200
    burst: 200
    max_inflight: 500
)yaml";

TEST(ConfigTest, LoadsValidatedImmutableRouteValues) {
  const Config config = LoadFromYaml(kValidConfig);

  ASSERT_EQ(config.routes.size(), 1U);
  const Route &route = config.routes.front();
  EXPECT_EQ(route.name, "orders");
  EXPECT_EQ(route.host, "api.demo.local");
  EXPECT_EQ(route.path_prefix, "/orders");
  ASSERT_EQ(route.endpoints.size(), 2U);
  EXPECT_EQ(route.endpoints[0].host, "127.0.0.1");
  EXPECT_EQ(route.endpoints[0].port, 9001);
  EXPECT_EQ(route.endpoints[0].weight, 2U);
  EXPECT_EQ(route.endpoints[0].address,
            (std::array<std::uint8_t, 4>{127, 0, 0, 1}));
  EXPECT_EQ(route.rate_limit, 1200U);
  EXPECT_EQ(route.burst, 200U);
  EXPECT_EQ(route.max_inflight, 500U);
}

TEST(ConfigTest, ParsesNumericIpv4LiteralForDirectConnect) {
  constexpr std::string_view yaml = R"yaml(
routes:
  - name: backend
    host: api.local
    path_prefix: /
    endpoints: [{host: 192.0.2.10, port: 9001, weight: 1}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
)yaml";

  const Config config = LoadFromYaml(yaml);
  ASSERT_EQ(config.routes.size(), 1U);
  ASSERT_EQ(config.routes[0].endpoints.size(), 1U);
  EXPECT_EQ(config.routes[0].endpoints[0].address,
            (std::array<std::uint8_t, 4>{192, 0, 2, 10}));
}

TEST(ConfigTest, RejectsEmptyRoutes) {
  EXPECT_THROW(static_cast<void>(LoadFromYaml("routes: []\n")), std::invalid_argument);
}

TEST(ConfigTest, RejectsDuplicateRouteNames) {
  constexpr std::string_view yaml = R"yaml(
routes:
  - name: duplicate
    host: a.local
    path_prefix: /
    endpoints: [{host: 127.0.0.1, port: 9001, weight: 1}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
  - name: duplicate
    host: b.local
    path_prefix: /
    endpoints: [{host: 127.0.0.1, port: 9002, weight: 1}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
)yaml";
  EXPECT_THROW(static_cast<void>(LoadFromYaml(yaml)), std::invalid_argument);
}

TEST(ConfigTest, RejectsDuplicateHostAndPathPrefix) {
  constexpr std::string_view yaml = R"yaml(
routes:
  - name: one
    host: api.local
    path_prefix: /v1
    endpoints: [{host: 127.0.0.1, port: 9001, weight: 1}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
  - name: two
    host: api.local
    path_prefix: /v1
    endpoints: [{host: 127.0.0.1, port: 9002, weight: 1}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
)yaml";
  EXPECT_THROW(static_cast<void>(LoadFromYaml(yaml)), std::invalid_argument);
}

TEST(ConfigTest, RejectsUnknownFields) {
  constexpr std::string_view yaml = R"yaml(
routes:
  - name: orders
    host: api.local
    path_prefix: /
    endpoints: [{host: 127.0.0.1, port: 9001, weight: 1}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
    surprise: no
)yaml";
  EXPECT_THROW(static_cast<void>(LoadFromYaml(yaml)), std::invalid_argument);
}

TEST(ConfigTest, RejectsInvalidEndpointPortWeightAndLimits) {
  for (const std::string_view replacement : {
           "port: 0", "port: 65536", "weight: 0", "rate_limit: 0", "burst: 0",
           "max_inflight: 0"}) {
    std::string yaml(kValidConfig);
    const auto key_end = replacement.find(':');
    const std::string key(replacement.substr(0, key_end));
    const auto position = yaml.find(key + ":");
    ASSERT_NE(position, std::string::npos) << replacement;
    const auto line_end = yaml.find('\n', position);
    yaml.replace(position, line_end - position, replacement);
    EXPECT_THROW(static_cast<void>(LoadFromYaml(yaml)), std::invalid_argument) << replacement;
  }
}

TEST(ConfigTest, LoadsAndValidatesTimeoutAndRetryFields) {
  std::string yaml(kValidConfig);
  const auto insertion = yaml.rfind("    max_inflight: 500");
  ASSERT_NE(insertion, std::string::npos);
  const auto line_end = yaml.find('\n', insertion);
  yaml.insert(line_end + 1,
              "    connect_timeout_ms: 11\n    first_byte_timeout_ms: 12\n"
              "    total_timeout_ms: 13\n    retry_budget: 1\n");
  const Config config = LoadFromYaml(yaml);
  EXPECT_EQ(config.routes[0].connect_timeout_ms, 11U);
  EXPECT_EQ(config.routes[0].first_byte_timeout_ms, 12U);
  EXPECT_EQ(config.routes[0].total_timeout_ms, 13U);
  EXPECT_EQ(config.routes[0].retry_budget, 1U);

  for (const std::string_view field : {"connect_timeout_ms", "first_byte_timeout_ms",
                                       "total_timeout_ms"}) {
    std::string invalid = yaml;
    const auto position = invalid.find(std::string(field) + ":");
    ASSERT_NE(position, std::string::npos);
    const auto end = invalid.find('\n', position);
    invalid.replace(position, end - position, std::string(field) + ": 0");
    EXPECT_THROW(static_cast<void>(LoadFromYaml(invalid)), std::invalid_argument) << field;
  }
  std::string retries_disabled = yaml;
  const auto retry_position = retries_disabled.find("retry_budget:");
  ASSERT_NE(retry_position, std::string::npos);
  const auto retry_end = retries_disabled.find('\n', retry_position);
  retries_disabled.replace(retry_position, retry_end - retry_position, "retry_budget: 0");
  EXPECT_EQ(LoadFromYaml(retries_disabled).routes[0].retry_budget, 0U);
  for (const std::string_view value : {"2", "01", "-1"}) {
    std::string invalid = yaml;
    const auto position = invalid.find("retry_budget:");
    ASSERT_NE(position, std::string::npos);
    const auto end = invalid.find('\n', position);
    invalid.replace(position, end - position, "retry_budget: " + std::string(value));
    EXPECT_THROW(static_cast<void>(LoadFromYaml(invalid)), std::invalid_argument) << value;
  }
  yaml.append("    unknown_timeout: 1\n");
  EXPECT_THROW(static_cast<void>(LoadFromYaml(yaml)), std::invalid_argument);
}

TEST(ConfigTest, RejectsMissingRequiredFieldsAndEmptyEndpoints) {
  for (const std::string_view removed : {"name", "host", "path_prefix", "endpoints",
                                         "rate_limit", "burst", "max_inflight"}) {
    std::string yaml(kValidConfig);
    const std::string needle = removed == "name" ? "  - name:" : "    " + std::string(removed) + ":";
    const auto position = yaml.find(needle);
    ASSERT_NE(position, std::string::npos) << removed;
    const auto line_end = yaml.find('\n', position);
    yaml.erase(position, line_end - position + 1);
    EXPECT_THROW(static_cast<void>(LoadFromYaml(yaml)), std::invalid_argument) << removed;
  }

  constexpr std::string_view empty_endpoints = R"yaml(
routes:
  - name: orders
    host: api.local
    path_prefix: /
    endpoints: []
    rate_limit: 1
    burst: 1
    max_inflight: 1
)yaml";
  EXPECT_THROW(static_cast<void>(LoadFromYaml(empty_endpoints)), std::invalid_argument);
}

TEST(ConfigTest, RejectsUnknownAndUnsupportedEndpointHosts) {
  for (const std::string_view replacement : {"host: localhost", "host: backend.local",
                                               "host: 127.0.0.256", "host: 127.0.0"}) {
    std::string yaml(kValidConfig);
    const auto position = yaml.find("      - host:");
    ASSERT_NE(position, std::string::npos);
    const auto line_end = yaml.find('\n', position);
    yaml.replace(position, line_end - position, "      - " + std::string(replacement));
    EXPECT_THROW(static_cast<void>(LoadFromYaml(yaml)), std::invalid_argument) << replacement;
  }

  constexpr std::string_view unknown_endpoint_field = R"yaml(
routes:
  - name: orders
    host: api.local
    path_prefix: /
    endpoints: [{host: 127.0.0.1, port: 9001, weight: 1, extra: true}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
)yaml";
  EXPECT_THROW(static_cast<void>(LoadFromYaml(unknown_endpoint_field)), std::invalid_argument);
}

TEST(ConfigTest, RejectsStrictSchemaViolations) {
  constexpr std::string_view unknown_top_level = R"yaml(
routes: []
extra: true
)yaml";
  constexpr std::string_view duplicate_route_field = R"yaml(
routes:
  - name: one
    name: two
    host: api.local
    path_prefix: /
    endpoints: [{host: 127.0.0.1, port: 9001, weight: 1}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
)yaml";
  constexpr std::string_view non_scalar_route_name = R"yaml(
routes:
  - name: {nested: value}
    host: api.local
    path_prefix: /
    endpoints: [{host: 127.0.0.1, port: 9001, weight: 1}]
    rate_limit: 1
    burst: 1
    max_inflight: 1
)yaml";
  constexpr std::string_view malformed = "routes: [\n";
  for (const std::string_view yaml :
       {unknown_top_level, duplicate_route_field, non_scalar_route_name, malformed}) {
    EXPECT_THROW(static_cast<void>(LoadFromYaml(yaml)), std::invalid_argument);
  }
}

TEST(ConfigTest, RejectsEmptyRouteFieldsAndNonAsciiHost) {
  for (const std::string_view replacement : {"name: ''", "host: ''", "path_prefix: ''",
                                               "host: api\xC3\xA9.local"}) {
    std::string yaml(kValidConfig);
    const std::size_t key_end = replacement.find(':');
    const std::string key(replacement.substr(0, key_end));
    const auto position = yaml.find(key == "name" ? "  - name:" : "    " + key + ":");
    ASSERT_NE(position, std::string::npos) << replacement;
    const auto line_end = yaml.find('\n', position);
    yaml.replace(position, line_end - position,
                 key == "name" ? "  - " + std::string(replacement)
                               : "    " + std::string(replacement));
    EXPECT_THROW(static_cast<void>(LoadFromYaml(yaml)), std::invalid_argument) << replacement;
  }
}

TEST(ConfigTest, EnforcesOriginFormPathPrefixSyntax) {
  for (const std::string_view prefix : {"/bad\\path", "/bad%zz", "/bad[bracket]"}) {
    std::string yaml(kValidConfig);
    const auto position = yaml.find("    path_prefix:");
    ASSERT_NE(position, std::string::npos);
    const auto line_end = yaml.find('\n', position);
    yaml.replace(position, line_end - position, "    path_prefix: " + std::string(prefix));
    EXPECT_THROW(static_cast<void>(LoadFromYaml(yaml)), std::invalid_argument) << prefix;
  }

  std::string yaml(kValidConfig);
  const auto position = yaml.find("    path_prefix:");
  ASSERT_NE(position, std::string::npos);
  const auto line_end = yaml.find('\n', position);
  yaml.replace(position, line_end - position, "    path_prefix: /api/%7Eitem;v=1");
  EXPECT_NO_THROW(static_cast<void>(LoadFromYaml(yaml)));
}

} // namespace
} // namespace aegisgate::config
