#include "aegisgate/observability/Metrics.h"

#include <chrono>
#include <string>

#include <gtest/gtest.h>

namespace aegisgate::observability {
namespace {

TEST(MetricsTest, RendersCompletedRequestAndProtectionMetricsAsPrometheusText) {
  Metrics metrics;
  metrics.SetActiveConnections(3);
  {
    auto request = metrics.BeginRequest("orders");
    request.Complete(200, "127.0.0.1:8080");
  }
  metrics.RecordImmediate("orders", 429, "", true);

  const std::string exposition = metrics.RenderPrometheus();

  EXPECT_NE(exposition.find("aegisgate_requests_total{route=\"orders\",status=\"200\",upstream=\"127.0.0.1:8080\"} 1\n"),
            std::string::npos);
  EXPECT_NE(exposition.find("aegisgate_requests_total{route=\"orders\",status=\"429\",upstream=\"\"} 1\n"),
            std::string::npos);
  EXPECT_NE(exposition.find("aegisgate_rate_limited_total{route=\"orders\"} 1\n"),
            std::string::npos);
  EXPECT_NE(exposition.find("aegisgate_active_connections 3\n"), std::string::npos);
  EXPECT_NE(exposition.find("aegisgate_inflight_requests 0\n"), std::string::npos);
  EXPECT_NE(exposition.find("aegisgate_request_duration_seconds_count{route=\"orders\"} 2\n"),
            std::string::npos);
}

TEST(MetricsTest, EscapesPrometheusLabelValuesAndDoesNotDoubleComplete) {
  Metrics metrics;
  auto request = metrics.BeginRequest("a\\b\"c\n");
  request.Complete(502, "upstream");
  request.Complete(200, "ignored");

  const std::string exposition = metrics.RenderPrometheus();
  EXPECT_NE(exposition.find("route=\"a\\\\b\\\"c\\n\""), std::string::npos);
  EXPECT_NE(exposition.find("status=\"502\""), std::string::npos);
  EXPECT_EQ(exposition.find("status=\"200\""), std::string::npos);
  EXPECT_NE(exposition.find("aegisgate_inflight_requests 0\n"), std::string::npos);
}

} // namespace
} // namespace aegisgate::observability
