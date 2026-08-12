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

TEST(MetricsTest, RendersReasonLabelForImmediate) {
  Metrics metrics;
  metrics.RecordImmediate("api", 503, {}, false, "no_healthy_endpoint");
  const std::string text = metrics.RenderPrometheus();
  EXPECT_NE(text.find("aegisgate_requests_total{route=\"api\",status=\"503\",upstream=\"\",reason=\"no_healthy_endpoint\"} 1\n"),
            std::string::npos);
}

TEST(MetricsTest, RendersCircuitStateAsOneHotGauge) {
  Metrics metrics;
  metrics.SetCircuitState("api", "127.0.0.1:9001", "open");
  metrics.SetUpstreamHealth("api", "127.0.0.1:9001", false);
  const std::string text = metrics.RenderPrometheus();
  EXPECT_NE(text.find("aegisgate_circuit_state{route=\"api\",upstream=\"127.0.0.1:9001\",state=\"open\"} 1\n"),
            std::string::npos);
  EXPECT_NE(text.find("aegisgate_circuit_state{route=\"api\",upstream=\"127.0.0.1:9001\",state=\"closed\"} 0\n"),
            std::string::npos);
  EXPECT_NE(text.find("aegisgate_circuit_state{route=\"api\",upstream=\"127.0.0.1:9001\",state=\"half_open\"} 0\n"),
            std::string::npos);
  EXPECT_NE(text.find("aegisgate_upstream_health{route=\"api\",upstream=\"127.0.0.1:9001\"} 0\n"),
            std::string::npos);
}

TEST(MetricsTest, EscapesStateLabels) {
  Metrics metrics;
  metrics.SetCircuitState("route\"with\"quote", "up\nstream", "closed");
  metrics.SetUpstreamHealth("route\"with\"quote", "up\nstream", true);
  const std::string text = metrics.RenderPrometheus();
  EXPECT_NE(text.find("route=\"route\\\"with\\\"quote\""), std::string::npos);
  EXPECT_NE(text.find("upstream=\"up\\nstream\""), std::string::npos);
}

TEST(MetricsTest, AggregatesStreamingBackpressurePauseAndResumeCounters) {
  Metrics first;
  Metrics second;
  first.RecordUpstreamReadPause();
  first.RecordUpstreamReadPause();
  first.RecordUpstreamReadResume();
  second.RecordUpstreamReadPause();
  second.RecordUpstreamReadResume();
  second.RecordUpstreamReadResume();

  Metrics::Data aggregate;
  Metrics::MergeInto(aggregate, first.Snapshot());
  Metrics::MergeInto(aggregate, second.Snapshot());
  const std::string text = Metrics::RenderPrometheus(aggregate, {});

  EXPECT_NE(text.find("aegisgate_upstream_read_pauses_total 3\n"), std::string::npos);
  EXPECT_NE(text.find("aegisgate_upstream_read_resumes_total 3\n"), std::string::npos);
}

} // namespace aegisgate::observability
