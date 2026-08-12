#include <gtest/gtest.h>

#include "aegisgate/health/EndpointHealth.h"
#include "aegisgate/health/ProtectionSnapshot.h"

namespace aegisgate::health {
namespace {

// --- HealthState ---

TEST(HealthStateTest, DefaultIsImplicitHealthy) {
  EndpointHealth health;
  EXPECT_EQ(health.State(), HealthState::kImplicitHealthy);
  EXPECT_TRUE(health.Healthy());
}

TEST(HealthStateTest, UnknownIsNotHealthy) {
  EndpointHealth health(HealthState::kUnknown);
  EXPECT_EQ(health.State(), HealthState::kUnknown);
  EXPECT_FALSE(health.Healthy());
}

TEST(HealthStateTest, HealthyIsHealthy) {
  EndpointHealth health(HealthState::kHealthy);
  EXPECT_TRUE(health.Healthy());
}

TEST(HealthStateTest, UnhealthyIsNotHealthy) {
  EndpointHealth health(HealthState::kUnhealthy);
  EXPECT_FALSE(health.Healthy());
}

TEST(HealthStateTest, SuccessfulCheckMakesHealthy) {
  EndpointHealth health(HealthState::kUnknown);
  health.RecordCheckResult(true);
  EXPECT_EQ(health.State(), HealthState::kHealthy);
  EXPECT_TRUE(health.Healthy());
}

TEST(HealthStateTest, FailedCheckMakesUnhealthy) {
  EndpointHealth health(HealthState::kHealthy);
  health.RecordCheckResult(false);
  EXPECT_EQ(health.State(), HealthState::kUnhealthy);
  EXPECT_FALSE(health.Healthy());
}

TEST(HealthStateTest, ImportState) {
  EndpointHealth health;
  health.ImportState(HealthState::kUnhealthy);
  EXPECT_EQ(health.State(), HealthState::kUnhealthy);
  EXPECT_FALSE(health.Healthy());
}

// --- Identity comparison ---

TEST(IdentityTest, SameRouteIdentity) {
  RouteIdentity a{"api", "example.com", "/v1"};
  RouteIdentity b{"api", "example.com", "/v1"};
  EXPECT_TRUE(SameRouteIdentity(a, b));
}

TEST(IdentityTest, DifferentRouteName) {
  RouteIdentity a{"api", "example.com", "/v1"};
  RouteIdentity b{"web", "example.com", "/v1"};
  EXPECT_FALSE(SameRouteIdentity(a, b));
}

TEST(IdentityTest, DifferentRouteHost) {
  RouteIdentity a{"api", "example.com", "/v1"};
  RouteIdentity b{"api", "other.com", "/v1"};
  EXPECT_FALSE(SameRouteIdentity(a, b));
}

TEST(IdentityTest, DifferentRoutePath) {
  RouteIdentity a{"api", "example.com", "/v1"};
  RouteIdentity b{"api", "example.com", "/v2"};
  EXPECT_FALSE(SameRouteIdentity(a, b));
}

TEST(IdentityTest, SameEndpointIdentity) {
  EndpointIdentity a{"127.0.0.1", {127, 0, 0, 1}, 8080};
  EndpointIdentity b{"127.0.0.1", {127, 0, 0, 1}, 8080};
  EXPECT_TRUE(SameEndpointIdentity(a, b));
}

TEST(IdentityTest, DifferentEndpointHost) {
  EndpointIdentity a{"127.0.0.1", {127, 0, 0, 1}, 8080};
  EndpointIdentity b{"10.0.0.1", {127, 0, 0, 1}, 8080};
  EXPECT_FALSE(SameEndpointIdentity(a, b));
}

TEST(IdentityTest, DifferentEndpointAddress) {
  EndpointIdentity a{"127.0.0.1", {127, 0, 0, 1}, 8080};
  EndpointIdentity b{"127.0.0.1", {10, 0, 0, 1}, 8080};
  EXPECT_FALSE(SameEndpointIdentity(a, b));
}

TEST(IdentityTest, DifferentEndpointPort) {
  EndpointIdentity a{"127.0.0.1", {127, 0, 0, 1}, 8080};
  EndpointIdentity b{"127.0.0.1", {127, 0, 0, 1}, 9090};
  EXPECT_FALSE(SameEndpointIdentity(a, b));
}

// --- Policy comparison ---

TEST(PolicyTest, SameHealthPolicyBothAbsent) {
  EXPECT_TRUE(SameHealthPolicy(std::nullopt, std::nullopt));
}

TEST(PolicyTest, SameHealthPolicyBothPresent) {
  config::HealthCheckSettings a{1000, 200};
  config::HealthCheckSettings b{1000, 200};
  EXPECT_TRUE(SameHealthPolicy(a, b));
}

TEST(PolicyTest, DifferentHealthPolicyOneAbsent) {
  config::HealthCheckSettings a{1000, 200};
  EXPECT_FALSE(SameHealthPolicy(a, std::nullopt));
  EXPECT_FALSE(SameHealthPolicy(std::nullopt, a));
}

TEST(PolicyTest, DifferentHealthPolicyInterval) {
  config::HealthCheckSettings a{1000, 200};
  config::HealthCheckSettings b{2000, 200};
  EXPECT_FALSE(SameHealthPolicy(a, b));
}

TEST(PolicyTest, SameBreakerPolicyBothAbsent) {
  EXPECT_TRUE(SameBreakerPolicy(std::nullopt, std::nullopt));
}

TEST(PolicyTest, SameBreakerPolicyBothPresent) {
  config::CircuitBreakerSettings a{10, 5, 500, 30, 2};
  config::CircuitBreakerSettings b{10, 5, 500, 30, 2};
  EXPECT_TRUE(SameBreakerPolicy(a, b));
}

TEST(PolicyTest, DifferentBreakerPolicyThreshold) {
  config::CircuitBreakerSettings a{10, 5, 500, 30, 2};
  config::CircuitBreakerSettings b{10, 5, 300, 30, 2};
  EXPECT_FALSE(SameBreakerPolicy(a, b));
}

// --- BucketSnapshot ---

TEST(BucketSnapshotTest, RelativeAgeFields) {
  BucketSnapshot bucket;
  bucket.age_from_export = std::chrono::milliseconds(100);
  bucket.success = 5;
  bucket.failure = 2;
  EXPECT_EQ(bucket.success, 5U);
  EXPECT_EQ(bucket.failure, 2U);
  EXPECT_EQ(bucket.age_from_export, std::chrono::milliseconds(100));
}

TEST(CircuitBreakerSnapshotTest, DefaultStateIsClosed) {
  CircuitBreakerSnapshot snap;
  EXPECT_EQ(snap.state, resilience::CircuitBreaker::State::kClosed);
  EXPECT_TRUE(snap.buckets.empty());
  EXPECT_EQ(snap.half_open_quota, 0U);
}

TEST(EndpointProtectionSnapshotTest, RouteAndEndpointIdentity) {
  EndpointProtectionSnapshot snap;
  snap.route = {"api", "example.com", "/v1"};
  snap.endpoint = {"127.0.0.1", {127, 0, 0, 1}, 8080};
  snap.health.state = HealthState::kHealthy;
  EXPECT_TRUE(SameRouteIdentity(snap.route, {"api", "example.com", "/v1"}));
  EXPECT_TRUE(SameEndpointIdentity(snap.endpoint, {"127.0.0.1", {127, 0, 0, 1}, 8080}));
  EXPECT_EQ(snap.health.state, HealthState::kHealthy);
}

} // namespace
} // namespace aegisgate::health
