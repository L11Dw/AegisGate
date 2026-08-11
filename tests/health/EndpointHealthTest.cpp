#include "aegisgate/health/EndpointHealth.h"

#include <gtest/gtest.h>

namespace aegisgate::health {

TEST(EndpointHealthTest, StartsHealthyAndTracksCheckResults) {
  EndpointHealth health;
  EXPECT_TRUE(health.Healthy());

  health.RecordCheckResult(false);
  EXPECT_FALSE(health.Healthy());

  health.RecordCheckResult(true);
  EXPECT_TRUE(health.Healthy());
}

} // namespace aegisgate::health
