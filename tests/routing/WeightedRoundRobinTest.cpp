#include "aegisgate/routing/WeightedRoundRobin.h"

#include <gtest/gtest.h>

#include <limits>

namespace aegisgate::routing {
namespace {

TEST(WeightedRoundRobinTest, ExpandsPositiveWeightsDeterministically) {
  const std::vector<config::Endpoint> endpoints{
      {"127.0.0.1", {127, 0, 0, 1}, 9001, 2},
      {"127.0.0.1", {127, 0, 0, 1}, 9002, 1},
  };
  WeightedRoundRobin selector(endpoints);

  std::vector<std::uint16_t> ports;
  for (int index = 0; index != 6; ++index) ports.push_back(selector.Next().port);

  EXPECT_EQ(ports, (std::vector<std::uint16_t>{9001, 9001, 9002, 9001, 9001, 9002}));
}

TEST(WeightedRoundRobinTest, OwnsEndpointValuesAfterConstruction) {
  WeightedRoundRobin selector({{"127.0.0.1", {127, 0, 0, 1}, 9001, 1}});
  EXPECT_EQ(selector.Next().port, 9001);
}

TEST(WeightedRoundRobinTest, AcceptsLargestLegalWeightWithoutExpandingIt) {
  WeightedRoundRobin selector({
      {"127.0.0.1", {127, 0, 0, 1}, 9001, std::numeric_limits<std::uint32_t>::max()},
      {"127.0.0.1", {127, 0, 0, 1}, 9002, 1},
  });

  EXPECT_EQ(selector.Next().port, 9001);
  EXPECT_EQ(selector.Next().port, 9001);
}

} // namespace
} // namespace aegisgate::routing
