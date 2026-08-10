#include "aegisgate/routing/WeightedRoundRobin.h"

#include <limits>
#include <stdexcept>

namespace aegisgate::routing {

WeightedRoundRobin::WeightedRoundRobin(const std::vector<config::Endpoint> &endpoints) {
  endpoints_ = endpoints;
  for (const config::Endpoint &endpoint : endpoints_) {
    if (endpoint.weight == 0 || total_weight_ >
                                    std::numeric_limits<std::uint64_t>::max() - endpoint.weight) {
      throw std::invalid_argument("invalid weighted upstream endpoints");
    }
    total_weight_ += endpoint.weight;
  }
  if (total_weight_ == 0) throw std::invalid_argument("empty weighted upstream endpoints");
}

const config::Endpoint &WeightedRoundRobin::Next() noexcept {
  const std::uint64_t selected_weight = next_weight_;
  next_weight_ = next_weight_ + 1 == total_weight_ ? 0 : next_weight_ + 1;
  std::uint64_t upper_bound = 0;
  for (const config::Endpoint &endpoint : endpoints_) {
    upper_bound += endpoint.weight;
    if (selected_weight < upper_bound) return endpoint;
  }
  // Constructor establishes the total invariant. This path is unreachable.
  return endpoints_.front();
}

} // namespace aegisgate::routing
