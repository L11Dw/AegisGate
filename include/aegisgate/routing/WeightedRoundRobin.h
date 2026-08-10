#pragma once

#include <cstddef>
#include <vector>

#include "aegisgate/config/Config.h"

namespace aegisgate::routing {

// A deterministic weighted schedule. It copies endpoints so a selector never
// borrows route/configuration storage whose lifetime a caller may end. It does
// not expand weights: a legal UINT32_MAX weight consumes constant storage.
class WeightedRoundRobin {
public:
  explicit WeightedRoundRobin(const std::vector<config::Endpoint> &endpoints);

  [[nodiscard]] const config::Endpoint &Next() noexcept;

private:
  std::vector<config::Endpoint> endpoints_;
  std::uint64_t total_weight_ = 0;
  std::uint64_t next_weight_ = 0;
};

} // namespace aegisgate::routing
