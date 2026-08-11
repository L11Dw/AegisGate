#include "aegisgate/runtime/SelectionState.h"

#include <limits>

namespace aegisgate::runtime {

SelectionState::SelectionState(const config::Config &config)
    : config_(config) {
  selectors_.reserve(config.routes.size());
  active_counts_.reserve(config.routes.size());
  for (const config::Route &route : config.routes) {
    selectors_.emplace_back(route.endpoints);
    std::vector<std::shared_ptr<routing::ActiveReservation::State>> counts;
    counts.reserve(route.endpoints.size());
    for (std::size_t index = 0; index < route.endpoints.size(); ++index) {
      counts.push_back(std::make_shared<routing::ActiveReservation::State>());
    }
    active_counts_.push_back(std::move(counts));
  }
}

std::optional<std::size_t> SelectionState::NextWeightedIndex(std::size_t route_index) noexcept {
  if (route_index >= selectors_.size()) return std::nullopt;
  const config::Endpoint &selected = selectors_[route_index].Next();
  // Content-match the selected copy back into the config list by address +
  // port (R-041): the selector stores copies, so identity is content.
  const std::vector<config::Endpoint> &endpoints = config_.routes[route_index].endpoints;
  for (std::size_t index = 0; index < endpoints.size(); ++index) {
    if (endpoints[index].address == selected.address && endpoints[index].port == selected.port) {
      return index;
    }
  }
  return std::nullopt;
}

std::optional<std::size_t>
SelectionState::NextLeastActiveIndex(std::size_t route_index,
                                     const std::set<std::size_t> &tried,
                                     const std::function<bool(std::size_t)> &eligible) noexcept {
  if (route_index >= selectors_.size()) return std::nullopt;
  const std::vector<config::Endpoint> &endpoints = config_.routes[route_index].endpoints;
  const std::size_t count = endpoints.size();
  if (count == 0) return std::nullopt;
  // Advance the weighted rotation cursor exactly once and locate its owner.
  const config::Endpoint &cursor_owner = selectors_[route_index].Next();
  std::size_t start = count;
  for (std::size_t index = 0; index < count; ++index) {
    if (endpoints[index].address == cursor_owner.address &&
        endpoints[index].port == cursor_owner.port) {
      start = index;
      break;
    }
  }
  if (start == count) return std::nullopt;  // unreachable: the owner is one of ours
  const auto active_at = [this, route_index](std::size_t index) {
    return active_counts_[route_index][index] ? active_counts_[route_index][index]->count : 0U;
  };
  // Pass 1: the minimum active value over eligible, untried candidates.
  std::uint32_t minimum = std::numeric_limits<std::uint32_t>::max();
  bool found = false;
  for (std::size_t step = 0; step < count; ++step) {
    const std::size_t index = (start + step) % count;
    if (tried.contains(index) || !eligible(index)) continue;
    minimum = std::min(minimum, active_at(index));
    found = true;
  }
  if (!found) return std::nullopt;
  // Pass 2: the first candidate in rotation order holding the minimum.
  for (std::size_t step = 0; step < count; ++step) {
    const std::size_t index = (start + step) % count;
    if (tried.contains(index) || !eligible(index)) continue;
    if (active_at(index) == minimum) return index;
  }
  return std::nullopt;
}

std::uint32_t SelectionState::ActiveFor(std::size_t route_index,
                                        std::size_t endpoint_index) const noexcept {
  if (route_index >= active_counts_.size() || endpoint_index >= active_counts_[route_index].size()) {
    return 0;
  }
  const auto &active = active_counts_[route_index][endpoint_index];
  return active ? active->count : 0U;
}

routing::ActiveReservation SelectionState::AcquireActive(std::size_t route_index,
                                                         std::size_t endpoint_index) noexcept {
  if (route_index >= active_counts_.size() || endpoint_index >= active_counts_[route_index].size()) {
    return routing::ActiveReservation();
  }
  const auto &active = active_counts_[route_index][endpoint_index];
  if (!active) return routing::ActiveReservation();
  ++active->count;
  return routing::ActiveReservation(active);
}

} // namespace aegisgate::runtime
