#include "aegisgate/routing/RouteTable.h"

#include <string_view>
#include <utility>

namespace aegisgate::routing {
namespace {

bool EqualsIgnoreCase(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) return false;
  for (std::size_t index = 0; index < left.size(); ++index) {
    const char normalized_left = left[index] >= 'A' && left[index] <= 'Z'
                                     ? static_cast<char>(left[index] - 'A' + 'a')
                                     : left[index];
    const char normalized_right = right[index] >= 'A' && right[index] <= 'Z'
                                      ? static_cast<char>(right[index] - 'A' + 'a')
                                      : right[index];
    if (normalized_left != normalized_right) {
      return false;
    }
  }
  return true;
}

bool IsOriginForm(std::string_view target) {
  if (target.empty() || target.front() != '/') return false;
  for (const unsigned char character : target) {
    if (character <= 0x20 || character == 0x7f || character == '#') return false;
  }
  return true;
}

std::string_view PathOnly(std::string_view target) {
  const std::size_t query = target.find('?');
  return target.substr(0, query);
}

bool PrefixMatches(std::string_view path, std::string_view prefix) {
  if (prefix == "/") return true;
  if (!path.starts_with(prefix)) return false;
  return path.size() == prefix.size() || prefix.back() == '/' || path[prefix.size()] == '/';
}

} // namespace

RouteTable::RouteTable(config::Config config) : config_(std::move(config)) {}

std::optional<std::size_t> RouteTable::Match(const config::Config &config, std::string_view host,
                                             std::string_view target) noexcept {
  if (!IsOriginForm(target)) return std::nullopt;
  const std::string_view path = PathOnly(target);
  std::optional<std::size_t> best;
  std::size_t best_prefix_size = 0;
  for (std::size_t index = 0; index < config.routes.size(); ++index) {
    const config::Route &route = config.routes[index];
    if (!EqualsIgnoreCase(route.host, host) || !PrefixMatches(path, route.path_prefix)) continue;
    if (!best.has_value() || route.path_prefix.size() > best_prefix_size) {
      best = index;
      best_prefix_size = route.path_prefix.size();
    }
  }
  return best;
}

std::optional<std::size_t> RouteTable::Match(std::string_view host,
                                             std::string_view target) const noexcept {
  return Match(config_, host, target);
}

} // namespace aegisgate::routing
