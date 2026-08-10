#include "aegisgate/config/Config.h"

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace aegisgate::config {
namespace {

[[noreturn]] void Invalid(std::string_view message) {
  throw std::invalid_argument("invalid gateway configuration: " + std::string(message));
}

std::string LowerAscii(std::string_view value) {
  std::string result;
  result.reserve(value.size());
  for (const char character : value) {
    result.push_back(character >= 'A' && character <= 'Z'
                         ? static_cast<char>(character - 'A' + 'a')
                         : character);
  }
  return result;
}

std::string RequireString(const YAML::Node &node, std::string_view field) {
  if (!node || !node.IsScalar()) Invalid("missing or non-scalar " + std::string(field));
  try {
    const std::string value = node.as<std::string>();
    if (value.empty()) Invalid("empty " + std::string(field));
    return value;
  } catch (const YAML::Exception &) {
    Invalid("invalid " + std::string(field));
  }
}

std::uint32_t RequirePositiveUnsigned(const YAML::Node &node, std::string_view field,
                                      std::uint32_t maximum) {
  const std::string value = RequireString(node, field);
  if (!std::all_of(value.begin(), value.end(), [](char character) {
        return character >= '0' && character <= '9';
      })) {
    Invalid("invalid unsigned " + std::string(field));
  }
  std::uint64_t parsed{};
  const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size() || parsed == 0 ||
      parsed > maximum) {
    Invalid("out-of-range " + std::string(field));
  }
  return static_cast<std::uint32_t>(parsed);
}

std::unordered_map<std::string, YAML::Node>
ReadObject(const YAML::Node &node, const std::unordered_set<std::string> &allowed,
           std::string_view context) {
  if (!node || !node.IsMap()) Invalid("non-object " + std::string(context));
  std::unordered_map<std::string, YAML::Node> fields;
  for (const auto &entry : node) {
    if (!entry.first.IsScalar()) Invalid("non-string key in " + std::string(context));
    std::string key;
    try {
      key = entry.first.as<std::string>();
    } catch (const YAML::Exception &) {
      Invalid("invalid key in " + std::string(context));
    }
    if (!allowed.contains(key)) Invalid("unknown " + std::string(context) + " field: " + key);
    if (!fields.emplace(std::move(key), entry.second).second) {
      Invalid("duplicate " + std::string(context) + " field");
    }
  }
  return fields;
}

const YAML::Node &RequireField(const std::unordered_map<std::string, YAML::Node> &fields,
                               std::string_view field, std::string_view context) {
  const auto iterator = fields.find(std::string(field));
  if (iterator == fields.end()) Invalid("missing " + std::string(context) + " field: " +
                                        std::string(field));
  return iterator->second;
}

bool IsDecimalPort(std::string_view value) {
  if (value.empty() || !std::all_of(value.begin(), value.end(), [](char character) {
        return character >= '0' && character <= '9';
      })) {
    return false;
  }
  std::uint64_t port{};
  const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), port);
  return error == std::errc{} && end == value.data() + value.size() && port >= 1 && port <= 65535;
}

bool IsValidRouteHost(std::string_view host) {
  if (host.empty() || host.size() > 253 || host.find('[') != std::string_view::npos ||
      host.find(']') != std::string_view::npos) {
    return false;
  }
  const std::size_t colon = host.find(':');
  if (colon != std::string_view::npos) {
    if (host.find(':', colon + 1) != std::string_view::npos || !IsDecimalPort(host.substr(colon + 1))) {
      return false;
    }
    host = host.substr(0, colon);
  }
  if (host.empty() || host.front() == '.' || host.back() == '.') return false;
  return std::all_of(host.begin(), host.end(), [](char character) {
    return (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') ||
           (character >= '0' && character <= '9') || character == '-' || character == '.';
  });
}

bool ParseNumericIpv4(std::string_view host, std::array<std::uint8_t, 4> *address) {
  std::size_t start{};
  for (int component = 0; component < 4; ++component) {
    const std::size_t end = host.find('.', start);
    const std::string_view part = host.substr(start, end == std::string_view::npos ? end : end - start);
    if (part.empty() || part.size() > 3 ||
        !std::all_of(part.begin(), part.end(), [](char character) {
          return character >= '0' && character <= '9';
        })) {
      return false;
    }
    std::uint32_t value{};
    const auto [parsed_end, error] = std::from_chars(part.data(), part.data() + part.size(), value);
    if (error != std::errc{} || parsed_end != part.data() + part.size() || value > 255) return false;
    (*address)[static_cast<std::size_t>(component)] = static_cast<std::uint8_t>(value);
    if (component == 3) return end == std::string_view::npos;
    if (end == std::string_view::npos) return false;
    start = end + 1;
  }
  return false;
}

bool IsHexDigit(unsigned char character) {
  return (character >= '0' && character <= '9') ||
         (character >= 'A' && character <= 'F') ||
         (character >= 'a' && character <= 'f');
}

bool IsUnreserved(unsigned char character) {
  return (character >= 'A' && character <= 'Z') ||
         (character >= 'a' && character <= 'z') ||
         (character >= '0' && character <= '9') || character == '-' ||
         character == '.' || character == '_' || character == '~';
}

bool IsSubDelimiter(unsigned char character) {
  switch (character) {
  case '!':
  case '$':
  case '&':
  case '\'':
  case '(':
  case ')':
  case '*':
  case '+':
  case ',':
  case ';':
  case '=':
    return true;
  default:
    return false;
  }
}

bool IsValidPathPrefix(std::string_view prefix) {
  if (prefix.empty() || prefix.front() != '/') return false;
  for (std::size_t index = 0; index < prefix.size(); ++index) {
    const unsigned char character = static_cast<unsigned char>(prefix[index]);
    if (IsUnreserved(character) || IsSubDelimiter(character) || character == ':' ||
        character == '@' || character == '/') {
      continue;
    }
    if (character != '%' || index + 2 >= prefix.size() ||
        !IsHexDigit(static_cast<unsigned char>(prefix[index + 1])) ||
        !IsHexDigit(static_cast<unsigned char>(prefix[index + 2]))) {
      return false;
    }
    index += 2;
  }
  return true;
}

Endpoint ParseEndpoint(const YAML::Node &node) {
  const auto fields = ReadObject(node, {"host", "port", "weight"}, "endpoint");
  Endpoint endpoint{RequireString(RequireField(fields, "host", "endpoint"), "endpoint host"), {},
                    static_cast<std::uint16_t>(RequirePositiveUnsigned(
                        RequireField(fields, "port", "endpoint"), "endpoint port", 65535)),
                    RequirePositiveUnsigned(RequireField(fields, "weight", "endpoint"),
                                            "endpoint weight", std::numeric_limits<std::uint32_t>::max())};
  if (!ParseNumericIpv4(endpoint.host, &endpoint.address)) Invalid("unsupported endpoint host");
  return endpoint;
}

Route ParseRoute(const YAML::Node &node) {
  const auto fields = ReadObject(node,
                                 {"name", "host", "path_prefix", "endpoints", "rate_limit", "burst",
                                  "max_inflight"},
                                 "route");
  Route route{RequireString(RequireField(fields, "name", "route"), "route name"),
              RequireString(RequireField(fields, "host", "route"), "route host"),
              RequireString(RequireField(fields, "path_prefix", "route"), "route path_prefix"),
              {}, RequirePositiveUnsigned(RequireField(fields, "rate_limit", "route"), "rate_limit",
                                         std::numeric_limits<std::uint32_t>::max()),
              RequirePositiveUnsigned(RequireField(fields, "burst", "route"), "burst",
                                         std::numeric_limits<std::uint32_t>::max()),
              RequirePositiveUnsigned(RequireField(fields, "max_inflight", "route"), "max_inflight",
                                         std::numeric_limits<std::uint32_t>::max())};
  if (!IsValidRouteHost(route.host)) Invalid("invalid route host");
  if (!IsValidPathPrefix(route.path_prefix)) Invalid("invalid route path_prefix");
  const YAML::Node &endpoints = RequireField(fields, "endpoints", "route");
  if (!endpoints.IsSequence() || endpoints.size() == 0) Invalid("empty or invalid endpoints");
  route.endpoints.reserve(endpoints.size());
  for (const auto &endpoint : endpoints) route.endpoints.push_back(ParseEndpoint(endpoint));
  return route;
}

} // namespace

Config LoadFromYaml(std::string_view yaml) {
  try {
    const YAML::Node document = YAML::Load(std::string(yaml));
    const auto top_level = ReadObject(document, {"routes"}, "top-level");
    const YAML::Node &routes = RequireField(top_level, "routes", "top-level");
    if (!routes.IsSequence() || routes.size() == 0) Invalid("empty or invalid routes");

    Config config;
    config.routes.reserve(routes.size());
    std::unordered_set<std::string> names;
    std::unordered_set<std::string> match_keys;
    for (const auto &node : routes) {
      Route route = ParseRoute(node);
      if (!names.emplace(route.name).second) Invalid("duplicate route name");
      const std::string match_key = LowerAscii(route.host) + '\n' + route.path_prefix;
      if (!match_keys.emplace(match_key).second) Invalid("duplicate route host and path_prefix");
      config.routes.push_back(std::move(route));
    }
    return config;
  } catch (const YAML::Exception &error) {
    throw std::invalid_argument("invalid gateway configuration: " + std::string(error.what()));
  }
}

} // namespace aegisgate::config
