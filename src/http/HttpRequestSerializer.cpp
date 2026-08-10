#include "aegisgate/http/HttpRequestSerializer.h"

#include <cctype>
#include <stdexcept>
#include <string_view>

namespace aegisgate::http {
namespace {

constexpr std::size_t kMaxBodyBytes = 1024 * 1024;

bool IsToken(std::string_view value) {
  for (const unsigned char character : value) {
    if (std::isalnum(character) != 0) {
      continue;
    }
    switch (character) {
    case '!':
    case '#':
    case '$':
    case '%':
    case '&':
    case '\'':
    case '*':
    case '+':
    case '-':
    case '.':
    case '^':
    case '_':
    case '`':
    case '|':
    case '~':
      break;
    default:
      return false;
    }
  }
  return !value.empty();
}

bool IsValidTarget(std::string_view target) {
  if (target.empty() || target.front() != '/') {
    return false;
  }
  for (const unsigned char character : target) {
    if (character <= 0x20 || character == 0x7f) {
      return false;
    }
  }
  return true;
}

bool IsValidFieldValue(std::string_view value) {
  for (const unsigned char character : value) {
    if (character == '\t') {
      continue;
    }
    if (character < 0x20 || character == 0x7f) {
      return false;
    }
  }
  return true;
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

} // namespace

std::string HttpRequestSerializer::Serialize(const HttpRequest &request) {
  if (!IsToken(request.method) || !IsValidTarget(request.target) ||
      request.version != "HTTP/1.1" || request.body.size() > kMaxBodyBytes) {
    throw std::invalid_argument("invalid HTTP request");
  }

  std::string result = request.method + " " + request.target + " " +
                       request.version + "\r\n";
  for (const auto &[name, value] : request.headers) {
    if (!IsToken(name) || !IsValidFieldValue(value)) {
      throw std::invalid_argument("invalid HTTP request header");
    }
    const std::string lower_name = LowerAscii(name);
    if (lower_name == "content-length" || lower_name == "transfer-encoding" ||
        lower_name == "connection") {
      throw std::invalid_argument("HTTP request framing is managed by Serialize");
    }
    result.append(name).append(": ").append(value).append("\r\n");
  }
  result.append("Content-Length: ").append(std::to_string(request.body.size()))
      .append("\r\nConnection: keep-alive\r\n\r\n")
      .append(request.body);
  return result;
}

} // namespace aegisgate::http
