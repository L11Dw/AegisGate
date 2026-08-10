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

bool IsValidUriComponent(std::string_view value, bool is_query) {
  for (std::size_t index = 0; index < value.size(); ++index) {
    const unsigned char character = static_cast<unsigned char>(value[index]);
    if (IsUnreserved(character) || IsSubDelimiter(character) || character == ':' ||
        character == '@' || character == '/' || (is_query && character == '?')) {
      continue;
    }
    if (character != '%' || index + 2 >= value.size() ||
        !IsHexDigit(static_cast<unsigned char>(value[index + 1])) ||
        !IsHexDigit(static_cast<unsigned char>(value[index + 2]))) {
      return false;
    }
    index += 2;
  }
  return true;
}

bool IsValidTarget(std::string_view target) {
  if (target.empty() || target.front() != '/') {
    return false;
  }
  const std::size_t query_start = target.find('?');
  const std::string_view path = target.substr(0, query_start);
  const std::string_view query = query_start == std::string_view::npos
                                     ? std::string_view{}
                                     : target.substr(query_start + 1);
  return IsValidUriComponent(path, false) && IsValidUriComponent(query, true);
}

bool IsValidFieldValue(std::string_view value) {
  for (const unsigned char character : value) {
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
