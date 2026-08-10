#include "aegisgate/http/HttpRequestSerializer.h"

#include <stdexcept>
#include <string_view>

namespace aegisgate::http {
namespace {

constexpr std::size_t kMaxBodyBytes = 1024 * 1024;
constexpr std::size_t kMaxRequestLineBytes = 8 * 1024;
constexpr std::size_t kMaxHeaderBytes = 32 * 1024;

bool IsToken(std::string_view value) {
  for (const unsigned char character : value) {
    if ((character >= 'A' && character <= 'Z') ||
        (character >= 'a' && character <= 'z') ||
        (character >= '0' && character <= '9')) {
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

bool TryAddWithinLimit(std::size_t *total, std::size_t value,
                       std::size_t limit) {
  if (value > limit - *total) {
    return false;
  }
  *total += value;
  return true;
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

std::string SerializeRequest(const HttpRequest &request) {
  if (!IsToken(request.method) || !IsValidTarget(request.target) ||
      request.version != "HTTP/1.1" || request.body.size() > kMaxBodyBytes) {
    throw std::invalid_argument("invalid HTTP request");
  }

  std::size_t request_line_bytes = 0;
  if (!TryAddWithinLimit(&request_line_bytes, request.method.size(),
                         kMaxRequestLineBytes) ||
      !TryAddWithinLimit(&request_line_bytes, 1, kMaxRequestLineBytes) ||
      !TryAddWithinLimit(&request_line_bytes, request.target.size(),
                         kMaxRequestLineBytes) ||
      !TryAddWithinLimit(&request_line_bytes, 1, kMaxRequestLineBytes) ||
      !TryAddWithinLimit(&request_line_bytes, request.version.size(),
                         kMaxRequestLineBytes)) {
    throw std::invalid_argument("HTTP request line exceeds limit");
  }

  std::size_t header_bytes = 0;
  std::size_t host_count = 0;
  for (const auto &[name, value] : request.headers) {
    if (!IsToken(name) || !IsValidFieldValue(value)) {
      throw std::invalid_argument("invalid HTTP request header");
    }
    if (!TryAddWithinLimit(&header_bytes, name.size(), kMaxHeaderBytes) ||
        !TryAddWithinLimit(&header_bytes, 2, kMaxHeaderBytes) ||
        !TryAddWithinLimit(&header_bytes, value.size(), kMaxHeaderBytes) ||
        !TryAddWithinLimit(&header_bytes, 2, kMaxHeaderBytes)) {
      throw std::invalid_argument("HTTP request headers exceed limit");
    }
    const std::string lower_name = LowerAscii(name);
    if (lower_name == "host") {
      ++host_count;
    }
    if (lower_name == "content-length" || lower_name == "transfer-encoding" ||
        lower_name == "connection") {
      throw std::invalid_argument("HTTP request framing is managed by Serialize");
    }
  }
  if (host_count != 1) {
    throw std::invalid_argument("HTTP/1.1 request requires exactly one Host header");
  }

  const std::string content_length = "Content-Length: " +
                                     std::to_string(request.body.size()) + "\r\n";
  constexpr std::string_view kConnection = "Connection: keep-alive\r\n";
  constexpr std::string_view kHeaderTerminator = "\r\n";
  if (!TryAddWithinLimit(&header_bytes, content_length.size(), kMaxHeaderBytes) ||
      !TryAddWithinLimit(&header_bytes, kConnection.size(), kMaxHeaderBytes) ||
      !TryAddWithinLimit(&header_bytes, kHeaderTerminator.size(), kMaxHeaderBytes)) {
    throw std::invalid_argument("HTTP request headers exceed limit");
  }

  std::string result;
  result.reserve(request_line_bytes + 2 + header_bytes + request.body.size());
  result.append(request.method).append(" ").append(request.target).append(" ").append(
      request.version).append("\r\n");
  for (const auto &[name, value] : request.headers) {
    result.append(name).append(": ").append(value).append("\r\n");
  }
  result.append(content_length).append(kConnection).append(kHeaderTerminator).append(
      request.body);
  return result;
}

} // namespace aegisgate::http
