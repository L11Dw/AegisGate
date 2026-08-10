#include "aegisgate/http/HttpRequestParser.h"

#include <cctype>
#include <limits>

namespace aegisgate::http {
namespace {

// Fixed MVP limits bound memory retained while a peer sends an incomplete
// request.  Route-specific limits belong to the later proxy/config layer.
constexpr std::size_t kMaxRequestLineBytes = 8 * 1024;
constexpr std::size_t kMaxHeaderBytes = 32 * 1024;
constexpr std::size_t kMaxBodyBytes = 1024 * 1024;

char LowerAscii(char value) {
  if (value >= 'A' && value <= 'Z') {
    return static_cast<char>(value - 'A' + 'a');
  }
  return value;
}

std::string LowerAscii(std::string_view value) {
  std::string result;
  result.reserve(value.size());
  for (const char character : value) {
    result.push_back(LowerAscii(character));
  }
  return result;
}

std::string_view TrimOws(std::string_view value) {
  while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
    value.remove_prefix(1);
  }
  while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) {
    value.remove_suffix(1);
  }
  return value;
}

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

bool ContainsChunked(std::string_view value) {
  while (!value.empty()) {
    const std::size_t comma = value.find(',');
    const std::string_view coding = TrimOws(value.substr(0, comma));
    if (LowerAscii(coding) == "chunked") {
      return true;
    }
    if (comma == std::string_view::npos) {
      return false;
    }
    value.remove_prefix(comma + 1);
  }
  return false;
}

bool HasBareLineFeed(std::string_view bytes) {
  for (std::size_t index = 0; index < bytes.size(); ++index) {
    if (bytes[index] == '\n' && (index == 0 || bytes[index - 1] != '\r')) {
      return true;
    }
  }
  return false;
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

bool ParseContentLength(std::string_view value, std::size_t *length) {
  if (value.empty()) {
    return false;
  }
  std::size_t parsed = 0;
  for (const char character : value) {
    if (character < '0' || character > '9') {
      return false;
    }
    const std::size_t digit = static_cast<std::size_t>(character - '0');
    if (parsed > (std::numeric_limits<std::size_t>::max() - digit) / 10) {
      return false;
    }
    parsed = parsed * 10 + digit;
  }
  *length = parsed;
  return true;
}

} // namespace

std::string_view HttpRequest::Header(std::string_view name) const {
  const auto found = headers.find(LowerAscii(name));
  return found == headers.end() ? std::string_view{}
                                : std::string_view(found->second);
}

ParseResult HttpRequestParser::Parse(net::Buffer &input) {
  if (result_ != ParseResult::kNeedMoreData) {
    return result_;
  }

  const std::string_view bytes = input.ReadableView();
  const std::size_t headers_end = bytes.find("\r\n\r\n");
  const std::string_view protocol_bytes =
      headers_end == std::string_view::npos ? bytes
                                            : bytes.substr(0, headers_end + 4);
  // A bare LF can otherwise make a malformed header look complete after a
  // later append, enabling request-smuggling-style parser disagreement.
  if (HasBareLineFeed(protocol_bytes)) {
    result_ = ParseResult::kError;
    return result_;
  }

  const std::size_t request_line_end = bytes.find("\r\n");
  if (request_line_end == std::string_view::npos) {
    if (bytes.size() > kMaxRequestLineBytes) {
      result_ = ParseResult::kError;
    }
    return result_;
  }
  if (request_line_end > kMaxRequestLineBytes) {
    result_ = ParseResult::kError;
    return result_;
  }

  const std::string_view request_line = bytes.substr(0, request_line_end);
  const std::size_t first_space = request_line.find(' ');
  const std::size_t second_space =
      first_space == std::string_view::npos
          ? std::string_view::npos
          : request_line.find(' ', first_space + 1);
  if (first_space == std::string_view::npos ||
      second_space == std::string_view::npos ||
      request_line.find(' ', second_space + 1) != std::string_view::npos ||
      first_space == 0 || second_space == first_space + 1 ||
      request_line.substr(second_space + 1) != "HTTP/1.1" ||
      !IsToken(request_line.substr(0, first_space))) {
    result_ = ParseResult::kError;
    return result_;
  }

  HttpRequest parsed;
  parsed.method = request_line.substr(0, first_space);
  parsed.target =
      request_line.substr(first_space + 1, second_space - first_space - 1);
  parsed.version = "HTTP/1.1";

  bool has_content_length = false;
  std::size_t content_length = 0;
  std::size_t cursor = request_line_end + 2;
  for (;;) {
    if (cursor > kMaxHeaderBytes + request_line_end + 2) {
      result_ = ParseResult::kError;
      return result_;
    }
    const std::size_t line_end = bytes.find("\r\n", cursor);
    if (line_end == std::string_view::npos) {
      if (bytes.size() > kMaxHeaderBytes + request_line_end + 2) {
        result_ = ParseResult::kError;
      }
      return result_;
    }
    if (line_end + 2 - (request_line_end + 2) > kMaxHeaderBytes) {
      result_ = ParseResult::kError;
      return result_;
    }
    if (line_end == cursor) {
      cursor += 2;
      break;
    }

    const std::string_view line = bytes.substr(cursor, line_end - cursor);
    const std::size_t colon = line.find(':');
    if (colon == std::string_view::npos || colon == 0 ||
        !IsToken(line.substr(0, colon))) {
      result_ = ParseResult::kError;
      return result_;
    }
    const std::string name = LowerAscii(line.substr(0, colon));
    const std::string_view raw_value = line.substr(colon + 1);
    if (!IsValidFieldValue(raw_value)) {
      result_ = ParseResult::kError;
      return result_;
    }
    const std::string_view value = TrimOws(raw_value);
    if (name == "content-length") {
      if (has_content_length || !ParseContentLength(value, &content_length) ||
          content_length > kMaxBodyBytes) {
        result_ = ParseResult::kError;
        return result_;
      }
      has_content_length = true;
    }
    if (name == "transfer-encoding" && ContainsChunked(value)) {
      result_ = ParseResult::kUnsupported;
      return result_;
    }
    parsed.headers.insert_or_assign(name, std::string(value));
    cursor = line_end + 2;
  }

  if (bytes.size() - cursor < content_length) {
    // Do not consume the request line or headers until its declared body is
    // complete: the next recv may append the remaining bytes to this Buffer.
    return result_;
  }
  parsed.body = bytes.substr(cursor, content_length);
  request_ = std::move(parsed);
  input.Retrieve(cursor + content_length);
  result_ = ParseResult::kComplete;
  return result_;
}

const HttpRequest &HttpRequestParser::Request() const noexcept {
  return request_;
}

void HttpRequestParser::Reset() {
  result_ = ParseResult::kNeedMoreData;
  request_ = HttpRequest{};
}

} // namespace aegisgate::http
