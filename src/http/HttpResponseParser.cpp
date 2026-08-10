#include "aegisgate/http/HttpResponseParser.h"

#include <cctype>
#include <limits>

namespace aegisgate::http {
namespace {

constexpr std::size_t kMaxStatusLineBytes = 8 * 1024;
constexpr std::size_t kMaxHeaderBytes = 32 * 1024;
constexpr std::size_t kMaxBodyBytes = 1024 * 1024;

char LowerAscii(char value) {
  return value >= 'A' && value <= 'Z'
             ? static_cast<char>(value - 'A' + 'a')
             : value;
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
    case '!': case '#': case '$': case '%': case '&': case '\'': case '*':
    case '+': case '-': case '.': case '^': case '_': case '`': case '|':
    case '~': break;
    default: return false;
    }
  }
  return !value.empty();
}

bool IsValidFieldValue(std::string_view value) {
  for (const unsigned char character : value) {
    if (character != '\t' && (character < 0x20 || character == 0x7f)) {
      return false;
    }
  }
  return true;
}

bool HasBareLineFeed(std::string_view bytes) {
  for (std::size_t index = 0; index < bytes.size(); ++index) {
    if (bytes[index] == '\n' && (index == 0 || bytes[index - 1] != '\r')) {
      return true;
    }
  }
  return false;
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
    const auto digit = static_cast<std::size_t>(character - '0');
    if (parsed > (std::numeric_limits<std::size_t>::max() - digit) / 10) {
      return false;
    }
    parsed = parsed * 10 + digit;
  }
  *length = parsed;
  return true;
}

} // namespace

ParseResult HttpResponseParser::Parse(net::Buffer &input) {
  if (result_ != ParseResult::kNeedMoreData) {
    return result_;
  }

  const std::string_view bytes = input.ReadableView();
  const std::size_t headers_end = bytes.find("\r\n\r\n");
  const std::string_view protocol = headers_end == std::string_view::npos
                                        ? bytes : bytes.substr(0, headers_end + 4);
  if (HasBareLineFeed(protocol)) {
    result_ = ParseResult::kError;
    return result_;
  }

  const std::size_t status_line_end = bytes.find("\r\n");
  if (status_line_end == std::string_view::npos) {
    if (bytes.size() > kMaxStatusLineBytes) result_ = ParseResult::kError;
    return result_;
  }
  if (status_line_end > kMaxStatusLineBytes) {
    result_ = ParseResult::kError;
    return result_;
  }

  const std::string_view status_line = bytes.substr(0, status_line_end);
  const std::size_t first_space = status_line.find(' ');
  const std::size_t second_space = first_space == std::string_view::npos
                                       ? std::string_view::npos
                                       : status_line.find(' ', first_space + 1);
  if (first_space != 8 || second_space != 12 || status_line.size() <= second_space + 1 ||
      status_line.substr(0, first_space) != "HTTP/1.1") {
    result_ = ParseResult::kError;
    return result_;
  }
  const std::string_view status_text = status_line.substr(first_space + 1, 3);
  if (status_text[0] < '2' || status_text[0] > '5' ||
      status_text[1] < '0' || status_text[1] > '9' ||
      status_text[2] < '0' || status_text[2] > '9' ||
      !IsValidFieldValue(status_line.substr(second_space + 1))) {
    result_ = ParseResult::kError;
    return result_;
  }

  HttpResponse parsed;
  parsed.status = (status_text[0] - '0') * 100 + (status_text[1] - '0') * 10 +
                  (status_text[2] - '0');
  parsed.reason = status_line.substr(second_space + 1);
  bool has_content_length = false;
  std::size_t content_length = 0;
  std::size_t cursor = status_line_end + 2;
  for (;;) {
    if (cursor > kMaxHeaderBytes + status_line_end + 2) {
      result_ = ParseResult::kError;
      return result_;
    }
    const std::size_t line_end = bytes.find("\r\n", cursor);
    if (line_end == std::string_view::npos) {
      // An unfinished line must still respect the per-line limit; waiting for
      // its CRLF would otherwise retain up to the much larger header block.
      if (bytes.size() - cursor > kMaxStatusLineBytes) {
        result_ = ParseResult::kError;
        return result_;
      }
      if (bytes.size() > kMaxHeaderBytes + status_line_end + 2) result_ = ParseResult::kError;
      return result_;
    }
    if (line_end + 2 - (status_line_end + 2) > kMaxHeaderBytes) {
      result_ = ParseResult::kError;
      return result_;
    }
    if (line_end == cursor) {
      cursor += 2;
      break;
    }
    const std::string_view line = bytes.substr(cursor, line_end - cursor);
    // Match the request parser's line-limit convention: the CRLF delimiter is
    // not counted, so a field-line of exactly 8 KiB remains valid.
    if (line.size() > kMaxStatusLineBytes) {
      result_ = ParseResult::kError;
      return result_;
    }
    const std::size_t colon = line.find(':');
    if (colon == std::string_view::npos || colon == 0 || !IsToken(line.substr(0, colon))) {
      result_ = ParseResult::kError;
      return result_;
    }
    const std::string_view raw_value = line.substr(colon + 1);
    if (!IsValidFieldValue(raw_value)) {
      result_ = ParseResult::kError;
      return result_;
    }
    const std::string_view value = TrimOws(raw_value);
    const std::string name = LowerAscii(line.substr(0, colon));
    if (name == "transfer-encoding") {
      result_ = ParseResult::kUnsupported;
      return result_;
    }
    if (name == "content-length") {
      if (has_content_length || !ParseContentLength(value, &content_length) ||
          content_length > kMaxBodyBytes) {
        result_ = ParseResult::kError;
        return result_;
      }
      has_content_length = true;
    }
    parsed.headers.emplace_back(line.substr(0, colon), value);
    cursor = line_end + 2;
  }

  const bool bodyless = parsed.status == 204 || parsed.status == 304;
  if (bodyless) {
    if (has_content_length && content_length != 0) {
      result_ = ParseResult::kError;
      return result_;
    }
    content_length = 0;
  } else if (!has_content_length) {
    result_ = ParseResult::kError;
    return result_;
  }
  if (bytes.size() - cursor < content_length) return result_;

  parsed.body = bytes.substr(cursor, content_length);
  response_ = std::move(parsed);
  input.Retrieve(cursor + content_length);
  result_ = ParseResult::kComplete;
  return result_;
}

const HttpResponse &HttpResponseParser::Response() const noexcept { return response_; }

void HttpResponseParser::Reset() {
  result_ = ParseResult::kNeedMoreData;
  response_ = HttpResponse{};
}

} // namespace aegisgate::http
