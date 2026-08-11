#include "aegisgate/http/HttpResponseParser.h"

#include <algorithm>
#include <cctype>
#include <limits>
#include <stdexcept>

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

  ParsedHeader parsed;
  const ParseResult header_result = ParseHeaderArea(input, parsed);
  if (header_result != ParseResult::kComplete) {
    result_ = header_result;
    return result_;
  }
  if (parsed.bodyless) {
    parsed.content_length = 0;
  }

  HttpResponse response;
  response.status = parsed.status;
  response.reason = parsed.reason;
  response.headers = std::move(parsed.headers);
  if (response_to_head_) {
    // A HEAD response never carries a body: it completes at the end of the
    // headers, carrying the declared entity length without consuming any
    // bytes beyond the header area.  Any bytes already buffered past the
    // headers stay in the input so a dirty connection cannot be reused.
    if (!parsed.bodyless) {
      if (parsed.has_content_length) {
        response.body_mode = ResponseBodyMode::kSuppressedWithKnownLength;
        response.content_length = parsed.content_length;
      } else {
        response.body_mode = ResponseBodyMode::kSuppressedWithUnknownLength;
      }
    }
    response_ = std::move(response);
    input.Retrieve(parsed.header_end);
    result_ = ParseResult::kComplete;
    return result_;
  }
  const std::string_view bytes = input.ReadableView();
  if (bytes.size() - parsed.header_end < parsed.content_length) {
    return result_;
  }

  response.body = std::string(bytes.substr(parsed.header_end, parsed.content_length));
  response_ = std::move(response);
  input.Retrieve(parsed.header_end + parsed.content_length);
  result_ = ParseResult::kComplete;
  return result_;
}

ParseResult HttpResponseParser::ParseHeaders(net::Buffer &input) {
  if (result_ != ParseResult::kNeedMoreData) {
    return result_;
  }

  ParsedHeader parsed;
  const ParseResult header_result = ParseHeaderArea(input, parsed);
  if (header_result != ParseResult::kComplete) {
    result_ = header_result;
    return result_;
  }

  if (parsed.bodyless) {
    head_ = HttpResponseHead{parsed.status, parsed.reason, std::move(parsed.headers)};
    body_complete_ = true;
  } else if (response_to_head_) {
    if (parsed.has_content_length) {
      head_ = HttpResponseHead{parsed.status, parsed.reason, std::move(parsed.headers),
                               ResponseBodyMode::kSuppressedWithKnownLength,
                               parsed.content_length};
    } else {
      head_ = HttpResponseHead{parsed.status, parsed.reason, std::move(parsed.headers),
                               ResponseBodyMode::kSuppressedWithUnknownLength};
    }
    body_complete_ = true;
  } else {
    head_ = HttpResponseHead{parsed.status, parsed.reason, std::move(parsed.headers),
                             ResponseBodyMode::kNormal, parsed.content_length};
    remaining_body_ = parsed.content_length;
  }
  input.Retrieve(parsed.header_end);
  return ParseResult::kComplete;
}

ParseResult HttpResponseParser::ConsumeBody(net::Buffer &input, const BodySink &sink) {
  if (!headers_complete_) {
    throw std::logic_error("response headers are not complete");
  }
  if (result_ != ParseResult::kNeedMoreData || body_complete_) {
    result_ = ParseResult::kComplete;
    return result_;
  }
  const std::string_view bytes = input.ReadableView();
  const std::size_t take = std::min(remaining_body_, bytes.size());
  if (take == 0) {
    return ParseResult::kNeedMoreData;
  }
  const bool accepted = sink(bytes.substr(0, take));
  if (!accepted) {
    return ParseResult::kNeedMoreData;
  }
  input.Retrieve(take);
  remaining_body_ -= take;
  if (remaining_body_ == 0) {
    body_complete_ = true;
    result_ = ParseResult::kComplete;
  }
  return result_;
}

const HttpResponse &HttpResponseParser::Response() const noexcept { return response_; }

const HttpResponseHead &HttpResponseParser::Head() const noexcept { return head_; }

bool HttpResponseParser::HeadersComplete() const noexcept { return headers_complete_; }

bool HttpResponseParser::BodyComplete() const noexcept { return body_complete_; }

void HttpResponseParser::Reset(bool response_to_head) {
  result_ = ParseResult::kNeedMoreData;
  headers_complete_ = false;
  response_to_head_ = response_to_head;
  response_ = HttpResponse{};
  head_ = HttpResponseHead{};
  remaining_body_ = 0;
  body_complete_ = false;
}

ParseResult HttpResponseParser::ParseHeaderArea(net::Buffer &input, ParsedHeader &out) {
  const std::string_view bytes = input.ReadableView();
  const std::size_t headers_end = bytes.find("\r\n\r\n");
  const std::string_view protocol = headers_end == std::string_view::npos
                                        ? bytes : bytes.substr(0, headers_end + 4);
  if (HasBareLineFeed(protocol)) {
    return ParseResult::kError;
  }

  const std::size_t status_line_end = bytes.find("\r\n");
  if (status_line_end == std::string_view::npos) {
    if (bytes.size() > kMaxStatusLineBytes) return ParseResult::kError;
    return ParseResult::kNeedMoreData;
  }
  if (status_line_end > kMaxStatusLineBytes) {
    return ParseResult::kError;
  }

  const std::string_view status_line = bytes.substr(0, status_line_end);
  const std::size_t first_space = status_line.find(' ');
  const std::size_t second_space = first_space == std::string_view::npos
                                       ? std::string_view::npos
                                       : status_line.find(' ', first_space + 1);
  if (first_space != 8 || second_space != 12 || status_line.size() <= second_space + 1 ||
      status_line.substr(0, first_space) != "HTTP/1.1") {
    return ParseResult::kError;
  }
  const std::string_view status_text = status_line.substr(first_space + 1, 3);
  if (status_text[0] < '2' || status_text[0] > '5' ||
      status_text[1] < '0' || status_text[1] > '9' ||
      status_text[2] < '0' || status_text[2] > '9' ||
      !IsValidFieldValue(status_line.substr(second_space + 1))) {
    return ParseResult::kError;
  }

  out.status = (status_text[0] - '0') * 100 + (status_text[1] - '0') * 10 +
               (status_text[2] - '0');
  out.reason = status_line.substr(second_space + 1);
  std::size_t cursor = status_line_end + 2;
  for (;;) {
    if (cursor > kMaxHeaderBytes + status_line_end + 2) {
      return ParseResult::kError;
    }
    const std::size_t line_end = bytes.find("\r\n", cursor);
    if (line_end == std::string_view::npos) {
      // An unfinished line must still respect the per-line limit; waiting for
      // its CRLF would otherwise retain up to the much larger header block.
      if (bytes.size() - cursor > kMaxStatusLineBytes) {
        return ParseResult::kError;
      }
      if (bytes.size() > kMaxHeaderBytes + status_line_end + 2) return ParseResult::kError;
      return ParseResult::kNeedMoreData;
    }
    if (line_end + 2 - (status_line_end + 2) > kMaxHeaderBytes) {
      return ParseResult::kError;
    }
    if (line_end == cursor) {
      cursor += 2;
      break;
    }
    const std::string_view line = bytes.substr(cursor, line_end - cursor);
    // Match the request parser's line-limit convention: the CRLF delimiter is
    // not counted, so a field-line of exactly 8 KiB remains valid.
    if (line.size() > kMaxStatusLineBytes) {
      return ParseResult::kError;
    }
    const std::size_t colon = line.find(':');
    if (colon == std::string_view::npos || colon == 0 || !IsToken(line.substr(0, colon))) {
      return ParseResult::kError;
    }
    const std::string_view raw_value = line.substr(colon + 1);
    if (!IsValidFieldValue(raw_value)) {
      return ParseResult::kError;
    }
    const std::string_view value = TrimOws(raw_value);
    const std::string name = LowerAscii(line.substr(0, colon));
    if (name == "transfer-encoding") {
      return ParseResult::kUnsupported;
    }
    if (name == "content-length") {
      if (out.has_content_length || !ParseContentLength(value, &out.content_length) ||
          out.content_length > kMaxBodyBytes) {
        return ParseResult::kError;
      }
      out.has_content_length = true;
    }
    out.headers.emplace_back(line.substr(0, colon), value);
    cursor = line_end + 2;
  }

  out.bodyless = out.status == 204 || out.status == 304;
  if (out.bodyless) {
    if (out.has_content_length && out.content_length != 0) {
      return ParseResult::kError;
    }
    out.content_length = 0;
  } else if (!out.has_content_length && !response_to_head_) {
    return ParseResult::kError;
  }
  out.header_end = cursor;
  headers_complete_ = true;
  return ParseResult::kComplete;
}

} // namespace aegisgate::http
