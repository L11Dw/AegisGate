#include "aegisgate/http/HttpResponse.h"

#include <cctype>
#include <stdexcept>
#include <string_view>

namespace aegisgate::http {
namespace {

bool HasLineBreak(std::string_view value) {
  return value.find_first_of("\r\n") != std::string_view::npos;
}

std::string Lowercase(std::string_view value) {
  std::string result;
  result.reserve(value.size());
  for (const unsigned char character : value) {
    result.push_back(static_cast<char>(std::tolower(character)));
  }
  return result;
}

} // namespace

std::string HttpResponse::Serialize() const {
  if (status < 200 || status > 599 || HasLineBreak(reason)) {
    throw std::invalid_argument("invalid HTTP response status line");
  }
  const bool bodyless_status = status == 204 || status == 304;
  if (bodyless_status) {
    // A bodyless status must not silently swallow framing metadata.
    if (!body.empty() || body_mode != ResponseBodyMode::kNormal ||
        content_length.has_value()) {
      throw std::invalid_argument("invalid bodyless HTTP response framing");
    }
  } else {
    switch (body_mode) {
    case ResponseBodyMode::kNormal:
      if (content_length.has_value()) {
        throw std::invalid_argument("Content-Length is managed by Serialize");
      }
      break;
    case ResponseBodyMode::kSuppressedWithKnownLength:
      if (!content_length.has_value() || !body.empty()) {
        throw std::invalid_argument(
            "suppressed body with known length requires Content-Length and no body");
      }
      break;
    case ResponseBodyMode::kSuppressedWithUnknownLength:
      if (content_length.has_value() || !body.empty()) {
        throw std::invalid_argument(
            "suppressed body with unknown length requires no Content-Length and no body");
      }
      break;
    }
  }

  std::string result = "HTTP/1.1 " + std::to_string(status) + " " + reason + "\r\n";
  for (const auto &[name, value] : headers) {
    if (name.empty() || HasLineBreak(name) || HasLineBreak(value)) {
      throw std::invalid_argument("invalid HTTP response header");
    }
    const std::string lowercase_name = Lowercase(name);
    if (lowercase_name == "content-length" || lowercase_name == "transfer-encoding") {
      throw std::invalid_argument("HTTP response framing is managed by Serialize");
    }
    result.append(name).append(": ").append(value).append("\r\n");
  }
  if (!bodyless_status && body_mode != ResponseBodyMode::kSuppressedWithUnknownLength) {
    const std::size_t length =
        body_mode == ResponseBodyMode::kSuppressedWithKnownLength ? *content_length
                                                                  : body.size();
    result.append("Content-Length: ").append(std::to_string(length)).append("\r\n");
  }
  result.append("\r\n");
  if (body_mode == ResponseBodyMode::kNormal) {
    result.append(body);
  }
  return result;
}

} // namespace aegisgate::http
