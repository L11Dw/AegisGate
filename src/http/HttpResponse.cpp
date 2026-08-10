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
  if (status < 100 || status > 999 || HasLineBreak(reason)) {
    throw std::invalid_argument("invalid HTTP response status line");
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
  result.append("Content-Length: ").append(std::to_string(body.size())).append("\r\n\r\n");
  result.append(body);
  return result;
}

} // namespace aegisgate::http
