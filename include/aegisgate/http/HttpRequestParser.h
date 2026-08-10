#pragma once

#include "aegisgate/net/Buffer.h"

#include <string>
#include <string_view>
#include <unordered_map>

namespace aegisgate::http {

enum class ParseResult { kNeedMoreData, kComplete, kError, kUnsupported };

struct HttpRequest {
  std::string method;
  std::string target;
  std::string version;
  std::string body;
  std::unordered_map<std::string, std::string> headers;

  [[nodiscard]] std::string_view Header(std::string_view name) const;
};

class HttpRequestParser {
public:
  [[nodiscard]] ParseResult Parse(net::Buffer &input);
  [[nodiscard]] const HttpRequest &Request() const noexcept;
  void Reset();

private:
  ParseResult result_ = ParseResult::kNeedMoreData;
  HttpRequest request_;
};

} // namespace aegisgate::http
