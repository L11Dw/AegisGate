#pragma once

#include "aegisgate/net/Buffer.h"

#include <string>
#include <string_view>
#include <unordered_map>

namespace aegisgate::http {

// kNeedMoreData is the only non-terminal result.  Terminal results remain
// stable until Reset() so callers cannot accidentally consume a request twice.
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
  // Parses at most one request and leaves any following bytes in input for the
  // next parser instance/reset cycle.  This handles TCP coalescing without
  // claiming HTTP pipelining support at the connection layer.
  [[nodiscard]] ParseResult Parse(net::Buffer &input);
  [[nodiscard]] const HttpRequest &Request() const noexcept;
  void Reset();

private:
  // The parser intentionally retains only a terminal result and reparses an
  // incomplete Buffer on the next call; it never consumes partial input.
  ParseResult result_ = ParseResult::kNeedMoreData;
  HttpRequest request_;
};

} // namespace aegisgate::http
