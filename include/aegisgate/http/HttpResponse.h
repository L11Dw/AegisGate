#pragma once

#include <string>
#include <utility>
#include <vector>

namespace aegisgate::http {

// A complete in-memory HTTP/1.1 response. Serialize() owns framing: callers
// provide ordinary headers and a body, while Content-Length is emitted exactly
// once from body.size().
struct HttpResponse {
  int status = 200;
  std::string reason = "OK";
  // HTTP permits repeated fields; preserving caller order makes serialized
  // output deterministic and leaves that extension possible without an API
  // change.
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;

  [[nodiscard]] std::string Serialize() const;
};

} // namespace aegisgate::http
