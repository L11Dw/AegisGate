#pragma once

#include <string>
#include <unordered_map>

namespace aegisgate::http {

// A complete in-memory HTTP/1.1 response. Serialize() owns framing: callers
// provide ordinary headers and a body, while Content-Length is emitted exactly
// once from body.size().
struct HttpResponse {
  int status = 200;
  std::string reason = "OK";
  std::unordered_map<std::string, std::string> headers;
  std::string body;

  [[nodiscard]] std::string Serialize() const;
};

} // namespace aegisgate::http
