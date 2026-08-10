#pragma once

#include "aegisgate/http/HttpRequestParser.h"

#include <string>

namespace aegisgate::http {

// Serializes one bounded HTTP/1.1 origin-form request.  It owns request
// framing, so callers cannot supply Content-Length, Transfer-Encoding, or
// Connection headers.
[[nodiscard]] std::string SerializeRequest(const HttpRequest &request);

// Compatibility façade for the internal name used before SerializeRequest was
// published.  New callers should use the free function above.
class HttpRequestSerializer {
public:
  [[nodiscard]] static std::string Serialize(const HttpRequest &request) {
    return SerializeRequest(request);
  }
};

} // namespace aegisgate::http
