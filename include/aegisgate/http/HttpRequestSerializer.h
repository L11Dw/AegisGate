#pragma once

#include "aegisgate/http/HttpRequestParser.h"

#include <string>

namespace aegisgate::http {

class HttpRequestSerializer {
public:
  // Serializes one bounded HTTP/1.1 origin-form request.  It owns request
  // framing, so callers cannot supply Content-Length, Transfer-Encoding, or
  // Connection headers.
  [[nodiscard]] static std::string Serialize(const HttpRequest &request);
};

} // namespace aegisgate::http
