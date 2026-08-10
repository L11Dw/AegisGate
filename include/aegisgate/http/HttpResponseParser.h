#pragma once

#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/http/HttpResponse.h"
#include "aegisgate/net/Buffer.h"

namespace aegisgate::http {

class HttpResponseParser {
public:
  // Parses exactly one complete response. Incomplete input remains untouched,
  // and terminal results remain stable until Reset().
  [[nodiscard]] ParseResult Parse(net::Buffer &input);
  [[nodiscard]] const HttpResponse &Response() const noexcept;
  void Reset();

private:
  ParseResult result_ = ParseResult::kNeedMoreData;
  HttpResponse response_;
};

} // namespace aegisgate::http
