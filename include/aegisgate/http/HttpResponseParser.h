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
  // True once a syntactically valid response header and framing declaration
  // has been received, even while its Content-Length body is incomplete.
  [[nodiscard]] bool HeadersComplete() const noexcept;
  // response_to_head marks the request method as HEAD: the response completes
  // at the end of the headers, only the header area is consumed, and the
  // declared entity length is carried without a body.
  void Reset(bool response_to_head = false);

private:
  ParseResult result_ = ParseResult::kNeedMoreData;
  bool headers_complete_ = false;
  bool response_to_head_ = false;
  HttpResponse response_;
};

} // namespace aegisgate::http
