#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/http/HttpResponse.h"
#include "aegisgate/net/Buffer.h"

namespace aegisgate::http {

class HttpResponseParser {
public:
  // Consumes one contiguous body segment.  Must take ownership of the view
  // before returning; returning false keeps the chunk in the input buffer so
  // the caller can pause upstream reading.
  using BodySink = std::function<bool(std::string_view)>;

  // Legacy path: parses exactly one complete response into Response().
  // Incomplete input remains untouched, and terminal results remain stable
  // until Reset().
  [[nodiscard]] ParseResult Parse(net::Buffer &input);
  // Streaming path: parses and validates only the response header.  On
  // success the header area is consumed and Head() is valid; body bytes are
  // delivered incrementally via ConsumeBody().  A HEAD or bodyless response
  // completes here (BodyComplete() == true).  Returns kComplete when the
  // header phase is done; the internal result stays kNeedMoreData until the
  // whole response terminates.
  [[nodiscard]] ParseResult ParseHeaders(net::Buffer &input);
  // Delivers up to the declared Content-Length in contiguous segments.  A
  // declined segment stays in the input; kComplete is returned once the
  // declared body is fully consumed.  Must only be called after a successful
  // ParseHeaders and while BodyComplete() is false.
  [[nodiscard]] ParseResult ConsumeBody(net::Buffer &input, const BodySink &sink);
  // Terminal legacy result; stable until Reset().
  [[nodiscard]] const HttpResponse &Response() const noexcept;
  // Valid after a successful ParseHeaders (streaming) or Parse (legacy).
  [[nodiscard]] const HttpResponseHead &Head() const noexcept;
  // True once a syntactically valid response header and framing declaration
  // has been received, even while its Content-Length body is incomplete.
  [[nodiscard]] bool HeadersComplete() const noexcept;
  // True when no body bytes remain: HEAD/bodyless responses complete at the
  // headers; framed responses complete when the declared length is consumed.
  [[nodiscard]] bool BodyComplete() const noexcept;
  // response_to_head marks the request method as HEAD: the response completes
  // at the end of the headers, only the header area is consumed, and the
  // declared entity length is carried without a body.
  void Reset(bool response_to_head = false);

private:
  struct ParsedHeader {
    int status = 200;
    std::string reason;
    std::vector<std::pair<std::string, std::string>> headers;
    bool has_content_length = false;
    std::size_t content_length = 0;
    bool bodyless = false;
    // Input offset just past the terminating blank line.
    std::size_t header_end = 0;
  };
  // Shared, read-only header phase used by both Parse() and ParseHeaders().
  // Never mutates the input; sets headers_complete_ only on success.  The
  // caller is responsible for recording the result and consuming input.
  [[nodiscard]] ParseResult ParseHeaderArea(net::Buffer &input, ParsedHeader &out);

  ParseResult result_ = ParseResult::kNeedMoreData;
  bool headers_complete_ = false;
  bool response_to_head_ = false;
  HttpResponse response_;
  HttpResponseHead head_;
  std::size_t remaining_body_ = 0;
  bool body_complete_ = false;
};

} // namespace aegisgate::http
