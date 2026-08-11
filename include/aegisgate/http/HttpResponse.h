#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace aegisgate::http {

// How the response body relates to the wire framing.  Normal responses emit
// Content-Length from body.size() and send the body; a suppressed body (a HEAD
// response) declares the entity length a GET would return but never sends the
// body, and must not silently claim the entity length is zero when the
// upstream declared a real one.
enum class ResponseBodyMode {
  kNormal,
  kSuppressedWithKnownLength,    // Content-Length: <content_length>, no body
  kSuppressedWithUnknownLength,  // no Content-Length, no body
};

// A complete in-memory HTTP/1.1 response. Serialize() owns framing: callers
// provide ordinary headers and a body, while Content-Length is emitted exactly
// once from body.size() unless body_mode suppresses the body.
struct HttpResponse {
  HttpResponse(int status_ = 200, std::string reason_ = "OK",
               std::vector<std::pair<std::string, std::string>> headers_ = {},
               std::string body_ = "",
               ResponseBodyMode body_mode_ = ResponseBodyMode::kNormal,
               std::optional<std::size_t> content_length_ = std::nullopt)
      : status(status_), reason(std::move(reason_)), headers(std::move(headers_)),
        body(std::move(body_)), body_mode(body_mode_), content_length(content_length_) {}

  int status = 200;
  std::string reason = "OK";
  // HTTP permits repeated fields; preserving caller order makes serialized
  // output deterministic and leaves that extension possible without an API
  // change.
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;
  ResponseBodyMode body_mode = ResponseBodyMode::kNormal;
  // The entity length declared on the wire; set only when body_mode is
  // kSuppressedWithKnownLength.
  std::optional<std::size_t> content_length;

  [[nodiscard]] std::string Serialize() const;
};

// A validated response header with its framing but no body bytes.  Streaming
// forwarding serializes this head before any body chunk arrives; the declared
// entity length is the framing, so kNormal heads must carry one and must never
// derive "Content-Length: 0" from an absent body.
struct HttpResponseHead {
  HttpResponseHead(int status_ = 200, std::string reason_ = "OK",
                   std::vector<std::pair<std::string, std::string>> headers_ = {},
                   ResponseBodyMode body_mode_ = ResponseBodyMode::kNormal,
                   std::optional<std::size_t> content_length_ = std::nullopt)
      : status(status_), reason(std::move(reason_)), headers(std::move(headers_)),
        body_mode(body_mode_), content_length(content_length_) {}

  int status = 200;
  std::string reason = "OK";
  std::vector<std::pair<std::string, std::string>> headers;
  ResponseBodyMode body_mode = ResponseBodyMode::kNormal;
  // The entity length declared on the wire.  Required for kNormal and
  // kSuppressedWithKnownLength; absent for bodyless and unknown-length heads.
  std::optional<std::size_t> content_length;

  [[nodiscard]] std::string Serialize() const;
};

} // namespace aegisgate::http
