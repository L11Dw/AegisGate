#include "aegisgate/http/HttpResponse.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <utility>

namespace aegisgate::http {
namespace {

TEST(HttpResponseTest, SerializesContentLengthResponse) {
  HttpResponse response{200, "OK", {{"content-type", "text/plain"}}, "hello"};

  EXPECT_EQ(response.Serialize(),
            "HTTP/1.1 200 OK\r\n"
            "content-type: text/plain\r\n"
            "Content-Length: 5\r\n\r\n"
            "hello");
}

TEST(HttpResponseTest, OmitsFramingForBodylessStatus) {
  HttpResponse response{204, "No Content", {}, ""};
  HttpResponse not_modified{304, "Not Modified", {}, ""};

  EXPECT_EQ(response.Serialize(), "HTTP/1.1 204 No Content\r\n\r\n");
  EXPECT_EQ(not_modified.Serialize(), "HTTP/1.1 304 Not Modified\r\n\r\n");
}

TEST(HttpResponseTest, PreservesProvidedHeaderOrder) {
  HttpResponse response{200,
                        "OK",
                        {{"x-first", "1"}, {"x-second", "2"}},
                        ""};

  EXPECT_EQ(response.Serialize(),
            "HTTP/1.1 200 OK\r\n"
            "x-first: 1\r\n"
            "x-second: 2\r\n"
            "Content-Length: 0\r\n\r\n");
}

TEST(HttpResponseTest, RejectsConflictingFramingHeaders) {
  EXPECT_THROW(
      static_cast<void>(
          HttpResponse{200, "OK", {{"Content-Length", "1"}}, "x"}.Serialize()),
      std::invalid_argument);
  EXPECT_THROW(static_cast<void>(
                   HttpResponse{200, "OK", {{"transfer-encoding", "chunked"}}, ""}
                       .Serialize()),
               std::invalid_argument);
}

TEST(HttpResponseTest, RejectsInvalidStatusOrHeaderInjection) {
  EXPECT_THROW(static_cast<void>(HttpResponse{99, "OK", {}, ""}.Serialize()),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(HttpResponse{200, "bad\r\nreason", {}, ""}.Serialize()),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(
                   HttpResponse{200, "OK", {{"x-test", "yes\nno"}}, ""}.Serialize()),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(HttpResponse{600, "invalid", {}, ""}.Serialize()),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(HttpResponse{101, "Switching Protocols", {}, ""}.Serialize()),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(HttpResponse{204, "No Content", {}, "x"}.Serialize()),
               std::invalid_argument);
}

TEST(HttpResponseTest, SerializesSuppressedBodyWithKnownLength) {
  HttpResponse response{200, "OK", {}, ""};
  response.body_mode = ResponseBodyMode::kSuppressedWithKnownLength;
  response.content_length = 5;

  EXPECT_EQ(response.Serialize(), "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n");
}

TEST(HttpResponseTest, OmitsContentLengthForUnknownLengthHead) {
  HttpResponse response{200, "OK", {}, ""};
  response.body_mode = ResponseBodyMode::kSuppressedWithUnknownLength;

  EXPECT_EQ(response.Serialize(), "HTTP/1.1 200 OK\r\n\r\n");
}

TEST(HttpResponseTest, RejectsInvalidBodyModeCombinations) {
  const auto build = [](ResponseBodyMode mode, std::optional<std::size_t> length,
                        std::string body) {
    HttpResponse response{200, "OK", {}, std::move(body)};
    response.body_mode = mode;
    response.content_length = length;
    return response;
  };
  EXPECT_THROW(static_cast<void>(
                   build(ResponseBodyMode::kSuppressedWithKnownLength, std::nullopt, "").Serialize()),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(
                   build(ResponseBodyMode::kSuppressedWithUnknownLength, std::size_t{5}, "").Serialize()),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(
                   build(ResponseBodyMode::kNormal, std::size_t{5}, "").Serialize()),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(
                   build(ResponseBodyMode::kSuppressedWithKnownLength, std::size_t{5}, "x").Serialize()),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(
                   build(ResponseBodyMode::kSuppressedWithUnknownLength, std::nullopt, "x").Serialize()),
               std::invalid_argument);
  // Bodyless statuses must not silently swallow framing metadata.
  HttpResponse no_content{204, "No Content", {}, ""};
  no_content.body_mode = ResponseBodyMode::kSuppressedWithKnownLength;
  no_content.content_length = 5;
  EXPECT_THROW(static_cast<void>(no_content.Serialize()), std::invalid_argument);
  HttpResponse not_modified{304, "Not Modified", {}, ""};
  not_modified.content_length = 5;
  EXPECT_THROW(static_cast<void>(not_modified.Serialize()), std::invalid_argument);
}

} // namespace
} // namespace aegisgate::http
