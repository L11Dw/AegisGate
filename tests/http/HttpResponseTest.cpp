#include "aegisgate/http/HttpResponse.h"

#include <gtest/gtest.h>

#include <stdexcept>

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

TEST(HttpResponseTest, SerializesEmptyBody) {
  HttpResponse response{204, "No Content", {}, ""};

  EXPECT_EQ(response.Serialize(),
            "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n");
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
}

} // namespace
} // namespace aegisgate::http
