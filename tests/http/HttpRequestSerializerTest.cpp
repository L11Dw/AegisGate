#include "aegisgate/http/HttpRequestSerializer.h"

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

namespace aegisgate::http {
namespace {

TEST(HttpRequestSerializerTest, SerializesGetWithManagedFraming) {
  const HttpRequest request{"GET", "/health", "HTTP/1.1", "",
                            {{"Host", "upstream.test"}}};

  EXPECT_EQ(HttpRequestSerializer::Serialize(request),
            "GET /health HTTP/1.1\r\n"
            "Host: upstream.test\r\n"
            "Content-Length: 0\r\n"
            "Connection: keep-alive\r\n\r\n");
}

TEST(HttpRequestSerializerTest, SerializesPostWithBody) {
  const HttpRequest request{"POST", "/submit", "HTTP/1.1", "hello",
                            {{"Content-Type", "text/plain"}}};

  EXPECT_EQ(HttpRequestSerializer::Serialize(request),
            "POST /submit HTTP/1.1\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: 5\r\n"
            "Connection: keep-alive\r\n\r\n"
            "hello");
}

TEST(HttpRequestSerializerTest, RejectsCallerControlledFramingAndConnection) {
  for (const std::string header : {"Content-Length", "transfer-encoding", "CONNECTION"}) {
    const HttpRequest request{"GET", "/", "HTTP/1.1", "", {{header, "value"}}};
    EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(request)),
                 std::invalid_argument)
        << header;
  }
}

TEST(HttpRequestSerializerTest, RejectsInvalidHeaderNameOrValue) {
  for (const auto &header : {std::pair{"", "value"}, std::pair{"bad name", "value"},
                             std::pair{"X-Test", "bad\r\nvalue"}}) {
    const HttpRequest request{"GET", "/", "HTTP/1.1", "", {header}};
    EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(request)),
                 std::invalid_argument);
  }
}

TEST(HttpRequestSerializerTest, RejectsLineBreakInjectionInRequestLine) {
  for (const auto &[method, target, version] :
       {std::tuple{"GE\r\nT", "/", "HTTP/1.1"},
        std::tuple{"GET", "/\r\nInjected: yes", "HTTP/1.1"},
        std::tuple{"GET", "/", "HTTP/1.1\r\nInjected: yes"}}) {
    const HttpRequest request{method, target, version, "", {}};
    EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(request)),
                 std::invalid_argument);
  }
}

TEST(HttpRequestSerializerTest, RequiresTokenMethodAndOriginFormTarget) {
  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(
                   HttpRequest{"GET POST", "/", "HTTP/1.1", "", {}})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(
                   HttpRequest{"GET", "", "HTTP/1.1", "", {}})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(
                   HttpRequest{"GET", "https://upstream.test/", "HTTP/1.1", "", {}})),
               std::invalid_argument);
}

TEST(HttpRequestSerializerTest, RejectsBodiesLargerThanOneMiB) {
  const HttpRequest request{"POST", "/upload", "HTTP/1.1",
                            std::string(1024 * 1024 + 1, 'x'), {}};

  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(request)),
               std::invalid_argument);
}

} // namespace
} // namespace aegisgate::http
