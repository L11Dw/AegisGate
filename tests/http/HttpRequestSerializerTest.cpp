#include "aegisgate/http/HttpRequestSerializer.h"

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

namespace aegisgate::http {
namespace {

constexpr std::size_t kMaxRequestLineBytes = 8 * 1024;
constexpr std::size_t kMaxHeaderBytes = 32 * 1024;

TEST(HttpRequestSerializerTest, SerializesGetWithManagedFraming) {
  const HttpRequest request{"GET", "/health", "HTTP/1.1", "",
                            {{"Host", "upstream.test"}}};

  EXPECT_EQ(SerializeRequest(request),
            "GET /health HTTP/1.1\r\n"
            "Host: upstream.test\r\n"
            "Content-Length: 0\r\n"
            "Connection: keep-alive\r\n\r\n");
}

TEST(HttpRequestSerializerTest, SerializesPostWithBody) {
  const HttpRequest request{"POST", "/submit", "HTTP/1.1", "hello",
                            {{"Host", "upstream.test"}, {"Content-Type", "text/plain"}}};

  const std::string serialized = HttpRequestSerializer::Serialize(request);
  EXPECT_TRUE(serialized.starts_with("POST /submit HTTP/1.1\r\n"));
  EXPECT_NE(serialized.find("Host: upstream.test\r\n"), std::string::npos);
  EXPECT_NE(serialized.find("Content-Type: text/plain\r\n"), std::string::npos);
  EXPECT_TRUE(serialized.ends_with("Content-Length: 5\r\n"
                                   "Connection: keep-alive\r\n\r\n"
                                   "hello"));
}

TEST(HttpRequestSerializerTest, RejectsCallerControlledFramingAndConnection) {
  for (const std::string header : {"Content-Length", "transfer-encoding", "CONNECTION"}) {
    const HttpRequest request{"GET", "/", "HTTP/1.1", "",
                              {{"Host", "upstream.test"}, {header, "value"}}};
    EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(request)),
                 std::invalid_argument)
        << header;
  }
}

TEST(HttpRequestSerializerTest, RejectsInvalidHeaderNameOrValue) {
  const std::vector<std::pair<std::string, std::string>> headers = {
      {"", "value"}, {"bad name", "value"}, {"X-Test", "bad\r\nvalue"},
      {"X-Test", std::string("bad\x01value")},
      {"X-Test", std::string("bad\x7fvalue")}};
  for (const auto &header : headers) {
    const HttpRequest request{"GET", "/", "HTTP/1.1", "",
                              {{"Host", "upstream.test"}, header}};
    EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(request)),
                 std::invalid_argument);
  }
}

TEST(HttpRequestSerializerTest, PreservesAllowedHorizontalTabInHeaderValue) {
  const HttpRequest request{"GET", "/", "HTTP/1.1", "",
                            {{"Host", "upstream.test"}, {"X-Note", "keep\tthis"}}};

  EXPECT_NE(HttpRequestSerializer::Serialize(request).find("X-Note: keep\tthis\r\n"),
            std::string::npos);
}

TEST(HttpRequestSerializerTest, RejectsLineBreakInjectionInRequestLine) {
  for (const auto &[method, target, version] :
       {std::tuple{"GE\r\nT", "/", "HTTP/1.1"},
        std::tuple{"GET", "/\r\nInjected: yes", "HTTP/1.1"},
        std::tuple{"GET", "/", "HTTP/1.1\r\nInjected: yes"}}) {
    const HttpRequest request{method, target, version, "", {{"Host", "upstream.test"}}};
    EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(request)),
                 std::invalid_argument);
  }
}

TEST(HttpRequestSerializerTest, RequiresTokenMethodAndOriginFormTarget) {
  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(
                   HttpRequest{"GET POST", "/", "HTTP/1.1", "",
                               {{"Host", "upstream.test"}}})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(
                   HttpRequest{"GET", "", "HTTP/1.1", "",
                               {{"Host", "upstream.test"}}})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(
                   HttpRequest{"GET", "https://upstream.test/", "HTTP/1.1", "",
                               {{"Host", "upstream.test"}}})),
               std::invalid_argument);
}

TEST(HttpRequestSerializerTest, AcceptsOriginFormPathAndQuery) {
  const HttpRequest request{"GET", "/a-._~!$&'()*+,;=:@/%2F?x=/a?b&y=%20",
                            "HTTP/1.1", "", {{"Host", "upstream.test"}}};

  const std::string serialized = HttpRequestSerializer::Serialize(request);
  EXPECT_TRUE(serialized.starts_with(
      "GET /a-._~!$&'()*+,;=:@/%2F?x=/a?b&y=%20 HTTP/1.1\r\n"));
  EXPECT_TRUE(serialized.ends_with("Content-Length: 0\r\n"
                                   "Connection: keep-alive\r\n\r\n"));
}

TEST(HttpRequestSerializerTest, RejectsInvalidOriginFormCharacters) {
  for (const std::string target : {"/a#fragment", "/a<bad>", "/[bad]",
                                   "/a%", "/a%2", "/a%ZZ", "/a b", "/a\x01",
                                   "/?query={bad}"}) {
    const HttpRequest request{"GET", target, "HTTP/1.1", "",
                              {{"Host", "upstream.test"}}};
    EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(request)),
                 std::invalid_argument)
        << target;
  }
}

TEST(HttpRequestSerializerTest, RejectsBodiesLargerThanOneMiB) {
  const HttpRequest request{"POST", "/upload", "HTTP/1.1",
                            std::string(1024 * 1024 + 1, 'x'),
                            {{"Host", "upstream.test"}}};

  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(request)),
               std::invalid_argument);
}

TEST(HttpRequestSerializerTest, SerializesBodyAtOneMiB) {
  const HttpRequest request{"POST", "/upload", "HTTP/1.1",
                            std::string(1024 * 1024, 'x'), {{"Host", "upstream.test"}}};

  const std::string serialized = HttpRequestSerializer::Serialize(request);
  EXPECT_TRUE(serialized.starts_with("POST /upload HTTP/1.1\r\n"));
  EXPECT_NE(serialized.find("Content-Length: 1048576\r\n"), std::string::npos);
  EXPECT_TRUE(serialized.ends_with(request.body));
}

TEST(HttpRequestSerializerTest, RequiresExactlyOneHostHeader) {
  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(
                   HttpRequest{"GET", "/", "HTTP/1.1", "", {}})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(
                   HttpRequest{"GET", "/", "HTTP/1.1", "",
                               {{"Host", "one.test"}, {"host", "two.test"}}})),
               std::invalid_argument);
}

TEST(HttpRequestSerializerTest, RejectsNonAsciiMethodAndHeaderName) {
  const std::string non_ascii(1, static_cast<char>(0xff));
  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(
                   HttpRequest{"GET" + non_ascii, "/", "HTTP/1.1", "",
                               {{"Host", "upstream.test"}}})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(
                   HttpRequest{"GET", "/", "HTTP/1.1", "",
                               {{"Host", "upstream.test"}, {"X-" + non_ascii, "x"}}})),
               std::invalid_argument);
}

TEST(HttpRequestSerializerTest, AcceptsRequestLineAndHeadersAtLimits) {
  const HttpRequest request_line_limit{
      "GET", "/" + std::string(kMaxRequestLineBytes - 14, 'a'), "HTTP/1.1", "",
      {{"Host", "upstream.test"}}};
  const HttpRequest header_limit{
      "GET", "/", "HTTP/1.1", "",
      {{"Host", std::string(kMaxHeaderBytes - 53, 'a')}}};

  EXPECT_NO_THROW(static_cast<void>(HttpRequestSerializer::Serialize(request_line_limit)));
  EXPECT_NO_THROW(static_cast<void>(HttpRequestSerializer::Serialize(header_limit)));
}

TEST(HttpRequestSerializerTest, RejectsRequestLineOverEightKiB) {
  const HttpRequest request{"GET", "/" + std::string(kMaxRequestLineBytes - 13, 'a'),
                            "HTTP/1.1", "", {{"Host", "upstream.test"}}};

  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(request)),
               std::invalid_argument);
}

TEST(HttpRequestSerializerTest, RejectsHeadersOverThirtyTwoKiB) {
  const HttpRequest request{"GET", "/", "HTTP/1.1", "",
                            {{"Host", std::string(kMaxHeaderBytes - 52, 'a')}}};

  EXPECT_THROW(static_cast<void>(HttpRequestSerializer::Serialize(request)),
               std::invalid_argument);
}

TEST(HttpRequestSerializerTest, RejectsOverLimitLegalTargetBeforeSerializingIt) {
  const HttpRequest request{"GET", "/" + std::string(kMaxRequestLineBytes - 13, 'a'),
                            "HTTP/1.1", "", {{"Host", "upstream.test"}}};

  EXPECT_THROW(static_cast<void>(SerializeRequest(request)), std::invalid_argument);
}

TEST(HttpRequestSerializerTest, RejectsOverLimitLegalHeaderValueBeforeSerializingIt) {
  const HttpRequest request{"GET", "/", "HTTP/1.1", "",
                            {{"Host", std::string(kMaxHeaderBytes - 52, 'a')}}};

  EXPECT_THROW(static_cast<void>(SerializeRequest(request)), std::invalid_argument);
}

} // namespace
} // namespace aegisgate::http
