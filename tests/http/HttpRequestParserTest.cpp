#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/net/Buffer.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

using aegisgate::http::HttpRequestParser;
using aegisgate::http::ParseResult;
using aegisgate::net::Buffer;

TEST(HttpRequestParserTest, ParsesGetWithoutBodyAndNormalizesHeaders) {
  Buffer input;
  input.Append(
      "GET /health HTTP/1.1\r\nHost: example.test\r\nX-Mode:  fast \t\r\n\r\n");
  HttpRequestParser parser;

  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  const auto &request = parser.Request();
  EXPECT_EQ(request.method, "GET");
  EXPECT_EQ(request.target, "/health");
  EXPECT_EQ(request.version, "HTTP/1.1");
  EXPECT_EQ(request.Header("host"), "example.test");
  EXPECT_EQ(request.Header("X-MODE"), "fast");
  EXPECT_TRUE(request.body.empty());
  EXPECT_TRUE(input.ReadableView().empty());
}

TEST(HttpRequestParserTest, ParsesContentLengthBodyAcrossInputSegments) {
  Buffer input;
  HttpRequestParser parser;
  input.Append("POST /submit HTTP/1.1\r\nContent-Length: 5\r\n\r\nhe");

  EXPECT_EQ(parser.Parse(input), ParseResult::kNeedMoreData);
  EXPECT_EQ(input.ReadableView(),
            "POST /submit HTTP/1.1\r\nContent-Length: 5\r\n\r\nhe");
  input.Append("llo");

  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  EXPECT_EQ(parser.Request().body, "hello");
  EXPECT_TRUE(input.ReadableView().empty());
}

TEST(HttpRequestParserTest, LeavesPipelinedRequestForReset) {
  Buffer input;
  input.Append("GET /one HTTP/1.1\r\n\r\nGET /two HTTP/1.1\r\n\r\n");
  HttpRequestParser parser;

  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  EXPECT_EQ(parser.Request().target, "/one");
  EXPECT_EQ(input.ReadableView(), "GET /two HTTP/1.1\r\n\r\n");

  parser.Reset();
  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  EXPECT_EQ(parser.Request().target, "/two");
  EXPECT_TRUE(input.ReadableView().empty());
}

TEST(HttpRequestParserTest, KeepsInputUntilFinalBodyByteArrives) {
  Buffer input;
  input.Append("POST /x HTTP/1.1\r\nContent-Length: 3\r\n\r\nab");
  HttpRequestParser parser;

  EXPECT_EQ(parser.Parse(input), ParseResult::kNeedMoreData);
  input.Append("\n");
  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  EXPECT_EQ(parser.Request().body, "ab\n");
}

TEST(HttpRequestParserTest, RejectsMalformedAndInvalidContentLengthRequests) {
  const std::vector<std::string> invalid_requests = {
      "GET / HTTP/1.0\r\n\r\n",
      "GET / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\na",
      "GET / HTTP/1.1\r\nContent-Length: one\r\n\r\n",
      "GET / HTTP/1.1\r\nContent-Length: 999999999999999999999999\r\n\r\n",
      "GET / HTTP/1.1\r\nBrokenHeader\r\n\r\n",
      "GET / HTTP/1.1\r\nX-Value: ok\rInjected: yes\r\n\r\n",
  };

  for (const auto &request : invalid_requests) {
    Buffer input;
    input.Append(request);
    HttpRequestParser parser;
    EXPECT_EQ(parser.Parse(input), ParseResult::kError) << request;
    EXPECT_EQ(parser.Parse(input), ParseResult::kError) << request;
  }
}

TEST(HttpRequestParserTest, RejectsChunkedTransferEncodingAsUnsupported) {
  Buffer input;
  input.Append("POST / HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n");
  HttpRequestParser parser;

  EXPECT_EQ(parser.Parse(input), ParseResult::kUnsupported);
  EXPECT_EQ(parser.Parse(input), ParseResult::kUnsupported);
}

TEST(HttpRequestParserTest, EnforcesLineHeaderAndBodyLimits) {
  Buffer line_input;
  line_input.Append("GET /" + std::string(8192, 'a') + " HTTP/1.1\r\n\r\n");
  HttpRequestParser line_parser;
  EXPECT_EQ(line_parser.Parse(line_input), ParseResult::kError);

  Buffer body_input;
  body_input.Append("POST / HTTP/1.1\r\nContent-Length: 1048577\r\n\r\n");
  HttpRequestParser body_parser;
  EXPECT_EQ(body_parser.Parse(body_input), ParseResult::kError);
}

} // namespace
