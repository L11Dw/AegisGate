#include "aegisgate/http/HttpResponseParser.h"
#include "aegisgate/http/HttpLimits.h"
#include "aegisgate/net/Buffer.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

using aegisgate::http::HttpResponseParser;
using aegisgate::http::ParseResult;
using aegisgate::http::ResponseBodyMode;
using aegisgate::http::HttpResponseHead;
using aegisgate::net::Buffer;

TEST(HttpResponseParserTest, ParsesSegmentedBodyAndLeavesPipelinedResponse) {
  Buffer input;
  HttpResponseParser parser;
  input.Append("HTTP/1.1 200 OK\r\nX-First:  one \t\r\nX-Second:\t2\r\nContent-Length: 5\r\n\r\nhe");

  EXPECT_EQ(parser.Parse(input), ParseResult::kNeedMoreData);
  EXPECT_EQ(input.ReadableView(),
            "HTTP/1.1 200 OK\r\nX-First:  one \t\r\nX-Second:\t2\r\nContent-Length: 5\r\n\r\nhe");
  input.Append("lloHTTP/1.1 204 No Content\r\n\r\n");

  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  const auto &response = parser.Response();
  EXPECT_EQ(response.status, 200);
  EXPECT_EQ(response.reason, "OK");
  EXPECT_EQ(response.headers, (std::vector<std::pair<std::string, std::string>>{
                                  {"X-First", "one"}, {"X-Second", "2"},
                                  {"Content-Length", "5"}}));
  EXPECT_EQ(response.body, "hello");
  EXPECT_EQ(input.ReadableView(), "HTTP/1.1 204 No Content\r\n\r\n");
}

TEST(HttpResponseParserTest, AcceptsBodylessStatusesWithEmptyOrZeroContentLength) {
  for (const std::string response : {"HTTP/1.1 204 No Content\r\n\r\n",
                                     "HTTP/1.1 304 Not Modified\r\nContent-Length: 0\r\n\r\n"}) {
    Buffer input;
    input.Append(response);
    HttpResponseParser parser;
    EXPECT_EQ(parser.Parse(input), ParseResult::kComplete) << response;
    EXPECT_TRUE(parser.Response().body.empty());
  }
}

TEST(HttpResponseParserTest, RejectsInvalidBodylessFraming) {
  for (const std::string response : {"HTTP/1.1 204 No Content\r\nContent-Length: 1\r\n\r\n",
                                     "HTTP/1.1 304 Not Modified\r\nContent-Length: 2\r\n\r\n"}) {
    Buffer input;
    input.Append(response);
    HttpResponseParser parser;
    EXPECT_EQ(parser.Parse(input), ParseResult::kError) << response;
  }
}

TEST(HttpResponseParserTest, RejectsMissingDuplicateInvalidAndOverlimitContentLength) {
  const std::vector<std::string> invalid = {
      "HTTP/1.1 200 OK\r\n\r\n", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
      "HTTP/1.1 200 OK\r\nContent-Length: nope\r\n\r\n",
      "HTTP/1.1 200 OK\r\nContent-Length: 999999999999999999999999\r\n\r\n",
      "HTTP/1.1 200 OK\r\nContent-Length: " +
          std::to_string(aegisgate::http::kMaxUpstreamResponseBodyBytes + 1) + "\r\n\r\n"};
  for (const auto &response : invalid) {
    Buffer input;
    input.Append(response);
    HttpResponseParser parser;
    EXPECT_EQ(parser.Parse(input), ParseResult::kError) << response;
  }
}

TEST(HttpResponseParserTest, AcceptsSixteenMiBContentLengthForStreaming) {
  Buffer input;
  input.Append("HTTP/1.1 200 OK\r\nContent-Length: " +
               std::to_string(aegisgate::http::kMaxUpstreamResponseBodyBytes) + "\r\n\r\n");
  HttpResponseParser parser;

  EXPECT_EQ(parser.ParseHeaders(input), ParseResult::kComplete);
  ASSERT_TRUE(parser.Head().content_length.has_value());
  EXPECT_EQ(*parser.Head().content_length, aegisgate::http::kMaxUpstreamResponseBodyBytes);
  EXPECT_FALSE(parser.BodyComplete());
}

TEST(HttpResponseParserTest, RejectsEveryTransferEncodingAsUnsupported) {
  for (const std::string value : {"gzip", "chunked"}) {
    Buffer input;
    input.Append("HTTP/1.1 200 OK\r\nTransfer-Encoding: " + value + "\r\nContent-Length: 0\r\n\r\n");
    HttpResponseParser parser;
    EXPECT_EQ(parser.Parse(input), ParseResult::kUnsupported) << value;
    EXPECT_EQ(parser.Parse(input), ParseResult::kUnsupported) << value;
  }
}

TEST(HttpResponseParserTest, RejectsMalformedLinesControlsAndInvalidStatus) {
  const std::vector<std::string> invalid = {
      "HTTP/1.1 200 OK\nContent-Length: 0\r\n\r\n",
      "HTTP/1.1 200\r\nContent-Length: 0\r\n\r\n",
      "HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n",
      "HTTP/1.1 199 Informational\r\nContent-Length: 0\r\n\r\n",
      "HTTP/1.1 600 Invalid\r\nContent-Length: 0\r\n\r\n",
      "HTTP/1.1 200 OK\r\nBad Header: x\r\nContent-Length: 0\r\n\r\n",
      "HTTP/1.1 200 OK\r\nX-Test: bad\x01\r\nContent-Length: 0\r\n\r\n"};
  for (const auto &response : invalid) {
    Buffer input;
    input.Append(response);
    HttpResponseParser parser;
    EXPECT_EQ(parser.Parse(input), ParseResult::kError) << response;
  }
}

TEST(HttpResponseParserTest, EnforcesLineHeaderAndBodyLimits) {
  Buffer line;
  line.Append("HTTP/1.1 200 " + std::string(8192, 'a') + "\r\nContent-Length: 0\r\n\r\n");
  HttpResponseParser line_parser;
  EXPECT_EQ(line_parser.Parse(line), ParseResult::kError);

  Buffer headers;
  headers.Append("HTTP/1.1 200 OK\r\nX-Large: " + std::string(32768, 'a'));
  HttpResponseParser header_parser;
  EXPECT_EQ(header_parser.Parse(headers), ParseResult::kError);
}

TEST(HttpResponseParserTest, RejectsHeaderFieldLineOverEightKiB) {
  Buffer input;
  input.Append("HTTP/1.1 200 OK\r\nX-Large: " + std::string(9 * 1024, 'a') +
               "\r\nContent-Length: 0\r\n\r\n");
  HttpResponseParser parser;

  EXPECT_EQ(parser.Parse(input), ParseResult::kError);
}

TEST(HttpResponseParserTest, RejectsIncompleteHeaderFieldLineOverEightKiB) {
  Buffer input;
  input.Append("HTTP/1.1 200 OK\r\nX: " + std::string(8190, 'a'));
  HttpResponseParser parser;

  EXPECT_EQ(parser.Parse(input), ParseResult::kError);
}

TEST(HttpResponseParserTest, WaitsForCompleteHeaderFieldLineAtEightKiB) {
  Buffer input;
  input.Append("HTTP/1.1 200 OK\r\nX: " + std::string(8189, 'a'));
  HttpResponseParser parser;

  EXPECT_EQ(parser.Parse(input), ParseResult::kNeedMoreData);
  input.Append("\r\nContent-Length: 0\r\n\r\n");
  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  EXPECT_EQ(parser.Response().headers[0].first, "X");
  EXPECT_EQ(parser.Response().headers[0].second, std::string(8189, 'a'));
}

TEST(HttpResponseParserTest, ResetAllowsReuseAfterTerminalResult) {
  Buffer input;
  input.Append("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n");
  HttpResponseParser parser;
  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  parser.Reset();
  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  EXPECT_EQ(parser.Response().status, 204);
  EXPECT_TRUE(input.ReadableView().empty());
}

} // namespace

TEST(HttpResponseParserTest, CompletesHeadResponseWithKnownLength) {
  Buffer input;
  HttpResponseParser parser;
  parser.Reset(true);
  input.Append("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n");

  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  const auto &response = parser.Response();
  EXPECT_EQ(response.body_mode, aegisgate::http::ResponseBodyMode::kSuppressedWithKnownLength);
  ASSERT_TRUE(response.content_length.has_value());
  EXPECT_EQ(*response.content_length, 5U);
  EXPECT_TRUE(response.body.empty());
  EXPECT_TRUE(input.ReadableView().empty());
}

TEST(HttpResponseParserTest, CompletesHeadResponseWithZeroContentLength) {
  Buffer input;
  HttpResponseParser parser;
  parser.Reset(true);
  input.Append("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");

  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  const auto &response = parser.Response();
  EXPECT_EQ(response.body_mode, aegisgate::http::ResponseBodyMode::kSuppressedWithKnownLength);
  ASSERT_TRUE(response.content_length.has_value());
  EXPECT_EQ(*response.content_length, 0U);
}

TEST(HttpResponseParserTest, CompletesHeadResponseWithoutContentLength) {
  Buffer input;
  HttpResponseParser parser;
  parser.Reset(true);
  input.Append("HTTP/1.1 200 OK\r\n\r\n");

  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  const auto &response = parser.Response();
  EXPECT_EQ(response.body_mode, aegisgate::http::ResponseBodyMode::kSuppressedWithUnknownLength);
  EXPECT_FALSE(response.content_length.has_value());
  EXPECT_TRUE(response.body.empty());
}

TEST(HttpResponseParserTest, HeadConsumesOnlyHeaderLeavingResidualBody) {
  Buffer input;
  HttpResponseParser parser;
  parser.Reset(true);
  input.Append("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello");

  EXPECT_EQ(parser.Parse(input), ParseResult::kComplete);
  // The suppressed body must stay in the buffer so a dirty connection can
  // never be reused or lent out.
  EXPECT_EQ(input.ReadableView(), "hello");
  EXPECT_TRUE(parser.Response().body.empty());
}

// --- streaming (header/body split) path ---

TEST(HttpResponseParserTest, ParserEmitsHeaderBeforeBody) {
  Buffer input;
  input.Append("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello");
  HttpResponseParser parser;

  EXPECT_EQ(parser.ParseHeaders(input), ParseResult::kComplete);
  EXPECT_TRUE(parser.HeadersComplete());
  EXPECT_FALSE(parser.BodyComplete());
  const HttpResponseHead &head = parser.Head();
  EXPECT_EQ(head.status, 200);
  EXPECT_EQ(head.body_mode, ResponseBodyMode::kNormal);
  ASSERT_TRUE(head.content_length.has_value());
  EXPECT_EQ(*head.content_length, 5);
  // The header area is consumed; the body stays for ConsumeBody.
  EXPECT_EQ(input.ReadableView(), "hello");
}

TEST(HttpResponseParserTest, ConsumeBodyDeliversSegmentsIncrementally) {
  HttpResponseParser parser;
  {
    Buffer input;
    input.Append("HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\nhello wo");
    EXPECT_EQ(parser.ParseHeaders(input), ParseResult::kComplete);
    std::string delivered;
    auto sink = [&delivered](std::string_view bytes) {
      delivered.append(bytes.data(), bytes.size());
      return true;
    };
    EXPECT_EQ(parser.ConsumeBody(input, sink), ParseResult::kNeedMoreData);
    EXPECT_EQ(delivered, "hello wo");
    EXPECT_FALSE(parser.BodyComplete());
    input.Append("rld");
    EXPECT_EQ(parser.ConsumeBody(input, sink), ParseResult::kComplete);
    EXPECT_EQ(delivered, "hello world");
    EXPECT_TRUE(parser.BodyComplete());
    EXPECT_EQ(input.ReadableBytes(), 0);
  }
}

TEST(HttpResponseParserTest, DeclinedSinkKeepsChunkInInput) {
  Buffer input;
  input.Append("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello");
  HttpResponseParser parser;
  EXPECT_EQ(parser.ParseHeaders(input), ParseResult::kComplete);

  EXPECT_EQ(parser.ConsumeBody(input, [](std::string_view) { return false; }),
            ParseResult::kNeedMoreData);
  // The declined chunk remains available for a later attempt.
  EXPECT_EQ(input.ReadableView(), "hello");
  EXPECT_FALSE(parser.BodyComplete());
}

TEST(HttpResponseParserTest, HeadResponseCompletesWithoutBodySink) {
  Buffer input;
  input.Append("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nunread");
  HttpResponseParser parser;
  parser.Reset(/*response_to_head=*/true);

  EXPECT_EQ(parser.ParseHeaders(input), ParseResult::kComplete);
  EXPECT_TRUE(parser.BodyComplete());
  const HttpResponseHead &head = parser.Head();
  EXPECT_EQ(head.body_mode, ResponseBodyMode::kSuppressedWithKnownLength);
  ASSERT_TRUE(head.content_length.has_value());
  EXPECT_EQ(*head.content_length, 5);
  // HEAD consumes only the header area; residual bytes are not body.
  EXPECT_EQ(input.ReadableView(), "unread");
}

TEST(HttpResponseParserTest, Streaming204CompletesWithoutBodySink) {
  Buffer input;
  input.Append("HTTP/1.1 204 No Content\r\n\r\n");
  HttpResponseParser parser;
  EXPECT_EQ(parser.ParseHeaders(input), ParseResult::kComplete);
  EXPECT_TRUE(parser.BodyComplete());
  EXPECT_EQ(parser.Head().body_mode, ResponseBodyMode::kNormal);
  EXPECT_FALSE(parser.Head().content_length.has_value());
}

TEST(HttpResponseParserTest, GetStillAwaitsBodyAfterReset) {
  Buffer input;
  HttpResponseParser parser;
  input.Append("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n");

  EXPECT_EQ(parser.Parse(input), ParseResult::kNeedMoreData);
  EXPECT_EQ(parser.Response().body_mode, aegisgate::http::ResponseBodyMode::kNormal);
}
