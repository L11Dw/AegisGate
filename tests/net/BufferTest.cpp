#include <stdexcept>
#include <string_view>

#include <gtest/gtest.h>

#include "aegisgate/net/Buffer.h"

namespace aegisgate::net {
namespace {

TEST(BufferTest, AppendingAfterRetrievePreservesUnreadBytes) {
  Buffer buffer;
  buffer.Append("abc");
  buffer.Retrieve(2U);
  buffer.Append("def");

  EXPECT_EQ(buffer.ReadableView(), "cdef");
}

TEST(BufferTest, FindsCrlfOnlyAfterBothBytesAreReadable) {
  Buffer buffer;
  buffer.Append("Host: demo\r");

  EXPECT_EQ(buffer.FindCrlf(), std::string_view::npos);

  buffer.Append("\n");

  EXPECT_EQ(buffer.FindCrlf(), 10U);
}

TEST(BufferTest, FindsCrlfRelativeToUnreadBytesAfterRetrieve) {
  Buffer buffer;
  buffer.Append("ignore: one\r\nHost: demo\r\n");
  buffer.Retrieve(13U);

  EXPECT_EQ(buffer.FindCrlf(), 10U);
}

TEST(BufferTest, RetrievingPastReadableBytesThrowsWithoutChangingBuffer) {
  Buffer buffer;
  buffer.Append("abc");

  EXPECT_THROW(buffer.Retrieve(buffer.ReadableBytes() + 1U), std::out_of_range);
  EXPECT_EQ(buffer.ReadableView(), "abc");
}

TEST(BufferTest, RetrieveAllClearsReadableBytesAndAllowsLaterAppend) {
  Buffer buffer;
  buffer.Append("abc");
  buffer.RetrieveAll();

  EXPECT_EQ(buffer.ReadableBytes(), 0U);

  buffer.Append("def");

  EXPECT_EQ(buffer.ReadableView(), "def");
}

TEST(BufferTest, EmptyAppendPreservesUnreadBytes) {
  Buffer buffer;
  buffer.Append("abc");
  buffer.Retrieve(1U);

  buffer.Append("");

  EXPECT_EQ(buffer.ReadableView(), "bc");
}

TEST(BufferTest, AppendCompactsWhenConsumedPrefixExceedsUnreadBytes) {
  Buffer buffer;
  buffer.Append("abcdef");
  buffer.Retrieve(5U);

  buffer.Append("XY");

  EXPECT_EQ(buffer.ReadableView(), "fXY");
}

} // namespace
} // namespace aegisgate::net
