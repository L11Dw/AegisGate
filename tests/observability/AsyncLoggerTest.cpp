// B1: AsyncLogger tests.
// AsyncLogger provides non-blocking JSON Lines structured logging.

#include <chrono>
#include <cstdio>
#include <fstream>
#include <string>
#include <thread>

#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "aegisgate/observability/AsyncLogger.h"

namespace aegisgate::observability {
namespace {

// ---------------------------------------------------------------------------
// LogRecord serialization
// ---------------------------------------------------------------------------

TEST(LogRecordTest, BasicFieldsSerializeCorrectly) {
  LogRecord record;
  record.timestamp_us = 1691922600123456LL;
  record.level = "info";
  record.event = "request_complete";
  record.route = "api";
  record.upstream = "127.0.0.1:8080";
  record.status = 200;
  record.latency_us = 1234;
  record.retries = 0;
  record.request_bytes = 256;
  record.response_bytes = 1024;

  const std::string json = SerializeLogRecord(record);
  EXPECT_NE(json.find("\"level\":\"info\""), std::string::npos);
  EXPECT_NE(json.find("\"event\":\"request_complete\""), std::string::npos);
  EXPECT_NE(json.find("\"route\":\"api\""), std::string::npos);
  EXPECT_NE(json.find("\"upstream\":\"127.0.0.1:8080\""), std::string::npos);
  EXPECT_NE(json.find("\"status\":200"), std::string::npos);
  EXPECT_NE(json.find("\"latency_us\":1234"), std::string::npos);
  EXPECT_NE(json.find("\"request_bytes\":256"), std::string::npos);
  EXPECT_NE(json.find("\"response_bytes\":1024"), std::string::npos);
}

TEST(LogRecordTest, OmitsZeroFields) {
  LogRecord record;
  record.timestamp_us = 1000;
  record.level = "info";
  record.event = "test";
  // All other fields are zero/default.

  const std::string json = SerializeLogRecord(record);
  EXPECT_EQ(json.find("\"status\""), std::string::npos);
  EXPECT_EQ(json.find("\"latency_us\""), std::string::npos);
  EXPECT_EQ(json.find("\"retries\""), std::string::npos);
  EXPECT_EQ(json.find("\"request_bytes\""), std::string::npos);
  EXPECT_EQ(json.find("\"response_bytes\""), std::string::npos);
  EXPECT_EQ(json.find("\"generation\""), std::string::npos);
  EXPECT_EQ(json.find("\"worker\""), std::string::npos);
}

TEST(LogRecordTest, JsonEscapesSpecialCharacters) {
  LogRecord record;
  record.timestamp_us = 1000;
  record.level = "info";
  record.event = "test";
  record.route = "api\"route";
  record.reason = "line1\nline2";

  const std::string json = SerializeLogRecord(record);
  EXPECT_NE(json.find("api\\\"route"), std::string::npos);
  EXPECT_NE(json.find("line1\\nline2"), std::string::npos);
}

TEST(LogRecordTest, TimestampIso8601) {
  LogRecord record;
  record.timestamp_us = 1691922600123456LL;
  record.level = "info";
  record.event = "test";

  const std::string json = SerializeLogRecord(record);
  // Should contain ISO 8601 format.
  EXPECT_NE(json.find("\"timestamp\":\""), std::string::npos);
}

// ---------------------------------------------------------------------------
// AsyncLogger behavior
// ---------------------------------------------------------------------------

TEST(AsyncLoggerTest, NonBlockingSubmit) {
  AsyncLogger logger("/dev/null", 256);
  // Submit should not block even when the queue is being consumed.
  for (int i = 0; i < 200; ++i) {
    LogRecord record;
    record.timestamp_us = i;
    record.level = "info";
    record.event = "test";
    (void)logger.Submit(std::move(record));
  }
  // All should have been accepted (queue is large enough).
  EXPECT_EQ(logger.dropped_total(), 0u);
}

TEST(AsyncLoggerTest, DropsDebugWhenQueueFull) {
  AsyncLogger logger("/dev/null", 2); // very small queue
  // Submit many records rapidly — some should be dropped.
  int dropped = 0;
  for (int i = 0; i < 1000; ++i) {
    LogRecord record;
    record.timestamp_us = i;
    record.level = "debug";
    record.event = "fill";
    if (!logger.Submit(std::move(record))) ++dropped;
  }
  // At least some should have been dropped (queue capacity is 2).
  EXPECT_GT(dropped + logger.dropped_total(), 0u);
}

TEST(AsyncLoggerTest, StopIsIdempotent) {
  AsyncLogger logger("/dev/null", 16);
  logger.Stop();
  logger.Stop(); // must not crash
}

TEST(AsyncLoggerTest, WriterExceptionDoesNotAffectCaller) {
  // Write to an invalid path — should degrade, not throw.
  AsyncLogger logger("/nonexistent/dir/log.jsonl", 16);
  LogRecord record;
  record.timestamp_us = 1;
  record.level = "info";
  record.event = "test";
  // Submit may succeed (queue accepts), but writer will fail.
  // The logger must not throw.
  EXPECT_NO_THROW((void)logger.Submit(std::move(record)));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

TEST(AsyncLoggerTest, ShutdownDrainsAndReportsDropCount) {
  AsyncLogger logger("/dev/null", 16);
  for (int i = 0; i < 10; ++i) {
    LogRecord record;
    record.timestamp_us = i;
    record.level = "info";
    record.event = "test";
    (void)logger.Submit(std::move(record));
  }
  // Stop should drain and return without hanging.
  logger.Stop();
  // dropped_total should be 0 (all records were accepted).
  EXPECT_EQ(logger.dropped_total(), 0u);
}

TEST(AsyncLoggerTest, SubmitAfterStopReturnsFalse) {
  AsyncLogger logger("/dev/null", 16);
  logger.Stop();
  LogRecord record;
  record.timestamp_us = 1;
  record.level = "info";
  record.event = "test";
  EXPECT_FALSE(logger.Submit(std::move(record)));
}

TEST(AsyncLoggerTest, ConcurrentSubmitAndStopIsSafe) {
  // Submit from multiple threads while Stop is called from another.
  AsyncLogger logger("/dev/null", 256);
  std::atomic<bool> stop_flag{false};
  std::vector<std::thread> submitters;
  for (int t = 0; t < 4; ++t) {
    submitters.emplace_back([&logger, &stop_flag] {
      for (int i = 0; !stop_flag.load(std::memory_order_relaxed); ++i) {
        LogRecord record;
        record.timestamp_us = i;
        record.level = "info";
        record.event = "concurrent";
        (void)logger.Submit(std::move(record));
      }
    });
  }
  // Let submissions run briefly.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  logger.Stop();
  stop_flag.store(true, std::memory_order_relaxed);
  for (auto &t : submitters) t.join();
  // No crash, no hang.
}

TEST(AsyncLoggerTest, CriticalSlotsAcceptWarnAndErrorWhenQueueFull) {
  AsyncLogger logger("/dev/null", 4);
  // Fill queue with debug records.
  for (int i = 0; i < 4; ++i) {
    LogRecord record;
    record.timestamp_us = i;
    record.level = "debug";
    record.event = "fill";
    EXPECT_TRUE(logger.Submit(std::move(record)));
  }
  // warn/error should still be accepted via critical slots.
  LogRecord warn_record;
  warn_record.timestamp_us = 100;
  warn_record.level = "warn";
  warn_record.event = "critical";
  EXPECT_TRUE(logger.Submit(std::move(warn_record)));

  LogRecord error_record;
  error_record.timestamp_us = 101;
  error_record.level = "error";
  error_record.event = "critical";
  EXPECT_TRUE(logger.Submit(std::move(error_record)));
}

TEST(AsyncLoggerTest, CriticalOverflowIsCounted) {
  AsyncLogger logger("/dev/null", 2);
  // Overflow critical slots (16) by submitting many error records.
  for (int i = 0; i < 1000; ++i) {
    LogRecord record;
    record.timestamp_us = 100 + i;
    record.level = "error";
    record.event = "overflow";
    (void)logger.Submit(std::move(record));
  }
  // Some should have overflowed critical slots.
  EXPECT_GT(logger.critical_overflow() + logger.dropped_total(), 0u);
}

TEST(AsyncLoggerTest, WriterFailureDoesNotBlockStop) {
  // Open a path that will fail — writer enters degraded mode.
  AsyncLogger logger("/nonexistent/dir/log.jsonl", 16);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_TRUE(logger.writer_failed());
  // Stop should return quickly, not hang.
  logger.Stop();
}

TEST(AsyncLoggerTest, SubmitAfterWriterFailureCountsIoDrop) {
  AsyncLogger logger("/nonexistent/dir/log.jsonl", 16);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_TRUE(logger.writer_failed());
  // Subsequent submits should count as I/O drops.
  LogRecord record;
  record.timestamp_us = 1;
  record.level = "info";
  record.event = "after_fail";
  EXPECT_FALSE(logger.Submit(std::move(record)));
  EXPECT_GT(logger.io_dropped_total(), 0u);
}

} // namespace
} // namespace aegisgate::observability
