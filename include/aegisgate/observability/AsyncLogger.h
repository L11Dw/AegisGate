#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace aegisgate::observability {

// A single log record — a pure value object.  No pointers to connections,
// transactions, routes, or other owner-managed resources.
struct LogRecord {
  std::int64_t timestamp_us = 0;     // Unix microseconds
  std::string level;                 // "debug" | "info" | "warn" | "error"
  std::string event;                 // event name
  std::uint64_t generation = 0;      // version, 0 = N/A
  std::uint32_t worker = UINT32_MAX; // worker index, UINT32_MAX = N/A
  std::string route;                 // route name
  std::string upstream;              // "host:port"
  std::uint16_t status = 0;          // HTTP status, 0 = N/A
  std::string reason;                // rejection/failure reason
  std::uint64_t latency_us = 0;      // latency in microseconds, 0 = N/A
  std::uint32_t retries = 0;         // retry count
  std::uint64_t request_bytes = 0;   // request body bytes
  std::uint64_t response_bytes = 0;  // response body bytes
};

// Serializes a LogRecord to a JSON string.  Omits zero/default fields.
// Handles JSON escaping for special characters.
[[nodiscard]] std::string SerializeLogRecord(const LogRecord &record);

// Non-blocking async logger.  Writers submit LogRecords to a bounded MPSC
// queue; a dedicated writer thread consumes the queue and writes JSON Lines
// to a file.  The logger never blocks the caller.
class AsyncLogger {
public:
  // path: output file path.  "/dev/null" for testing.
  // capacity: maximum queue size.
  explicit AsyncLogger(std::string path, std::size_t capacity = 4096);
  ~AsyncLogger();

  AsyncLogger(const AsyncLogger &) = delete;
  AsyncLogger &operator=(const AsyncLogger &) = delete;

  // Thread-safe.  Returns true if the record was enqueued.  Returns false
  // if the queue is full (debug/info dropped) or the logger is stopped.
  [[nodiscard]] bool Submit(LogRecord record);

  // Stops the logger.  Drains the queue with a deadline, then joins the
  // writer thread.  Idempotent.
  void Stop() noexcept;

  // Monotonic count of dropped records (debug/info only).
  [[nodiscard]] std::uint64_t dropped_total() const noexcept {
    return dropped_total_.load(std::memory_order_relaxed);
  }

private:
  void WriterLoop() noexcept;

  const std::string path_;
  const std::size_t capacity_;
  mutable std::mutex mu_;
  std::vector<LogRecord> queue_;
  int wake_fd_ = -1;
  std::thread writer_;
  std::atomic<std::uint64_t> dropped_total_{0};
  bool stopped_ = false;
  // Reserved slots for warn/error when queue is full.
  static constexpr std::size_t kCriticalSlots = 16;
  std::size_t critical_used_ = 0;
};

} // namespace aegisgate::observability
