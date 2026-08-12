#include "aegisgate/observability/AsyncLogger.h"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <system_error>

#include <unistd.h>
#include <sys/eventfd.h>

namespace aegisgate::observability {

namespace {

// Escape a string for JSON.  Handles \n, \r, \t, \\, \".
std::string JsonEscape(std::string_view sv) {
  std::string result;
  result.reserve(sv.size() + 8);
  for (char c : sv) {
    switch (c) {
    case '"':  result += "\\\""; break;
    case '\\': result += "\\\\"; break;
    case '\n': result += "\\n"; break;
    case '\r': result += "\\r"; break;
    case '\t': result += "\\t"; break;
    default:   result += c; break;
    }
  }
  return result;
}

std::string TimestampIso8601(std::int64_t us) {
  auto tp = std::chrono::system_clock::time_point(std::chrono::microseconds(us));
  auto tt = std::chrono::system_clock::to_time_t(tp);
  auto us_part = us % 1'000'000;
  struct tm tm_buf{};
  gmtime_r(&tt, &tm_buf);
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%06dZ",
                tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
                tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec,
                static_cast<int>(us_part));
  return buf;
}

} // namespace

std::string SerializeLogRecord(const LogRecord &record) {
  std::ostringstream oss;
  oss << '{';
  oss << "\"timestamp\":\"" << TimestampIso8601(record.timestamp_us) << '"';
  oss << ",\"level\":\"" << JsonEscape(record.level) << '"';
  oss << ",\"event\":\"" << JsonEscape(record.event) << '"';
  if (record.generation > 0) oss << ",\"generation\":" << record.generation;
  if (record.worker != UINT32_MAX) oss << ",\"worker\":" << record.worker;
  if (!record.route.empty()) oss << ",\"route\":\"" << JsonEscape(record.route) << '"';
  if (!record.upstream.empty()) oss << ",\"upstream\":\"" << JsonEscape(record.upstream) << '"';
  if (record.status > 0) oss << ",\"status\":" << record.status;
  if (!record.reason.empty()) oss << ",\"reason\":\"" << JsonEscape(record.reason) << '"';
  if (record.latency_us > 0) oss << ",\"latency_us\":" << record.latency_us;
  if (record.retries > 0) oss << ",\"retries\":" << record.retries;
  if (record.request_bytes > 0) oss << ",\"request_bytes\":" << record.request_bytes;
  if (record.response_bytes > 0) oss << ",\"response_bytes\":" << record.response_bytes;
  oss << '}';
  return oss.str();
}

AsyncLogger::AsyncLogger(std::string path, std::size_t capacity)
    : path_(std::move(path)), capacity_(capacity) {
  wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wake_fd_ < 0) {
    throw std::system_error(errno, std::generic_category(), "eventfd");
  }
  writer_ = std::thread([this] { WriterLoop(); });
}

AsyncLogger::~AsyncLogger() { Stop(); }

bool AsyncLogger::Submit(LogRecord record) {
  {
    std::lock_guard<std::mutex> guard(mu_);
    if (stopped_) return false;
    // Check if this is a critical (warn/error) level.
    const bool is_critical = (record.level == "warn" || record.level == "error");
    if (queue_.size() >= capacity_) {
      if (is_critical && critical_used_ < kCriticalSlots) {
        // Use a reserved critical slot.
        ++critical_used_;
      } else {
        // Drop and count.
        dropped_total_.fetch_add(1, std::memory_order_relaxed);
        return false;
      }
    }
    queue_.push_back(std::move(record));
  }
  // Wake the writer thread.
  const std::uint64_t one = 1;
  for (;;) {
    const ssize_t n = ::write(wake_fd_, &one, sizeof(one));
    if (n == sizeof(one)) break;
    if (n < 0 && errno == EINTR) continue;
    break; // EAGAIN
  }
  return true;
}

void AsyncLogger::Stop() noexcept {
  {
    std::lock_guard<std::mutex> guard(mu_);
    if (stopped_) return;
    stopped_ = true;
  }
  // Wake the writer to process remaining records.
  const std::uint64_t one = 1;
  for (;;) {
    const ssize_t n = ::write(wake_fd_, &one, sizeof(one));
    if (n == sizeof(one)) break;
    if (n < 0 && errno == EINTR) continue;
    break;
  }
  if (writer_.joinable()) writer_.join();
  if (wake_fd_ >= 0) {
    (void)::close(wake_fd_);
    wake_fd_ = -1;
  }
}

void AsyncLogger::WriterLoop() noexcept {
  std::ofstream ofs;
  ofs.open(path_, std::ios::app);
  if (!ofs.is_open()) {
    // Degraded mode: just count drops and return.
    return;
  }

  for (;;) {
    // Wait for wake.
    std::uint64_t counter = 0;
    for (;;) {
      const ssize_t n = ::read(wake_fd_, &counter, sizeof(counter));
      if (n == sizeof(counter)) break;
      if (n < 0 && errno == EINTR) continue;
      break; // EAGAIN
    }

    // Drain the queue.
    std::vector<LogRecord> batch;
    {
      std::lock_guard<std::mutex> guard(mu_);
      batch.swap(queue_);
      critical_used_ = 0;
      if (batch.empty() && stopped_) return;
    }

    // Write each record.
    for (const auto &record : batch) {
      const std::string json = SerializeLogRecord(record);
      ofs << json << '\n';
      if (!ofs.good()) {
        // Write failed — degrade to counting.
        dropped_total_.fetch_add(batch.size(), std::memory_order_relaxed);
        return;
      }
    }
    ofs.flush();
  }
}

} // namespace aegisgate::observability
