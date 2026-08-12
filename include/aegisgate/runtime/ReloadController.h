#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "aegisgate/config/Config.h"

namespace aegisgate::runtime {

// Owns the blocking file I/O and YAML parsing side of reload.  It has no
// reference to Gateway, EventLoop, workers or coordinators: its only output
// is a value result carried over an eventfd to the control loop.  Requests
// that arrive while one parse is running coalesce into one additional latest
// parse (latest-wins).
class ReloadController {
public:
  struct Result {
    std::optional<config::Config> candidate;
    std::string error;
    std::uint64_t sequence = 0;
  };

  explicit ReloadController(std::string config_path);
  ~ReloadController();

  ReloadController(const ReloadController &) = delete;
  ReloadController &operator=(const ReloadController &) = delete;

  // Thread-safe and nonblocking.  Starts a background parse or coalesces
  // one pending request.  Returns false only after Stop.
  [[nodiscard]] bool Request();

  // Control-loop owner only.  Drains all completed results.  Returns the
  // results in sequence order (oldest first).  The caller should use only
  // the last result (latest-wins).
  [[nodiscard]] std::vector<Result> Drain();

  // Stops the background thread.  Idempotent.  After Stop, Request returns
  // false and Drain returns empty.
  void Stop() noexcept;

  // File descriptor for epoll registration (eventfd).
  [[nodiscard]] int wake_fd() const noexcept;

private:
  void ParseLoop() noexcept;

  std::string config_path_;
  mutable std::mutex mu_;
  std::deque<Result> results_;
  std::thread parser_;
  int wake_fd_ = -1;
  std::uint64_t next_sequence_ = 1;
  bool stopping_ = false;
  bool parsing_ = false;
  bool pending_ = false;
};

} // namespace aegisgate::runtime
