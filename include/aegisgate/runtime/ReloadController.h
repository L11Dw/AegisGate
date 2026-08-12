#pragma once

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
// reference to Gateway, EventLoop, workers or coordinators: its only output is
// a value result carried over an eventfd to the control loop.  Requests that
// arrive while one parse is running coalesce into one additional latest parse.
class ReloadController {
public:
  struct Result {
    std::optional<config::Config> candidate;
    std::string error;
    std::thread::id parser_thread;
  };

  explicit ReloadController(std::string config_path);
  ~ReloadController();

  ReloadController(const ReloadController &) = delete;
  ReloadController &operator=(const ReloadController &) = delete;

  // Thread-safe and nonblocking.  Starts a background parse or coalesces one
  // pending request.  False only after Stop or with an empty path.
  [[nodiscard]] bool Request();
  // Control-loop owner only: drains the wake counter and transfers every
  // parse result.  Gateway selects the newest result and publishes atomically.
  [[nodiscard]] std::vector<Result> Drain();
  void Stop() noexcept;

  [[nodiscard]] int wake_fd() const noexcept;

private:
  void ParseLoop() noexcept;
  [[nodiscard]] bool WakeLocked() noexcept;

  std::string config_path_;
  mutable std::mutex mutex_;
  std::deque<Result> results_;
  std::thread parser_;
  int wake_fd_ = -1;
  bool stopping_ = false;
  bool parsing_ = false;
  bool pending_ = false;
};

} // namespace aegisgate::runtime
