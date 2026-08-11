#pragma once

#include <cstddef>
#include <stdexcept>

namespace aegisgate::net {

// Immutable hysteresis bounds for one connection's downstream write queue.
// Upstream reading pauses when the queue reaches the high watermark and
// resumes only after a drain drops it to the low watermark, so a single byte
// in either direction cannot toggle pause state.  The values are per-route
// configuration carried by a future immutable config snapshot; nothing here
// references routes, pools or global state.
class StreamFlowControl {
public:
  constexpr StreamFlowControl() : StreamFlowControl(256 * 1024, 128 * 1024) {}
  constexpr StreamFlowControl(std::size_t high_watermark, std::size_t low_watermark)
      : high_watermark_(high_watermark), low_watermark_(low_watermark) {
    if (low_watermark == 0 || low_watermark >= high_watermark) {
      throw std::invalid_argument("stream flow control requires 0 < low < high");
    }
  }

  [[nodiscard]] constexpr std::size_t HighWatermark() const noexcept {
    return high_watermark_;
  }
  [[nodiscard]] constexpr std::size_t LowWatermark() const noexcept {
    return low_watermark_;
  }

private:
  std::size_t high_watermark_;
  std::size_t low_watermark_;
};

} // namespace aegisgate::net
