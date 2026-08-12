#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

#include "aegisgate/runtime/RuntimeGeneration.h"

namespace aegisgate::runtime {

// A small cross-thread mailbox for generation lifecycle notifications.  It is
// deliberately separate from data-plane task queues: these messages carry no
// request data and are consumed only by Gateway's control EventLoop.  A worker
// may post a value, but it must never stop a Coordinator or destroy a
// loop-owned object itself.
class GenerationMailbox {
public:
  enum class Kind : std::uint8_t {
    kLastRequestLeaseReleased,
    kWorkerBalancesReturned,
    kReaperFinished,
  };

  struct Event {
    Kind kind;
    RuntimeGenerationRef generation;
  };

  explicit GenerationMailbox(std::size_t capacity = 16);
  ~GenerationMailbox();

  GenerationMailbox(const GenerationMailbox &) = delete;
  GenerationMailbox &operator=(const GenerationMailbox &) = delete;

  // Thread-safe.  A successful post transfers one value-owned event to the
  // control loop and signals its eventfd.  False means the mailbox has been
  // closed or its fixed control-plane capacity was exhausted; callers must
  // treat that as a lifecycle invariant violation, never silently drop it.
  [[nodiscard]] bool Post(Event event) noexcept;
  // Control-loop owner only.  Drains the eventfd and moves every queued event
  // to the caller.  It is intentionally not a callback API, so the Gateway
  // retains the sole authority to decide when retirement work starts.
  [[nodiscard]] std::vector<Event> Drain();
  // Called by the control loop only after all workers/reapers that can post
  // have joined.  Further posts fail; close is idempotent.
  void Close() noexcept;

  [[nodiscard]] int wake_fd() const noexcept;
  [[nodiscard]] std::size_t pending() const noexcept;

private:
  [[nodiscard]] bool WakeLocked() noexcept;

  const std::size_t capacity_;
  mutable std::mutex mutex_;
  std::deque<Event> events_;
  int wake_fd_ = -1;
  bool closed_ = false;
};

} // namespace aegisgate::runtime
