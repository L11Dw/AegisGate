#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>

namespace aegisgate::runtime {

// Minimal cross-thread wake signal for the generation retirement pipeline.
//
// The mailbox stores NO events — it is a single pending-flag + eventfd.
// After waking, the control loop scans every retiring generation's actual
// retirement_state() and drives the next action.  This guarantees:
// - fixed, tiny memory footprint (one mutex, one atomic, one fd);
// - no unbounded queue, no silent drops, no capacity concerns;
// - the control loop always sees the true state, not a stale event.
class GenerationMailbox {
public:
  GenerationMailbox();
  ~GenerationMailbox();

  GenerationMailbox(const GenerationMailbox &) = delete;
  GenerationMailbox &operator=(const GenerationMailbox &) = delete;

  // Thread-safe.  Sets the pending flag and wakes the control loop via
  // eventfd.  After Close(), this is a harmless no-op.  Returns false
  // only if the mailbox has been closed.
  [[nodiscard]] bool Wake();

  // Control-loop only.  Returns true if at least one Wake() was pending
  // since the last Drain().  Clears the pending flag and consumes the
  // eventfd counter.  After Close(), always returns false.
  [[nodiscard]] bool Drain();

  // File descriptor for epoll registration (eventfd).
  [[nodiscard]] int wake_fd() const noexcept;

  // Idempotent.  After Close(), Wake() is a no-op and Drain() returns
  // false.  Safe to call from any thread (the fd is closed under mutex).
  void Close() noexcept;

private:
  mutable std::mutex mu_;
  int wake_fd_ = -1;
  bool closed_ = false;
};

} // namespace aegisgate::runtime
