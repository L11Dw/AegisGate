#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "aegisgate/runtime/WorkerRuntime.h"

namespace aegisgate::runtime {

// A fixed set of I/O workers.  New client connections are handed off with
// round-robin assignment; connection-level scheduling never considers
// request-level state (least-active is a request concern).  Workers are
// started and stopped together.
class WorkerSet {
public:
  // Throws std::invalid_argument when count is zero (a misconfiguration;
  // config parsing already rejects it).
  explicit WorkerSet(std::size_t count);
  ~WorkerSet();

  WorkerSet(const WorkerSet &) = delete;
  WorkerSet &operator=(const WorkerSet &) = delete;

  void Start();
  // Stops every worker (each rejects new tasks, drains accepted ones and
  // joins).  Idempotent; safe to call without Start().
  void StopAll() noexcept;
  [[nodiscard]] std::size_t size() const noexcept;
  [[nodiscard]] WorkerRuntime &At(std::size_t index);
  // The round-robin target plus its index, so callers never back-look up the
  // index by pointer (R-070).
  struct WorkerHandle {
    std::size_t index;
    WorkerRuntime &worker;
  };
  // Round-robin assignment; thread-safe for concurrent accepters.
  [[nodiscard]] WorkerHandle Next() noexcept;

private:
  std::vector<std::unique_ptr<WorkerRuntime>> workers_;
  std::atomic<std::uint64_t> next_{0};
};

} // namespace aegisgate::runtime
