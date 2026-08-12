#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
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
  // config parsing already rejects it).  `factory` is a constructor-injected
  // test seam (default = a fresh WorkerRuntime) used to inject a worker whose
  // Start() fails, exercising the partial-start rollback.
  explicit WorkerSet(std::size_t count,
                     std::function<std::unique_ptr<WorkerRuntime>()> factory = {});
  ~WorkerSet();

  WorkerSet(const WorkerSet &) = delete;
  WorkerSet &operator=(const WorkerSet &) = delete;

  // Starts every worker; on the first failure it stops (drains + joins) the
  // workers already started and rethrows, leaving no running worker behind
  // (R-067).
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
