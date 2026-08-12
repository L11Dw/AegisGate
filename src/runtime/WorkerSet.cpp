#include "aegisgate/runtime/WorkerSet.h"

#include <stdexcept>

namespace aegisgate::runtime {

WorkerSet::WorkerSet(std::size_t count,
                     std::function<std::unique_ptr<WorkerRuntime>()> factory) {
  if (count == 0) {
    throw std::invalid_argument("worker count must be positive");
  }
  workers_.reserve(count);
  for (std::size_t index = 0; index != count; ++index) {
    workers_.push_back(factory ? factory() : std::make_unique<WorkerRuntime>());
  }
}

WorkerSet::~WorkerSet() { StopAll(); }

void WorkerSet::Start() {
  // R-067: a partial start must roll back the workers already started (drain +
  // join) before rethrowing, so no worker is left running after a failure.
  std::size_t started = 0;
  try {
    for (; started < workers_.size(); ++started) workers_[started]->Start();
  } catch (...) {
    for (std::size_t index = 0; index < started; ++index) workers_[index]->Stop();
    throw;
  }
}

void WorkerSet::StopAll() noexcept {
  for (const auto &worker : workers_) worker->Stop();
}

std::size_t WorkerSet::size() const noexcept { return workers_.size(); }

WorkerRuntime &WorkerSet::At(std::size_t index) {
  if (index >= workers_.size()) {
    throw std::out_of_range("worker index out of range");
  }
  return *workers_[index];
}

WorkerSet::WorkerHandle WorkerSet::Next() noexcept {
  const std::uint64_t index = next_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
  return {static_cast<std::size_t>(index), *workers_[index]};
}

} // namespace aegisgate::runtime
