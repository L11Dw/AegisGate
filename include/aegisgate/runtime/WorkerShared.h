#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "aegisgate/net/StreamFlowControl.h"
#include "aegisgate/runtime/RuntimeGeneration.h"

namespace aegisgate::runtime {

// The thread-safe bundle shared by worker data planes.  There is exactly one
// mutable publication point: current_generation.  A worker loads it once per
// new request, then the transaction retains that generation's request lease;
// it must never look up a mutable "current coordinator" on a retry.
struct WorkerShared {
  std::atomic<RuntimeGenerationRef> current_generation;
  net::StreamFlowControl flow_control;
  std::shared_ptr<void> lifetime_token;
  // Renders the aggregated /metrics text (worker-local counters summed plus
  // the coordinator protection state); callable from any worker thread.
  std::function<std::string()> metrics_renderer;
};

} // namespace aegisgate::runtime
