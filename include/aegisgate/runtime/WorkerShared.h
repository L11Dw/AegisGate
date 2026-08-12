#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "aegisgate/health/Coordinator.h"
#include "aegisgate/net/StreamFlowControl.h"
#include "aegisgate/resilience/GlobalAdmission.h"
#include "aegisgate/runtime/ConfigSnapshot.h"

namespace aegisgate::runtime {

// The immutable, thread-safe bundle every worker shares.  config_snapshot is
// the request-binding configuration (accessed with std::atomic_load/
// std::atomic_store on the member, never a plain racing copy); coordinator is
// the C1' global coordination handle; admissions hold the global token credit
// and in-flight counters per route; lifetime_token is the gateway's lifecycle
// token observed weakly by in-flight transactions.
struct WorkerShared {
  // The request-binding configuration.  Published and read only through the
  // atomic shared_ptr interface so a concurrent reload can never race a
  // plain copy.
  std::atomic<ConfigSnapshotRef> config_snapshot;
  std::shared_ptr<health::Coordinator> coordinator;
  std::vector<std::shared_ptr<resilience::GlobalAdmission>> admissions;
  std::uint32_t worker_count = 1;
  net::StreamFlowControl flow_control;
  std::shared_ptr<void> lifetime_token;
  // Renders the aggregated /metrics text (worker-local counters summed plus
  // the coordinator protection state); callable from any worker thread.
  std::function<std::string()> metrics_renderer;
};

} // namespace aegisgate::runtime
