#pragma once

#include <cstdint>
#include <memory>

#include "aegisgate/config/Config.h"

namespace aegisgate::runtime {

// Immutable configuration captured at request-binding time.  Requests and
// in-flight transactions hold a shared_ptr to the snapshot they started
// with; a future reload (M4) publishes a new snapshot without touching the
// old one.  Access to a live snapshot pointer must go through
// std::atomic_load/std::atomic_store on the shared_ptr, never a plain copy
// racing a concurrent publish.
struct ConfigSnapshot {
  std::uint64_t version = 1;
  config::Config config;
};

using ConfigSnapshotRef = std::shared_ptr<const ConfigSnapshot>;

} // namespace aegisgate::runtime
