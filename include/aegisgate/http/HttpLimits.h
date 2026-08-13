#pragma once

#include <cstddef>

namespace aegisgate::http {

// Fixed protocol-safety limits.  Request bodies remain intentionally small
// because AegisGate does not stream request bodies.  Upstream responses are
// forwarded incrementally, so their independently bounded Content-Length may
// be larger without requiring the gateway to retain the full body.
inline constexpr std::size_t kMaxRequestBodyBytes = 1024 * 1024;
inline constexpr std::size_t kMaxUpstreamResponseBodyBytes = 16 * 1024 * 1024;

} // namespace aegisgate::http
