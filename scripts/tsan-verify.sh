#!/usr/bin/env bash
# R-069: ThreadSanitizer gate for the multi-worker data plane.
#
# ASan cannot detect data races; this builds a ThreadSanitizer configuration
# and runs the full test suite with halt_on_error, so the first reported race
# aborts the run (non-zero exit).  Targeted coverage includes WorkerRuntime,
# fd handoff, OutcomeChannel, ProbeSlotState, GlobalAdmission and shutdown.
#
# Usage: ./scripts/tsan-verify.sh
set -euo pipefail
cd "$(dirname "$0")/.."
cmake -S . -B build/tsan \
  -DCMAKE_CXX_FLAGS="-fsanitize=thread -g" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
cmake --build build/tsan -j"$(nproc)"
TSAN_OPTIONS="halt_on_error=1" ./build/tsan/tests/aegisgate_tests
