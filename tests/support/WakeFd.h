#pragma once

// Test-only helpers for signalling and consuming one-byte control signals on
// wake descriptors.  GCC 15 treats a discarded result of a warn_unused_result
// syscall as an error even when cast to void; these helpers give the discard
// an explicit, checked meaning instead of hiding it.  They are for wake
// descriptors only: gate or protocol data must keep exact byte-count checks.

#include <cerrno>
#include <cstring>
#include <string>

#include <fcntl.h>
#include <unistd.h>

namespace aegisgate::test {

// Writes one control byte on a wake descriptor.  EINTR is retried.  On a
// descriptor confirmed nonblocking, EAGAIN/EWOULDBLOCK means an earlier wake
// is still pending and counts as success.  Any other failure is recorded in
// error for the main test thread to assert; the caller may discard the return
// value because the failure has already been captured.
inline bool SignalWakeFd(int fd, char byte, std::string &error) {
  const int flags = ::fcntl(fd, F_GETFL);
  const bool nonblocking = flags >= 0 && (flags & O_NONBLOCK) != 0;
  for (;;) {
    const ssize_t count = ::write(fd, &byte, 1);
    if (count == 1) return true;
    if (count < 0 && errno == EINTR) continue;
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) && nonblocking) {
      return true;  // an earlier wake is still pending
    }
    error = "failed to signal wake fd: " + std::string(std::strerror(errno));
    return false;
  }
}

// Reads one byte from a wake descriptor and requires exactly expected.
// EINTR is retried; a mismatch, EOF and read errors are recorded in error.
inline bool ConsumeWakeFd(int fd, char expected, std::string &error) {
  for (;;) {
    char byte = '\0';
    const ssize_t count = ::read(fd, &byte, 1);
    if (count == 1) {
      if (byte != expected) {
        error = "unexpected byte on wake fd";
        return false;
      }
      return true;
    }
    if (count == 0) {
      error = "wake fd closed unexpectedly";
      return false;
    }
    if (count < 0 && errno == EINTR) continue;
    error = "failed to read wake fd: " + std::string(std::strerror(errno));
    return false;
  }
}

} // namespace aegisgate::test
