#pragma once

#include <unistd.h>

namespace aegisgate::net {

// Move-only RAII descriptor owner.  A handler that adopts the descriptor moves
// it into its own storage (or release()s it into a connection); otherwise the
// destructor closes it exactly once.  This is the ownership unit passed across
// the fd-handoff boundary (R-056/R-065): on any exception before adoption the
// owner closes the descriptor exactly once, and after adoption the new owner is
// the sole closer — a raw int can never be closed twice or leak.
class FdOwner {
public:
  explicit FdOwner(int fd) : fd_(fd) {}
  ~FdOwner() { if (fd_ >= 0) (void)::close(fd_); }
  FdOwner(const FdOwner &) = delete;
  FdOwner &operator=(const FdOwner &) = delete;
  FdOwner(FdOwner &&other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
  FdOwner &operator=(FdOwner &&other) noexcept {
    if (this == &other) return *this;
    if (fd_ >= 0) (void)::close(fd_);
    fd_ = other.fd_;
    other.fd_ = -1;
    return *this;
  }

  [[nodiscard]] int get() const noexcept { return fd_; }
  // Transfers ownership to the caller; this owner becomes empty.
  [[nodiscard]] int release() noexcept {
    const int fd = fd_;
    fd_ = -1;
    return fd;
  }
  explicit operator bool() const noexcept { return fd_ >= 0; }

private:
  int fd_ = -1;
};

} // namespace aegisgate::net
