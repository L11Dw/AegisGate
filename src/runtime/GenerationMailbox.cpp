#include "aegisgate/runtime/GenerationMailbox.h"

#include <cerrno>
#include <cstdint>
#include <system_error>
#include <unistd.h>
#include <sys/eventfd.h>

namespace aegisgate::runtime {

GenerationMailbox::GenerationMailbox() {
  const int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(), "eventfd");
  }
  wake_fd_ = fd;
}

GenerationMailbox::~GenerationMailbox() { Close(); }

bool GenerationMailbox::Wake() {
  std::lock_guard<std::mutex> guard(mu_);
  if (closed_) return false;
  const std::uint64_t one = 1;
  for (;;) {
    const ssize_t n = ::write(wake_fd_, &one, sizeof(one));
    if (n == sizeof(one)) return true;
    if (n < 0 && errno == EINTR) continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // Counter already saturated — a wake is pending.  Treat as success.
      return true;
    }
    // Hard error (EBADF, EFAULT, etc.).  The fd is likely invalid.
    return false;
  }
}

bool GenerationMailbox::Drain() {
  std::lock_guard<std::mutex> guard(mu_);
  if (closed_) return false;
  // Consume the eventfd counter.
  std::uint64_t counter = 0;
  for (;;) {
    const ssize_t n = ::read(wake_fd_, &counter, sizeof(counter));
    if (n == sizeof(counter)) break;
    if (n < 0 && errno == EINTR) continue;
    break; // EAGAIN: no pending wake
  }
  // Return true if at least one Wake() fired (counter > 0) or if
  // Drain() is called for the first time with no pending wake.
  // The caller always scans retiring generations anyway, so the
  // exact count is irrelevant — only the "something changed" signal.
  return counter > 0;
}

int GenerationMailbox::wake_fd() const noexcept {
  std::lock_guard<std::mutex> guard(mu_);
  return closed_ ? -1 : wake_fd_;
}

void GenerationMailbox::Close() noexcept {
  std::lock_guard<std::mutex> guard(mu_);
  if (closed_) return;
  closed_ = true;
  const int fd = wake_fd_;
  wake_fd_ = -1;
  if (fd >= 0) (void)::close(fd);
}

} // namespace aegisgate::runtime
