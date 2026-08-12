#include "aegisgate/runtime/GenerationMailbox.h"

#include <cerrno>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <sys/eventfd.h>
#include <unistd.h>

namespace aegisgate::runtime {

GenerationMailbox::GenerationMailbox(std::size_t capacity) : capacity_(capacity) {
  if (capacity_ == 0) {
    throw std::invalid_argument("generation mailbox capacity must be positive");
  }
  wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (wake_fd_ < 0) {
    throw std::system_error(errno, std::generic_category(), "eventfd");
  }
}

GenerationMailbox::~GenerationMailbox() { Close(); }

bool GenerationMailbox::Post(Event event) noexcept {
  if (!event.generation) return false;
  std::lock_guard<std::mutex> guard(mutex_);
  if (closed_ || events_.size() >= capacity_) return false;
  try {
    events_.push_back(std::move(event));
  } catch (...) {
    return false;
  }
  if (WakeLocked()) return true;
  events_.pop_back();
  return false;
}

std::vector<GenerationMailbox::Event> GenerationMailbox::Drain() {
  std::deque<Event> queued;
  int fd = -1;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    fd = wake_fd_;
  }
  if (fd >= 0) {
    std::uint64_t counter = 0;
    for (;;) {
      const ssize_t count = ::read(fd, &counter, sizeof(counter));
      if (count == static_cast<ssize_t>(sizeof(counter))) continue;
      if (count < 0 && errno == EINTR) continue;
      break;
    }
  }
  {
    std::lock_guard<std::mutex> guard(mutex_);
    queued.swap(events_);
  }
  return {std::make_move_iterator(queued.begin()), std::make_move_iterator(queued.end())};
}

void GenerationMailbox::Close() noexcept {
  int fd = -1;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (closed_) return;
    closed_ = true;
    fd = std::exchange(wake_fd_, -1);
  }
  if (fd >= 0) (void)::close(fd);
}

int GenerationMailbox::wake_fd() const noexcept {
  std::lock_guard<std::mutex> guard(mutex_);
  return wake_fd_;
}

std::size_t GenerationMailbox::pending() const noexcept {
  std::lock_guard<std::mutex> guard(mutex_);
  return events_.size();
}

bool GenerationMailbox::WakeLocked() noexcept {
  if (wake_fd_ < 0) return false;
  const std::uint64_t counter = 1;
  for (;;) {
    const ssize_t count = ::write(wake_fd_, &counter, sizeof(counter));
    if (count == static_cast<ssize_t>(sizeof(counter))) return true;
    if (count < 0 && errno == EINTR) continue;
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
    return false;
  }
}

} // namespace aegisgate::runtime
