#include "aegisgate/net/TimerQueue.h"

#include <array>
#include <cerrno>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

#include <sys/timerfd.h>
#include <unistd.h>

#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"

namespace aegisgate::net {
namespace {

timespec ToTimespec(TimerQueue::Clock::duration duration) {
  if (duration <= TimerQueue::Clock::duration::zero()) duration = std::chrono::nanoseconds(1);
  const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
  const auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration - seconds);
  return {seconds.count(), nanoseconds.count()};
}

} // namespace

TimerQueue::TimerQueue(EventLoop &loop)
    : loop_(loop), timer_fd_(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)) {
  if (timer_fd_ < 0) throw std::system_error(errno, std::generic_category(), "timerfd_create");
  channel_ = std::make_unique<Channel>(loop_, timer_fd_);
  channel_->SetReadCallback([this] { HandleReadable(); });
  channel_->EnableReading();
}

TimerQueue::~TimerQueue() {
  channel_.reset();
  if (timer_fd_ >= 0) (void)::close(timer_fd_);
}

TimerQueue::TimerId TimerQueue::ScheduleAt(Clock::time_point deadline, Callback callback) {
  if (!callback) throw std::invalid_argument("timer callback is required");
  if (next_identifier_ == 0) throw std::overflow_error("timer identifier overflow");
  const TimerId identifier = next_identifier_++;
  const auto position = timers_.emplace(Key{deadline, identifier}, std::move(callback));
  by_identifier_.emplace(identifier, position.first);
  Rearm();
  return identifier;
}

TimerQueue::TimerId TimerQueue::ScheduleAfter(Clock::duration delay, Callback callback) {
  if (delay < Clock::duration::zero()) throw std::invalid_argument("timer delay cannot be negative");
  return ScheduleAt(Clock::now() + delay, std::move(callback));
}

bool TimerQueue::Cancel(TimerId identifier) {
  const auto position = by_identifier_.find(identifier);
  if (position == by_identifier_.end()) return false;
  timers_.erase(position->second);
  by_identifier_.erase(position);
  Rearm();
  return true;
}

std::size_t TimerQueue::PendingCount() const noexcept { return timers_.size(); }

void TimerQueue::HandleReadable() {
  std::array<std::uint64_t, 1> expirations{};
  for (;;) {
    const ssize_t count = ::read(timer_fd_, expirations.data(), sizeof(expirations));
    if (count == static_cast<ssize_t>(sizeof(expirations))) continue;
    if (count < 0 && errno == EINTR) continue;
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
    if (count < 0) throw std::system_error(errno, std::generic_category(), "timerfd read");
    throw std::runtime_error("short timerfd read");
  }

  const auto now = Clock::now();
  // Erase and invoke each callback one at a time.  A callback due at the same
  // instant can therefore cancel a later timer before it is detached from the
  // queue; pre-collecting all callbacks would violate Cancel's contract.
  while (!timers_.empty() && timers_.begin()->first.first <= now) {
    auto position = timers_.begin();
    Callback callback = std::move(position->second);
    by_identifier_.erase(position->first.second);
    timers_.erase(position);
    callback();
  }
  Rearm();
}

void TimerQueue::Rearm() {
  itimerspec specification{};
  if (!timers_.empty()) {
    specification.it_value = ToTimespec(timers_.begin()->first.first - Clock::now());
  }
  if (::timerfd_settime(timer_fd_, 0, &specification, nullptr) < 0) {
    throw std::system_error(errno, std::generic_category(), "timerfd_settime");
  }
}

} // namespace aegisgate::net
