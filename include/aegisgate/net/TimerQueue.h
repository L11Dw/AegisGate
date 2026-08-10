#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <unordered_map>

namespace aegisgate::net {

class Channel;
class EventLoop;

// Event-loop-thread confined timerfd queue. Callbacks may schedule or cancel
// timers; a cancelled timer never runs after its cancellation returns.
class TimerQueue {
public:
  using Clock = std::chrono::steady_clock;
  using TimerId = std::uint64_t;
  using Callback = std::function<void()>;

  explicit TimerQueue(EventLoop &loop);
  ~TimerQueue();

  TimerQueue(const TimerQueue &) = delete;
  TimerQueue &operator=(const TimerQueue &) = delete;

  [[nodiscard]] TimerId ScheduleAt(Clock::time_point deadline, Callback callback);
  [[nodiscard]] TimerId ScheduleAfter(Clock::duration delay, Callback callback);
  [[nodiscard]] bool Cancel(TimerId identifier);
  [[nodiscard]] std::size_t PendingCount() const noexcept;

private:
  using Key = std::pair<Clock::time_point, TimerId>;
  using Timers = std::map<Key, Callback>;

  void HandleReadable();
  void Rearm();

  EventLoop &loop_;
  int timer_fd_ = -1;
  std::unique_ptr<Channel> channel_;
  Timers timers_;
  std::unordered_map<TimerId, Timers::iterator> by_identifier_;
  TimerId next_identifier_ = 1;
};

} // namespace aegisgate::net
