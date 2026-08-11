#pragma once

#include <cstdint>
#include <functional>
#include <thread>
#include <unordered_map>
#include <vector>

namespace aegisgate::net {

class Channel;

class EventLoop {
public:
  EventLoop();
  ~EventLoop();

  EventLoop(const EventLoop &) = delete;
  EventLoop &operator=(const EventLoop &) = delete;

  void Loop();
  void Quit() noexcept;
  // True while a Channel::HandleEvent callback is on the stack of the dispatch
  // thread.  Readable only on the dispatch thread or while the loop is idle.
  [[nodiscard]] bool IsDispatchingEvent() const noexcept { return dispatching_event_; }
  // Must be called on the EventLoop construction/dispatch thread. Runs after
  // the current epoll event batch has finished dispatching; tasks queued by a
  // task are drained at that same safe point. It is therefore safe for
  // destroying an owner whose Channel callback requested its own removal.
  void QueueAfterCurrentBatch(std::function<void()> task);
  void UpdateChannel(Channel &channel);
  void RemoveChannel(Channel &channel);

private:
  int epoll_fd_ = -1;
  std::thread::id owner_thread_;
  bool quit_ = false;
  bool dispatching_event_ = false;
  std::uint64_t next_registration_token_ = 1;
  std::unordered_map<std::uint64_t, Channel *> registrations_;
  std::vector<std::function<void()>> deferred_tasks_;
};

} // namespace aegisgate::net
