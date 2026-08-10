#pragma once

#include <cstdint>
#include <unordered_map>

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
  void UpdateChannel(Channel &channel);
  void RemoveChannel(Channel &channel);

private:
  int epoll_fd_ = -1;
  bool quit_ = false;
  std::uint64_t next_registration_token_ = 1;
  std::unordered_map<std::uint64_t, Channel *> registrations_;
};

} // namespace aegisgate::net
