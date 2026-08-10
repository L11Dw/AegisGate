#pragma once

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
};

} // namespace aegisgate::net
