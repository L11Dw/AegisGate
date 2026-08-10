#include "aegisgate/net/Channel.h"

#include <sys/epoll.h>

#include "aegisgate/net/EventLoop.h"

namespace aegisgate::net {

Channel::Channel(EventLoop &loop, int fd) noexcept : loop_(loop), fd_(fd) {}

Channel::~Channel() noexcept {
  if (!added_) {
    return;
  }

  try {
    DisableAll();
  } catch (...) {
  }
}

void Channel::SetReadCallback(ReadCallback callback) {
  read_callback_ = std::move(callback);
}

void Channel::EnableReading() {
  events_ = EPOLLIN | EPOLLRDHUP;
  loop_.UpdateChannel(*this);
}

void Channel::DisableAll() {
  events_ = 0;
  loop_.RemoveChannel(*this);
}

void Channel::Remove() { loop_.RemoveChannel(*this); }

void Channel::HandleEvent(std::uint32_t events) {
  if ((events & (EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0U &&
      read_callback_) {
    read_callback_();
  }
}

} // namespace aegisgate::net
