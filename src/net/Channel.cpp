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

void Channel::SetWriteCallback(WriteCallback callback) {
  write_callback_ = std::move(callback);
}

void Channel::EnableReading() {
  events_ |= EPOLLIN | EPOLLRDHUP;
  UpdateOrRemove();
}

void Channel::DisableReading() {
  events_ &= ~static_cast<std::uint32_t>(EPOLLIN);
  if ((events_ & EPOLLOUT) == 0U) {
    events_ &= ~static_cast<std::uint32_t>(EPOLLRDHUP);
  }
  UpdateOrRemove();
}

void Channel::EnableWriting() {
  // A write-only Channel must wait for space, not repeatedly wake on a
  // level-triggered peer half-close. Read interest already carries RDHUP.
  events_ |= EPOLLOUT;
  UpdateOrRemove();
}

void Channel::DisableWriting() {
  events_ &= ~static_cast<std::uint32_t>(EPOLLOUT);
  if ((events_ & EPOLLIN) == 0U) {
    events_ &= ~static_cast<std::uint32_t>(EPOLLRDHUP);
  }
  UpdateOrRemove();
}

void Channel::DisableAll() {
  events_ = 0;
  UpdateOrRemove();
}

void Channel::Remove() { loop_.RemoveChannel(*this); }

void Channel::HandleEvent(std::uint32_t events) {
  if ((events & (EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0U &&
      read_callback_) {
    read_callback_();
    return;
  }
  if ((events & EPOLLOUT) != 0U && write_callback_) {
    write_callback_();
  }
}

void Channel::UpdateOrRemove() {
  if (events_ == 0U) {
    loop_.RemoveChannel(*this);
    return;
  }
  loop_.UpdateChannel(*this);
}

} // namespace aegisgate::net
