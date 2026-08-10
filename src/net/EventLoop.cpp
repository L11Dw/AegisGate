#include "aegisgate/net/EventLoop.h"

#include <array>
#include <cerrno>
#include <system_error>

#include <sys/epoll.h>
#include <unistd.h>

#include "aegisgate/net/Channel.h"

namespace aegisgate::net {

EventLoop::EventLoop() : epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)) {
  if (epoll_fd_ < 0) {
    throw std::system_error(errno, std::generic_category(), "epoll_create1");
  }
}

EventLoop::~EventLoop() {
  if (epoll_fd_ >= 0) {
    (void)::close(epoll_fd_);
  }
}

void EventLoop::Loop() {
  quit_ = false;
  std::array<epoll_event, 16> active_events{};
  while (!quit_) {
    const int event_count = ::epoll_wait(
        epoll_fd_, active_events.data(), static_cast<int>(active_events.size()), -1);
    if (event_count < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(), "epoll_wait");
    }

    for (int index = 0; index < event_count; ++index) {
      auto *channel = static_cast<Channel *>(active_events[index].data.ptr);
      channel->HandleEvent(active_events[index].events);
      if (quit_) {
        break;
      }
    }
  }
}

void EventLoop::Quit() noexcept { quit_ = true; }

void EventLoop::UpdateChannel(Channel &channel) {
  epoll_event event{};
  event.events = channel.events_;
  event.data.ptr = &channel;
  const int operation = channel.added_ ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
  if (::epoll_ctl(epoll_fd_, operation, channel.fd_, &event) < 0) {
    throw std::system_error(errno, std::generic_category(), "epoll_ctl update");
  }
  channel.added_ = true;
}

void EventLoop::RemoveChannel(Channel &channel) {
  if (!channel.added_) {
    return;
  }
  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, channel.fd_, nullptr) < 0) {
    throw std::system_error(errno, std::generic_category(), "epoll_ctl remove");
  }
  channel.added_ = false;
}

} // namespace aegisgate::net
