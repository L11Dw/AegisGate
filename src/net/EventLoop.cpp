#include "aegisgate/net/EventLoop.h"

#include <array>
#include <cerrno>
#include <stdexcept>
#include <system_error>

#include <sys/epoll.h>
#include <unistd.h>

#include "aegisgate/net/Channel.h"

namespace aegisgate::net {

EventLoop::EventLoop()
    : epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)), owner_thread_(std::this_thread::get_id()) {
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
      const std::uint64_t token = active_events[index].data.u64;
      const auto registration = registrations_.find(token);
      if (registration == registrations_.end()) {
        continue;
      }
      registration->second->HandleEvent(active_events[index].events);
      if (quit_) {
        break;
      }
    }
    // A callback may have closed its own Channel.  Defer its owner teardown
    // until no Channel::HandleEvent frame from this epoll batch is active.
    while (!deferred_tasks_.empty()) {
      auto deferred = std::move(deferred_tasks_);
      deferred_tasks_.clear();
      for (auto &task : deferred) task();
    }
  }
}

void EventLoop::Quit() noexcept { quit_ = true; }

void EventLoop::QueueAfterCurrentBatch(std::function<void()> task) {
  if (std::this_thread::get_id() != owner_thread_) {
    throw std::logic_error("deferred task must be queued on the EventLoop thread");
  }
  if (!task) return;
  deferred_tasks_.push_back(std::move(task));
}

void EventLoop::UpdateChannel(Channel &channel) {
  epoll_event event{};
  event.events = channel.events_;
  const int operation = channel.added_ ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;

  if (!channel.added_) {
    if (next_registration_token_ == 0) {
      throw std::overflow_error("channel registration token overflow");
    }
    channel.registration_token_ = next_registration_token_++;
    registrations_.emplace(channel.registration_token_, &channel);
  }
  event.data.u64 = channel.registration_token_;

  if (::epoll_ctl(epoll_fd_, operation, channel.fd_, &event) < 0) {
    if (!channel.added_) {
      registrations_.erase(channel.registration_token_);
      channel.registration_token_ = 0;
    }
    throw std::system_error(errno, std::generic_category(), "epoll_ctl update");
  }
  channel.added_ = true;
}

void EventLoop::RemoveChannel(Channel &channel) {
  if (!channel.added_) {
    return;
  }

  // Remove the lookup first: epoll_wait may already have returned this token.
  registrations_.erase(channel.registration_token_);
  channel.registration_token_ = 0;
  channel.added_ = false;
  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, channel.fd_, nullptr) < 0) {
    throw std::system_error(errno, std::generic_category(), "epoll_ctl remove");
  }
}

} // namespace aegisgate::net
