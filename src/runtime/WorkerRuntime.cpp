#include "aegisgate/runtime/WorkerRuntime.h"

#include <cerrno>
#include <stdexcept>
#include <system_error>

#include <sys/eventfd.h>
#include <unistd.h>

#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"

namespace aegisgate::runtime {

WorkerRuntime::WorkerRuntime(std::size_t task_capacity)
    : task_capacity_(task_capacity), wake_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
  if (task_capacity_ == 0) {
    throw std::invalid_argument("worker task capacity must be positive");
  }
  if (wake_fd_.load(std::memory_order_relaxed) < 0) {
    throw std::system_error(errno, std::generic_category(), "eventfd");
  }
}

WorkerRuntime::~WorkerRuntime() { Stop(); }

void WorkerRuntime::Start() {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true)) {
    throw std::logic_error("worker already started");
  }
  if (stopping_.load(std::memory_order_relaxed)) {
    started_.store(false);
    throw std::logic_error("worker already stopped");
  }
  thread_ = std::thread([this] { Run(); });
}

void WorkerRuntime::Stop() noexcept {
  {
    std::lock_guard<std::mutex> guard(queue_mutex_);
    stopping_.store(true, std::memory_order_relaxed);
  }
  // Final wake: a worker parked in epoll_wait must observe the stop without
  // waiting for another producer.  EINTR is retried; EAGAIN/EWOULDBLOCK means
  // a wake is already pending; any other error is harmless because the queue
  // state was frozen under the mutex above.
  const int fd = wake_fd_.load(std::memory_order_relaxed);
  if (fd >= 0) {
    const std::uint64_t counter = 1;
    for (;;) {
      const ssize_t count = ::write(fd, &counter, sizeof(counter));
      if (count == static_cast<ssize_t>(sizeof(counter))) break;
      if (count < 0 && errno == EINTR) continue;
      break;
    }
  }
  if (thread_.joinable()) thread_.join();
  // The worker thread closes the descriptor at the end of Run(); exchange
  // makes this idempotent for the never-started and double-Stop cases.
  const int closed = wake_fd_.exchange(-1, std::memory_order_relaxed);
  if (closed >= 0) (void)::close(closed);
}

bool WorkerRuntime::IsOwnerThread() const noexcept {
  return std::this_thread::get_id() == owner_thread_.load(std::memory_order_acquire);
}

std::thread::id WorkerRuntime::WorkerThreadId() const noexcept {
  return owner_thread_.load(std::memory_order_acquire);
}

bool WorkerRuntime::Post(std::function<void()> task) {
  if (!task) return false;
  return PostTask(Task(std::move(task)));
}

bool WorkerRuntime::PostWithLoop(std::function<void(net::EventLoop &)> task) {
  if (!task) return false;
  return PostTask(Task(std::move(task)));
}

bool WorkerRuntime::PostTask(Task task) {
  if (!started_.load(std::memory_order_acquire)) return false;
  const std::uint64_t wake = 1;
  std::lock_guard<std::mutex> guard(queue_mutex_);
  if (stopping_.load(std::memory_order_relaxed)) return false;
  if (tasks_.size() >= task_capacity_) return false;
  tasks_.push_back(std::move(task));
  // The wake write happens while the queue is locked so a failure can pop the
  // task back: a false return must mean "not accepted, caller still owns it".
  // EINTR is retried; EAGAIN/EWOULDBLOCK on a nonblocking eventfd means a
  // wake is already pending, which cannot lose this task.
  const int fd = wake_fd_.load(std::memory_order_relaxed);
  if (fd < 0) {
    tasks_.pop_back();
    return false;
  }
  for (;;) {
    const ssize_t count = ::write(fd, &wake, sizeof(wake));
    if (count == static_cast<ssize_t>(sizeof(wake))) return true;
    if (count < 0 && errno == EINTR) continue;
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
    tasks_.pop_back();
    return false;
  }
}

void WorkerRuntime::Run() {
  owner_thread_.store(std::this_thread::get_id(), std::memory_order_release);
  const int fd = wake_fd_.load(std::memory_order_relaxed);
  if (fd < 0) return;  // unreachable: Start() rejects a stopped worker
  // The EventLoop is constructed on this thread so its owner-thread
  // assertions hold for every registration and callback.
  net::EventLoop loop;
  net::Channel wake(loop, fd);
  wake.SetReadCallback([this, &loop] { HandleWake(loop); });
  wake.EnableReading();
  loop.Loop();
  // Unregister before closing so the Channel destructor never touches the
  // EventLoop with a closed descriptor (R-003).
  wake.Remove();
  const int closed = wake_fd_.exchange(-1, std::memory_order_relaxed);
  if (closed >= 0) (void)::close(closed);
}

void WorkerRuntime::HandleWake(net::EventLoop &loop) {
  // Drain the counter once: a read resets it, and any wake written after this
  // read leaves the counter at one, which epoll delivers again (eventfd
  // counters cannot lose a wake the way a pipe byte can).
  const int fd = wake_fd_.load(std::memory_order_relaxed);
  if (fd >= 0) {
    std::uint64_t counter = 0;
    for (;;) {
      const ssize_t count = ::read(fd, &counter, sizeof(counter));
      if (count == static_cast<ssize_t>(sizeof(counter))) break;
      if (count < 0 && errno == EINTR) continue;
      break;  // EAGAIN/EWOULDBLOCK: no pending wake
    }
  }
  DrainQueue(loop);
  {
    std::lock_guard<std::mutex> guard(queue_mutex_);
    if (stopping_.load(std::memory_order_relaxed) && tasks_.empty()) {
      loop.Quit();
    }
  }
}

void WorkerRuntime::DrainQueue(net::EventLoop &loop) {
  for (;;) {
    Task task;
    {
      std::lock_guard<std::mutex> guard(queue_mutex_);
      if (tasks_.empty()) return;
      task = std::move(tasks_.front());
      tasks_.pop_front();
    }
    try {
      if (task.with_loop) {
        task.with_loop(loop);
      } else {
        task.plain();
      }
    } catch (...) {
      // A control task must never break the worker's dispatch loop or its
      // stop decision; the exception is absorbed here.
    }
  }
}

} // namespace aegisgate::runtime
