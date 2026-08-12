#include "aegisgate/runtime/WorkerRuntime.h"

#include <cerrno>
#include <condition_variable>
#include <stdexcept>
#include <system_error>

#include <sys/eventfd.h>
#include <unistd.h>

#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"

namespace aegisgate::runtime {

WorkerRuntime::WorkerRuntime(
    std::size_t task_capacity, std::function<ssize_t(int, const void *, std::size_t)> wake_writer)
    : task_capacity_(task_capacity), wake_writer_(std::move(wake_writer)),
      wake_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
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
  // Wait until Run() has registered the loop and set running_, so a Post
  // immediately after Start is never rejected by the startup window.
  std::unique_lock<std::mutex> lock(queue_mutex_);
  ready_cv_.wait(lock, [this] { return loop_ready_; });
  if (stopping_.load(std::memory_order_relaxed)) {
    throw std::logic_error("worker loop failed to start");
  }
}

void WorkerRuntime::Stop() noexcept {
  // R-064: Stop() called from the worker's own thread cannot join itself.  The
  // stop is still requested (the loop quits at the next wake and the thread
  // exits on its own); a later external Stop (or the destructor) joins it.  The
  // wake descriptor is left to Run() so the loop never epolls a closed fd.
  const bool on_owner = std::this_thread::get_id() == owner_thread_.load(std::memory_order_acquire);
  {
    // The final wake write is serialized with Run()'s exchange+close under the
    // queue mutex (TSAN): a concurrent write and close of the same descriptor
    // would otherwise race the fd number.  The stop signal always uses the real
    // write (never the injected seam): it must wake a parked worker regardless
    // of test injection.
    std::lock_guard<std::mutex> guard(queue_mutex_);
    stopping_.store(true, std::memory_order_relaxed);
    const std::uint64_t counter = 1;
    const int fd = wake_fd_.load(std::memory_order_relaxed);
    if (fd >= 0) {
      for (;;) {
        const ssize_t count = ::write(fd, &counter, sizeof(counter));
        if (count == static_cast<ssize_t>(sizeof(counter))) break;
        if (count < 0 && errno == EINTR) continue;
        break;  // EAGAIN: a wake is already pending; other errors: no reader
      }
    }
  }
  if (on_owner) return;
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
  // Accept only while the worker loop is running: a task posted after the loop
  // has exited would never be drained.
  if (stopping_.load(std::memory_order_relaxed) || !running_) return false;
  if (tasks_.size() >= task_capacity_) return false;
  tasks_.push_back(std::move(task));
  // The wake write happens while the queue is locked so a failure can pop the
  // task back: a false return must mean "not accepted, caller still owns it".
  if (!WakeLocked(&wake)) {
    tasks_.pop_back();
    return false;
  }
  return true;
}

bool WorkerRuntime::PostShutdown(std::function<void(net::EventLoop &)> task) {
  if (!task) return false;
  const std::uint64_t wake = 1;
  std::lock_guard<std::mutex> guard(queue_mutex_);
  // Accept only while the worker loop is running: a shutdown task posted to an
  // exited loop would be a "fake success" — the destroy task would never run.
  if (!started_.load(std::memory_order_relaxed) || !running_ ||
      stopping_.load(std::memory_order_relaxed) || shutdown_task_) {
    return false;
  }
  shutdown_task_ = std::move(task);
  if (!WakeLocked(&wake)) {
    // Hard wake failure (descriptor closed): roll the reservation back so the
    // caller's false is an honest "not accepted", never a fake success.
    shutdown_task_ = std::function<void(net::EventLoop &)>();
    return false;
  }
  return true;
}

bool WorkerRuntime::WakeLocked(const std::uint64_t *counter) noexcept {
  const int fd = wake_fd_.load(std::memory_order_relaxed);
  if (fd < 0) return false;
  for (;;) {
    const ssize_t count = wake_writer_ ? wake_writer_(fd, counter, sizeof(*counter))
                                       : ::write(fd, counter, sizeof(*counter));
    if (count == static_cast<ssize_t>(sizeof(*counter))) return true;
    if (count < 0 && errno == EINTR) continue;
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
    return false;
  }
}

bool WorkerRuntime::PostFd(int fd, std::function<void(net::FdOwner)> handler) {
  if (fd < 0 || !handler) {
    if (fd >= 0) (void)::close(fd);
    return false;
  }
  auto wrapped = [fd, handler = std::move(handler)]() mutable {
    // The handler adopts the move-only owner; a throw destroys it and closes
    // the descriptor exactly once, whether or not it was already handed on.
    net::FdOwner owned(fd);
    handler(std::move(owned));
  };
  if (!PostTask(Task(std::move(wrapped)))) {
    // Rejected: the task was popped and destroyed without running, and its
    // destructor does not close the raw fd; the API owns it here.
    (void)::close(fd);
    return false;
  }
  return true;
}

void WorkerRuntime::Run() {
  owner_thread_.store(std::this_thread::get_id(), std::memory_order_release);
  const int fd = wake_fd_.load(std::memory_order_relaxed);
  if (fd < 0) return;  // unreachable: Start() rejects a stopped worker
  try {
    // The EventLoop is constructed on this thread so its owner-thread
    // assertions hold for every registration and callback.
    net::EventLoop loop;
    net::Channel wake(loop, fd);
    wake.SetReadCallback([this, &loop] { HandleWake(loop); });
    wake.EnableReading();
    {
      std::lock_guard<std::mutex> guard(queue_mutex_);
      running_ = true;
      loop_ready_ = true;
    }
    ready_cv_.notify_all();
    loop.Loop();
    // Unregister before closing so the Channel destructor never touches the
    // EventLoop with a closed descriptor (R-003).  The exchange is serialized
    // with Stop()'s final wake write under the queue mutex (TSAN): once the
    // descriptor is -1 here, no concurrent Stop can still write it.  running_
    // is cleared first so a concurrent Post/PostShutdown stops being accepted.
    wake.Remove();
    int closed = -1;
    {
      std::lock_guard<std::mutex> guard(queue_mutex_);
      running_ = false;
      closed = wake_fd_.exchange(-1, std::memory_order_relaxed);
    }
    if (closed >= 0) (void)::close(closed);
  } catch (...) {
    // A loop setup failure must wake Start() and mark the worker unusable
    // instead of leaving it permanently in the starting state.
    {
      std::lock_guard<std::mutex> guard(queue_mutex_);
      stopping_.store(true, std::memory_order_relaxed);
      running_ = false;
      loop_ready_ = true;
    }
    ready_cv_.notify_all();
  }
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
  DrainShutdownTask(loop);
  {
    std::lock_guard<std::mutex> guard(queue_mutex_);
    if (stopping_.load(std::memory_order_relaxed) && tasks_.empty() && !shutdown_task_) {
      loop.Quit();
    }
  }
}

void WorkerRuntime::DrainShutdownTask(net::EventLoop &loop) {
  std::function<void(net::EventLoop &)> task;
  {
    std::lock_guard<std::mutex> guard(queue_mutex_);
    task = std::move(shutdown_task_);
  }
  if (task) {
    try {
      task(loop);
    } catch (...) {
      // A shutdown task must never break the stop decision.
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
