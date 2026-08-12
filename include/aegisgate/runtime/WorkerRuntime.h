#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <utility>

namespace aegisgate::net {
class EventLoop;
} // namespace aegisgate::net

namespace aegisgate::runtime {

// One I/O worker: an EventLoop constructed and driven on a dedicated thread,
// woken by an eventfd used strictly as a control wake (never for request
// data).  Tasks posted from any thread are value-captured and execute on the
// worker thread only; the queue is bounded and Post returns false when it is
// full or the worker is stopping.  On a false return the caller keeps
// ownership of anything the task would have consumed (close the fd / return
// the lease): the worker never finalizes resources of rejected tasks.
class WorkerRuntime {
public:
  // `wake_writer` is a constructor-injected test seam replacing the eventfd
  // write (default = real write); it lets tests force EINTR/EAGAIN/hard wake
  // failures deterministically.  It is never a mutable global.
  explicit WorkerRuntime(
      std::size_t task_capacity = 1024,
      std::function<ssize_t(int, const void *, std::size_t)> wake_writer = {});
  ~WorkerRuntime();

  WorkerRuntime(const WorkerRuntime &) = delete;
  WorkerRuntime &operator=(const WorkerRuntime &) = delete;

  // Spawns the worker thread, which constructs its EventLoop in-thread so the
  // loop's owner-thread assertions hold.  Throws std::logic_error when called
  // twice or after Stop().
  void Start();
  // Rejects new tasks (Post returns false), drains every already-accepted
  // task on the worker thread, closes the wake descriptor exactly once, and
  // joins the thread.  Idempotent; also closes the wake descriptor when the
  // worker was never started.
  void Stop() noexcept;
  // True from the worker thread only, after Start().
  [[nodiscard]] bool IsOwnerThread() const noexcept;
  // The worker thread's id once Start() has begun running it; a default id
  // before that.
  [[nodiscard]] std::thread::id WorkerThreadId() const noexcept;
  // Posts a value-captured control task.  Returns false without accepting
  // when the task is empty, the worker was not started, the queue is full or
  // the worker is stopping.  Tasks are drained on the worker thread; a
  // throwing task is absorbed so the dispatch loop and the stop decision
  // survive it.
  [[nodiscard]] bool Post(std::function<void()> task);
  // Posts a task that receives the worker's own EventLoop reference when it
  // runs, so loop-attached objects (TimerQueue, Channels) can be constructed
  // and destroyed on their owner thread.  Same acceptance rules as Post().
  [[nodiscard]] bool PostWithLoop(std::function<void(net::EventLoop &)> task);
  // fd handoff with an explicit ownership contract (R-056): on success the
  // worker runs handler(fd) and the handler is the sole fd owner from then on;
  // on any rejection (not started, queue full, stopping, wake failure) the API
  // closes `fd` itself and returns false, so the caller must not close it.  A
  // throwing handler is absorbed and its fd closed by the wrapper, so a
  // partially initialized resource can never leak the descriptor.
  [[nodiscard]] bool PostFd(int fd, std::function<void(int)> handler);

private:
  struct Task {
    std::function<void()> plain;
    std::function<void(net::EventLoop &)> with_loop;
    Task() = default;
    explicit Task(std::function<void()> value) : plain(std::move(value)) {}
    explicit Task(std::function<void(net::EventLoop &)> value) : with_loop(std::move(value)) {}
  };

  void Run();
  void HandleWake(net::EventLoop &loop);
  void DrainQueue(net::EventLoop &loop);
  [[nodiscard]] bool PostTask(Task task);

  std::size_t task_capacity_;
  // Constructor-injected test seam for the wake write (see the constructor).
  std::function<ssize_t(int, const void *, std::size_t)> wake_writer_;
  // Owned by this object; written by the worker thread only during stop.
  // -1 means closed.  Exchanged atomically so close is exactly once.
  std::atomic<int> wake_fd_;
  std::mutex queue_mutex_;
  std::deque<Task> tasks_;
  std::atomic_bool started_{false};
  std::atomic_bool stopping_{false};
  std::atomic<std::thread::id> owner_thread_{};
  std::thread thread_;
};

} // namespace aegisgate::runtime
