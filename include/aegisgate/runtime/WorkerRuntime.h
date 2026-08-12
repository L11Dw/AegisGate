#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <utility>

#include "aegisgate/net/Fd.h"

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
  // Reserved shutdown/destroy slot (R-063/R-067): a single task that is always
  // accepted while the worker runs, even when the control queue is full, and is
  // drained on the worker thread before the loop quits.  This is how an owner
  // tears down its loop-attached objects without spinning on a full queue and
  // without leaking them.  Returns false only when the worker is already
  // stopping or a shutdown task is already pending — the caller must treat that
  // as an explicit shutdown failure, never as a leak to swallow.
  [[nodiscard]] bool PostShutdown(std::function<void(net::EventLoop &)> task);
  // fd handoff with an explicit ownership contract (R-056): on success the
  // worker runs handler(fd) and the handler adopts the move-only FdOwner (moves
  // it into its storage or release()s it into a connection); on any rejection
  // (not started, queue full, stopping, wake failure) the API closes the
  // descriptor itself and returns false.  A throwing handler destroys its
  // FdOwner, closing the descriptor exactly once — the raw int is never passed,
  // so the wrapper can never double-close an adopted fd.
  [[nodiscard]] bool PostFd(int fd, std::function<void(net::FdOwner)> handler);

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
  void DrainShutdownTask(net::EventLoop &loop);
  [[nodiscard]] bool PostTask(Task task);
  // The eventfd wake write, EINTR/EAGAIN-safe, using the injected seam when
  // present.  Must be called with queue_mutex_ held.  Returns false on a hard
  // failure (descriptor closed) so the caller can roll back the enqueue.
  [[nodiscard]] bool WakeLocked(const std::uint64_t *counter) noexcept;

  std::size_t task_capacity_;
  // Constructor-injected test seam for the wake write (see the constructor).
  std::function<ssize_t(int, const void *, std::size_t)> wake_writer_;
  // Owned by this object; written by the worker thread only during stop.
  // -1 means closed.  Exchanged atomically so close is exactly once.
  std::atomic<int> wake_fd_;
  std::mutex queue_mutex_;
  std::condition_variable ready_cv_;
  std::deque<Task> tasks_;
  // The reserved shutdown/destroy task; guarded by queue_mutex_.
  std::function<void(net::EventLoop &)> shutdown_task_;
  // True once the worker loop is registered and running (guarded by
  // queue_mutex_); Post/PostShutdown require it so a worker whose loop has
  // exited never accepts a task that would never run.
  bool running_ = false;
  // One-shot, set true by Run() once the loop is ready; Start() waits on it
  // so a Post immediately after Start is never rejected by the startup window.
  bool loop_ready_ = false;
  std::atomic_bool started_{false};
  std::atomic_bool stopping_{false};
  std::atomic<std::thread::id> owner_thread_{};
  std::thread thread_;
};

} // namespace aegisgate::runtime
