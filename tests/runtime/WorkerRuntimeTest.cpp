#include <array>
#include <atomic>
#include <future>
#include <chrono>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/net/EventLoop.h"
#include "aegisgate/runtime/WorkerRuntime.h"

#include "../support/WakeFd.h"

namespace aegisgate::runtime {
namespace {

using Deadline = std::chrono::steady_clock::time_point;

Deadline TestDeadline() { return std::chrono::steady_clock::now() + std::chrono::seconds(5); }

int RemainingMilliseconds(Deadline deadline) {
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
      deadline - std::chrono::steady_clock::now());
  return remaining.count() > 0 ? static_cast<int>(remaining.count()) : 0;
}

bool WaitFor(int fd, short events, Deadline deadline) {
  pollfd descriptor{fd, events, 0};
  return ::poll(&descriptor, 1, RemainingMilliseconds(deadline)) > 0;
}

// A value-captured task executes on the worker thread; it must never run
// GTest assertions there, so every task records into shared state that the
// main test thread asserts after the worker has been joined.
TEST(WorkerRuntimeTest, RunsPostedTaskOnWorkerThreadAndSignalsWake) {
  std::array<int, 2> done{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, done.data()), 0);
  std::string error;
  WorkerRuntime worker;
  worker.Start();
  std::mutex mutex;
  bool ran = false;
  bool ran_on_owner = false;
  std::thread::id worker_id{};
  ASSERT_TRUE(worker.Post([&] {
    {
      std::lock_guard<std::mutex> guard(mutex);
      ran = true;
      ran_on_owner = worker.IsOwnerThread();
      worker_id = std::this_thread::get_id();
    }
    (void)test::SignalWakeFd(done[1], 'd', error);
  }));
  ASSERT_TRUE(WaitFor(done[0], POLLIN, TestDeadline()));
  char byte = '\0';
  ASSERT_EQ(::read(done[0], &byte, 1), 1);
  worker.Stop();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_FALSE(worker.IsOwnerThread());
  {
    std::lock_guard<std::mutex> guard(mutex);
    EXPECT_TRUE(ran);
    EXPECT_TRUE(ran_on_owner);
    EXPECT_NE(worker_id, std::this_thread::get_id());
    EXPECT_EQ(worker.WorkerThreadId(), worker_id);
  }
  EXPECT_EQ(::close(done[0]), 0);
  EXPECT_EQ(::close(done[1]), 0);
}

TEST(WorkerRuntimeTest, ExecutesAcceptedTasksInPostOrder) {
  std::array<int, 2> done{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, done.data()), 0);
  std::string error;
  WorkerRuntime worker;
  worker.Start();
  std::mutex mutex;
  std::vector<int> order;
  for (int index = 0; index != 3; ++index) {
    ASSERT_TRUE(worker.Post([&, index] {
      {
        std::lock_guard<std::mutex> guard(mutex);
        order.push_back(index);
      }
      if (index == 2) (void)test::SignalWakeFd(done[1], 'd', error);
    }));
  }
  ASSERT_TRUE(WaitFor(done[0], POLLIN, TestDeadline()));
  char byte = '\0';
  ASSERT_EQ(::read(done[0], &byte, 1), 1);
  worker.Stop();
  EXPECT_TRUE(error.empty()) << error;
  {
    std::lock_guard<std::mutex> guard(mutex);
    EXPECT_EQ(order, (std::vector<int>{0, 1, 2}));
  }
  EXPECT_EQ(::close(done[0]), 0);
  EXPECT_EQ(::close(done[1]), 0);
}

// The queue is bounded: once full, Post returns false and the caller keeps
// ownership of whatever the task would have consumed (an fd to close or a
// lease to return).  Accepted tasks must each run exactly once.
TEST(WorkerRuntimeTest, FullQueueRejectsTaskAndCallerRemainsOwner) {
  std::array<int, 2> gate{};
  std::array<int, 2> done{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, gate.data()), 0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, done.data()), 0);
  std::string error;
  WorkerRuntime worker(/*task_capacity=*/2);
  worker.Start();
  std::atomic_int accepted_runs{0};
  // Two blocking tasks fill the queue; they wait on the gate until released.
  for (int index = 0; index != 2; ++index) {
    ASSERT_TRUE(worker.Post([&] {
      char release = '\0';
      if (::read(gate[0], &release, 1) != 1 || release != 'r') {
        if (error.empty()) error = "gate read failed";
      }
      ++accepted_runs;
      (void)test::SignalWakeFd(done[1], 'd', error);
    }));
  }
  // The queue is full: further posts fail and are not accepted.
  EXPECT_FALSE(worker.Post([&] { ++accepted_runs; }));
  EXPECT_FALSE(worker.Post([] {}));
  // Release both accepted tasks; each completes exactly once.
  ASSERT_EQ(::write(gate[1], "rr", 2), 2);
  for (int index = 0; index != 2; ++index) {
    ASSERT_TRUE(WaitFor(done[0], POLLIN, TestDeadline()));
    char byte = '\0';
    ASSERT_EQ(::read(done[0], &byte, 1), 1);
  }
  worker.Stop();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(accepted_runs.load(), 2);
  EXPECT_EQ(::close(gate[0]), 0);
  EXPECT_EQ(::close(gate[1]), 0);
  EXPECT_EQ(::close(done[0]), 0);
  EXPECT_EQ(::close(done[1]), 0);
}

TEST(WorkerRuntimeTest, RejectsPostsBeforeStartAndAfterStop) {
  WorkerRuntime worker;
  EXPECT_FALSE(worker.Post([] {}));
  worker.Start();
  EXPECT_TRUE(worker.Post([] {}));
  worker.Stop();
  worker.Stop();  // idempotent
  EXPECT_FALSE(worker.Post([] {}));
}

TEST(WorkerRuntimeTest, StopDrainsAcceptedTasksBeforeJoining) {
  std::array<int, 2> done{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, done.data()), 0);
  std::string error;
  std::atomic_int runs{0};
  WorkerRuntime worker;
  worker.Start();
  for (int index = 0; index != 5; ++index) {
    ASSERT_TRUE(worker.Post([&] {
      ++runs;
      (void)test::SignalWakeFd(done[1], 'd', error);
    }));
  }
  // Stop() must reject new work, drain every already-accepted task on the
  // worker thread, and only then join: after it returns, all five ran.
  worker.Stop();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(runs.load(), 5);
  EXPECT_EQ(::close(done[0]), 0);
  EXPECT_EQ(::close(done[1]), 0);
}

TEST(WorkerRuntimeTest, ThrowingTaskDoesNotBreakWorker) {
  std::array<int, 2> done{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, done.data()), 0);
  std::string error;
  WorkerRuntime worker;
  worker.Start();
  ASSERT_TRUE(worker.Post([] { throw std::runtime_error("control task boom"); }));
  ASSERT_TRUE(worker.Post([&] { (void)test::SignalWakeFd(done[1], 'd', error); }));
  ASSERT_TRUE(WaitFor(done[0], POLLIN, TestDeadline()));
  char byte = '\0';
  ASSERT_EQ(::read(done[0], &byte, 1), 1);
  worker.Stop();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(::close(done[0]), 0);
  EXPECT_EQ(::close(done[1]), 0);
}

// Many rapid posts from a foreign thread coalesce into one wake; the eventfd
// counter semantics must not lose any task (the EAGAIN wake path is real here:
// a saturated eventfd counter makes the producer's write return EAGAIN, which
// counts as success because a wake is already pending).
TEST(WorkerRuntimeTest, BulkPostsFromForeignThreadAllExecute) {
  std::array<int, 2> done{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, done.data()), 0);
  std::string error;
  constexpr int kTasks = 200;
  std::atomic_int runs{0};
  WorkerRuntime worker;
  worker.Start();
  std::thread producer([&] {
    for (int index = 0; index != kTasks; ++index) {
      if (!worker.Post([&] {
            if (++runs == kTasks) (void)test::SignalWakeFd(done[1], 'd', error);
          })) {
        if (error.empty()) error = "post rejected unexpectedly";
        return;
      }
    }
  });
  ASSERT_TRUE(WaitFor(done[0], POLLIN, TestDeadline()));
  char byte = '\0';
  ASSERT_EQ(::read(done[0], &byte, 1), 1);
  producer.join();
  worker.Stop();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(runs.load(), kTasks);
  EXPECT_EQ(::close(done[0]), 0);
  EXPECT_EQ(::close(done[1]), 0);
}

TEST(WorkerRuntimeTest, PostsFromMultipleThreadsAllExecute) {
  std::array<int, 2> done{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, done.data()), 0);
  std::string error;
  constexpr int kTasksPerProducer = 100;
  constexpr int kTotal = kTasksPerProducer * 2;
  std::atomic_int runs{0};
  WorkerRuntime worker;
  worker.Start();
  std::thread first([&] {
    for (int index = 0; index != kTasksPerProducer; ++index) {
      if (!worker.Post([&] {
            if (++runs == kTotal) (void)test::SignalWakeFd(done[1], 'd', error);
          })) {
        if (error.empty()) error = "first producer post rejected";
        return;
      }
    }
  });
  std::thread second([&] {
    for (int index = 0; index != kTasksPerProducer; ++index) {
      if (!worker.Post([&] {
            if (++runs == kTotal) (void)test::SignalWakeFd(done[1], 'd', error);
          })) {
        if (error.empty()) error = "second producer post rejected";
        return;
      }
    }
  });
  ASSERT_TRUE(WaitFor(done[0], POLLIN, TestDeadline()));
  char byte = '\0';
  ASSERT_EQ(::read(done[0], &byte, 1), 1);
  first.join();
  second.join();
  worker.Stop();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(runs.load(), kTotal);
  EXPECT_EQ(::close(done[0]), 0);
  EXPECT_EQ(::close(done[1]), 0);
}

TEST(WorkerRuntimeTest, RejectsZeroTaskCapacity) {
  EXPECT_THROW(WorkerRuntime(0), std::invalid_argument);
}

// A loop-bound task receives the worker's own EventLoop reference and runs on
// the worker thread, so loop-attached objects (TimerQueue, Channels) can be
// constructed and destroyed on their owner thread.
TEST(WorkerRuntimeTest, PostWithLoopRunsOnWorkerLoop) {
  std::array<int, 2> done{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, done.data()), 0);
  std::string error;
  std::mutex mutex;
  bool ran = false;
  bool ran_on_loop_owner = false;
  std::thread::id looped_id{};
  std::thread::id plain_id{};
  WorkerRuntime worker;
  worker.Start();
  ASSERT_TRUE(worker.PostWithLoop([&](net::EventLoop &loop) {
    {
      std::lock_guard<std::mutex> guard(mutex);
      ran = true;
      ran_on_loop_owner = loop.IsOwnerThread();
      looped_id = std::this_thread::get_id();
    }
    (void)test::SignalWakeFd(done[1], 'd', error);
  }));
  ASSERT_TRUE(WaitFor(done[0], POLLIN, TestDeadline()));
  char byte = '\0';
  ASSERT_EQ(::read(done[0], &byte, 1), 1);
  // A plain task runs on the same worker thread as the loop-bound task.
  ASSERT_TRUE(worker.Post([&] {
    {
      std::lock_guard<std::mutex> guard(mutex);
      plain_id = std::this_thread::get_id();
    }
    (void)test::SignalWakeFd(done[1], 'd', error);
  }));
  ASSERT_TRUE(WaitFor(done[0], POLLIN, TestDeadline()));
  ASSERT_EQ(::read(done[0], &byte, 1), 1);
  worker.Stop();
  EXPECT_TRUE(error.empty()) << error;
  {
    std::lock_guard<std::mutex> guard(mutex);
    EXPECT_TRUE(ran);
    EXPECT_TRUE(ran_on_loop_owner);
    EXPECT_EQ(plain_id, looped_id);
  }
  EXPECT_EQ(::close(done[0]), 0);
  EXPECT_EQ(::close(done[1]), 0);
}

TEST(WorkerRuntimeTest, RejectsLoopTaskWhenFullOrStopped) {
  std::array<int, 2> gate{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, gate.data()), 0);
  WorkerRuntime worker(/*task_capacity=*/1);
  EXPECT_FALSE(worker.PostWithLoop([](net::EventLoop &) {}));  // before start
  worker.Start();
  // Fill the single slot with a blocking plain task, then the loop task must
  // be rejected without displacing it.
  ASSERT_TRUE(worker.Post([&] {
    char release = '\0';
    (void)::read(gate[0], &release, 1);
  }));
  EXPECT_FALSE(worker.PostWithLoop([](net::EventLoop &) {}));
  EXPECT_FALSE(worker.Post([] {}));
  ASSERT_EQ(::write(gate[1], "r", 1), 1);
  worker.Stop();
  EXPECT_EQ(::close(gate[0]), 0);
  EXPECT_EQ(::close(gate[1]), 0);
}

} // namespace


// R-056: PostFd transfers ownership on success; on rejection the API closes
// the descriptor itself, so the caller never double-closes.
TEST(WorkerRuntimeTest, PostFdRejectsWhenFullAndClosesFd) {
  std::array<int, 2> gate{};
  std::array<int, 2> done{};
  std::array<int, 2> peer{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, gate.data()), 0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, done.data()), 0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, peer.data()), 0);
  std::string error;
  std::atomic_int accepted_runs{0};
  WorkerRuntime worker(/*task_capacity=*/2);
  worker.Start();
  // Fill the queue with two blocking tasks.
  for (int index = 0; index != 2; ++index) {
    ASSERT_TRUE(worker.Post([&] {
      char release = '\0';
      (void)::read(gate[0], &release, 1);
      ++accepted_runs;
      (void)test::SignalWakeFd(done[1], 'd', error);
    }));
  }
  // Queue full: PostFd rejects and the API closes the handed descriptor.
  EXPECT_FALSE(worker.PostFd(peer[1], [](int) {}));
  EXPECT_TRUE(WaitFor(peer[0], POLLIN | POLLHUP, TestDeadline()));
  char byte = '\0';
  EXPECT_EQ(::read(peer[0], &byte, 1), 0) << "peer must see EOF (fd closed by the API)";
  // Release the accepted tasks; each completes exactly once.
  ASSERT_EQ(::write(gate[1], "rr", 2), 2);
  for (int index = 0; index != 2; ++index) {
    ASSERT_TRUE(WaitFor(done[0], POLLIN, TestDeadline()));
    ASSERT_EQ(::read(done[0], &byte, 1), 1);
  }
  worker.Stop();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(accepted_runs.load(), 2);
  EXPECT_EQ(::close(gate[0]), 0);
  EXPECT_EQ(::close(gate[1]), 0);
  EXPECT_EQ(::close(done[0]), 0);
  EXPECT_EQ(::close(done[1]), 0);
  EXPECT_EQ(::close(peer[0]), 0);
  // peer[1] was closed by PostFd; closing it again would double-close.
}

TEST(WorkerRuntimeTest, PostFdRejectsWhenStoppedAndClosesFd) {
  std::array<int, 2> peer{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, peer.data()), 0);
  WorkerRuntime worker;
  worker.Start();
  worker.Stop();
  EXPECT_FALSE(worker.PostFd(peer[1], [](int) {}));
  EXPECT_TRUE(WaitFor(peer[0], POLLIN | POLLHUP, TestDeadline()));
  char byte = '\0';
  EXPECT_EQ(::read(peer[0], &byte, 1), 0) << "peer must see EOF (fd closed by the API)";
  EXPECT_EQ(::close(peer[0]), 0);
}

// R-056: on acceptance the handler is the sole fd owner.
TEST(WorkerRuntimeTest, PostFdRunsHandlerWithOwnedFd) {
  std::array<int, 2> peer{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, peer.data()), 0);
  std::atomic_int received{-1};
  std::promise<void> handled;
  auto future = handled.get_future();
  WorkerRuntime worker;
  worker.Start();
  ASSERT_TRUE(worker.PostFd(peer[1], [&](int fd) {
    received.store(fd);
    (void)::close(fd);  // the handler owns and closes the descriptor
    handled.set_value();
  }));
  future.get();
  EXPECT_EQ(received.load(), peer[1]);
  EXPECT_TRUE(WaitFor(peer[0], POLLIN | POLLHUP, TestDeadline()));
  char byte = '\0';
  EXPECT_EQ(::read(peer[0], &byte, 1), 0);
  worker.Stop();
  EXPECT_EQ(::close(peer[0]), 0);
}

// R-066: a throwing handler is absorbed and its fd closed by the wrapper, so
// no partially initialized resource can leak the descriptor.
TEST(WorkerRuntimeTest, PostFdHandlerThrowClosesFd) {
  std::array<int, 2> peer{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, peer.data()), 0);
  WorkerRuntime worker;
  worker.Start();
  ASSERT_TRUE(worker.PostFd(peer[1], [](int) { throw std::runtime_error("handler boom"); }));
  EXPECT_TRUE(WaitFor(peer[0], POLLIN | POLLHUP, TestDeadline()));
  char byte = '\0';
  EXPECT_EQ(::read(peer[0], &byte, 1), 0) << "peer must see EOF (wrapper closed the fd)";
  worker.Stop();
  EXPECT_EQ(::close(peer[0]), 0);
}

// R-056 wake-writer seam: a hard wake failure pops the task and returns false;
// PostFd closes the handed fd.
TEST(WorkerRuntimeTest, WakeWriteFailurePopsTaskAndClosesFd) {
  std::array<int, 2> peer{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, peer.data()), 0);
  WorkerRuntime worker(/*task_capacity=*/2, [](int, const void *, std::size_t) -> ssize_t {
    errno = EBADF;
    return -1;
  });
  worker.Start();
  std::atomic_int runs{0};
  EXPECT_FALSE(worker.Post([&] { ++runs; }));
  EXPECT_FALSE(worker.PostFd(peer[1], [](int) {}));
  EXPECT_TRUE(WaitFor(peer[0], POLLIN | POLLHUP, TestDeadline()));
  char byte = '\0';
  EXPECT_EQ(::read(peer[0], &byte, 1), 0);
  worker.Stop();
  EXPECT_EQ(runs.load(), 0) << "a popped task must never run";
  EXPECT_EQ(::close(peer[0]), 0);
}

// The wake write is retried on EINTR.
TEST(WorkerRuntimeTest, WakeWriteEintrRetriedThenAccepted) {
  int calls = 0;
  WorkerRuntime worker(2, [&calls](int, const void *, std::size_t) -> ssize_t {
    ++calls;
    if (calls == 1) {
      errno = EINTR;
      return -1;
    }
    return sizeof(std::uint64_t);
  });
  worker.Start();
  std::atomic_int runs{0};
  ASSERT_TRUE(worker.Post([&] { ++runs; }));
  worker.Stop();
  EXPECT_EQ(runs.load(), 1);
}

// EAGAIN on the wake write means a wake is already pending: no loss.
TEST(WorkerRuntimeTest, WakeWriteEagainMeansWakePending) {
  WorkerRuntime worker(2, [](int, const void *, std::size_t) -> ssize_t {
    errno = EAGAIN;
    return -1;
  });
  worker.Start();
  std::atomic_int runs{0};
  ASSERT_TRUE(worker.Post([&] { ++runs; }));
  worker.Stop();
  EXPECT_EQ(runs.load(), 1);
}


// R-064: Stop() called from the worker's own thread must not self-join; the
// stop is requested, the loop quits and the thread exits on its own, and a
// later external Stop joins it.
TEST(WorkerRuntimeTest, StopFromOwnerThreadDoesNotSelfJoin) {
  std::promise<void> stopped;
  auto future = stopped.get_future();
  WorkerRuntime worker;
  worker.Start();
  ASSERT_TRUE(worker.Post([&] {
    worker.Stop();  // self-stop from the worker thread
    stopped.set_value();
  }));
  // Returns without deadlocking: the self-Stop skipped the join.
  future.get();
  // A subsequent external Stop joins the thread that quit on its own.
  worker.Stop();
  EXPECT_FALSE(worker.IsOwnerThread());
}

} // namespace aegisgate::runtime