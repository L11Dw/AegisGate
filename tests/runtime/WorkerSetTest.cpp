#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/runtime/WorkerSet.h"

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

TEST(WorkerSetTest, RejectsZeroWorkers) {
  EXPECT_THROW(WorkerSet(0), std::invalid_argument);
}

TEST(WorkerSetTest, RoundRobinDistributesEvenly) {
  WorkerSet set(3);
  std::array<std::size_t, 3> counts{};
  for (int index = 0; index != 7; ++index) {
    const WorkerSet::WorkerHandle handle = set.Next();
    ++counts[handle.index];
    EXPECT_EQ(&handle.worker, &set.At(handle.index));
  }
  EXPECT_EQ(counts, (std::array<std::size_t, 3>{3, 2, 2}));
}

TEST(WorkerSetTest, RunsPostedTasksOnEveryWorker) {
  std::array<int, 2> done{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, done.data()), 0);
  std::string error;
  constexpr std::size_t kWorkers = 3;
  std::atomic_int ran{0};
  WorkerSet set(kWorkers);
  set.Start();
  for (std::size_t index = 0; index != kWorkers; ++index) {
    ASSERT_TRUE(set.At(index).Post([&] {
      ++ran;
      (void)test::SignalWakeFd(done[1], 'd', error);
    }));
  }
  for (std::size_t index = 0; index != kWorkers; ++index) {
    ASSERT_TRUE(WaitFor(done[0], POLLIN, TestDeadline()));
    char byte = '\0';
    ASSERT_EQ(::read(done[0], &byte, 1), 1);
  }
  set.StopAll();
  set.StopAll();  // idempotent
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(ran.load(), static_cast<int>(kWorkers));
  EXPECT_EQ(::close(done[0]), 0);
  EXPECT_EQ(::close(done[1]), 0);
}

TEST(WorkerSetTest, StopAllDrainsAcceptedTasks) {
  std::array<int, 2> done{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, done.data()), 0);
  std::string error;
  std::atomic_int ran{0};
  WorkerSet set(2);
  set.Start();
  for (std::size_t index = 0; index != 4; ++index) {
    ASSERT_TRUE(set.At(index % 2).Post([&] {
      ++ran;
      (void)test::SignalWakeFd(done[1], 'd', error);
    }));
  }
  set.StopAll();
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(ran.load(), 4);
  EXPECT_EQ(::close(done[0]), 0);
  EXPECT_EQ(::close(done[1]), 0);
}

} // namespace
// R-067: a partial Start must stop (drain + join) the workers already started
// before rethrowing, leaving no running worker behind.
TEST(WorkerSetTest, PartialStartRollsBackStartedWorkers) {
  int calls = 0;
  WorkerSet set(2, [&calls]() -> std::unique_ptr<WorkerRuntime> {
    ++calls;
    if (calls == 2) {
      auto worker = std::make_unique<WorkerRuntime>();
      worker->Stop();  // its Start() throws "worker already stopped"
      return worker;
    }
    return std::make_unique<WorkerRuntime>();
  });
  EXPECT_THROW(set.Start(), std::logic_error);
  // The first worker was rolled back: it rejects new tasks.
  EXPECT_FALSE(set.At(0).Post([] {}));
  // Destructor (StopAll) remains safe over the rolled-back set.
}

} // namespace aegisgate::runtime

