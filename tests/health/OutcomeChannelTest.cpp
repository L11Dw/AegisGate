#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/resource.h>
#include <poll.h>
#include <sys/wait.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "aegisgate/health/OutcomeChannel.h"

namespace aegisgate::health {
namespace {

using namespace std::chrono_literals;

// A published outcome is signaled on the channel's wake descriptor: the
// coordinator loop is woken to drain.  Tests poll it as a barrier that a
// publish happened (the counter is drained by the drain itself).
bool WaitForWake(int fd, std::chrono::steady_clock::time_point deadline) {
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
      deadline - std::chrono::steady_clock::now());
  pollfd descriptor{fd, POLLIN, 0};
  return ::poll(&descriptor, 1, remaining.count() > 0 ? static_cast<int>(remaining.count()) : 0) > 0;
}

std::chrono::steady_clock::time_point TestDeadline() {
  return std::chrono::steady_clock::now() + std::chrono::seconds(5);
}

AttemptResult ResultAt(std::size_t route, std::size_t endpoint, std::uint64_t sequence) {
  AttemptResult result;
  result.route_index = route;
  result.endpoint_index = endpoint;
  result.permit.probe = (sequence % 2) == 1;
  result.permit.generation = 7;
  result.permit.probe_id = sequence;
  result.success = (sequence % 3) != 0;
  return result;
}

// The credit pool is bounded by the channel capacity: reservations consume,
// consumption restores, and the ring cannot hold more than capacity.
TEST(OutcomeChannelTest, ReserveConsumesAndPublishRestoresCredit) {
  OutcomeChannel channel(/*capacity=*/3);
  auto first = channel.TryReserve();
  auto second = channel.TryReserve();
  auto third = channel.TryReserve();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(third.has_value());
  EXPECT_EQ(channel.pending(), 0U);  // reserved, not yet published

  // Capacity exhausted: the fourth reservation is refused and counted.
  EXPECT_FALSE(channel.TryReserve().has_value());
  EXPECT_EQ(channel.rejected(), 1U);
  EXPECT_FALSE(channel.TryReserve().has_value());
  EXPECT_EQ(channel.rejected(), 2U);

  first->Publish(ResultAt(0, 0, 1));
  second->Publish(ResultAt(0, 0, 2));
  EXPECT_EQ(channel.pending(), 2U);

  std::vector<std::uint64_t> consumed;
  EXPECT_EQ(channel.DrainOnCoordinatorLoop([&](const AttemptResult &result) {
    consumed.push_back(result.permit.probe_id);
  }), 2U);
  EXPECT_EQ(consumed, (std::vector<std::uint64_t>{1, 2}));
  // Each consumed result restored one credit.
  EXPECT_TRUE(channel.TryReserve().has_value());
  third->Cancel();
}

// The drain delivers exactly the published results, in FIFO order; a single
// producer's publishes are serialized, so its order is preserved within the
// ring.
TEST(OutcomeChannelTest, PublishThenDrainDeliversInOrder) {
  constexpr std::size_t kCount = 6;
  OutcomeChannel channel(/*capacity=*/kCount);
  std::vector<OutcomeChannel::Reservation> reservations;
  reservations.reserve(kCount);
  for (std::size_t index = 0; index < kCount; ++index) {
    auto reservation = channel.TryReserve();
    ASSERT_TRUE(reservation.has_value());
    reservation->Publish(ResultAt(0, 0, static_cast<std::uint64_t>(index)));
    reservations.push_back(std::move(*reservation));
  }
  // Every publish signalled the wake descriptor.
  EXPECT_TRUE(WaitForWake(channel.WakeFd(), TestDeadline()));

  std::vector<std::uint64_t> consumed;
  const std::size_t drained = channel.DrainOnCoordinatorLoop([&](const AttemptResult &result) {
    consumed.push_back(result.permit.probe_id);
  });
  EXPECT_EQ(drained, kCount);
  std::vector<std::uint64_t> expected;
  for (std::size_t index = 0; index < kCount; ++index) expected.push_back(index);
  EXPECT_EQ(consumed, expected);
}

// Two producers publish concurrently; the single-consumer drain delivers every
// result exactly once and preserves each producer's own order.
TEST(OutcomeChannelTest, ConcurrentPublishDeliversExactlyOnce) {
  constexpr std::size_t kPerProducer = 8;
  constexpr std::size_t kCount = kPerProducer * 2;
  OutcomeChannel channel(/*capacity=*/kCount);
  std::vector<OutcomeChannel::Reservation> reservations;
  for (std::size_t index = 0; index < kCount; ++index) {
    auto reservation = channel.TryReserve();
    ASSERT_TRUE(reservation.has_value());
    reservations.push_back(std::move(*reservation));
  }

  std::thread first([&] {
    for (std::size_t index = 0; index < kPerProducer; ++index) {
      reservations[index].Publish(ResultAt(0, 0, static_cast<std::uint64_t>(index)));
    }
  });
  std::thread second([&] {
    for (std::size_t index = 0; index < kPerProducer; ++index) {
      reservations[kPerProducer + index].Publish(ResultAt(0, 0,
                                                          static_cast<std::uint64_t>(100 + index)));
    }
  });
  first.join();
  second.join();

  std::vector<std::uint64_t> consumed;
  const std::size_t drained = channel.DrainOnCoordinatorLoop([&](const AttemptResult &result) {
    consumed.push_back(result.permit.probe_id);
  });
  EXPECT_EQ(drained, kCount);
  // Every result delivered exactly once.
  std::set<std::uint64_t> seen(consumed.begin(), consumed.end());
  EXPECT_EQ(seen.size(), kCount);
  // Each producer's own publishes arrive in order.
  std::vector<std::uint64_t> first_stream, second_stream;
  for (const std::uint64_t id : consumed) {
    if (id < 100) {
      first_stream.push_back(id);
    } else {
      second_stream.push_back(id);
    }
  }
  EXPECT_EQ(first_stream, (std::vector<std::uint64_t>{0, 1, 2, 3, 4, 5, 6, 7}));
  std::vector<std::uint64_t> second_expected;
  for (std::size_t index = 0; index < kPerProducer; ++index) second_expected.push_back(100 + index);
  EXPECT_EQ(second_stream, second_expected);
}

// Cancel returns the credit without enqueuing anything; the breaker never sees
// the outcome.
TEST(OutcomeChannelTest, CancelReturnsCreditWithoutPublish) {
  OutcomeChannel channel(/*capacity=*/2);
  auto first = channel.TryReserve();
  auto second = channel.TryReserve();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  first->Cancel();
  second->Cancel();

  std::size_t drained = 0;
  EXPECT_EQ(channel.DrainOnCoordinatorLoop([&](const AttemptResult &) { ++drained; }), 0U);
  EXPECT_EQ(drained, 0U);
  EXPECT_EQ(channel.pending(), 0U);
  // Both credits restored; hold the reservations so the capacity check is
  // meaningful (a discarded reservation returns its credit on destruction).
  auto third = channel.TryReserve();
  auto fourth = channel.TryReserve();
  ASSERT_TRUE(third.has_value());
  ASSERT_TRUE(fourth.has_value());
  EXPECT_FALSE(channel.TryReserve().has_value());
}

// Reservations are move-only; an unpublished reservation returns its credit on
// destruction no matter how it was moved.
TEST(OutcomeChannelTest, ReservationMoveOnlyAndAutoCancel) {
  OutcomeChannel channel(/*capacity=*/1);
  auto reservation = channel.TryReserve();
  ASSERT_TRUE(reservation.has_value());

  OutcomeChannel::Reservation moved(std::move(*reservation));
  EXPECT_FALSE(reservation->operator bool());  // moved-from is empty

  // An empty reservation is inert.
  moved.Publish(ResultAt(0, 0, 1));
  EXPECT_EQ(channel.pending(), 1U);
  EXPECT_EQ(channel.DrainOnCoordinatorLoop([&](const AttemptResult &) {}), 1U);

  // Drop an unpublished reservation: the destructor returns the credit.
  {
    auto held = channel.TryReserve();
    ASSERT_TRUE(held.has_value());
  }
  auto after = channel.TryReserve();
  ASSERT_TRUE(after.has_value());
  EXPECT_FALSE(channel.TryReserve().has_value());  // capacity exhausted again
}

// Once stopping, no new reservation is issued; published results already in
// the ring are still drainable.
TEST(OutcomeChannelTest, BeginStoppingRejectsNewReserve) {
  OutcomeChannel channel(/*capacity=*/2);
  auto first = channel.TryReserve();
  ASSERT_TRUE(first.has_value());
  first->Publish(ResultAt(0, 0, 9));
  channel.BeginStopping();

  EXPECT_FALSE(channel.TryReserve().has_value());
  EXPECT_EQ(channel.rejected(), 1U);
  std::vector<std::uint64_t> consumed;
  EXPECT_EQ(channel.DrainOnCoordinatorLoop([&](const AttemptResult &result) {
    consumed.push_back(result.permit.probe_id);
  }), 1U);
  EXPECT_EQ(consumed, (std::vector<std::uint64_t>{9}));
}

// A retry must be able to reserve a second outcome before the first is
// drained: the capacity therefore counts retry attempts
// (max_inflight x (1 + retry_budget)), not just in-flight requests.
TEST(OutcomeChannelTest, RetryCanReserveSecondOutcomeBeforeFirstIsDrained) {
  // max_inflight=1, retry_budget=1 -> capacity 2.
  const std::size_t capacity = OutcomeChannel::CapacityForRoute(
      /*max_inflight=*/1, /*retry_budget=*/1);
  EXPECT_EQ(capacity, 2U);
  OutcomeChannel channel(capacity);

  auto first = channel.TryReserve();
  ASSERT_TRUE(first.has_value());
  first->Publish(ResultAt(0, 0, 1));
  // The first result is published but not yet drained; the retry attempt must
  // still get a fresh reservation instead of being blocked into a 502.
  auto retry = channel.TryReserve();
  ASSERT_TRUE(retry.has_value());
  retry->Publish(ResultAt(0, 0, 2));

  std::vector<std::uint64_t> consumed;
  EXPECT_EQ(channel.DrainOnCoordinatorLoop([&](const AttemptResult &result) {
    consumed.push_back(result.permit.probe_id);
  }), 2U);
  EXPECT_EQ(consumed, (std::vector<std::uint64_t>{1, 2}));
  // Both credits restored; hold the reservations so the exhaustion check is
  // meaningful.
  auto held_first = channel.TryReserve();
  auto held_second = channel.TryReserve();
  ASSERT_TRUE(held_first.has_value());
  ASSERT_TRUE(held_second.has_value());
  EXPECT_FALSE(channel.TryReserve().has_value());
}

// Capacity is computed in uint64 and rejected when it overflows or exceeds the
// global safety ceiling.
TEST(OutcomeChannelTest, CapacityOverflowRejected) {
  EXPECT_EQ(OutcomeChannel::CapacityForRoute(/*max_inflight=*/2, /*retry_budget=*/0), 2U);
  EXPECT_EQ(OutcomeChannel::CapacityForRoute(2, 4), 10U);

  // max_inflight * (1 + retry_budget) exceeds the safety ceiling.
  EXPECT_THROW((void)OutcomeChannel::CapacityForRoute(1'048'576, 1), std::invalid_argument);
  // A uint32 overflow would corrupt the ceiling check if computed in 32 bits.
  EXPECT_THROW((void)OutcomeChannel::CapacityForRoute(0xFFFFFFFFU, 0xFFFFFFFFU),
               std::invalid_argument);
  EXPECT_THROW(OutcomeChannel(0), std::invalid_argument);
}

} // namespace


// R-065: a constructor failure before eventfd() must not close a reused
// descriptor through the partially-constructed State destructor (in particular
// stdin, fd 0).  wake_fd defaults to -1, so the destructor closes nothing.
// The failure injection runs in a forked child because exhausting RLIMIT_NOFILE
// in-process breaks a sanitizer runtime's own descriptors; under a sanitizer
// build the test is skipped (the -1 default is a plain code property).  The
// preprocessor guard lives inside one TEST so gtest_add_tests never sees a
// duplicate name.
TEST(OutcomeChannelTest, EventfdFailureDoesNotCloseStdin) {
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
  GTEST_SKIP() << "setrlimit-based fd exhaustion is incompatible with sanitizer runtimes";
#else
  const pid_t pid = ::fork();
  ASSERT_NE(pid, -1);
  if (pid == 0) {
    struct rlimit limit{};
    if (::getrlimit(RLIMIT_NOFILE, &limit) != 0) _exit(10);
    limit.rlim_cur = 0;  // exhaust the descriptor table so eventfd() fails
    if (::setrlimit(RLIMIT_NOFILE, &limit) != 0) _exit(11);
    try {
      (void)OutcomeChannel(8);
      _exit(12);  // construction unexpectedly succeeded
    } catch (const std::system_error &) {
    } catch (...) {
      _exit(13);
    }
    _exit(::fcntl(0, F_GETFD) == -1 ? 14 : 0);
  }
  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status)) << "child crashed during failure injection";
  EXPECT_EQ(WEXITSTATUS(status), 0) << "child reported " << WEXITSTATUS(status);
#endif
}

} // namespace aegisgate::health