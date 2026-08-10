#include <chrono>

#include <gtest/gtest.h>

#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/TimerQueue.h"

namespace aegisgate::net {
namespace {

TEST(TimerQueueTest, FiresDueCallbackAndCanStopTheLoop) {
  EventLoop loop;
  TimerQueue timers(loop);
  bool fired = false;

  (void)timers.ScheduleAfter(std::chrono::milliseconds(1), [&] {
    fired = true;
    loop.Quit();
  });
  loop.Loop();

  EXPECT_TRUE(fired);
}

TEST(TimerQueueTest, CancelPreventsCallbackWhileLaterTimerStillFires) {
  EventLoop loop;
  TimerQueue timers(loop);
  bool cancelled_fired = false;
  bool later_fired = false;

  const auto cancelled = timers.ScheduleAfter(std::chrono::milliseconds(1), [&] {
    cancelled_fired = true;
  });
  EXPECT_TRUE(timers.Cancel(cancelled));
  (void)timers.ScheduleAfter(std::chrono::milliseconds(2), [&] {
    later_fired = true;
    loop.Quit();
  });
  loop.Loop();

  EXPECT_FALSE(cancelled_fired);
  EXPECT_TRUE(later_fired);
  EXPECT_FALSE(timers.Cancel(cancelled));
}

TEST(TimerQueueTest, ReportsOnlyTimersThatRemainScheduled) {
  EventLoop loop;
  TimerQueue timers(loop);
  const auto first = timers.ScheduleAfter(std::chrono::seconds(1), [] {});
  const auto second = timers.ScheduleAfter(std::chrono::seconds(1), [] {});
  EXPECT_EQ(timers.PendingCount(), 2U);
  EXPECT_TRUE(timers.Cancel(first));
  EXPECT_EQ(timers.PendingCount(), 1U);
  EXPECT_TRUE(timers.Cancel(second));
  EXPECT_EQ(timers.PendingCount(), 0U);
}

TEST(TimerQueueTest, DueCallbackCanCancelAnotherTimerWithTheSameDeadline) {
  EventLoop loop;
  TimerQueue timers(loop);
  bool second_fired = false;
  TimerQueue::TimerId second = 0;
  const auto deadline = TimerQueue::Clock::now() + std::chrono::milliseconds(1);
  (void)timers.ScheduleAt(deadline, [&] {
    EXPECT_TRUE(timers.Cancel(second));
    loop.Quit();
  });
  second = timers.ScheduleAt(deadline, [&] { second_fired = true; });
  loop.Loop();
  EXPECT_FALSE(second_fired);
  EXPECT_EQ(timers.PendingCount(), 0U);
}

} // namespace
} // namespace aegisgate::net
