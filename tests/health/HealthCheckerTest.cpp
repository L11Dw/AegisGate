#include "aegisgate/health/HealthChecker.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/config/Config.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/Socket.h"
#include "aegisgate/net/TimerQueue.h"

#include "support/WakeFd.h"

namespace aegisgate::health {
namespace {

using Deadline = std::chrono::steady_clock::time_point;

Deadline TestDeadline() { return std::chrono::steady_clock::now() + std::chrono::seconds(5); }

int RemainingMilliseconds(Deadline deadline) {
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
      deadline - std::chrono::steady_clock::now());
  return remaining.count() > 0 ? static_cast<int>(remaining.count()) : 0;
}

int AcceptBlocking(const net::Socket &listener, Deadline deadline) {
  for (;;) {
    const int fd = listener.Accept();
    if (fd >= 0) {
      const int flags = ::fcntl(fd, F_GETFL);
      if (flags >= 0) (void)::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
      return fd;
    }
    pollfd descriptor{listener.Fd(), POLLIN, 0};
    if (::poll(&descriptor, 1, RemainingMilliseconds(deadline)) <= 0) return -1;
  }
}

// One loopback backend answering each accepted connection from a sequence of
// responses; an empty response holds the connection open without writing (a
// slow backend) until the worker observes the stop signal.  All I/O failures
// are recorded into the caller's error string (mutex-protected) and asserted
// by the main thread after Stop(); no GTest assertions run on the worker
// thread, and no fixed sleeps pace the worker.
class SequencedBackend {
public:
  explicit SequencedBackend(std::vector<std::string> responses, std::string &error)
      : listener_(net::Socket::ListenLoopback()), responses_(std::move(responses)),
        error_(&error) {
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, stop_fds_.data()) != 0) {
      throw std::system_error(errno, std::generic_category(), "socketpair backend stop");
    }
    thread_ = std::thread([this] { Serve(); });
  }

  ~SequencedBackend() {
    Stop();
    listener_.Close();
  }

  // Signals the worker to release any held connection and exit, then joins
  // and releases the stop descriptors.  Idempotent: a second call (e.g. from
  // the destructor) is a no-op.  The signal write goes through a local string
  // and is merged into the shared error under the mutex, never written
  // concurrently with the worker's Fail().
  void Stop() {
    if (stop_fds_[1] < 0) return;
    std::string local_error;
    (void)test::SignalWakeFd(stop_fds_[1], 's', local_error);
    if (!local_error.empty()) Fail(std::move(local_error));
    if (thread_.joinable()) thread_.join();
    (void)::close(stop_fds_[0]);
    (void)::close(stop_fds_[1]);
    stop_fds_[0] = -1;
    stop_fds_[1] = -1;
  }

  [[nodiscard]] std::uint16_t port() const { return listener_.BoundPort(); }

private:
  void Fail(std::string message) {
    std::lock_guard<std::mutex> guard(mutex_);
    error_->append(std::move(message)).append("; ");
  }

  // Waits up to the deadline for the stop byte and validates it: only an
  // actual 's' counts as a stop request; poll timeouts are treated as a
  // bounded no-stop, and any other outcome is reported through Fail().
  bool StopRequested() {
    for (;;) {
      pollfd descriptor{stop_fds_[0], POLLIN, 0};
      const int ready = ::poll(&descriptor, 1, 3000);
      if (ready > 0) break;
      if (ready < 0 && errno == EINTR) continue;
      return false;
    }
    char byte = '\0';
    const ssize_t count = ::read(stop_fds_[0], &byte, 1);
    if (count == 1 && byte == 's') return true;
    if (count == 0) {
      Fail("stop fd closed unexpectedly");
    } else if (count < 0) {
      Fail("stop fd read failed");
    } else {
      Fail("unexpected stop byte");
    }
    return true;
  }

  void Serve() {
    for (const auto &response : responses_) {
      const int fd = AcceptBlocking(listener_, TestDeadline());
      if (fd < 0) {
        Fail("accept timed out");
        return;
      }
      std::array<char, 512> request{};
      if (::read(fd, request.data(), request.size()) < 0 && errno != EINTR) {
        Fail("read failed");
      }
      if (response.empty()) {
        // A slow backend: hold the connection without writing until the
        // worker observes the stop byte (consumed and validated inside
        // StopRequested).
        (void)StopRequested();
      } else if (::write(fd, response.data(), response.size()) !=
                 static_cast<ssize_t>(response.size())) {
        Fail("write failed");
      }
      (void)::close(fd);
    }
  }

  net::Socket listener_;
  std::vector<std::string> responses_;
  std::string *error_;
  std::mutex mutex_;
  std::array<int, 2> stop_fds_{-1, -1};
  std::thread thread_;
};

config::Endpoint Endpoint(std::uint16_t port) {
  return {"127.0.0.1", {127, 0, 0, 1}, port, 1};
}

TEST(HealthCheckerTest, HealthyOn2xxAndRecoversFromFailure) {
  std::string backend_error;
  SequencedBackend backend(
      {"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
       "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"},
      backend_error);
  std::array<int, 2> wake{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(wake[0], &byte, 1), 1);
    loop.Quit();
  });
  wake_channel.EnableReading();
  net::TimerQueue timers(loop);

  std::string wake_error;
  std::vector<bool> results;
  HealthChecker checker(loop, timers, Endpoint(backend.port()),
                        {std::chrono::milliseconds(50), std::chrono::milliseconds(150)},
                        [&](bool healthy) {
    results.push_back(healthy);
    if (results.size() == 2) {
      (void)test::SignalWakeFd(wake[1], 'q', wake_error);
    }
  });
  checker.Start();
  loop.Loop();
  backend.Stop();
  wake_channel.Remove();
  EXPECT_EQ(::close(wake[0]), 0);
  EXPECT_EQ(::close(wake[1]), 0);

  EXPECT_TRUE(wake_error.empty()) << wake_error;
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  ASSERT_EQ(results.size(), 2U);
  EXPECT_FALSE(results[0]);
  EXPECT_TRUE(results[1]);
}

TEST(HealthCheckerTest, RequiresContentLengthForHealthy) {
  std::string backend_error;
  SequencedBackend backend(
      {"HTTP/1.1 204 No Content\r\n\r\n",
       "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"},
      backend_error);
  std::array<int, 2> wake{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(wake[0], &byte, 1), 1);
    loop.Quit();
  });
  wake_channel.EnableReading();
  net::TimerQueue timers(loop);

  std::string wake_error;
  std::vector<bool> results;
  HealthChecker checker(loop, timers, Endpoint(backend.port()),
                        {std::chrono::milliseconds(50), std::chrono::milliseconds(150)},
                        [&](bool healthy) {
    results.push_back(healthy);
    if (results.size() == 2) {
      (void)test::SignalWakeFd(wake[1], 'q', wake_error);
    }
  });
  checker.Start();
  loop.Loop();
  backend.Stop();
  wake_channel.Remove();
  EXPECT_EQ(::close(wake[0]), 0);
  EXPECT_EQ(::close(wake[1]), 0);

  EXPECT_TRUE(wake_error.empty()) << wake_error;
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  ASSERT_EQ(results.size(), 2U);
  EXPECT_FALSE(results[0]);
  EXPECT_TRUE(results[1]);
}

TEST(HealthCheckerTest, TimesOutSlowCheckAsUnhealthy) {
  std::string backend_error;
  SequencedBackend backend({""}, backend_error);
  std::array<int, 2> wake{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(wake[0], &byte, 1), 1);
    loop.Quit();
  });
  wake_channel.EnableReading();
  net::TimerQueue timers(loop);

  std::string wake_error;
  std::vector<bool> results;
  HealthChecker checker(loop, timers, Endpoint(backend.port()),
                        {std::chrono::milliseconds(500), std::chrono::milliseconds(100)},
                        [&](bool healthy) {
    results.push_back(healthy);
    if (results.size() == 1) {
      (void)test::SignalWakeFd(wake[1], 'q', wake_error);
    }
  });
  checker.Start();
  loop.Loop();
  backend.Stop();
  wake_channel.Remove();
  EXPECT_EQ(::close(wake[0]), 0);
  EXPECT_EQ(::close(wake[1]), 0);

  EXPECT_TRUE(wake_error.empty()) << wake_error;
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  ASSERT_EQ(results.size(), 1U);
  EXPECT_FALSE(results[0]);
}

TEST(HealthCheckerTest, ProtocolErrorMarksUnhealthy) {
  std::string backend_error;
  SequencedBackend backend({"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"},
                           backend_error);
  std::array<int, 2> wake{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(wake[0], &byte, 1), 1);
    loop.Quit();
  });
  wake_channel.EnableReading();
  net::TimerQueue timers(loop);

  std::string wake_error;
  std::vector<bool> results;
  HealthChecker checker(loop, timers, Endpoint(backend.port()),
                        {std::chrono::milliseconds(500), std::chrono::milliseconds(150)},
                        [&](bool healthy) {
    results.push_back(healthy);
    if (results.size() == 1) {
      (void)test::SignalWakeFd(wake[1], 'q', wake_error);
    }
  });
  checker.Start();
  loop.Loop();
  backend.Stop();
  wake_channel.Remove();
  EXPECT_EQ(::close(wake[0]), 0);
  EXPECT_EQ(::close(wake[1]), 0);

  EXPECT_TRUE(wake_error.empty()) << wake_error;
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  ASSERT_EQ(results.size(), 1U);
  EXPECT_FALSE(results[0]);
}

TEST(HealthCheckerTest, DestroyedCheckerIgnoresStaleResults) {
  std::string backend_error;
  SequencedBackend backend({"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"}, backend_error);
  std::array<int, 2> wake{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake[0]);
  bool watchdog_fired = false;
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    if (::read(wake[0], &byte, 1) != 1 || byte != 'q') watchdog_fired = true;
    loop.Quit();
  });
  wake_channel.EnableReading();
  net::TimerQueue timers(loop);

  int callbacks = 0;
  auto checker = std::make_unique<HealthChecker>(
      loop, timers, Endpoint(backend.port()),
      HealthCheckConfig{std::chrono::milliseconds(500), std::chrono::milliseconds(150)},
      [&](bool) { ++callbacks; });
  checker->Start();
  checker.reset();
  (void)test::SignalWakeFd(wake[1], 'q', backend_error);
  loop.Loop();
  backend.Stop();
  wake_channel.Remove();
  EXPECT_FALSE(watchdog_fired);
  EXPECT_EQ(callbacks, 0);
  EXPECT_TRUE(backend_error.empty()) << backend_error;
  EXPECT_EQ(::close(wake[0]), 0);
  EXPECT_EQ(::close(wake[1]), 0);
}


TEST(HealthCheckerTest, RepeatedStopIsSafe) {
  std::string backend_error;
  SequencedBackend backend({"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"},
                           backend_error);
  std::array<int, 2> wake{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(wake[0], &byte, 1), 1);
    loop.Quit();
  });
  wake_channel.EnableReading();
  net::TimerQueue timers(loop);

  std::string wake_error;
  HealthChecker checker(loop, timers, Endpoint(backend.port()),
                        {std::chrono::milliseconds(500), std::chrono::milliseconds(150)},
                        [&](bool) { (void)test::SignalWakeFd(wake[1], 'q', wake_error); });
  checker.Start();
  loop.Loop();
  wake_channel.Remove();
  EXPECT_EQ(::close(wake[0]), 0);
  EXPECT_EQ(::close(wake[1]), 0);

  backend.Stop();  // First stop: signal, join, release descriptors.
  backend.Stop();  // Idempotent: must be a no-op.
  EXPECT_TRUE(wake_error.empty()) << wake_error;
  EXPECT_TRUE(backend_error.empty()) << backend_error;
}

} // namespace
} // namespace aegisgate::health
