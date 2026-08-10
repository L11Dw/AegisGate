#include "aegisgate/mock/MockBackend.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <string>
#include <string_view>
#include <thread>

#include <gtest/gtest.h>

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/Socket.h"

namespace aegisgate::mock {
namespace {

using Deadline = std::chrono::steady_clock::time_point;

Deadline TestDeadline() { return std::chrono::steady_clock::now() + std::chrono::seconds(3); }

int RemainingMilliseconds(Deadline deadline) {
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
  return remaining.count() > 0 ? static_cast<int>(remaining.count()) : 0;
}

bool WaitFor(int fd, short events, Deadline deadline) {
  pollfd descriptor{fd, events, 0};
  return ::poll(&descriptor, 1, RemainingMilliseconds(deadline)) > 0;
}

bool WriteAll(int fd, std::string_view bytes, Deadline deadline, std::string &error) {
  std::size_t sent = 0;
  while (sent < bytes.size()) {
    const ssize_t count = ::write(fd, bytes.data() + sent, bytes.size() - sent);
    if (count > 0) { sent += static_cast<std::size_t>(count); continue; }
    if (count < 0 && errno == EINTR) continue;
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) && WaitFor(fd, POLLOUT, deadline)) continue;
    error = "write failed";
    return false;
  }
  return true;
}

std::string ReadExact(int fd, std::size_t size, Deadline deadline, std::string &error) {
  std::string result(size, '\0');
  std::size_t received = 0;
  while (received < size) {
    if (!WaitFor(fd, POLLIN | POLLHUP, deadline)) { error = "read timed out"; return {}; }
    const ssize_t count = ::read(fd, result.data() + received, size - received);
    if (count > 0) { received += static_cast<std::size_t>(count); continue; }
    if (count < 0 && errno == EINTR) continue;
    error = "unexpected EOF or read error";
    return {};
  }
  return result;
}

template <typename Client>
void RunWithBackend(const MockBackendOptions &options, Client client) {
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
  MockBackend backend(loop, options, "127.0.0.1", 0);
  backend.Start();
  std::thread client_thread([&] { client(backend.port(), wake[1]); });
  loop.Loop();
  client_thread.join();
  wake_channel.Remove();
  EXPECT_EQ(::close(wake[0]), 0);
  EXPECT_EQ(::close(wake[1]), 0);
}

TEST(MockBackendTest, ReturnsConfigured5xxOverRealTcp) {
  std::string error;
  std::string response;
  RunWithBackend(MockBackendOptions{.status = 503}, [&](std::uint16_t port, int wake_fd) {
    net::Socket socket = net::Socket::ConnectLoopback(port);
    constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: mock.test\r\n\r\n";
    constexpr std::string_view expected = "HTTP/1.1 503 Mock Failure\r\nContent-Length: 0\r\n\r\n";
    if (WriteAll(socket.Fd(), request, TestDeadline(), error)) response = ReadExact(socket.Fd(), expected.size(), TestDeadline(), error);
    if (::write(wake_fd, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(response, "HTTP/1.1 503 Mock Failure\r\nContent-Length: 0\r\n\r\n");
}

TEST(MockBackendTest, ResetsConnectionWithoutWritingAnHttpResponse) {
  std::string error;
  ssize_t received = -2;
  RunWithBackend(MockBackendOptions{.reset = true}, [&](std::uint16_t port, int wake_fd) {
    net::Socket socket = net::Socket::ConnectLoopback(port);
    constexpr std::string_view request = "GET / HTTP/1.1\r\nHost: mock.test\r\n\r\n";
    if (WriteAll(socket.Fd(), request, TestDeadline(), error)) {
      if (!WaitFor(socket.Fd(), POLLIN | POLLHUP, TestDeadline())) error = "reset timed out";
      else { char byte = '\0'; received = ::read(socket.Fd(), &byte, 1); }
    }
    if (::write(wake_fd, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(received, 0);
}

TEST(MockBackendTest, DelaysNonblockinglyAndRejectsTheSecondRequestAtConcurrencyCap) {
  std::string error;
  std::string rejected;
  RunWithBackend(MockBackendOptions{.delay = std::chrono::milliseconds(400), .max_inflight = 1},
                 [&](std::uint16_t port, int wake_fd) {
    net::Socket first = net::Socket::ConnectLoopback(port);
    constexpr std::string_view first_request = "GET /slow HTTP/1.1\r\nHost: mock.test\r\n\r\n";
    if (!WriteAll(first.Fd(), first_request, TestDeadline(), error)) {
      (void)::write(wake_fd, "q", 1);
      return;
    }
    // poll is a bounded observation, not a sleep: it proves the timer has
    // deferred the first response before the next request is admitted.
    if (WaitFor(first.Fd(), POLLIN | POLLHUP,
                std::chrono::steady_clock::now() + std::chrono::milliseconds(75))) {
      error = "delayed response arrived too early";
      (void)::write(wake_fd, "q", 1);
      return;
    }
    net::Socket second = net::Socket::ConnectLoopback(port);
    constexpr std::string_view second_request = "GET /other HTTP/1.1\r\nHost: mock.test\r\n\r\n";
    constexpr std::string_view expected = "HTTP/1.1 503 Mock Capacity\r\nContent-Length: 0\r\n\r\n";
    if (WriteAll(second.Fd(), second_request, TestDeadline(), error)) {
      rejected = ReadExact(second.Fd(), expected.size(), TestDeadline(), error);
    }
    if (::write(wake_fd, "q", 1) != 1 && error.empty()) error = "wake failed";
  });
  EXPECT_TRUE(error.empty()) << error;
  EXPECT_EQ(rejected, "HTTP/1.1 503 Mock Capacity\r\nContent-Length: 0\r\n\r\n");
}

TEST(MockBackendTest, ValidatesStartupOptions) {
  net::EventLoop loop;
  EXPECT_THROW((MockBackend(loop, MockBackendOptions{.status = 99}, "127.0.0.1", 0)), std::invalid_argument);
  EXPECT_THROW((MockBackend(loop, MockBackendOptions{.status = 100}, "127.0.0.1", 0)), std::invalid_argument);
  EXPECT_THROW((MockBackend(loop, MockBackendOptions{.max_inflight = 0}, "127.0.0.1", 0)), std::invalid_argument);
}

} // namespace
} // namespace aegisgate::mock
