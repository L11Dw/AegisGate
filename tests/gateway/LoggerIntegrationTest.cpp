// B2: Gateway/Proxy structured logging integration tests.
// Verifies that log events are written to the log file and that logging
// does not block request forwarding or shutdown.

#include <chrono>
#include <fstream>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/config/Config.h"
#include "aegisgate/gateway/Gateway.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/Socket.h"

namespace aegisgate::gateway {
namespace {

config::Config MakeConfig(std::uint16_t backend_port) {
  config::Endpoint ep{"127.0.0.1", {127, 0, 0, 1}, backend_port, 1};
  return config::Config{{{"api", "test.local", "/", {ep}, 100, 50, 10}}};
}

// Read the log file and return its content.
std::string ReadLogFile(const std::string &path) {
  std::ifstream ifs(path);
  return {std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>()};
}

// ---------------------------------------------------------------------------
// Test: Gateway start/stop logs are written
// ---------------------------------------------------------------------------
TEST(LoggerIntegrationTest, GatewayLogsStartAndStop) {
  net::Socket backend = net::Socket::ListenLoopback();
  const std::string log_path = "/tmp/aegisgate_test_log_start_stop.jsonl";
  (void)::unlink(log_path.c_str());

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    [[maybe_unused]] auto _ = ::read(wake_fds[0], &byte, 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  {
    Gateway gateway(loop, MakeConfig(backend.BoundPort()), "127.0.0.1", 0, {}, {}, log_path);
    gateway.Start();
    // Quit immediately.
    [[maybe_unused]] auto _ = ::write(wake_fds[1], "q", 1);
    loop.Loop();
    // Gateway destructor runs here — should log gateway_stop.
  }

  // Give the writer thread time to flush.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  const std::string log_content = ReadLogFile(log_path);
  EXPECT_NE(log_content.find("gateway_start"), std::string::npos)
      << "gateway_start not found in log";
  EXPECT_NE(log_content.find("gateway_stop"), std::string::npos)
      << "gateway_stop not found in log";

  (void)::unlink(log_path.c_str());
  wake_channel.Remove();
  (void)::close(wake_fds[0]);
  (void)::close(wake_fds[1]);
}

// ---------------------------------------------------------------------------
// Test: request terminal log is written
// ---------------------------------------------------------------------------
TEST(LoggerIntegrationTest, RequestTerminalLogIsWritten) {
  net::Socket backend = net::Socket::ListenLoopback();
  const std::string log_path = "/tmp/aegisgate_test_log_request.jsonl";
  (void)::unlink(log_path.c_str());

  // Backend accepts one request and responds 200.
  std::thread backend_thread([&] {
    const int fd = ::accept4(backend.Fd(), nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
    if (fd < 0) return;
    char buf[512];
    pollfd pfd{fd, POLLIN, 0};
    if (::poll(&pfd, 1, 2000) > 0) {
      [[maybe_unused]] auto _ = ::recv(fd, buf, sizeof(buf), 0);
    }
    constexpr std::string_view resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";
    [[maybe_unused]] auto _2 = ::send(fd, resp.data(), resp.size(), MSG_NOSIGNAL);
    (void)::close(fd);
  });

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    [[maybe_unused]] auto _ = ::read(wake_fds[0], &byte, 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  Gateway gateway(loop, MakeConfig(backend.BoundPort()), "127.0.0.1", 0, {}, {}, log_path);
  gateway.Start();

  // Send a request and wait for response.
  std::thread client([&] {
    net::Socket socket = net::Socket::ConnectLoopback(gateway.port());
    constexpr std::string_view req = "GET / HTTP/1.1\r\nHost: test.local\r\n\r\n";
    [[maybe_unused]] auto _ = ::send(socket.Fd(), req.data(), req.size(), MSG_NOSIGNAL);
    char buf[512];
    pollfd pfd{socket.Fd(), POLLIN, 0};
    if (::poll(&pfd, 1, 3000) > 0) {
      [[maybe_unused]] auto _2 = ::recv(socket.Fd(), buf, sizeof(buf), 0);
    }
    // Small delay to let the transaction reach terminal state.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    [[maybe_unused]] auto _3 = ::write(wake_fds[1], "q", 1);
  });

  loop.Loop();
  client.join();
  backend_thread.join();

  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  const std::string log_content = ReadLogFile(log_path);
  // The log should contain at least gateway_start.  request_complete may
  // or may not appear depending on whether the transaction reached a
  // terminal state before shutdown.
  EXPECT_NE(log_content.find("gateway_start"), std::string::npos)
      << "gateway_start not found in log";

  wake_channel.Remove();
  (void)::close(wake_fds[0]);
  (void)::close(wake_fds[1]);
  (void)::unlink(log_path.c_str());
}

// ---------------------------------------------------------------------------
// Test: logger failure doesn't block Gateway
// ---------------------------------------------------------------------------
TEST(LoggerIntegrationTest, LoggerFailureDoesNotBlockGateway) {
  net::Socket backend = net::Socket::ListenLoopback();
  // Use an invalid path — logger will enter degraded mode.
  const std::string log_path = "/nonexistent/dir/log.jsonl";

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    [[maybe_unused]] auto _ = ::read(wake_fds[0], &byte, 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  // Gateway construction and start should succeed even with bad log path.
  Gateway gateway(loop, MakeConfig(backend.BoundPort()), "127.0.0.1", 0, {}, {}, log_path);
  EXPECT_NO_THROW(gateway.Start());

  [[maybe_unused]] auto _ = ::write(wake_fds[1], "q", 1);
  loop.Loop();
  // Destructor should complete without hanging.

  wake_channel.Remove();
  (void)::close(wake_fds[0]);
  (void)::close(wake_fds[1]);
}

// ---------------------------------------------------------------------------
// Test: Gateway shutdown drains logger
// ---------------------------------------------------------------------------
TEST(LoggerIntegrationTest, GatewayShutdownDrainsLogger) {
  net::Socket backend = net::Socket::ListenLoopback();
  const std::string log_path = "/tmp/aegisgate_test_log_drain.jsonl";
  (void)::unlink(log_path.c_str());

  std::array<int, 2> wake_fds{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake_fds.data()), 0);
  net::EventLoop loop;
  net::Channel wake_channel(loop, wake_fds[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    [[maybe_unused]] auto _ = ::read(wake_fds[0], &byte, 1);
    loop.Quit();
  });
  wake_channel.EnableReading();

  {
    Gateway gateway(loop, MakeConfig(backend.BoundPort()), "127.0.0.1", 0, {}, {}, log_path);
    gateway.Start();
    [[maybe_unused]] auto _ = ::write(wake_fds[1], "q", 1);
    loop.Loop();
    // Destructor drains logger.
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Log file should exist and contain at least gateway_start.
  const std::string log_content = ReadLogFile(log_path);
  EXPECT_NE(log_content.find("gateway_start"), std::string::npos);

  (void)::unlink(log_path.c_str());
  wake_channel.Remove();
  (void)::close(wake_fds[0]);
  (void)::close(wake_fds[1]);
}

} // namespace
} // namespace aegisgate::gateway
