#include <array>
#include <cerrno>
#include <chrono>
#include <memory>
#include <thread>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/net/ClientConnection.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"

namespace aegisgate::net {
namespace {

TEST(ClientConnectionTest, ResumesAndDeliversBufferedPipelinedRequest) {
  std::array<int, 2> sockets{};
  std::array<int, 2> wake_sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0,
                         wake_sockets.data()),
            0);

  constexpr std::string_view first_request = "GET /first HTTP/1.1\r\n\r\n";
  constexpr std::string_view second_request = "GET /second HTTP/1.1\r\n\r\n";
  const std::string requests = std::string(first_request) + std::string(second_request);

  EventLoop loop;
  int callback_count = 0;
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &request) {
        ++callback_count;
        EXPECT_TRUE(client.reading_paused());
        EXPECT_EQ(request.target,
                  callback_count == 1 ? "/first" : "/second");
        loop.Quit();
      });
  connection.Start();

  ASSERT_EQ(::write(sockets[1], requests.data(), requests.size()),
            static_cast<ssize_t>(requests.size()));
  loop.Loop();

  EXPECT_EQ(callback_count, 1);
  EXPECT_TRUE(connection.reading_paused());
  Channel wake_channel(loop, wake_sockets[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    EXPECT_EQ(::read(wake_sockets[0], &byte, 1), 1);
    connection.ResumeReading();
    loop.Quit();
  });
  wake_channel.EnableReading();
  ASSERT_EQ(::write(wake_sockets[1], "q", 1), 1);
  loop.Loop();

  EXPECT_EQ(callback_count, 2);
  EXPECT_TRUE(connection.reading_paused());
  connection.Close();
  wake_channel.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
  EXPECT_EQ(::close(wake_sockets[0]), 0);
  EXPECT_EQ(::close(wake_sockets[1]), 0);
}

TEST(ClientConnectionTest, DeliversSegmentedPostAndPausesBeforeCallback) {
  std::array<int, 2> sockets{};
  std::array<int, 2> wake_sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0,
                         wake_sockets.data()),
            0);

  EventLoop loop;
  int callback_count = 0;
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &request) {
        ++callback_count;
        EXPECT_EQ(request.method, "POST");
        EXPECT_EQ(request.target, "/submit");
        EXPECT_EQ(request.Header("host"), "example.test");
        EXPECT_EQ(request.body, "hello");
        EXPECT_TRUE(client.reading_paused());
        loop.Quit();
      });
  connection.Start();

  constexpr std::string_view first_segment =
      "POST /submit HTTP/1.1\r\nHost: example.test\r\nContent-Length: 5\r\n\r\nhe";
  ASSERT_EQ(::write(sockets[1], first_segment.data(), first_segment.size()),
            static_cast<ssize_t>(first_segment.size()));
  std::thread append_body([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(::write(sockets[1], "llo", 3), 3);
  });
  loop.Loop();
  append_body.join();

  EXPECT_EQ(callback_count, 1);
  EXPECT_TRUE(connection.reading_paused());
  constexpr std::string_view second_request = "GET /next HTTP/1.1\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], second_request.data(), second_request.size()),
            static_cast<ssize_t>(second_request.size()));
  Channel wake_channel(loop, wake_sockets[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    EXPECT_EQ(::read(wake_sockets[0], &byte, 1), 1);
    loop.Quit();
  });
  wake_channel.EnableReading();
  std::thread wake_loop([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(::write(wake_sockets[1], "q", 1), 1);
  });
  loop.Loop();
  wake_loop.join();
  EXPECT_EQ(callback_count, 1);

  connection.Close();
  wake_channel.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
  EXPECT_EQ(::close(wake_sockets[0]), 0);
  EXPECT_EQ(::close(wake_sockets[1]), 0);
}

TEST(ClientConnectionTest, CallbackCanDestroyOwnerDuringRead) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  std::unique_ptr<ClientConnection> connection;
  bool callback_called = false;
  connection = std::make_unique<ClientConnection>(
      loop, sockets[0], [&](ClientConnection &, const http::HttpRequest &request) {
        connection.reset();
        EXPECT_EQ(request.method, "POST");
        EXPECT_EQ(request.body, "hello");
        callback_called = true;
        loop.Quit();
      });
  connection->Start();

  constexpr std::string_view request =
      "POST /destroy HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()),
            static_cast<ssize_t>(request.size()));
  loop.Loop();

  EXPECT_TRUE(callback_called);
  EXPECT_EQ(connection, nullptr);
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, CallbackCanDestroyOwnerDuringResumeReading) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  std::unique_ptr<ClientConnection> connection;
  int callback_count = 0;
  connection = std::make_unique<ClientConnection>(
      loop, sockets[0], [&](ClientConnection &, const http::HttpRequest &request) {
        ++callback_count;
        if (callback_count == 1) {
          EXPECT_EQ(request.method, "GET");
          EXPECT_EQ(request.body, "");
          loop.Quit();
          return;
        }
        connection.reset();
        EXPECT_EQ(request.method, "POST");
        EXPECT_EQ(request.body, "hello");
        loop.Quit();
      });
  connection->Start();

  constexpr std::string_view requests =
      "GET /first HTTP/1.1\r\n\r\n"
      "POST /destroy HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello";
  ASSERT_EQ(::write(sockets[1], requests.data(), requests.size()),
            static_cast<ssize_t>(requests.size()));
  loop.Loop();

  ASSERT_EQ(callback_count, 1);
  ASSERT_NE(connection, nullptr);
  connection->ResumeReading();

  EXPECT_EQ(callback_count, 2);
  EXPECT_EQ(connection, nullptr);
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, CloseIsIdempotent) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  ClientConnection connection(loop, sockets[0],
                              [](ClientConnection &, const http::HttpRequest &) {});
  connection.Start();
  connection.Close();
  connection.Close();

  errno = 0;
  EXPECT_EQ(::fcntl(sockets[0], F_GETFD), -1);
  EXPECT_EQ(errno, EBADF);
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, ParserErrorClosesConnection) {
  std::array<int, 2> sockets{};
  std::array<int, 2> wake_sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0,
                         wake_sockets.data()),
            0);

  EventLoop loop;
  int callback_count = 0;
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &, const http::HttpRequest &) {
        ++callback_count;
      });
  connection.Start();
  Channel wake_channel(loop, wake_sockets[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    EXPECT_EQ(::read(wake_sockets[0], &byte, 1), 1);
    loop.Quit();
  });
  wake_channel.EnableReading();
  constexpr std::string_view invalid_request = "GET / HTTP/1.0\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], invalid_request.data(), invalid_request.size()),
            static_cast<ssize_t>(invalid_request.size()));

  std::thread quit_loop([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(::write(wake_sockets[1], "q", 1), 1);
  });
  loop.Loop();
  quit_loop.join();

  EXPECT_EQ(callback_count, 0);
  errno = 0;
  EXPECT_EQ(::fcntl(sockets[0], F_GETFD), -1);
  EXPECT_EQ(errno, EBADF);
  wake_channel.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
  EXPECT_EQ(::close(wake_sockets[0]), 0);
  EXPECT_EQ(::close(wake_sockets[1]), 0);
}

} // namespace
} // namespace aegisgate::net
