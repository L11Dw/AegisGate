#include <array>
#include <cerrno>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/http/HttpResponse.h"
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

TEST(ClientConnectionTest, WritesResponseThenResumesReadingForNextRequest) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  int request_count = 0;
  std::string received;
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &request) {
        ++request_count;
        EXPECT_EQ(request.target, request_count == 1 ? "/first" : "/second");
        EXPECT_TRUE(client.reading_paused());
        if (request_count == 1) {
          client.SendResponse(http::HttpResponse{200, "OK", {}, "hello"});
        } else {
          loop.Quit();
        }
      });
  Channel client_reader(loop, sockets[1]);
  client_reader.SetReadCallback([&] {
    std::array<char, 128> bytes{};
    const ssize_t count = ::read(sockets[1], bytes.data(), bytes.size());
    if (count <= 0) {
      ADD_FAILURE() << "client observed EOF/error before the second request";
      loop.Quit();
      return;
    }
    received.append(bytes.data(), static_cast<std::size_t>(count));
    constexpr std::string_view expected =
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    if (received == expected) {
      constexpr std::string_view second_request = "GET /second HTTP/1.1\r\n\r\n";
      ASSERT_EQ(::write(sockets[1], second_request.data(), second_request.size()),
                static_cast<ssize_t>(second_request.size()));
    }
  });
  connection.Start();
  client_reader.EnableReading();

  constexpr std::string_view first_request = "GET /first HTTP/1.1\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], first_request.data(), first_request.size()),
            static_cast<ssize_t>(first_request.size()));
  loop.Loop();

  EXPECT_EQ(request_count, 2);
  EXPECT_EQ(received, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello");
  connection.Close();
  client_reader.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, ClosesAfterWritingConnectionCloseResponse) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  std::string received;
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &) {
        client.SendResponse(
            http::HttpResponse{200, "OK", {{"Connection", "close"}}, "bye"});
      });
  Channel client_reader(loop, sockets[1]);
  client_reader.SetReadCallback([&] {
    std::array<char, 128> bytes{};
    const ssize_t count = ::read(sockets[1], bytes.data(), bytes.size());
    if (count > 0) {
      received.append(bytes.data(), static_cast<std::size_t>(count));
      return;
    }
    EXPECT_EQ(count, 0);
    loop.Quit();
  });
  connection.Start();
  client_reader.EnableReading();

  constexpr std::string_view request = "GET /close HTTP/1.1\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()),
            static_cast<ssize_t>(request.size()));
  loop.Loop();

  EXPECT_EQ(received,
            "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 3\r\n\r\nbye");
  client_reader.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, ClosesAfterWritingResponseToConnectionCloseRequest) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  std::string received;
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &) {
        client.SendResponse(http::HttpResponse{200, "OK", {}, "bye"});
      });
  Channel client_reader(loop, sockets[1]);
  client_reader.SetReadCallback([&] {
    std::array<char, 128> bytes{};
    const ssize_t count = ::read(sockets[1], bytes.data(), bytes.size());
    if (count > 0) {
      received.append(bytes.data(), static_cast<std::size_t>(count));
      return;
    }
    EXPECT_EQ(count, 0);
    loop.Quit();
  });
  connection.Start();
  client_reader.EnableReading();

  constexpr std::string_view request =
      "GET /close HTTP/1.1\r\nConnection: close\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()),
            static_cast<ssize_t>(request.size()));
  loop.Loop();

  EXPECT_EQ(received, "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nbye");
  client_reader.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, DrainsLargeResponseAcrossWritableEvents) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  // This exceeds the default AF_UNIX stream send buffer, so the first write
  // must leave bytes for a later EPOLLOUT dispatch without relying on the
  // sandbox-restricted SO_SNDBUF socket option.
  const std::string body(1024 * 1024, 'x');
  const std::string expected = "HTTP/1.1 200 OK\r\nContent-Length: " +
                               std::to_string(body.size()) + "\r\n\r\n" + body;
  EventLoop loop;
  int request_count = 0;
  std::string received;
  received.reserve(expected.size());
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &) {
        ++request_count;
        client.SendResponse(http::HttpResponse{200, "OK", {}, body});
      });
  Channel client_reader(loop, sockets[1]);
  client_reader.SetReadCallback([&] {
    std::array<char, 16 * 1024> bytes{};
    const ssize_t count = ::read(sockets[1], bytes.data(), bytes.size());
    ASSERT_GT(count, 0);
    received.append(bytes.data(), static_cast<std::size_t>(count));
    if (received.size() == expected.size()) {
      loop.Quit();
    }
  });
  connection.Start();
  client_reader.EnableReading();

  constexpr std::string_view request = "GET /large HTTP/1.1\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()),
            static_cast<ssize_t>(request.size()));
  loop.Loop();

  EXPECT_EQ(request_count, 1);
  EXPECT_EQ(received, expected);
  connection.Close();
  client_reader.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, RespondsToCompleteRequestBeforePeerHalfClose) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  int request_count = 0;
  std::string received;
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &request) {
        ++request_count;
        EXPECT_EQ(request.target, "/half-close");
        client.SendResponse(http::HttpResponse{200, "OK", {}, "done"});
      });
  Channel client_reader(loop, sockets[1]);
  client_reader.SetReadCallback([&] {
    std::array<char, 128> bytes{};
    const ssize_t count = ::read(sockets[1], bytes.data(), bytes.size());
    if (count > 0) {
      received.append(bytes.data(), static_cast<std::size_t>(count));
      return;
    }
    EXPECT_EQ(count, 0);
    loop.Quit();
  });
  connection.Start();
  client_reader.EnableReading();

  constexpr std::string_view request = "GET /half-close HTTP/1.1\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()),
            static_cast<ssize_t>(request.size()));
  if (::shutdown(sockets[1], SHUT_WR) < 0) {
    if (errno == EPERM) {
      GTEST_SKIP() << "the restricted sandbox blocks shutdown(2)";
    }
    FAIL() << "shutdown failed with errno=" << errno;
  }
  loop.Loop();

  EXPECT_EQ(request_count, 1);
  EXPECT_EQ(received, "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone");
  client_reader.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, DrainsLargeResponseAfterPeerHalfClose) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  const std::string body(1024 * 1024, 'h');
  const std::string expected = "HTTP/1.1 200 OK\r\nContent-Length: " +
                               std::to_string(body.size()) + "\r\n\r\n" + body;
  EventLoop loop;
  std::string received;
  received.reserve(expected.size());
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &) {
        client.SendResponse(http::HttpResponse{200, "OK", {}, body});
      });
  Channel client_reader(loop, sockets[1]);
  client_reader.SetReadCallback([&] {
    std::array<char, 16 * 1024> bytes{};
    const ssize_t count = ::read(sockets[1], bytes.data(), bytes.size());
    if (count > 0) {
      received.append(bytes.data(), static_cast<std::size_t>(count));
      return;
    }
    EXPECT_EQ(count, 0);
    loop.Quit();
  });
  connection.Start();
  client_reader.EnableReading();

  constexpr std::string_view request = "GET /half-large HTTP/1.1\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()),
            static_cast<ssize_t>(request.size()));
  if (::shutdown(sockets[1], SHUT_WR) < 0) {
    if (errno == EPERM) {
      GTEST_SKIP() << "the restricted sandbox blocks shutdown(2)";
    }
    FAIL() << "shutdown failed with errno=" << errno;
  }
  loop.Loop();

  EXPECT_EQ(received, expected);
  client_reader.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, HalfClosedSlowPeerDoesNotStarveOtherReadyChannel) {
  std::array<int, 2> sockets{};
  std::array<int, 2> work_sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0,
                         work_sockets.data()),
            0);

  const std::string body(1024 * 1024, 's');
  const std::string expected = "HTTP/1.1 200 OK\r\nContent-Length: " +
                               std::to_string(body.size()) + "\r\n\r\n" + body;
  EventLoop loop;
  std::string received;
  received.reserve(expected.size());
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &) {
        client.SendResponse(http::HttpResponse{200, "OK", {}, body});
      });
  Channel client_reader(loop, sockets[1]);
  client_reader.SetReadCallback([&] {
    std::array<char, 16 * 1024> bytes{};
    const ssize_t count = ::read(sockets[1], bytes.data(), bytes.size());
    if (count > 0) {
      received.append(bytes.data(), static_cast<std::size_t>(count));
      return;
    }
    EXPECT_EQ(count, 0);
    loop.Quit();
  });
  bool work_called = false;
  Channel work_channel(loop, work_sockets[0]);
  work_channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(work_sockets[0], &byte, 1), 1);
    EXPECT_EQ(byte, 'w');
    work_called = true;
    client_reader.EnableReading();
  });
  connection.Start();
  work_channel.EnableReading();

  constexpr std::string_view request = "GET /half-starved HTTP/1.1\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()),
            static_cast<ssize_t>(request.size()));
  if (::shutdown(sockets[1], SHUT_WR) < 0) {
    if (errno == EPERM) {
      GTEST_SKIP() << "the restricted sandbox blocks shutdown(2)";
    }
    FAIL() << "shutdown failed with errno=" << errno;
  }
  std::thread wake_other_channel([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(::write(work_sockets[1], "w", 1), 1);
  });
  loop.Loop();
  wake_other_channel.join();

  EXPECT_TRUE(work_called);
  EXPECT_EQ(received, expected);
  client_reader.Remove();
  work_channel.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
  EXPECT_EQ(::close(work_sockets[0]), 0);
  EXPECT_EQ(::close(work_sockets[1]), 0);
}

TEST(ClientConnectionTest, StreamsHeaderAndBodyWithoutExtraBytes) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  int request_count = 0;
  std::string received;
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &request) {
        ++request_count;
        EXPECT_EQ(request.target, "/stream");
        http::HttpResponseHead head{200,
                                    "OK",
                                    {{"content-type", "text/plain"}},
                                    aegisgate::http::ResponseBodyMode::kNormal,
                                    std::size_t{5}};
        client.BeginResponse(head);
        (void)client.WriteResponseBody("hello");
        client.FinishResponse();
      });
  Channel client_reader(loop, sockets[1]);
  client_reader.SetReadCallback([&] {
    std::array<char, 128> bytes{};
    const ssize_t count = ::read(sockets[1], bytes.data(), bytes.size());
    if (count > 0) {
      received.append(bytes.data(), static_cast<std::size_t>(count));
      return;
    }
    EXPECT_EQ(count, 0);
    loop.Quit();
  });
  connection.Start();
  client_reader.EnableReading();

  constexpr std::string_view request = "GET /stream HTTP/1.1\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()),
            static_cast<ssize_t>(request.size()));
  if (::shutdown(sockets[1], SHUT_WR) < 0) {
    if (errno == EPERM) {
      GTEST_SKIP() << "the restricted sandbox blocks shutdown(2)";
    }
    FAIL() << "shutdown failed with errno=" << errno;
  }
  loop.Loop();

  EXPECT_EQ(request_count, 1);
  EXPECT_EQ(received,
            "HTTP/1.1 200 OK\r\n"
            "content-type: text/plain\r\n"
            "Content-Length: 5\r\n\r\n"
            "hello");
  connection.Close();
  client_reader.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, ReportsHighCrossingAndNotifiesLowWaterOnce) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  constexpr std::size_t kFirstBody = 512 * 1024;
  constexpr std::size_t kSecondBody = 256 * 1024;
  constexpr std::size_t kTotalBody = kFirstBody + kSecondBody;
  EventLoop loop;
  bool crossed = false;
  int drained_calls = 0;
  std::string received;
  received.reserve(kTotalBody + 64);
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &) {
        http::HttpResponseHead head{200, "OK", {},
                                    aegisgate::http::ResponseBodyMode::kNormal, kTotalBody};
        client.BeginResponse(head);
        const std::string chunk(64 * 1024, 'x');
        for (std::size_t written = 0; written < kFirstBody; written += chunk.size()) {
          crossed = client.WriteResponseBody(chunk) || crossed;
        }
        // The response stays open: the drained low-water notification must
        // fire before FinishResponse is allowed to close the stream.
      });
  connection.SetWriteDrainedCallback([&] {
    ++drained_calls;
    if (drained_calls == 1) {
      const std::string chunk(64 * 1024, 'z');
      for (std::size_t written = 0; written < kSecondBody; written += chunk.size()) {
        (void)connection.WriteResponseBody(chunk);
      }
      connection.FinishResponse();
    }
  });
  Channel client_reader(loop, sockets[1]);
  client_reader.SetReadCallback([&] {
    std::array<char, 16 * 1024> bytes{};
    const ssize_t count = ::read(sockets[1], bytes.data(), bytes.size());
    if (count > 0) {
      received.append(bytes.data(), static_cast<std::size_t>(count));
      if (received.size() == 43 + kTotalBody) {
        loop.Quit();
      }
      return;
    }
    EXPECT_EQ(count, 0);
    loop.Quit();
  });
  connection.Start();
  client_reader.EnableReading();

  constexpr std::string_view request = "GET /large-stream HTTP/1.1\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()),
            static_cast<ssize_t>(request.size()));
  if (::shutdown(sockets[1], SHUT_WR) < 0) {
    if (errno == EPERM) {
      GTEST_SKIP() << "the restricted sandbox blocks shutdown(2)";
    }
    FAIL() << "shutdown failed with errno=" << errno;
  }
  loop.Loop();

  EXPECT_TRUE(crossed);
  EXPECT_EQ(drained_calls, 1);
  EXPECT_EQ(received.size(), 43 + kTotalBody);
  EXPECT_EQ(received.substr(0, 43),
            "HTTP/1.1 200 OK\r\nContent-Length: 786432\r\n\r\n");
  EXPECT_EQ(received.substr(43, kFirstBody), std::string(kFirstBody, 'x'));
  EXPECT_EQ(received.substr(43 + kFirstBody), std::string(kSecondBody, 'z'));
  connection.Close();
  client_reader.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, PeerCloseFiresAbortOnce) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  int abort_calls = 0;
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &) {
        http::HttpResponseHead head{200, "OK", {},
                                    aegisgate::http::ResponseBodyMode::kNormal,
                                    std::size_t{1024 * 1024}};
        client.BeginResponse(head);
        // Overflow the peer's socket buffer so the write stays pending on
        // EPOLLOUT and the peer's reset is observable via EPOLLERR.
        const std::string chunk(64 * 1024, 'y');
        for (int i = 0; i < 16; ++i) {
          (void)client.WriteResponseBody(chunk);
        }
      });
  connection.SetRequestAbortCallback([&] {
    ++abort_calls;
    loop.Quit();
  });
  Channel peer(loop, sockets[1]);
  peer.SetReadCallback([&] {
    std::array<char, 4096> bytes{};
    const ssize_t count = ::read(sockets[1], bytes.data(), bytes.size());
    ASSERT_GT(count, 0);
    // The peer stops reading, then resets the connection.
    peer.Remove();
    struct linger reset = {1, 0};
    EXPECT_EQ(::setsockopt(sockets[1], SOL_SOCKET, SO_LINGER, &reset, sizeof(reset)), 0);
    EXPECT_EQ(::close(sockets[1]), 0);
    loop.Quit();
  });
  connection.Start();
  peer.EnableReading();

  constexpr std::string_view request = "GET /reset HTTP/1.1\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()),
            static_cast<ssize_t>(request.size()));
  loop.Loop();

  // The reset is observed on the next loop pass.
  loop.Loop();
  EXPECT_EQ(abort_calls, 1);
  connection.Close();
  EXPECT_EQ(abort_calls, 1);  // owner Close() does not repeat the abort
}

TEST(ClientConnectionTest, ConnectionCloseRequestStreamsFullBodyBeforeClosing) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  constexpr std::size_t kBodySize = 512 * 1024;
  EventLoop loop;
  std::string received;
  received.reserve(kBodySize + 64);
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &) {
        http::HttpResponseHead head{200, "OK", {},
                                    aegisgate::http::ResponseBodyMode::kNormal, kBodySize};
        client.BeginResponse(head);
        const std::string chunk(64 * 1024, 'c');
        for (std::size_t written = 0; written < kBodySize; written += chunk.size()) {
          (void)client.WriteResponseBody(chunk);
        }
        client.FinishResponse();
      });
  Channel client_reader(loop, sockets[1]);
  client_reader.SetReadCallback([&] {
    std::array<char, 16 * 1024> bytes{};
    const ssize_t count = ::read(sockets[1], bytes.data(), bytes.size());
    if (count > 0) {
      received.append(bytes.data(), static_cast<std::size_t>(count));
      if (received.size() == 43 + kBodySize) {
        loop.Quit();
      }
      return;
    }
    EXPECT_EQ(count, 0);  // Connection: close closes after the full response
    loop.Quit();
  });
  connection.Start();
  client_reader.EnableReading();

  // A Connection: close request must not close the connection while the
  // committed stream is still unfinished (R-048).
  constexpr std::string_view request = "GET /close HTTP/1.1\r\nConnection: close\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()),
            static_cast<ssize_t>(request.size()));
  loop.Loop();

  EXPECT_EQ(received.size(), 43 + kBodySize);
  EXPECT_EQ(received.substr(0, 43),
            "HTTP/1.1 200 OK\r\nContent-Length: 524288\r\n\r\n");
  EXPECT_EQ(received.substr(43), std::string(kBodySize, 'c'));
  connection.Close();
  client_reader.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, AbortDoesNotEmitSyntheticResponse) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  std::string received;
  ClientConnection connection(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &) {
        http::HttpResponseHead head{200, "OK", {},
                                    aegisgate::http::ResponseBodyMode::kNormal,
                                    std::size_t{5}};
        client.BeginResponse(head);
        client.AbortResponse();
      });
  connection.Start();

  constexpr std::string_view request = "GET /abort HTTP/1.1\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], request.data(), request.size()),
            static_cast<ssize_t>(request.size()));
  Channel peer(loop, sockets[1]);
  peer.SetReadCallback([&] {
    std::array<char, 4096> bytes{};
    const ssize_t count = ::read(sockets[1], bytes.data(), bytes.size());
    if (count > 0) {
      received.append(bytes.data(), static_cast<std::size_t>(count));
      return;
    }
    EXPECT_EQ(count, 0);  // the abort closes the connection
    loop.Quit();
  });
  peer.EnableReading();
  loop.Loop();

  // Nothing beyond the already-committed header may reach the peer: no body
  // bytes and no synthetic error response.
  EXPECT_LE(received.size(), 38);
  EXPECT_EQ(received.find("hello"), std::string::npos);
  EXPECT_EQ(received.find("Bad Gateway"), std::string::npos);
  peer.Remove();
  EXPECT_EQ(::close(sockets[1]), 0);
}

TEST(ClientConnectionTest, ResumeAfterWriteCanDestroyConnectionInBufferedCallback) {
  std::array<int, 2> sockets{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()),
            0);

  EventLoop loop;
  int callback_count = 0;
  std::unique_ptr<ClientConnection> connection;
  connection = std::make_unique<ClientConnection>(
      loop, sockets[0], [&](ClientConnection &client, const http::HttpRequest &request) {
        ++callback_count;
        if (callback_count == 1) {
          EXPECT_EQ(request.target, "/first");
          client.SendResponse(http::HttpResponse{200, "OK", {}, ""});
          return;
        }
        EXPECT_EQ(request.target, "/second");
        connection.reset();
        loop.Quit();
      });
  connection->Start();

  constexpr std::string_view requests =
      "GET /first HTTP/1.1\r\n\r\n"
      "GET /second HTTP/1.1\r\n\r\n";
  ASSERT_EQ(::write(sockets[1], requests.data(), requests.size()),
            static_cast<ssize_t>(requests.size()));
  loop.Loop();

  EXPECT_EQ(callback_count, 2);
  EXPECT_EQ(connection, nullptr);
  EXPECT_EQ(::close(sockets[1]), 0);
}

} // namespace
} // namespace aegisgate::net
