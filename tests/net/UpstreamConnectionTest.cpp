#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/http/HttpRequestSerializer.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/Socket.h"
#include "aegisgate/net/UpstreamConnection.h"

namespace aegisgate::net {
namespace {

http::HttpRequest PostRequest(std::string body = "payload") {
  return {"POST", "/submit", "HTTP/1.1", std::move(body), {{"Host", "upstream.test"}}};
}

int AcceptBlocking(const Socket &listener) {
  for (;;) {
    const int fd = listener.Accept();
    if (fd >= 0) {
      const int flags = ::fcntl(fd, F_GETFL);
      if (flags >= 0) (void)::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
      return fd;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

std::string ReadAllRequest(int fd, std::size_t expected_size) {
  std::string request;
  request.resize(expected_size);
  std::size_t received = 0;
  while (received != expected_size) {
    const ssize_t count = ::read(fd, request.data() + received, expected_size - received);
    if (count > 0) {
      received += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) continue;
    return {};
  }
  return request;
}

TEST(UpstreamConnectionTest, SendsExactPostAndParsesSegmentedResponseOnce) {
  Socket listener = Socket::ListenLoopback();
  const http::HttpRequest request = PostRequest("hello");
  const std::string expected_request = http::SerializeRequest(request);
  std::string received_request;
  std::thread server([&] {
    const int fd = AcceptBlocking(listener);
    received_request = ReadAllRequest(fd, expected_request.size());
    ASSERT_EQ(::write(fd, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhe", 40), 40);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    ASSERT_EQ(::write(fd, "llo", 3), 3);
    EXPECT_EQ(::close(fd), 0);
  });

  EventLoop loop;
  int callback_count = 0;
  UpstreamResult result = UpstreamResult::kReadError;
  http::HttpResponse response;
  UpstreamConnection connection(loop, listener.BoundPort(), [&](UpstreamResult value, http::HttpResponse parsed) {
    ++callback_count;
    result = value;
    response = std::move(parsed);
    loop.Quit();
  });
  connection.Start(request);
  loop.Loop();
  server.join();

  EXPECT_EQ(received_request, expected_request);
  EXPECT_EQ(callback_count, 1);
  EXPECT_EQ(result, UpstreamResult::kSuccess);
  EXPECT_EQ(response.status, 200);
  EXPECT_EQ(response.body, "hello");
}

TEST(UpstreamConnectionTest, ReportsConnectErrorExactlyOnce) {
  Socket listener = Socket::ListenLoopback();
  const std::uint16_t unused_port = listener.BoundPort();
  listener.Close();
  EventLoop loop;
  int callback_count = 0;
  UpstreamConnection connection(loop, unused_port, [&](UpstreamResult result, http::HttpResponse) {
    EXPECT_EQ(result, UpstreamResult::kConnectError);
    ++callback_count;
    loop.Quit();
  });
  connection.Start(PostRequest());
  loop.Loop();
  EXPECT_EQ(callback_count, 1);
}

TEST(UpstreamConnectionTest, InvalidRequestCompletesOnceAndCannotBeRestarted) {
  EventLoop loop;
  int callback_count = 0;
  UpstreamConnection connection(loop, 1, [&](UpstreamResult result, http::HttpResponse) {
    EXPECT_EQ(result, UpstreamResult::kProtocolError);
    ++callback_count;
  });
  http::HttpRequest invalid = PostRequest();
  invalid.headers.clear();

  EXPECT_NO_THROW(connection.Start(invalid));
  EXPECT_EQ(callback_count, 1);
  EXPECT_THROW(connection.Start(PostRequest()), std::logic_error);
}

TEST(UpstreamConnectionTest, ReportsEofBeforeACompleteResponse) {
  Socket listener = Socket::ListenLoopback();
  std::thread server([&] {
    const int fd = AcceptBlocking(listener);
    EXPECT_EQ(ReadAllRequest(fd, http::SerializeRequest(PostRequest()).size()),
              http::SerializeRequest(PostRequest()));
    EXPECT_EQ(::write(fd, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhe", 40), 40);
    EXPECT_EQ(::shutdown(fd, SHUT_WR), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(::close(fd), 0);
  });
  EventLoop loop;
  UpstreamResult result = UpstreamResult::kSuccess;
  UpstreamConnection connection(loop, listener.BoundPort(), [&](UpstreamResult value, http::HttpResponse) {
    result = value;
    loop.Quit();
  });
  connection.Start(PostRequest());
  loop.Loop();
  server.join();
  EXPECT_EQ(result, UpstreamResult::kEof);
}

TEST(UpstreamConnectionTest, MapsChunkedAndInvalidResponsesToTerminalResults) {
  for (const auto &[wire, expected] : std::array<std::pair<std::string_view, UpstreamResult>, 2>{
           {{"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n", UpstreamResult::kUnsupported},
            {"not an HTTP response\r\n", UpstreamResult::kProtocolError}}}) {
    Socket listener = Socket::ListenLoopback();
    std::thread server([&] {
      const int fd = AcceptBlocking(listener);
      EXPECT_EQ(::write(fd, wire.data(), wire.size()), static_cast<ssize_t>(wire.size()));
      EXPECT_EQ(::close(fd), 0);
    });
    EventLoop loop;
    UpstreamResult result = UpstreamResult::kSuccess;
    UpstreamConnection connection(loop, listener.BoundPort(), [&](UpstreamResult value, http::HttpResponse) {
      result = value;
      loop.Quit();
    });
    connection.Start(PostRequest());
    loop.Loop();
    server.join();
    EXPECT_EQ(result, expected);
  }
}

TEST(UpstreamConnectionTest, CallbackMayDestroyOwnerAndRetainResponse) {
  Socket listener = Socket::ListenLoopback();
  std::thread server([&] {
    const int fd = AcceptBlocking(listener);
    constexpr std::string_view response = "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone";
    EXPECT_EQ(::write(fd, response.data(), response.size()), static_cast<ssize_t>(response.size()));
    EXPECT_EQ(::close(fd), 0);
  });
  EventLoop loop;
  std::unique_ptr<UpstreamConnection> connection;
  std::string body;
  connection = std::make_unique<UpstreamConnection>(loop, listener.BoundPort(), [&](UpstreamResult result, http::HttpResponse response) {
    connection.reset();
    EXPECT_EQ(result, UpstreamResult::kSuccess);
    body = std::move(response.body);
    loop.Quit();
  });
  connection->Start(PostRequest());
  loop.Loop();
  server.join();
  EXPECT_EQ(connection, nullptr);
  EXPECT_EQ(body, "done");
}

TEST(UpstreamConnectionTest, ParsesConnectionCloseResponseAndRejectsSecondStart) {
  Socket listener = Socket::ListenLoopback();
  std::thread server([&] {
    const int fd = AcceptBlocking(listener);
    EXPECT_EQ(ReadAllRequest(fd, http::SerializeRequest(PostRequest()).size()),
              http::SerializeRequest(PostRequest()));
    constexpr std::string_view response =
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok";
    EXPECT_EQ(::write(fd, response.data(), response.size()), static_cast<ssize_t>(response.size()));
    EXPECT_EQ(::shutdown(fd, SHUT_WR), 0);
    EXPECT_EQ(::close(fd), 0);
  });
  EventLoop loop;
  UpstreamConnection connection(loop, listener.BoundPort(), [&](UpstreamResult result, http::HttpResponse response) {
    EXPECT_EQ(result, UpstreamResult::kSuccess);
    EXPECT_EQ(response.body, "ok");
    loop.Quit();
  });
  connection.Start(PostRequest());
  loop.Loop();
  server.join();
  EXPECT_THROW(connection.Start(PostRequest()), std::logic_error);
}

TEST(UpstreamConnectionTest, CloseActiveConnectionRemovesChannelBeforeLaterLoopDispatch) {
  Socket listener = Socket::ListenLoopback();
  std::thread server([&] {
    const int fd = AcceptBlocking(listener);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(::close(fd), 0);
  });
  EventLoop loop;
  int callback_count = 0;
  UpstreamConnection connection(loop, listener.BoundPort(), [&](UpstreamResult, http::HttpResponse) {
    ++callback_count;
  });
  connection.Start(PostRequest());
  connection.Close();
  server.join();
  EXPECT_EQ(callback_count, 0);
}

TEST(UpstreamConnectionTest, DrainsLargeRequestAcrossWritableEvents) {
  Socket listener = Socket::ListenLoopback();
  const http::HttpRequest request = PostRequest(std::string(256 * 1024, 'x'));
  const std::string expected = http::SerializeRequest(request);
  std::atomic<bool> matched = false;
  std::thread server([&] {
    const int fd = AcceptBlocking(listener);
    int receive_buffer = 1024;
    EXPECT_EQ(::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &receive_buffer, sizeof(receive_buffer)), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    matched = ReadAllRequest(fd, expected.size()) == expected;
    constexpr std::string_view response = "HTTP/1.1 204 No Content\r\n\r\n";
    EXPECT_EQ(::write(fd, response.data(), response.size()), static_cast<ssize_t>(response.size()));
    EXPECT_EQ(::close(fd), 0);
  });
  EventLoop loop;
  UpstreamResult result = UpstreamResult::kReadError;
  UpstreamConnection connection(loop, listener.BoundPort(), [&](UpstreamResult value, http::HttpResponse) {
    result = value;
    loop.Quit();
  });
  connection.Start(request);
  loop.Loop();
  server.join();
  EXPECT_TRUE(matched.load());
  EXPECT_EQ(result, UpstreamResult::kSuccess);
}

} // namespace
} // namespace aegisgate::net
