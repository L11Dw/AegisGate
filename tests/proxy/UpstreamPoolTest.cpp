#include "aegisgate/proxy/UpstreamPool.h"
#include "aegisgate/http/HttpRequestSerializer.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/Socket.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <string>
#include <thread>

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace aegisgate::proxy {
namespace {

using namespace std::chrono_literals;

config::Endpoint LoopbackEndpoint(std::uint16_t port) {
  return {"127.0.0.1", {127, 0, 0, 1}, port, 1};
}

http::HttpRequest Request() {
  return {"GET", "/pooled", "HTTP/1.1", "", {{"Host", "pool.test"}}};
}

int AcceptUntil(const net::Socket &listener, std::chrono::steady_clock::time_point deadline) {
  while (std::chrono::steady_clock::now() < deadline) {
    const int fd = listener.Accept();
    if (fd >= 0) return fd;
    pollfd descriptor{listener.Fd(), POLLIN, 0};
    (void)::poll(&descriptor, 1, 10);
  }
  return -1;
}

bool ReadExactUntil(int fd, std::string *output, std::size_t bytes,
                    std::chrono::steady_clock::time_point deadline) {
  output->clear();
  output->resize(bytes);
  std::size_t received = 0;
  while (received < bytes && std::chrono::steady_clock::now() < deadline) {
    pollfd descriptor{fd, POLLIN, 0};
    if (::poll(&descriptor, 1, 10) <= 0) continue;
    const ssize_t count = ::read(fd, output->data() + received, bytes - received);
    if (count > 0) { received += static_cast<std::size_t>(count); continue; }
    if (count < 0 && errno == EINTR) continue;
    return false;
  }
  return received == bytes;
}

bool WriteAll(int fd, std::string_view bytes) {
  std::size_t sent = 0;
  while (sent < bytes.size()) {
    const ssize_t count = ::write(fd, bytes.data() + sent, bytes.size() - sent);
    if (count > 0) { sent += static_cast<std::size_t>(count); continue; }
    if (count < 0 && errno == EINTR) continue;
    return false;
  }
  return true;
}

TEST(UpstreamPoolTest, OwnsOneIdleDequePerLiteralEndpoint) {
  net::EventLoop loop;
  UpstreamPool pool(loop);
  const config::Endpoint first{"127.0.0.1", {127, 0, 0, 1}, 9001, 1};
  const config::Endpoint second{"127.0.0.1", {127, 0, 0, 1}, 9002, 1};

  EXPECT_EQ(pool.IdleCount(first), 0U);
  EXPECT_EQ(pool.IdleCount(second), 0U);
}

TEST(UpstreamPoolTest, ReusesOneAcceptedKeepAliveConnectionForSequentialRequests) {
  net::Socket listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint = LoopbackEndpoint(listener.BoundPort());
  const std::string expected = http::SerializeRequest(Request());
  std::atomic_int accepted = 0;
  std::atomic_bool server_ok = true;
  std::thread server([&] {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    const int fd = AcceptUntil(listener, deadline);
    if (fd < 0) { server_ok = false; return; }
    ++accepted;
    std::string received;
    for (int index = 0; index != 2; ++index) {
      if (!ReadExactUntil(fd, &received, expected.size(), deadline) || received != expected ||
          !WriteAll(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")) {
        server_ok = false;
        break;
      }
    }
    (void)::close(fd);
  });

  net::EventLoop loop;
  UpstreamPool pool(loop);
  int callbacks = 0;
  pool.Execute(endpoint, Request(), [&](net::UpstreamResult result, http::HttpResponse response) {
    EXPECT_EQ(result, net::UpstreamResult::kSuccess);
    EXPECT_EQ(response.body, "ok");
    ++callbacks;
    if (callbacks == 1) {
      pool.Execute(endpoint, Request(), [&](net::UpstreamResult second, http::HttpResponse reply) {
        EXPECT_EQ(second, net::UpstreamResult::kSuccess);
        EXPECT_EQ(reply.body, "ok");
        ++callbacks;
        loop.Quit();
      });
    }
  });
  loop.Loop();
  server.join();

  EXPECT_TRUE(server_ok);
  EXPECT_EQ(callbacks, 2);
  EXPECT_EQ(accepted, 1);
}

TEST(UpstreamPoolTest, DoesNotReuseConnectionCloseResponse) {
  net::Socket listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint = LoopbackEndpoint(listener.BoundPort());
  const std::string expected = http::SerializeRequest(Request());
  std::atomic_int accepted = 0;
  std::atomic_bool server_ok = true;
  std::thread server([&] {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    for (int index = 0; index != 2; ++index) {
      const int fd = AcceptUntil(listener, deadline);
      if (fd < 0) { server_ok = false; return; }
      ++accepted;
      std::string received;
      const std::string_view response = index == 0
          ? "HTTP/1.1 200 OK\r\nConnection: Keep-Alive, Close\r\nContent-Length: 2\r\n\r\nok"
          : "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
      if (!ReadExactUntil(fd, &received, expected.size(), deadline) || received != expected ||
          !WriteAll(fd, response)) server_ok = false;
      (void)::close(fd);
      if (!server_ok) return;
    }
  });

  net::EventLoop loop;
  UpstreamPool pool(loop);
  int callbacks = 0;
  pool.Execute(endpoint, Request(), [&](net::UpstreamResult result, http::HttpResponse) {
    EXPECT_EQ(result, net::UpstreamResult::kSuccess);
    ++callbacks;
    pool.Execute(endpoint, Request(), [&](net::UpstreamResult second, http::HttpResponse) {
      EXPECT_EQ(second, net::UpstreamResult::kSuccess);
      ++callbacks;
      loop.Quit();
    });
  });
  loop.Loop();
  server.join();

  EXPECT_TRUE(server_ok);
  EXPECT_EQ(callbacks, 2);
  EXPECT_EQ(accepted, 2);
}

TEST(UpstreamPoolTest, KeepsOnlyTheOldestIdleConnectionPerEndpoint) {
  net::Socket listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint = LoopbackEndpoint(listener.BoundPort());
  const std::string expected = http::SerializeRequest(Request());
  std::atomic_int accepted = 0;
  std::atomic_int peer_eofs = 0;
  std::atomic_bool server_ok = true;
  std::thread server([&] {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    std::array<int, 2> peers{-1, -1};
    std::string received;
    for (int index = 0; index != 2; ++index) {
      peers[static_cast<std::size_t>(index)] = AcceptUntil(listener, deadline);
      if (peers[static_cast<std::size_t>(index)] < 0 ||
          !ReadExactUntil(peers[static_cast<std::size_t>(index)], &received, expected.size(), deadline) ||
          received != expected) {
        server_ok = false;
        return;
      }
      ++accepted;
    }
    for (const int fd : peers) {
      if (!WriteAll(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")) server_ok = false;
    }
    for (const int fd : peers) {
      pollfd descriptor{fd, POLLIN | POLLRDHUP, 0};
      if (::poll(&descriptor, 1, 5000) > 0 && (descriptor.revents & (POLLIN | POLLRDHUP)) != 0) {
        char byte = '\0';
        if (::recv(fd, &byte, 1, MSG_DONTWAIT) == 0) ++peer_eofs;
      }
      (void)::close(fd);
    }
  });

  net::EventLoop loop;
  UpstreamPool pool(loop);
  int callbacks = 0;
  bool callback_error = false;
  const auto completed = [&](net::UpstreamResult result, http::HttpResponse response) {
    if (result != net::UpstreamResult::kSuccess || response.body != "ok") callback_error = true;
    ++callbacks;
    if (callbacks == 2) loop.Quit();
  };
  pool.Execute(endpoint, Request(), completed);
  pool.Execute(endpoint, Request(), completed);
  loop.Loop();
  server.join();

  EXPECT_FALSE(callback_error);
  EXPECT_EQ(callbacks, 2);
  EXPECT_EQ(accepted, 2);
  EXPECT_EQ(pool.IdleCount(endpoint), 1U);
  EXPECT_TRUE(server_ok);
  EXPECT_EQ(peer_eofs, 1);
}

TEST(UpstreamPoolTest, DiscardsAnIdleConnectionClosedAfterItsResponseBeforeBorrowing) {
  net::Socket listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint = LoopbackEndpoint(listener.BoundPort());
  const std::string expected = http::SerializeRequest(Request());
  std::array<int, 2> control{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, control.data()), 0);
  std::atomic_int accepted = 0;
  std::atomic_bool server_ok = true;
  std::thread server([&] {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    int fd = AcceptUntil(listener, deadline);
    if (fd < 0) { server_ok = false; return; }
    ++accepted;
    std::string received;
    if (!ReadExactUntil(fd, &received, expected.size(), deadline) || received != expected ||
        !WriteAll(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")) {
      server_ok = false;
      (void)::close(fd);
      return;
    }
    pollfd control_descriptor{control[1], POLLIN, 0};
    if (::poll(&control_descriptor, 1, 5000) <= 0 ||
        ::read(control[1], received.data(), 1) != 1) {
      server_ok = false;
      (void)::close(fd);
      return;
    }
    (void)::shutdown(fd, SHUT_WR);
    (void)::close(fd);
    if (::write(control[1], "x", 1) != 1) { server_ok = false; return; }

    fd = AcceptUntil(listener, deadline);
    if (fd < 0) { server_ok = false; return; }
    ++accepted;
    if (!ReadExactUntil(fd, &received, expected.size(), deadline) || received != expected ||
        !WriteAll(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")) server_ok = false;
    (void)::close(fd);
  });

  net::EventLoop loop;
  UpstreamPool pool(loop);
  int callbacks = 0;
  bool callback_error = false;
  std::size_t first_idle_count = 0;
  net::UpstreamResult first_result = net::UpstreamResult::kReadError;
  net::UpstreamResult second_result = net::UpstreamResult::kReadError;
  net::Channel control_channel(loop, control[0]);
  control_channel.SetReadCallback([&] {
    char signal = '\0';
    if (::read(control[0], &signal, 1) != 1 || signal != 'x') {
      callback_error = true;
      loop.Quit();
      return;
    }
    pool.Execute(endpoint, Request(), [&](net::UpstreamResult result, http::HttpResponse) {
      second_result = result;
      if (result != net::UpstreamResult::kSuccess) callback_error = true;
      ++callbacks;
      loop.Quit();
    });
  });
  control_channel.EnableReading();
  pool.Execute(endpoint, Request(), [&](net::UpstreamResult result, http::HttpResponse) {
    first_result = result;
    if (result != net::UpstreamResult::kSuccess) callback_error = true;
    ++callbacks;
    first_idle_count = pool.IdleCount(endpoint);
    if (::write(control[0], "c", 1) != 1) {
      callback_error = true;
      loop.Quit();
    }
  });
  loop.Loop();
  server.join();
  control_channel.DisableAll();
  (void)::close(control[0]);
  (void)::close(control[1]);

  EXPECT_TRUE(server_ok);
  EXPECT_FALSE(callback_error);
  EXPECT_EQ(callbacks, 2);
  EXPECT_EQ(first_result, net::UpstreamResult::kSuccess);
  EXPECT_EQ(second_result, net::UpstreamResult::kSuccess);
  EXPECT_EQ(first_idle_count, 1U);
  EXPECT_EQ(accepted, 2);
}

// R-042 red test: CancelAll() must never destroy the Channel whose
// Channel::HandleEvent stack is currently executing.  The progress callback
// fires CancelAll from inside that stack; the response callback must be
// suppressed, the descriptor must still end up closed, and ASan must stay
// clean.
TEST(UpstreamPoolTest, CancelAllInsideProgressCallbackIsSafe) {
  net::Socket listener = net::Socket::ListenLoopback();
  const config::Endpoint endpoint = LoopbackEndpoint(listener.BoundPort());
  const std::string expected = http::SerializeRequest(Request());
  std::array<int, 2> wake{};
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, wake.data()), 0);
  std::string server_error;
  std::thread server([&] {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    const int fd = AcceptUntil(listener, deadline);
    if (fd < 0) { server_error = "accept timed out"; return; }
    std::string received;
    if (!ReadExactUntil(fd, &received, expected.size(), deadline) || received != expected) {
      server_error = "request mismatch";
      (void)::close(fd);
      if (::write(wake[1], "q", 1) != 1) server_error = "wake failed";
      return;
    }
    // The pool must close the descriptor once CancelAll takes effect.  A poll
    // hit alone is not EOF: recv() must return 0.
    pollfd descriptor{fd, POLLHUP | POLLIN, 0};
    bool saw_eof = false;
    if (::poll(&descriptor, 1, 5000) > 0) {
      char byte = '\0';
      saw_eof = ::recv(fd, &byte, 1, 0) == 0;
    }
    (void)::close(fd);
    if (!saw_eof) server_error = "no EOF after cancel";
    if (::write(wake[1], "q", 1) != 1 && server_error.empty()) server_error = "wake failed";
  });

  net::EventLoop loop;
  UpstreamPool pool(loop);
  net::Channel wake_channel(loop, wake[0]);
  wake_channel.SetReadCallback([&] {
    char byte = '\0';
    ASSERT_EQ(::read(wake[0], &byte, 1), 1);
    loop.Quit();
  });
  wake_channel.EnableReading();
  int callbacks = 0;
  bool callback_error = false;
  pool.Execute(endpoint, Request(),
               [&](net::UpstreamResult, http::HttpResponse) {
                 // Suppressed: CancelAll must not deliver a terminal result.
                 callback_error = true;
                 ++callbacks;
               },
               [&](net::UpstreamProgress progress) {
                 if (progress == net::UpstreamProgress::kConnected) {
                   pool.CancelAll();  // inside this connection's callback stack
                 }
               });
  loop.Loop();
  server.join();
  wake_channel.Remove();
  (void)::close(wake[0]);
  (void)::close(wake[1]);

  EXPECT_FALSE(callback_error);
  EXPECT_EQ(callbacks, 0);
  EXPECT_TRUE(server_error.empty()) << server_error;
}

} // namespace
} // namespace aegisgate::proxy
