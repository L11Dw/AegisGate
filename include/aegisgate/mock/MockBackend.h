#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace aegisgate::http { class HttpRequest; }
namespace aegisgate::net {
class Acceptor;
class ClientConnection;
class EventLoop;
class TimerQueue;
}

namespace aegisgate::mock {

struct MockBackendOptions {
  int status = 200;
  std::chrono::milliseconds delay{};
  bool reset = false;
  std::size_t max_inflight = 64;
  // Successful responses carry this many deterministic 'x' bytes.  The
  // option exists to exercise Content-Length streaming and backpressure in
  // integration/benchmark clients without adding chunked support.
  std::size_t body_bytes = 0;
};

class MockBackend {
public:
  MockBackend(net::EventLoop &loop, MockBackendOptions options, std::string_view address,
              std::uint16_t port);
  ~MockBackend();
  MockBackend(const MockBackend &) = delete;
  MockBackend &operator=(const MockBackend &) = delete;
  void Start();
  [[nodiscard]] std::uint16_t port() const;

private:
  struct State { MockBackend *owner = nullptr; bool scheduled = false; std::vector<std::uint64_t> closed; };
  void Accept(int fd);
  void HandleRequest(net::ClientConnection &client, const http::HttpRequest &request);
  void Deliver(std::uint64_t id);
  void Reap(std::vector<std::uint64_t> ids);
  void Send(net::ClientConnection &client, bool capacity);
  static void Closed(net::EventLoop &loop, std::weak_ptr<State> state, std::uint64_t id);

  net::EventLoop &loop_;
  MockBackendOptions options_;
  std::string response_body_;
  std::shared_ptr<State> state_;
  std::unique_ptr<net::TimerQueue> timers_;
  std::unique_ptr<net::Acceptor> acceptor_;
  std::unordered_map<std::uint64_t, std::unique_ptr<net::ClientConnection>> clients_;
  std::unordered_map<net::ClientConnection *, std::uint64_t> ids_;
  std::unordered_map<std::uint64_t, std::uint64_t> pending_;
  std::uint64_t next_id_ = 1;
  std::size_t inflight_ = 0;
};

} // namespace aegisgate::mock
