#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>

#include "aegisgate/config/Config.h"
#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/net/UpstreamConnection.h"

namespace aegisgate::net { class EventLoop; }

namespace aegisgate::proxy {

// Event-loop-thread confined pool. It has exactly one FIFO idle deque per
// literal address/port and never loans the same connection concurrently.
class UpstreamPool {
public:
  using ResponseCallback = net::UpstreamConnection::ResponseCallback;
  using ProgressCallback = net::UpstreamConnection::ProgressCallback;

  explicit UpstreamPool(net::EventLoop &loop);
  ~UpstreamPool();

  UpstreamPool(const UpstreamPool &) = delete;
  UpstreamPool &operator=(const UpstreamPool &) = delete;

  net::UpstreamConnection *Execute(const config::Endpoint &endpoint,
                                   const http::HttpRequest &request,
                                   ResponseCallback callback,
                                   ProgressCallback progress = {});
  // Aborts an active exchange. It suppresses its response callback and cannot
  // return the descriptor to idle storage.
  [[nodiscard]] bool Cancel(net::UpstreamConnection *connection) noexcept;
  [[nodiscard]] std::size_t IdleCount(const config::Endpoint &endpoint) const noexcept;

private:
  struct Key {
    std::array<std::uint8_t, 4> address{};
    std::uint16_t port{};
    [[nodiscard]] bool operator<(const Key &other) const noexcept {
      return address != other.address ? address < other.address : port < other.port;
    }
  };
  using Connection = std::unique_ptr<net::UpstreamConnection>;

  [[nodiscard]] static Key ToKey(const config::Endpoint &endpoint) noexcept;
  void Complete(net::UpstreamConnection *connection, Key key, ResponseCallback callback,
                net::UpstreamResult result, http::HttpResponse response);

  net::EventLoop &loop_;
  std::map<Key, std::deque<Connection>> idle_;
  std::map<net::UpstreamConnection *, Connection> active_;
};

} // namespace aegisgate::proxy
