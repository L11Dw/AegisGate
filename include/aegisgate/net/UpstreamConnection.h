#pragma once

#include <array>
#include <cstdint>
#include <functional>
#include <memory>

#include "aegisgate/config/Config.h"

#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/http/HttpResponse.h"
#include "aegisgate/http/HttpResponseParser.h"
#include "aegisgate/net/Buffer.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/Socket.h"

namespace aegisgate::net {

class EventLoop;

enum class UpstreamResult {
  kSuccess,
  kConnectError,
  kWriteError,
  kReadError,
  kEof,
  kProtocolError,
  kUnsupported,
};

// Owns a serial upstream exchange on one literal IPv4 connection. A clean
// HTTP/1.1 keep-alive response leaves it idle for a pool to reuse; no
// pipelining, DNS, TLS, retries or timers are supported here.
class UpstreamConnection {
public:
  using ResponseCallback = std::function<void(UpstreamResult, http::HttpResponse)>;

  UpstreamConnection(EventLoop &loop, std::uint16_t port, ResponseCallback callback);
  UpstreamConnection(EventLoop &loop, config::Endpoint endpoint, ResponseCallback callback);
  ~UpstreamConnection();

  UpstreamConnection(const UpstreamConnection &) = delete;
  UpstreamConnection &operator=(const UpstreamConnection &) = delete;
  UpstreamConnection(UpstreamConnection &&) = delete;
  UpstreamConnection &operator=(UpstreamConnection &&) = delete;

  void Start(const http::HttpRequest &request);
  void SetResponseCallback(ResponseCallback callback);
  void Close() noexcept;
  [[nodiscard]] bool Reusable() const noexcept;
  // Re-probes an idle descriptor immediately before a pool lends it. Any EOF,
  // socket error, or queued unread byte makes the connection unsafe and closes
  // it; EINTR retries and only EAGAIN/EWOULDBLOCK is healthy.
  [[nodiscard]] bool HealthyForReuse() noexcept;

private:
  enum class State { kIdle, kConnecting, kWriting, kReading, kFinished };

  void HandleRead();
  void HandleWrite();
  void Finish(UpstreamResult result);
  [[nodiscard]] bool PeerHasClosedOrSentExtraBytes() const noexcept;
  [[nodiscard]] bool ResponseRequestsClose() const noexcept;

  EventLoop &loop_;
  std::array<std::uint8_t, 4> address_{127, 0, 0, 1};
  std::uint16_t port_;
  // Channel is destroyed before Socket so it cannot unregister a closed fd.
  std::unique_ptr<Socket> socket_;
  std::unique_ptr<Channel> channel_;
  Buffer input_;
  Buffer output_;
  http::HttpResponseParser parser_;
  ResponseCallback callback_;
  State state_ = State::kIdle;
  bool reusable_ = false;
};

} // namespace aegisgate::net
