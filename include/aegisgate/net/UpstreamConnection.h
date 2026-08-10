#pragma once

#include <cstdint>
#include <functional>
#include <memory>

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

// Owns one loopback upstream exchange. It intentionally supports no retries,
// pooling, DNS, TLS, timeouts, or pipelining.
class UpstreamConnection {
public:
  using ResponseCallback = std::function<void(UpstreamResult, http::HttpResponse)>;

  UpstreamConnection(EventLoop &loop, std::uint16_t port, ResponseCallback callback);
  ~UpstreamConnection();

  UpstreamConnection(const UpstreamConnection &) = delete;
  UpstreamConnection &operator=(const UpstreamConnection &) = delete;
  UpstreamConnection(UpstreamConnection &&) = delete;
  UpstreamConnection &operator=(UpstreamConnection &&) = delete;

  void Start(const http::HttpRequest &request);
  void Close() noexcept;

private:
  enum class State { kIdle, kConnecting, kWriting, kReading, kFinished };

  void HandleRead();
  void HandleWrite();
  void Finish(UpstreamResult result);

  EventLoop &loop_;
  std::uint16_t port_;
  // Channel is destroyed before Socket so it cannot unregister a closed fd.
  std::unique_ptr<Socket> socket_;
  std::unique_ptr<Channel> channel_;
  Buffer input_;
  Buffer output_;
  http::HttpResponseParser parser_;
  ResponseCallback callback_;
  State state_ = State::kIdle;
};

} // namespace aegisgate::net
