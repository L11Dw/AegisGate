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

enum class UpstreamProgress { kConnected, kRequestWritten, kFirstByte, kResponseHeader };

// Owns a serial upstream exchange on one literal IPv4 connection. A clean
// HTTP/1.1 keep-alive response leaves it idle for a pool to reuse; no
// pipelining, DNS, TLS, retries or timers are supported here.
class UpstreamConnection {
public:
  using ResponseCallback = std::function<void(UpstreamResult, http::HttpResponse)>;
  using ProgressCallback = std::function<void(UpstreamProgress)>;
  // Streaming mode: the header callback fires exactly once after the
  // validated response head; body bytes are handed to the sink, which must
  // take ownership of the view before returning (returning false keeps the
  // chunk in the input buffer and pauses reading).  One mode per exchange.
  using HeaderCallback = std::function<void(const http::HttpResponseHead &)>;
  using BodySink = http::HttpResponseParser::BodySink;

  UpstreamConnection(EventLoop &loop, std::uint16_t port, ResponseCallback callback);
  UpstreamConnection(EventLoop &loop, config::Endpoint endpoint, ResponseCallback callback);
  ~UpstreamConnection();

  UpstreamConnection(const UpstreamConnection &) = delete;
  UpstreamConnection &operator=(const UpstreamConnection &) = delete;
  UpstreamConnection(UpstreamConnection &&) = delete;
  UpstreamConnection &operator=(UpstreamConnection &&) = delete;

  void Start(const http::HttpRequest &request);
  void SetResponseCallback(ResponseCallback callback);
  void SetProgressCallback(ProgressCallback callback);
  // Switches this exchange to streaming delivery: the header callback
  // replaces the terminal response callback for the header phase, and the
  // sink receives body bytes.  Must be set before Start().
  void SetStreamingCallbacks(HeaderCallback header_callback, BodySink body_sink);
  // Stops reading the upstream descriptor (idempotent, safe from any callback
  // stack).  ResumeReading re-enables reading only in the reading state and
  // first consumes any residual input before recv()ing again.
  void PauseReading() noexcept;
  void ResumeReading() noexcept;
  // Logically cancels the exchange: all callbacks are cleared so neither a
  // terminal result nor a progress event is delivered (the response callback
  // may hold a transaction's shared_ptr).  Safe to call from inside either
  // callback's stack because invocation sites copy the callback first.
  void SuppressCallbacks() noexcept;
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
  void HandleStreamingRead();
  // Consumes everything parseable in the input buffer (header phase and body
  // chunks).  Returns false when processing must stop: a terminal Finish or a
  // declined sink chunk (which pauses reading to bound the input buffer).
  [[nodiscard]] bool ConsumeStreamingInput();
  void Finish(UpstreamResult result);
  [[nodiscard]] bool PeerHasClosedOrSentExtraBytes() const noexcept;
  [[nodiscard]] bool ResponseRequestsClose() const noexcept;
  [[nodiscard]] bool HeadRequestsClose() const noexcept;

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
  ProgressCallback progress_callback_;
  HeaderCallback header_callback_;
  BodySink body_sink_;
  State state_ = State::kIdle;
  bool reusable_ = false;
  bool first_byte_reported_ = false;
  bool response_header_reported_ = false;
  bool header_reported_ = false;
  // Downstream backpressure pause: while set, the streaming read loop stops
  // recv()ing even inside its current batch (the epoll interest is disabled
  // separately), so the kernel queue absorbs the remainder.  Distinct from
  // "chunk consumed": a paused connection has consumed everything already
  // handed to the body sink.
  bool reading_paused_ = false;
};

} // namespace aegisgate::net
