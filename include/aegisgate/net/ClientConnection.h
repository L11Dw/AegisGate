#pragma once

#include <functional>
#include <memory>

#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/http/HttpResponse.h"
#include "aegisgate/net/Buffer.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/Socket.h"
#include "aegisgate/net/StreamFlowControl.h"

namespace aegisgate::net {

class EventLoop;

class ClientConnection {
public:
  using RequestCallback =
      std::function<void(ClientConnection &, const http::HttpRequest &)>;
  using CloseCallback = std::function<void()>;
  // Fired once after a write drain drops the queued response bytes to or
  // below the low watermark.  The owner may be destroyed inside the callback:
  // it is copied out first and no member is touched afterwards.
  using WriteDrainedCallback = std::function<void()>;
  // Fired at most once when the peer connection is gone (RST, error or a
  // failed write) while a streaming response is pending.  Owner Close() never
  // repeats it.  Copied out before invocation, same owner rules as above.
  using RequestAbortCallback = std::function<void()>;

  ClientConnection(EventLoop &loop, int fd, RequestCallback callback,
                   StreamFlowControl flow_control = {});
  ~ClientConnection();

  ClientConnection(const ClientConnection &) = delete;
  ClientConnection &operator=(const ClientConnection &) = delete;
  ClientConnection(ClientConnection &&) = delete;
  ClientConnection &operator=(ClientConnection &&) = delete;

  void Start();
  void SetCloseCallback(CloseCallback callback);
  void ResumeReading();
  void SendResponse(const http::HttpResponse &response);
  // Streaming response API: BeginResponse commits the validated head and
  // starts writing; WriteResponseBody appends body chunks and returns true
  // when this call crossed the high watermark; FinishResponse completes the
  // response and resumes request reading once the queue drains;
  // AbortResponse closes without writing anything further.
  void BeginResponse(const http::HttpResponseHead &head);
  [[nodiscard]] bool WriteResponseBody(std::string_view bytes);
  void FinishResponse();
  void AbortResponse() noexcept;
  [[nodiscard]] std::size_t QueuedResponseBytes() const noexcept;
  void SetWriteDrainedCallback(WriteDrainedCallback callback);
  void SetRequestAbortCallback(RequestAbortCallback callback);
  // Clears both stream callbacks so a finished transaction cannot be retained
  // by the connection (R-043 symmetric).  Safe to call from either callback.
  void ClearStreamCallbacks() noexcept;
  void Close() noexcept;
  [[nodiscard]] bool reading_paused() const noexcept;
  [[nodiscard]] std::weak_ptr<void> LifetimeToken() const noexcept;

private:
  void HandleRead();
  void HandleWrite();
  void DeliverParsedRequest();
  void NotifyRequestAbort() noexcept;
  void NotifyDrainedIfBelowLow(bool was_above_low) noexcept;

  // Declaration order makes Channel tear down before Socket closes its fd.
  Socket socket_;
  Channel channel_;
  Buffer input_;
  Buffer output_;
  http::HttpRequestParser parser_;
  RequestCallback request_callback_;
  CloseCallback close_callback_;
  StreamFlowControl flow_control_;
  WriteDrainedCallback write_drained_callback_;
  RequestAbortCallback request_abort_callback_;
  bool reading_paused_ = false;
  bool writing_ = false;
  bool close_after_write_ = false;
  bool response_committed_ = false;
  bool response_finished_ = false;
  bool abort_fired_ = false;
  // Declare last so the token expires before any other member is destroyed.
  std::shared_ptr<int> lifetime_ = std::make_shared<int>(0);
};

} // namespace aegisgate::net
