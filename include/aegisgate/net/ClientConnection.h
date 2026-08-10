#pragma once

#include <functional>
#include <memory>

#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/http/HttpResponse.h"
#include "aegisgate/net/Buffer.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/Socket.h"

namespace aegisgate::net {

class EventLoop;

class ClientConnection {
public:
  using RequestCallback =
      std::function<void(ClientConnection &, const http::HttpRequest &)>;
  using CloseCallback = std::function<void()>;

  ClientConnection(EventLoop &loop, int fd, RequestCallback callback);
  ~ClientConnection();

  ClientConnection(const ClientConnection &) = delete;
  ClientConnection &operator=(const ClientConnection &) = delete;
  ClientConnection(ClientConnection &&) = delete;
  ClientConnection &operator=(ClientConnection &&) = delete;

  void Start();
  void SetCloseCallback(CloseCallback callback);
  void ResumeReading();
  void SendResponse(const http::HttpResponse &response);
  void Close() noexcept;
  [[nodiscard]] bool reading_paused() const noexcept;
  [[nodiscard]] std::weak_ptr<void> LifetimeToken() const noexcept;

private:
  void HandleRead();
  void HandleWrite();
  void DeliverParsedRequest();

  // Declaration order makes Channel tear down before Socket closes its fd.
  Socket socket_;
  Channel channel_;
  Buffer input_;
  Buffer output_;
  http::HttpRequestParser parser_;
  RequestCallback request_callback_;
  CloseCallback close_callback_;
  bool reading_paused_ = false;
  bool writing_ = false;
  bool close_after_write_ = false;
  // Declare last so the token expires before any other member is destroyed.
  std::shared_ptr<int> lifetime_ = std::make_shared<int>(0);
};

} // namespace aegisgate::net
