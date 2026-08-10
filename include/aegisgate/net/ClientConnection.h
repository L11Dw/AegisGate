#pragma once

#include <functional>

#include "aegisgate/http/HttpRequestParser.h"
#include "aegisgate/net/Buffer.h"
#include "aegisgate/net/Channel.h"
#include "aegisgate/net/Socket.h"

namespace aegisgate::net {

class EventLoop;

class ClientConnection {
public:
  using RequestCallback =
      std::function<void(ClientConnection &, const http::HttpRequest &)>;

  ClientConnection(EventLoop &loop, int fd, RequestCallback callback);
  ~ClientConnection();

  ClientConnection(const ClientConnection &) = delete;
  ClientConnection &operator=(const ClientConnection &) = delete;
  ClientConnection(ClientConnection &&) = delete;
  ClientConnection &operator=(ClientConnection &&) = delete;

  void Start();
  void ResumeReading();
  void Close() noexcept;
  [[nodiscard]] bool reading_paused() const noexcept;

private:
  void HandleRead();

  // Declaration order makes Channel tear down before Socket closes its fd.
  Socket socket_;
  Channel channel_;
  Buffer input_;
  http::HttpRequestParser parser_;
  RequestCallback request_callback_;
  bool reading_paused_ = false;
};

} // namespace aegisgate::net
