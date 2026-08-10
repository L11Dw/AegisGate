#pragma once

#include <cstdint>
#include <functional>
#include <string_view>

#include "aegisgate/net/Channel.h"
#include "aegisgate/net/Socket.h"

namespace aegisgate::net {

class EventLoop;

class Acceptor {
public:
  using NewConnectionCallback = std::function<void(int)>;

  Acceptor(EventLoop &loop, std::string_view address, std::uint16_t port);
  ~Acceptor() noexcept = default;

  Acceptor(const Acceptor &) = delete;
  Acceptor &operator=(const Acceptor &) = delete;
  Acceptor(Acceptor &&) = delete;
  Acceptor &operator=(Acceptor &&) = delete;

  void SetNewConnectionCallback(NewConnectionCallback callback);
  void Listen();
  [[nodiscard]] std::uint16_t port() const;

private:
  void HandleRead();

  // Member order ensures the Channel unregisters before the listening socket
  // is closed during destruction.
  Socket listen_socket_;
  Channel accept_channel_;
  NewConnectionCallback new_connection_callback_;
};

} // namespace aegisgate::net
