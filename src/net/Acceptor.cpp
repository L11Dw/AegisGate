#include "aegisgate/net/Acceptor.h"

#include <cerrno>
#include <stdexcept>
#include <system_error>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "aegisgate/net/EventLoop.h"

namespace aegisgate::net {
namespace {

[[nodiscard]] Socket CreateListeningSocket(std::string_view address,
                                           std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(), "socket");
  }
  Socket listener(fd);

  int reuse_address = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse_address,
                   sizeof(reuse_address)) < 0) {
    throw std::system_error(errno, std::generic_category(), "setsockopt");
  }

  sockaddr_in socket_address{};
  socket_address.sin_family = AF_INET;
  socket_address.sin_port = htons(port);
  const std::string address_string(address);
  if (::inet_pton(AF_INET, address_string.c_str(), &socket_address.sin_addr) !=
      1) {
    throw std::invalid_argument("Acceptor address must be a valid IPv4 address");
  }

  if (::bind(fd, reinterpret_cast<const sockaddr *>(&socket_address),
             sizeof(socket_address)) < 0) {
    throw std::system_error(errno, std::generic_category(), "bind");
  }
  return listener;
}

} // namespace

Acceptor::Acceptor(EventLoop &loop, std::string_view address, std::uint16_t port)
    : listen_socket_(CreateListeningSocket(address, port)),
      accept_channel_(loop, listen_socket_.Fd()) {
  accept_channel_.SetReadCallback([this] { HandleRead(); });
}

void Acceptor::SetNewConnectionCallback(NewConnectionCallback callback) {
  new_connection_callback_ = std::move(callback);
}

void Acceptor::Listen() {
  if (::listen(listen_socket_.Fd(), SOMAXCONN) < 0) {
    throw std::system_error(errno, std::generic_category(), "listen");
  }
  accept_channel_.EnableReading();
}

std::uint16_t Acceptor::port() const { return listen_socket_.BoundPort(); }

void Acceptor::HandleRead() {
  for (;;) {
    const int accepted_fd = listen_socket_.Accept();
    if (accepted_fd == -1) {
      return;
    }
    if (!new_connection_callback_) {
      (void)::close(accepted_fd);
      continue;
    }
    try {
      new_connection_callback_(accepted_fd);
    } catch (...) {
      (void)::close(accepted_fd);
      throw;
    }
  }
}

} // namespace aegisgate::net
