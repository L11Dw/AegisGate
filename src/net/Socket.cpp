#include "aegisgate/net/Socket.h"

#include <cerrno>
#include <system_error>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace aegisgate::net {
namespace {

[[noreturn]] void ThrowSystemError(const char *operation) {
  throw std::system_error(errno, std::generic_category(), operation);
}

sockaddr_in LoopbackAddress(std::uint16_t port) {
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  return address;
}

} // namespace

Socket::Socket(int fd) noexcept : fd_(fd) {}

Socket::~Socket() { Close(); }

Socket::Socket(Socket &&other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

Socket &Socket::operator=(Socket &&other) noexcept {
  if (this != &other) {
    Close();
    fd_ = other.fd_;
    other.fd_ = -1;
  }
  return *this;
}

Socket Socket::ListenLoopback() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    ThrowSystemError("socket");
  }
  Socket listener(fd);

  int reuse_address = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse_address,
                   sizeof(reuse_address)) < 0) {
    ThrowSystemError("setsockopt");
  }

  const sockaddr_in address = LoopbackAddress(0);
  if (::bind(fd, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) <
      0) {
    ThrowSystemError("bind");
  }
  if (::listen(fd, SOMAXCONN) < 0) {
    ThrowSystemError("listen");
  }
  return listener;
}

Socket Socket::ConnectLoopback(std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                          0);
  if (fd < 0) {
    ThrowSystemError("socket");
  }
  Socket client(fd);
  const sockaddr_in address = LoopbackAddress(port);
  if (::connect(fd, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) <
          0 &&
      errno != EINPROGRESS) {
    ThrowSystemError("connect");
  }
  return client;
}

int Socket::Accept() const {
  const int accepted_fd =
      ::accept4(fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
  if (accepted_fd >= 0) {
    return accepted_fd;
  }
  if (errno == EAGAIN || errno == EWOULDBLOCK) {
    return -1;
  }
  ThrowSystemError("accept4");
}

std::uint16_t Socket::BoundPort() const {
  sockaddr_in address{};
  socklen_t address_length = sizeof(address);
  if (::getsockname(fd_, reinterpret_cast<sockaddr *>(&address), &address_length) <
      0) {
    ThrowSystemError("getsockname");
  }
  return ntohs(address.sin_port);
}

bool Socket::IsNonblocking() const {
  return IsNonblocking(fd_);
}

bool Socket::IsNonblocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL);
  if (flags < 0) {
    ThrowSystemError("fcntl");
  }
  return (flags & O_NONBLOCK) != 0;
}

bool Socket::Valid() const noexcept { return fd_ >= 0; }

int Socket::Fd() const noexcept { return fd_; }

void Socket::Close() noexcept {
  if (fd_ >= 0) {
    (void)::close(fd_);
    fd_ = -1;
  }
}

} // namespace aegisgate::net
