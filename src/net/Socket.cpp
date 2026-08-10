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

sockaddr_in Ipv4Address(const std::array<std::uint8_t, 4> &bytes, std::uint16_t port) {
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl((static_cast<std::uint32_t>(bytes[0]) << 24U) |
                                  (static_cast<std::uint32_t>(bytes[1]) << 16U) |
                                  (static_cast<std::uint32_t>(bytes[2]) << 8U) |
                                  static_cast<std::uint32_t>(bytes[3]));
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

  const sockaddr_in address = Ipv4Address({127, 0, 0, 1}, 0);
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
  Socket client = CreateNonblockingTcp();
  if (client.ConnectToLoopback(port) == ConnectResult::kError) {
    ThrowSystemError("connect");
  }
  return client;
}

Socket Socket::CreateNonblockingTcp() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) ThrowSystemError("socket");
  return Socket(fd);
}

Socket::ConnectResult Socket::ConnectToLoopback(std::uint16_t port) noexcept {
  return ConnectToIpv4({127, 0, 0, 1}, port);
}

Socket::ConnectResult Socket::ConnectToIpv4(const std::array<std::uint8_t, 4> &bytes,
                                             std::uint16_t port) noexcept {
  const sockaddr_in address = Ipv4Address(bytes, port);
  for (;;) {
    if (::connect(fd_, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) == 0) {
      return ConnectResult::kConnected;
    }
    if (errno == EINTR) continue;
    if (errno == EINPROGRESS || errno == EAGAIN || errno == EWOULDBLOCK) {
      return ConnectResult::kInProgress;
    }
    return ConnectResult::kError;
  }
}

int Socket::PendingError() const {
  int error = 0;
  socklen_t length = sizeof(error);
  if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &length) < 0) {
    ThrowSystemError("getsockopt SO_ERROR");
  }
  return error;
}

int Socket::Accept() const {
  for (;;) {
    const int accepted_fd =
        ::accept4(fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (accepted_fd >= 0) {
      return accepted_fd;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return -1;
    }
    ThrowSystemError("accept4");
  }
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
