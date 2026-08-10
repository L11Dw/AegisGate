#pragma once

#include <cstdint>

namespace aegisgate::net {

class Socket {
public:
  enum class ConnectResult { kConnected, kInProgress, kError };
  Socket() noexcept = default;
  explicit Socket(int fd) noexcept;
  ~Socket();

  Socket(const Socket &) = delete;
  Socket &operator=(const Socket &) = delete;
  Socket(Socket &&other) noexcept;
  Socket &operator=(Socket &&other) noexcept;

  [[nodiscard]] static Socket ListenLoopback();
  [[nodiscard]] static Socket ConnectLoopback(std::uint16_t port);
  [[nodiscard]] static Socket CreateNonblockingTcp();

  // Starts a nonblocking IPv4 loopback connect. Both successful states must
  // pass PendingError() before application bytes may be sent.
  [[nodiscard]] ConnectResult ConnectToLoopback(std::uint16_t port) noexcept;
  [[nodiscard]] int PendingError() const;

  // Returns -1 when a nonblocking listener has no pending connection.
  [[nodiscard]] int Accept() const;
  [[nodiscard]] std::uint16_t BoundPort() const;
  [[nodiscard]] bool IsNonblocking() const;
  [[nodiscard]] static bool IsNonblocking(int fd);
  [[nodiscard]] bool Valid() const noexcept;
  [[nodiscard]] int Fd() const noexcept;
  void Close() noexcept;

private:
  int fd_ = -1;
};

} // namespace aegisgate::net
