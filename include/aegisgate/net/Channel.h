#pragma once

#include <cstdint>
#include <functional>

namespace aegisgate::net {

class EventLoop;

// A non-owning registration of one file descriptor with an EventLoop.
// Every registered Channel must be destroyed before its EventLoop. A removed
// Channel may outlive its EventLoop.
class Channel {
public:
  using ReadCallback = std::function<void()>;
  using WriteCallback = std::function<void()>;

  Channel(EventLoop &loop, int fd) noexcept;
  ~Channel() noexcept;

  Channel(const Channel &) = delete;
  Channel &operator=(const Channel &) = delete;
  Channel(Channel &&) = delete;
  Channel &operator=(Channel &&) = delete;

  void SetReadCallback(ReadCallback callback);
  void SetWriteCallback(WriteCallback callback);
  void EnableReading();
  void DisableReading();
  void EnableWriting();
  void DisableWriting();
  void DisableAll();
  void Remove();

private:
  friend class EventLoop;

  void HandleEvent(std::uint32_t events);
  void UpdateOrRemove();

  EventLoop &loop_;
  int fd_ = -1;
  std::uint32_t events_ = 0;
  std::uint64_t registration_token_ = 0;
  bool added_ = false;
  ReadCallback read_callback_;
  WriteCallback write_callback_;
};

} // namespace aegisgate::net
