#include "aegisgate/net/ClientConnection.h"

#include <array>
#include <cerrno>
#include <system_error>

#include <fcntl.h>
#include <sys/socket.h>

#include "aegisgate/net/EventLoop.h"

namespace aegisgate::net {
namespace {

void SetNonblocking(int fd) {
  int flags = 0;
  do {
    flags = ::fcntl(fd, F_GETFL);
  } while (flags < 0 && errno == EINTR);
  if (flags < 0) {
    throw std::system_error(errno, std::generic_category(), "fcntl F_GETFL");
  }

  while (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    if (errno != EINTR) {
      throw std::system_error(errno, std::generic_category(), "fcntl F_SETFL");
    }
  }
}

} // namespace

ClientConnection::ClientConnection(EventLoop &loop, int fd, RequestCallback callback)
    : socket_(fd), channel_(loop, fd), request_callback_(std::move(callback)) {
  SetNonblocking(fd);
  channel_.SetReadCallback([this] { HandleRead(); });
}

ClientConnection::~ClientConnection() { Close(); }

void ClientConnection::Start() {
  if (socket_.Valid() && !reading_paused_) {
    channel_.EnableReading();
  }
}

void ClientConnection::ResumeReading() {
  if (!socket_.Valid() || !reading_paused_) {
    return;
  }

  parser_.Reset();
  reading_paused_ = false;
  if (input_.ReadableBytes() != 0) {
    switch (parser_.Parse(input_)) {
    case http::ParseResult::kNeedMoreData:
      break;
    case http::ParseResult::kComplete: {
      channel_.DisableAll();
      reading_paused_ = true;
      const RequestCallback callback = request_callback_;
      const http::HttpRequest request = parser_.Request();
      if (callback) {
        callback(*this, request);
      }
      return;
    }
    case http::ParseResult::kError:
    case http::ParseResult::kUnsupported:
      Close();
      return;
    }
  }
  channel_.EnableReading();
}

void ClientConnection::Close() noexcept {
  if (!socket_.Valid()) {
    return;
  }
  try {
    channel_.DisableAll();
  } catch (...) {
  }
  socket_.Close();
}

bool ClientConnection::reading_paused() const noexcept { return reading_paused_; }

void ClientConnection::HandleRead() {
  if (reading_paused_ || !socket_.Valid()) {
    return;
  }

  std::array<char, 64 * 1024> bytes{};
  for (;;) {
    const ssize_t count = ::recv(socket_.Fd(), bytes.data(), bytes.size(), 0);
    if (count > 0) {
      input_.Append(std::string_view(bytes.data(), static_cast<std::size_t>(count)));
      switch (parser_.Parse(input_)) {
      case http::ParseResult::kNeedMoreData:
        continue;
      case http::ParseResult::kComplete: {
        channel_.DisableAll();
        reading_paused_ = true;
        const RequestCallback callback = request_callback_;
        const http::HttpRequest request = parser_.Request();
        if (callback) {
          callback(*this, request);
        }
        return;
      }
      case http::ParseResult::kError:
      case http::ParseResult::kUnsupported:
        Close();
        return;
      }
    }
    if (count == 0) {
      Close();
      return;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    Close();
    return;
  }
}

} // namespace aegisgate::net
