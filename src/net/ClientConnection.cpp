#include "aegisgate/net/ClientConnection.h"

#include <array>
#include <cerrno>
#include <cctype>
#include <stdexcept>
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

bool EqualsIgnoreCase(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (std::size_t index = 0; index < left.size(); ++index) {
    if (std::tolower(static_cast<unsigned char>(left[index])) !=
        std::tolower(static_cast<unsigned char>(right[index]))) {
      return false;
    }
  }
  return true;
}

bool ContainsConnectionClose(std::string_view value) {
  while (!value.empty()) {
    const std::size_t comma = value.find(',');
    std::string_view token = value.substr(0, comma);
    while (!token.empty() && std::isspace(static_cast<unsigned char>(token.front()))) {
      token.remove_prefix(1);
    }
    while (!token.empty() && std::isspace(static_cast<unsigned char>(token.back()))) {
      token.remove_suffix(1);
    }
    if (EqualsIgnoreCase(token, "close")) {
      return true;
    }
    if (comma == std::string_view::npos) {
      return false;
    }
    value.remove_prefix(comma + 1);
  }
  return false;
}

bool ResponseWantsClose(const http::HttpResponse &response) {
  for (const auto &[name, value] : response.headers) {
    if (EqualsIgnoreCase(name, "connection") && ContainsConnectionClose(value)) {
      return true;
    }
  }
  return false;
}

} // namespace

ClientConnection::ClientConnection(EventLoop &loop, int fd, RequestCallback callback)
    : socket_(fd), channel_(loop, fd), request_callback_(std::move(callback)) {
  SetNonblocking(fd);
  channel_.SetReadCallback([this] { HandleRead(); });
  channel_.SetWriteCallback([this] { HandleWrite(); });
}

ClientConnection::~ClientConnection() { Close(); }

void ClientConnection::Start() {
  if (socket_.Valid() && !reading_paused_) {
    channel_.EnableReading();
  }
}

void ClientConnection::SetCloseCallback(CloseCallback callback) {
  close_callback_ = std::move(callback);
}

void ClientConnection::ResumeReading() {
  if (!socket_.Valid() || !reading_paused_ || writing_) {
    return;
  }

  parser_.Reset();
  reading_paused_ = false;
  if (input_.ReadableBytes() != 0) {
    switch (parser_.Parse(input_)) {
    case http::ParseResult::kNeedMoreData:
      break;
    case http::ParseResult::kComplete: {
      DeliverParsedRequest();
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

void ClientConnection::SendResponse(const http::HttpResponse &response) {
  if (!socket_.Valid() || !reading_paused_ || writing_) {
    throw std::logic_error("response is not valid in the current connection state");
  }

  const std::string serialized = response.Serialize();
  output_.Append(serialized);
  writing_ = true;
  close_after_write_ = close_after_write_ || ResponseWantsClose(response);
  HandleWrite();
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
  try {
    if (close_callback_) close_callback_();
  } catch (...) {
  }
}

bool ClientConnection::reading_paused() const noexcept { return reading_paused_; }

std::weak_ptr<void> ClientConnection::LifetimeToken() const noexcept {
  return lifetime_;
}

void ClientConnection::HandleRead() {
  if (!socket_.Valid()) {
    return;
  }
  if (reading_paused_) {
    // With EPOLLIN disabled, this callback is a terminal event. A peer may
    // half-close after submitting a complete request; preserve the accepted
    // response and close only after its pending bytes drain. Terminal events
    // take precedence over EPOLLOUT in Channel, so retry the write here.
    close_after_write_ = true;
    if (writing_) {
      HandleWrite();
    }
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
        DeliverParsedRequest();
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

void ClientConnection::HandleWrite() {
  if (!socket_.Valid() || !writing_) {
    return;
  }

  while (output_.ReadableBytes() != 0U) {
    const std::string_view bytes = output_.ReadableView();
    const ssize_t count = ::send(socket_.Fd(), bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (count > 0) {
      output_.Retrieve(static_cast<std::size_t>(count));
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      channel_.EnableWriting();
      return;
    }
    Close();
    return;
  }

  writing_ = false;
  channel_.DisableWriting();
  if (close_after_write_) {
    Close();
    return;
  }
  ResumeReading();
}

void ClientConnection::DeliverParsedRequest() {
  channel_.DisableReading();
  reading_paused_ = true;
  close_after_write_ = ContainsConnectionClose(parser_.Request().Header("connection"));
  const RequestCallback callback = request_callback_;
  const http::HttpRequest request = parser_.Request();
  if (callback) {
    callback(*this, request);
  }
}

} // namespace aegisgate::net
