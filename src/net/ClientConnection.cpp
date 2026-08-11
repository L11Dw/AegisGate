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

bool HeadWantsClose(const http::HttpResponseHead &head) {
  for (const auto &[name, value] : head.headers) {
    if (EqualsIgnoreCase(name, "connection") && ContainsConnectionClose(value)) {
      return true;
    }
  }
  return false;
}

} // namespace

ClientConnection::ClientConnection(EventLoop &loop, int fd, RequestCallback callback,
                                   StreamFlowControl flow_control)
    : socket_(fd), channel_(loop, fd), request_callback_(std::move(callback)),
      flow_control_(flow_control) {
  SetNonblocking(fd);
  // Bound the kernel send queue (symmetric with the upstream connection) so a
  // slow peer cannot absorb an unbounded response in the kernel: the queued
  // bytes then cross the high watermark, pause the upstream read, and apply
  // real receive-window backpressure instead of unbounded memory use.
  int send_buffer = 64 * 1024;
  (void)::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &send_buffer, sizeof(send_buffer));
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
  // A new request cycle starts: the previous response's streaming state must
  // not leak into the next SendResponse/BeginResponse.
  response_committed_ = false;
  response_finished_ = false;
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
  if (!socket_.Valid() || !reading_paused_ || writing_ || response_committed_ ||
      response_finished_) {
    throw std::logic_error("response is not valid in the current connection state");
  }

  const std::string serialized = response.Serialize();
  output_.Append(serialized);
  writing_ = true;
  close_after_write_ = close_after_write_ || ResponseWantsClose(response);
  HandleWrite();
}

void ClientConnection::BeginResponse(const http::HttpResponseHead &head) {
  if (!socket_.Valid() || !reading_paused_ || writing_ || response_committed_ ||
      response_finished_) {
    throw std::logic_error("response is not valid in the current connection state");
  }

  const std::string serialized = head.Serialize();
  output_.Append(serialized);
  writing_ = true;
  response_committed_ = true;
  close_after_write_ = close_after_write_ || HeadWantsClose(head);
  HandleWrite();
}

bool ClientConnection::WriteResponseBody(std::string_view bytes) {
  if (!socket_.Valid() || !response_committed_ || response_finished_) {
    throw std::logic_error("response body is not valid in the current connection state");
  }
  if (bytes.empty()) {
    return false;
  }
  output_.Append(bytes);
  if (!writing_) {
    writing_ = true;
  }
  HandleWrite();
  // True while the queued bytes sit at or above the high watermark: the
  // caller must keep the upstream reading paused until a low-water drain.
  return output_.ReadableBytes() >= flow_control_.HighWatermark();
}

void ClientConnection::FinishResponse() {
  if (!socket_.Valid() || !response_committed_ || response_finished_) {
    throw std::logic_error("response is not valid in the current connection state");
  }
  response_finished_ = true;
  if (!writing_) {
    ResumeReading();
  }
}

void ClientConnection::AbortResponse() noexcept { Close(); }

std::size_t ClientConnection::QueuedResponseBytes() const noexcept {
  return output_.ReadableBytes();
}

void ClientConnection::SetWriteDrainedCallback(WriteDrainedCallback callback) {
  write_drained_callback_ = std::move(callback);
}

void ClientConnection::SetRequestAbortCallback(RequestAbortCallback callback) {
  request_abort_callback_ = std::move(callback);
}

void ClientConnection::ClearStreamCallbacks() noexcept {
  write_drained_callback_ = nullptr;
  request_abort_callback_ = nullptr;
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
    // With EPOLLIN disabled, this callback is a terminal event: only
    // EPOLLERR/EPOLLHUP/EPOLLRDHUP can fire here, so the peer connection is
    // gone (RST or error) or half-closed.  Notify the streaming transaction
    // once so it can cancel the upstream exchange; the accepted response
    // still drains to the peer if the socket is writable.  Terminal events
    // take precedence over EPOLLOUT in Channel, so retry the write here.
    NotifyRequestAbort();
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

  const bool was_above_low = output_.ReadableBytes() > flow_control_.LowWatermark();
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
      NotifyDrainedIfBelowLow(was_above_low);
      channel_.EnableWriting();
      return;
    }
    NotifyRequestAbort();
    Close();
    return;
  }

  writing_ = false;
  channel_.DisableWriting();
  if (close_after_write_) {
    Close();
    return;
  }
  if (response_committed_ && !response_finished_) {
    // Streaming: more body chunks may follow, so request reading stays paused
    // and the peer is not notified until FinishResponse().
    NotifyDrainedIfBelowLow(was_above_low);
    return;
  }
  ResumeReading();
}

void ClientConnection::NotifyRequestAbort() noexcept {
  if (abort_fired_) {
    return;
  }
  abort_fired_ = true;
  const RequestAbortCallback callback = request_abort_callback_;
  if (callback) {
    callback();
  }
}

void ClientConnection::NotifyDrainedIfBelowLow(bool was_above_low) noexcept {
  if (!response_committed_ || response_finished_ || !was_above_low ||
      output_.ReadableBytes() > flow_control_.LowWatermark()) {
    return;
  }
  const WriteDrainedCallback callback = write_drained_callback_;
  if (callback) {
    callback();
  }
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
