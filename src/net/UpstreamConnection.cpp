#include "aegisgate/net/UpstreamConnection.h"

#include <array>
#include <cerrno>
#include <stdexcept>

#include <sys/socket.h>

#include "aegisgate/net/EventLoop.h"
#include "aegisgate/http/HttpRequestSerializer.h"

namespace aegisgate::net {

UpstreamConnection::UpstreamConnection(EventLoop &loop, std::uint16_t port,
                                       ResponseCallback callback)
    : loop_(loop), port_(port), callback_(std::move(callback)) {}

UpstreamConnection::~UpstreamConnection() { Close(); }

void UpstreamConnection::Start(const http::HttpRequest &request) {
  if (state_ != State::kIdle) {
    throw std::logic_error("upstream connection may only be started once");
  }

  try {
    output_.Append(http::SerializeRequest(request));
  } catch (const std::invalid_argument &) {
    Finish(UpstreamResult::kProtocolError);
    return;
  }
  socket_ = std::make_unique<Socket>(Socket::CreateNonblockingTcp());
  channel_ = std::make_unique<Channel>(loop_, socket_->Fd());
  channel_->SetReadCallback([this] { HandleRead(); });
  channel_->SetWriteCallback([this] { HandleWrite(); });
  state_ = State::kConnecting;
  const Socket::ConnectResult connect_result = socket_->ConnectToLoopback(port_);
  if (connect_result == Socket::ConnectResult::kError) {
    Finish(UpstreamResult::kConnectError);
    return;
  }
  channel_->EnableWriting();
  if (connect_result == Socket::ConnectResult::kConnected) HandleWrite();
}

void UpstreamConnection::Close() noexcept {
  if (state_ == State::kFinished) return;
  state_ = State::kFinished;
  if (channel_) {
    try { channel_->DisableAll(); } catch (...) {}
  }
  channel_.reset();
  if (socket_) socket_->Close();
  socket_.reset();
}

void UpstreamConnection::HandleRead() {
  if (state_ == State::kConnecting || state_ == State::kWriting) {
    HandleWrite();
    return;
  }
  if (state_ != State::kReading) return;

  std::array<char, 64 * 1024> bytes{};
  for (;;) {
    const ssize_t count = ::recv(socket_->Fd(), bytes.data(), bytes.size(), 0);
    if (count > 0) {
      input_.Append(std::string_view(bytes.data(), static_cast<std::size_t>(count)));
      switch (parser_.Parse(input_)) {
      case http::ParseResult::kNeedMoreData:
        continue;
      case http::ParseResult::kComplete:
        Finish(UpstreamResult::kSuccess);
        return;
      case http::ParseResult::kError:
        Finish(UpstreamResult::kProtocolError);
        return;
      case http::ParseResult::kUnsupported:
        Finish(UpstreamResult::kUnsupported);
        return;
      }
    }
    if (count == 0) {
      Finish(UpstreamResult::kEof);
      return;
    }
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) return;
    Finish(UpstreamResult::kReadError);
    return;
  }
}

void UpstreamConnection::HandleWrite() {
  if (state_ == State::kConnecting) {
    int pending_error = 0;
    try {
      pending_error = socket_->PendingError();
    } catch (...) {
      Finish(UpstreamResult::kConnectError);
      return;
    }
    if (pending_error != 0) {
      Finish(UpstreamResult::kConnectError);
      return;
    }
    state_ = State::kWriting;
  }
  if (state_ != State::kWriting) return;

  while (output_.ReadableBytes() != 0U) {
    const std::string_view bytes = output_.ReadableView();
    const ssize_t count = ::send(socket_->Fd(), bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (count > 0) {
      output_.Retrieve(static_cast<std::size_t>(count));
      continue;
    }
    if (count < 0 && errno == EINTR) continue;
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      channel_->EnableWriting();
      return;
    }
    Finish(UpstreamResult::kWriteError);
    return;
  }

  channel_->DisableWriting();
  state_ = State::kReading;
  channel_->EnableReading();
}

void UpstreamConnection::Finish(UpstreamResult result) {
  if (state_ == State::kFinished) return;
  state_ = State::kFinished;
  if (channel_) {
    try { channel_->DisableAll(); } catch (...) {}
  }
  channel_.reset();
  if (socket_) socket_->Close();
  socket_.reset();
  const ResponseCallback callback = callback_;
  http::HttpResponse response;
  if (result == UpstreamResult::kSuccess) response = parser_.Response();
  if (callback) callback(result, std::move(response));
}

} // namespace aegisgate::net
