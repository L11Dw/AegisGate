#include "aegisgate/net/UpstreamConnection.h"

#include <array>
#include <cerrno>
#include <stdexcept>
#include <string_view>

#include <sys/socket.h>

#include "aegisgate/net/EventLoop.h"
#include "aegisgate/http/HttpRequestSerializer.h"

namespace aegisgate::net {

UpstreamConnection::UpstreamConnection(EventLoop &loop, std::uint16_t port,
                                       ResponseCallback callback)
    : loop_(loop), port_(port), callback_(std::move(callback)) {}

UpstreamConnection::UpstreamConnection(EventLoop &loop, config::Endpoint endpoint,
                                       ResponseCallback callback)
    : loop_(loop), address_(endpoint.address), port_(endpoint.port), callback_(std::move(callback)) {}

UpstreamConnection::~UpstreamConnection() { Close(); }

void UpstreamConnection::Start(const http::HttpRequest &request) {
  if (state_ != State::kIdle) {
    throw std::logic_error("upstream connection is not idle");
  }

  reusable_ = false;
  input_.RetrieveAll();
  output_.RetrieveAll();
  parser_.Reset();
  first_byte_reported_ = false;
  response_header_reported_ = false;

  try {
    output_.Append(http::SerializeRequest(request));
  } catch (const std::invalid_argument &) {
    Finish(UpstreamResult::kProtocolError);
    return;
  }
  if (socket_) {
    state_ = State::kWriting;
    // An idle pooled descriptor completed its TCP handshake on a prior
    // exchange.  Its new transaction must still clear the connect deadline.
    if (progress_callback_) progress_callback_(UpstreamProgress::kConnected);
    channel_->EnableWriting();
    HandleWrite();
    return;
  }
  socket_ = std::make_unique<Socket>(Socket::CreateNonblockingTcp());
  // Bound one nonblocking upstream exchange's kernel queue. Besides bounding
  // memory, this ensures EPOLLOUT backpressure is exercised rather than
  // allowing an unbounded local loopback send to mask write-state bugs.
  int send_buffer = 64 * 1024;
  if (::setsockopt(socket_->Fd(), SOL_SOCKET, SO_SNDBUF, &send_buffer, sizeof(send_buffer)) < 0) {
    Finish(UpstreamResult::kConnectError);
    return;
  }
  channel_ = std::make_unique<Channel>(loop_, socket_->Fd());
  channel_->SetReadCallback([this] { HandleRead(); });
  channel_->SetWriteCallback([this] { HandleWrite(); });
  state_ = State::kConnecting;
  const Socket::ConnectResult connect_result = socket_->ConnectToIpv4(address_, port_);
  if (connect_result == Socket::ConnectResult::kError) {
    Finish(UpstreamResult::kConnectError);
    return;
  }
  channel_->EnableWriting();
  if (connect_result == Socket::ConnectResult::kConnected) HandleWrite();
}

void UpstreamConnection::SetResponseCallback(ResponseCallback callback) {
  if (state_ != State::kIdle) throw std::logic_error("upstream connection is not idle");
  callback_ = std::move(callback);
}

void UpstreamConnection::SetProgressCallback(ProgressCallback callback) {
  if (state_ != State::kIdle) throw std::logic_error("upstream connection is not idle");
  progress_callback_ = std::move(callback);
}

bool UpstreamConnection::Reusable() const noexcept { return state_ == State::kIdle && reusable_; }

bool UpstreamConnection::HealthyForReuse() noexcept {
  if (!Reusable()) return false;
  if (!PeerHasClosedOrSentExtraBytes()) return true;
  Close();
  return false;
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
      if (!first_byte_reported_) {
        first_byte_reported_ = true;
        if (progress_callback_) progress_callback_(UpstreamProgress::kFirstByte);
      }
      const auto parse_result = parser_.Parse(input_);
      if (parser_.HeadersComplete() && !response_header_reported_) {
        response_header_reported_ = true;
        if (progress_callback_) progress_callback_(UpstreamProgress::kResponseHeader);
      }
      switch (parse_result) {
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
    if (progress_callback_) progress_callback_(UpstreamProgress::kConnected);
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
  // The first-byte deadline begins only once the complete serialized request
  // has reached the kernel. A backpressured send must remain a write/connect
  // concern, not be misreported as an origin first-byte timeout.
  if (progress_callback_) progress_callback_(UpstreamProgress::kRequestWritten);
  channel_->EnableReading();
}

void UpstreamConnection::Finish(UpstreamResult result) {
  if (state_ == State::kFinished) return;
  const bool keep_alive = result == UpstreamResult::kSuccess && input_.ReadableBytes() == 0U &&
                          !ResponseRequestsClose() && !PeerHasClosedOrSentExtraBytes();
  http::HttpResponse response;
  if (result == UpstreamResult::kSuccess) response = parser_.Response();
  ResponseCallback callback = std::move(callback_);
  if (keep_alive) {
    reusable_ = true;
    state_ = State::kIdle;
    if (channel_) {
      try { channel_->DisableAll(); } catch (...) { Close(); }
    }
    if (callback) callback(result, std::move(response));
    return;
  }
  state_ = State::kFinished;
  if (channel_) {
    try { channel_->DisableAll(); } catch (...) {}
  }
  channel_.reset();
  if (socket_) socket_->Close();
  socket_.reset();
  if (callback) callback(result, std::move(response));
}

bool UpstreamConnection::PeerHasClosedOrSentExtraBytes() const noexcept {
  char byte = '\0';
  for (;;) {
    const ssize_t count = ::recv(socket_->Fd(), &byte, 1, MSG_PEEK | MSG_DONTWAIT);
    if (count == 0 || count > 0) return true;
    if (errno == EINTR) continue;
    return errno != EAGAIN && errno != EWOULDBLOCK;
  }
}

bool UpstreamConnection::ResponseRequestsClose() const noexcept {
  for (const auto &[name, value] : parser_.Response().headers) {
    if (name.size() != 10) continue;
    constexpr std::string_view kConnection = "connection";
    bool is_connection = true;
    for (std::size_t index = 0; index < name.size(); ++index) {
      const char lower = name[index] >= 'A' && name[index] <= 'Z'
                             ? static_cast<char>(name[index] - 'A' + 'a') : name[index];
      if (lower != kConnection[index]) { is_connection = false; break; }
    }
    if (!is_connection) continue;
    std::size_t start = 0;
    while (start < value.size()) {
      const std::size_t comma = value.find(',', start);
      const std::string_view value_view(value);
      std::string_view token = value_view.substr(start, comma == std::string_view::npos
                                                            ? comma : comma - start);
      while (!token.empty() && (token.front() == ' ' || token.front() == '\t')) token.remove_prefix(1);
      while (!token.empty() && (token.back() == ' ' || token.back() == '\t')) token.remove_suffix(1);
      if (token.size() == 5) {
        constexpr std::string_view kClose = "close";
        bool is_close = true;
        for (std::size_t index = 0; index < token.size(); ++index) {
          const char lower = token[index] >= 'A' && token[index] <= 'Z'
                                 ? static_cast<char>(token[index] - 'A' + 'a') : token[index];
          if (lower != kClose[index]) { is_close = false; break; }
        }
        if (is_close) return true;
      }
      if (comma == std::string_view::npos) break;
      start = comma + 1;
    }
  }
  return false;
}

} // namespace aegisgate::net
