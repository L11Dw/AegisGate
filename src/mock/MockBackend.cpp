#include "aegisgate/mock/MockBackend.h"

#include <stdexcept>
#include <system_error>
#include <utility>

#include <unistd.h>

#include "aegisgate/http/HttpResponse.h"
#include "aegisgate/http/HttpLimits.h"
#include "aegisgate/net/Acceptor.h"
#include "aegisgate/net/ClientConnection.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/net/TimerQueue.h"

namespace aegisgate::mock {

MockBackend::MockBackend(net::EventLoop &loop, MockBackendOptions options, std::string_view address,
                         std::uint16_t port)
    : loop_(loop), options_(options), response_body_(options.body_bytes, 'x'),
      state_(std::make_shared<State>()),
      timers_(std::make_unique<net::TimerQueue>(loop)),
      acceptor_(std::make_unique<net::Acceptor>(loop, address, port)) {
  if (options_.status < 200 || options_.status > 599) throw std::invalid_argument("mock status must be 200..599");
  if (options_.delay < std::chrono::milliseconds::zero()) throw std::invalid_argument("mock delay cannot be negative");
  if (options_.max_inflight == 0) throw std::invalid_argument("mock max_inflight must be positive");
  if (options_.body_bytes > http::kMaxUpstreamResponseBodyBytes) {
    throw std::invalid_argument("mock body exceeds upstream response limit");
  }
  state_->owner = this;
  acceptor_->SetNewConnectionCallback([this](int fd) { Accept(fd); });
}

MockBackend::~MockBackend() {
  state_->owner = nullptr;
  for (const auto &[id, timer] : pending_) { (void)id; (void)timers_->Cancel(timer); }
  pending_.clear();
  ids_.clear();
  clients_.clear();
  acceptor_.reset();
  timers_.reset();
}

void MockBackend::Start() { acceptor_->Listen(); }
std::uint16_t MockBackend::port() const { return acceptor_->port(); }

void MockBackend::Accept(int fd) {
  if (next_id_ == 0) { (void)::close(fd); return; }
  const std::uint64_t id = next_id_++;
  bool inserted = false;
  try {
    auto client = std::make_unique<net::ClientConnection>(loop_, fd,
        [this](net::ClientConnection &connection, const http::HttpRequest &request) { HandleRequest(connection, request); });
    client->SetCloseCallback([&loop = loop_, weak = std::weak_ptr<State>(state_), id] { Closed(loop, weak, id); });
    net::ClientConnection *raw = client.get();
    const auto result = clients_.emplace(id, std::move(client));
    if (!result.second) throw std::logic_error("duplicate mock client id");
    inserted = true;
    ids_.emplace(raw, id);
    result.first->second->Start();
  } catch (...) {
    if (inserted) { ids_.erase(clients_.at(id).get()); clients_.erase(id); }
  }
}

void MockBackend::HandleRequest(net::ClientConnection &client, const http::HttpRequest &) {
  if (options_.reset) { client.Close(); return; }
  if (inflight_ >= options_.max_inflight) { Send(client, true); return; }
  if (options_.delay == std::chrono::milliseconds::zero()) { Send(client, false); return; }
  const auto id = ids_.find(&client);
  if (id == ids_.end()) { client.Close(); return; }
  ++inflight_;
  std::uint64_t timer = 0;
  try {
    const std::weak_ptr<State> weak = state_;
    timer = timers_->ScheduleAfter(options_.delay, [weak, value = id->second] {
      if (const auto state = weak.lock(); state && state->owner) state->owner->Deliver(value);
    });
    pending_.emplace(id->second, timer);
  } catch (...) {
    if (timer != 0) (void)timers_->Cancel(timer);
    if (inflight_ != 0) --inflight_;
    Send(client, true);
  }
}

void MockBackend::Deliver(std::uint64_t id) {
  const auto pending = pending_.find(id);
  if (pending == pending_.end()) return;
  pending_.erase(pending);
  if (inflight_ != 0) --inflight_;
  const auto client = clients_.find(id);
  if (client != clients_.end()) Send(*client->second, false);
}

void MockBackend::Send(net::ClientConnection &client, bool capacity) {
  const int status = capacity ? 503 : options_.status;
  const char *reason = capacity ? "Mock Capacity" : (status >= 500 ? "Mock Failure" : "Mock Response");
  const std::string_view body = !capacity && status >= 200 && status < 300
                                    ? std::string_view(response_body_) : std::string_view{};
  try { client.SendResponse(http::HttpResponse{status, reason, {}, std::string(body)}); }
  catch (const std::logic_error &) { client.Close(); }
  catch (const std::system_error &) { client.Close(); }
}

void MockBackend::Closed(net::EventLoop &loop, std::weak_ptr<State> weak, std::uint64_t id) {
  const auto state = weak.lock();
  if (!state || !state->owner) return;
  state->closed.push_back(id);
  if (state->scheduled) return;
  state->scheduled = true;
  loop.QueueAfterCurrentBatch([state] {
    state->scheduled = false;
    if (!state->owner) return;
    auto ids = std::move(state->closed);
    state->closed.clear();
    state->owner->Reap(std::move(ids));
  });
}

void MockBackend::Reap(std::vector<std::uint64_t> ids) {
  for (const auto id : ids) {
    const auto pending = pending_.find(id);
    if (pending != pending_.end()) {
      (void)timers_->Cancel(pending->second);
      pending_.erase(pending);
      if (inflight_ != 0) --inflight_;
    }
    const auto client = clients_.find(id);
    if (client != clients_.end()) { ids_.erase(client->second.get()); clients_.erase(client); }
  }
}

} // namespace aegisgate::mock
