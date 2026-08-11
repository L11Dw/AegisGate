#include "aegisgate/proxy/UpstreamPool.h"

#include <cassert>
#include <stdexcept>
#include <utility>

#include "aegisgate/net/EventLoop.h"

namespace aegisgate::proxy {

UpstreamPool::UpstreamPool(net::EventLoop &loop) : loop_(loop) {}
UpstreamPool::~UpstreamPool() = default;

net::UpstreamConnection *UpstreamPool::Execute(
    const config::Endpoint &endpoint, const http::HttpRequest &request,
    ResponseCallback callback, ProgressCallback progress,
    net::UpstreamConnection::HeaderCallback header,
    net::UpstreamConnection::BodySink body) {
  const Key key = ToKey(endpoint);
  Connection connection;
  auto &idle = idle_[key];
  while (!idle.empty()) {
    connection = std::move(idle.front());
    idle.pop_front();
    if (connection->HealthyForReuse()) break;
    connection.reset();
  }
  if (!connection) {
    connection = std::make_unique<net::UpstreamConnection>(loop_, endpoint, ResponseCallback{});
  }
  net::UpstreamConnection *raw = connection.get();
  const auto [position, inserted] = active_.emplace(raw, std::move(connection));
  if (!inserted) throw std::logic_error("upstream connection already active");
  raw->SetResponseCallback([this, raw, key, callback = std::move(callback)](
                               net::UpstreamResult result, http::HttpResponse response) mutable {
    Complete(raw, key, std::move(callback), result, std::move(response));
  });
  raw->SetProgressCallback(std::move(progress));
  if (header || body) {
    raw->SetStreamingCallbacks(std::move(header), std::move(body));
  }
  try {
    raw->Start(request);
  } catch (...) {
    // Start may fail before it has completed through Complete().  Do not leave
    // a connection (and its callback-captured transaction) stranded active.
    const auto active = active_.find(raw);
    if (active != active_.end()) {
      active->second->Close();
      active_.erase(active);
    }
    throw;
  }
  return active_.contains(raw) ? raw : nullptr;
}

bool UpstreamPool::Cancel(net::UpstreamConnection *connection) noexcept {
  const auto active = active_.find(connection);
  if (active == active_.end()) return false;
  active->second->Close();
  active_.erase(active);
  return true;
}

void UpstreamPool::CancelAll() noexcept {
  // Contract: callable only from the EventLoop owner thread (the gateway
  // teardown path runs on that thread as well).  A non-owner call would race
  // on dispatching_event_ and active_; debug builds reject it outright.
  assert(loop_.IsOwnerThread());
  // Take ownership of every active exchange immediately and suppress both
  // callbacks (logical cancellation: no terminal result, no progress event,
  // and the response callback's captured transaction is released via RAII).
  auto pending = std::make_shared<std::vector<Connection>>();
  for (auto &[connection, owned] : active_) {
    (void)connection;
    pending->push_back(std::move(owned));
  }
  active_.clear();
  for (auto &connection : *pending) connection->SuppressCallbacks();
  // Closing inside a Channel::HandleEvent stack would destroy the currently
  // dispatching Channel; defer the actual Close to the end of this epoll batch
  // (the deferred closure owns the connections).  Outside a callback stack the
  // close is safe immediately.
  if (loop_.IsDispatchingEvent()) {
    loop_.QueueAfterCurrentBatch([pending] {
      for (auto &connection : *pending) connection->Close();
      pending->clear();
    });
  } else {
    for (auto &connection : *pending) connection->Close();
  }
}

std::size_t UpstreamPool::IdleCount(const config::Endpoint &endpoint) const noexcept {
  const auto iterator = idle_.find(ToKey(endpoint));
  return iterator == idle_.end() ? 0U : iterator->second.size();
}

UpstreamPool::Key UpstreamPool::ToKey(const config::Endpoint &endpoint) noexcept {
  return {endpoint.address, endpoint.port};
}

void UpstreamPool::Complete(net::UpstreamConnection *connection, Key key,
                            ResponseCallback callback, net::UpstreamResult result,
                            http::HttpResponse response) {
  const auto active = active_.find(connection);
  if (active == active_.end()) return;
  Connection owned = std::move(active->second);
  active_.erase(active);
  if (owned->Reusable()) {
    auto &idle = idle_[key];
    // Keep the earliest successfully-idled descriptor. A per-endpoint cap of
    // one bounds resources and preserves FIFO borrow order; the newer return
    // is actively closed rather than silently retaining a second idle fd.
    if (idle.empty()) {
      idle.push_back(std::move(owned));
    } else {
      owned->Close();
    }
  }
  if (callback) callback(result, std::move(response));
}

} // namespace aegisgate::proxy
