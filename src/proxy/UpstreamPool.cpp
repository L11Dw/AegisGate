#include "aegisgate/proxy/UpstreamPool.h"

#include <stdexcept>
#include <utility>

#include "aegisgate/net/EventLoop.h"

namespace aegisgate::proxy {

UpstreamPool::UpstreamPool(net::EventLoop &loop) : loop_(loop) {}
UpstreamPool::~UpstreamPool() = default;

void UpstreamPool::Execute(const config::Endpoint &endpoint, const http::HttpRequest &request,
                           ResponseCallback callback) {
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
  raw->Start(request);
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
