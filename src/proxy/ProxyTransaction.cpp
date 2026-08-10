#include "aegisgate/proxy/ProxyTransaction.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "aegisgate/net/ClientConnection.h"
#include "aegisgate/net/EventLoop.h"

namespace aegisgate::proxy {
namespace {

bool EqualsIgnoreCase(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) return false;
  for (std::size_t index = 0; index < left.size(); ++index) {
    if (std::tolower(static_cast<unsigned char>(left[index])) !=
        std::tolower(static_cast<unsigned char>(right[index]))) {
      return false;
    }
  }
  return true;
}

std::string LowerAscii(std::string_view value) {
  std::string result;
  result.reserve(value.size());
  for (const unsigned char character : value) {
    result.push_back(static_cast<char>(std::tolower(character)));
  }
  return result;
}

void AddConnectionTokens(std::string_view value, std::unordered_set<std::string> &names) {
  while (!value.empty()) {
    const std::size_t comma = value.find(',');
    std::string_view token = value.substr(0, comma);
    while (!token.empty() && (token.front() == ' ' || token.front() == '\t')) token.remove_prefix(1);
    while (!token.empty() && (token.back() == ' ' || token.back() == '\t')) token.remove_suffix(1);
    if (!token.empty()) names.emplace(LowerAscii(token));
    if (comma == std::string_view::npos) return;
    value.remove_prefix(comma + 1);
  }
}

template <typename HeaderRange>
std::unordered_set<std::string> HopByHopNames(const HeaderRange &headers) {
  std::unordered_set<std::string> names{
      "connection", "content-length", "keep-alive", "proxy-authenticate",
      "proxy-authorization", "proxy-connection", "te", "trailer",
      "transfer-encoding", "upgrade"};
  for (const auto &header : headers) {
    if (EqualsIgnoreCase(header.first, "connection")) {
      AddConnectionTokens(header.second, names);
    }
  }
  return names;
}

void StripHopByHopHeaders(http::HttpRequest &request) {
  const auto names = HopByHopNames(request.headers);
  std::erase_if(request.headers, [&names](const auto &header) {
    return names.contains(LowerAscii(header.first));
  });
}

void StripHopByHopHeaders(http::HttpResponse &response) {
  const auto names = HopByHopNames(response.headers);
  std::erase_if(response.headers, [&names](const auto &header) {
    return names.contains(LowerAscii(header.first));
  });
}

} // namespace

ProxyTransaction::ProxyTransaction(net::EventLoop &loop, net::ClientConnection &client,
                                   std::uint16_t upstream_port, http::HttpRequest request)
    : loop_(loop), client_(&client), client_lifetime_(client.LifetimeToken()),
      upstream_port_(upstream_port), request_(std::move(request)) {}

std::shared_ptr<ProxyTransaction>
ProxyTransaction::Start(net::EventLoop &loop, net::ClientConnection &client,
                        std::uint16_t upstream_port, http::HttpRequest request) {
  const auto transaction = std::shared_ptr<ProxyTransaction>(
      new ProxyTransaction(loop, client, upstream_port, std::move(request)));
  transaction->StartUpstream();
  return transaction;
}

void ProxyTransaction::StartUpstream() {
  const auto self = shared_from_this();
  try {
    // Each HTTP hop owns its framing and connection-scoped fields.
    http::HttpRequest upstream_request = request_;
    StripHopByHopHeaders(upstream_request);
    upstream_ = std::make_unique<net::UpstreamConnection>(
        loop_, upstream_port_, [self](net::UpstreamResult result, http::HttpResponse response) {
          self->HandleUpstream(result, std::move(response));
        });
    starting_upstream_ = true;
    upstream_->Start(upstream_request);
    starting_upstream_ = false;
    if (finished_) upstream_.reset();
  } catch (const std::invalid_argument &) {
    starting_upstream_ = false;
    HandleUpstream(net::UpstreamResult::kConnectError, {});
  } catch (const std::system_error &) {
    starting_upstream_ = false;
    HandleUpstream(net::UpstreamResult::kConnectError, {});
  }
}

void ProxyTransaction::HandleUpstream(net::UpstreamResult result, http::HttpResponse response) {
  if (finished_) return;
  finished_ = true;
  // UpstreamConnection can complete synchronously from Start().  Do not
  // destroy that object while its Start()/Finish() stack frame is active.
  if (!starting_upstream_) upstream_.reset();

  const auto client_lifetime = client_lifetime_.lock();
  if (!client_lifetime) {
    return;
  }
  const auto self = shared_from_this();

  try {
    if (result == net::UpstreamResult::kSuccess) {
      StripHopByHopHeaders(response);
      client_->SendResponse(response);
    } else {
      client_->SendResponse(http::HttpResponse{502, "Bad Gateway", {}, ""});
    }
  } catch (const std::logic_error &) {
  } catch (const std::system_error &) {
  }
}

} // namespace aegisgate::proxy
