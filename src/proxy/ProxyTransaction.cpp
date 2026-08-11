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
#include "aegisgate/proxy/UpstreamPool.h"
#include "aegisgate/resilience/RouteAdmission.h"

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
                                   std::uint16_t upstream_port, http::HttpRequest request,
                                   std::shared_ptr<resilience::RouteAdmission> admission)
    : loop_(loop), client_(&client), client_lifetime_(client.LifetimeToken()),
      upstream_port_(upstream_port), request_(std::move(request)), admission_(std::move(admission)) {}

ProxyTransaction::ProxyTransaction(net::EventLoop &loop, net::ClientConnection &client,
                                   config::Endpoint endpoint, http::HttpRequest request,
                                   std::shared_ptr<UpstreamPool> pool,
                                   std::shared_ptr<resilience::RouteAdmission> admission,
                                   net::TimerQueue *timers, UpstreamPolicy policy,
                                   std::shared_ptr<observability::Metrics> metrics,
                                   std::string route_name, BreakerProvider breaker_provider)
    : loop_(loop), client_(&client), client_lifetime_(client.LifetimeToken()),
      endpoint_(std::move(endpoint)), request_(std::move(request)), admission_(std::move(admission)),
      metrics_(std::move(metrics)), route_name_(std::move(route_name)), pool_(std::move(pool)),
      timers_(timers), breaker_provider_(std::move(breaker_provider)),
      policy_(std::move(policy)) {}

std::shared_ptr<ProxyTransaction>
ProxyTransaction::Start(net::EventLoop &loop, net::ClientConnection &client,
                        std::uint16_t upstream_port, http::HttpRequest request,
                        std::shared_ptr<resilience::RouteAdmission> admission) {
  const auto transaction = std::shared_ptr<ProxyTransaction>(
      new ProxyTransaction(loop, client, upstream_port, std::move(request), std::move(admission)));
  transaction->Begin();
  return transaction;
}

std::shared_ptr<ProxyTransaction>
ProxyTransaction::Start(net::EventLoop &loop, net::ClientConnection &client,
                        config::Endpoint endpoint, http::HttpRequest request,
                        std::shared_ptr<UpstreamPool> pool,
                        std::shared_ptr<resilience::RouteAdmission> admission,
                        net::TimerQueue *timers, UpstreamPolicy policy,
                        std::shared_ptr<observability::Metrics> metrics, std::string route_name,
                        BreakerProvider breaker_provider) {
  if (!pool) throw std::invalid_argument("upstream pool is required");
  const auto transaction = std::shared_ptr<ProxyTransaction>(new ProxyTransaction(
      loop, client, std::move(endpoint), std::move(request), std::move(pool), std::move(admission),
      timers, std::move(policy), std::move(metrics), std::move(route_name),
      std::move(breaker_provider)));
  transaction->Begin();
  return transaction;
}

void ProxyTransaction::Begin() {
  if (metrics_) {
    try {
      metric_request_ = metrics_->BeginRequest(route_name_);
    } catch (...) {
      // Observability must not turn a serviceable request into a failed one.
      metrics_.reset();
    }
  }
  if (admission_) {
    reservation_ = admission_->TryAcquire(resilience::TokenBucket::Clock::now());
    if (!reservation_) {
      HandleAdmissionRejected();
      return;
    }
  }
  ArmTotalDeadline();
  StartUpstream();
}

void ProxyTransaction::StartUpstream() {
  CancelAttemptDeadlines();
  ++generation_;
  connected_ = false;
  response_header_received_ = false;
  // Each upstream attempt carries its own breaker link; a retry to a
  // different endpoint obtains a fresh permit from that endpoint's breaker.
  if (endpoint_.has_value() && breaker_provider_) {
    breaker_link_ = breaker_provider_(*endpoint_);
  }
  ArmConnectDeadline();
  const auto self = shared_from_this();
  try {
    // Each HTTP hop owns its framing and connection-scoped fields.
    http::HttpRequest upstream_request = request_;
    StripHopByHopHeaders(upstream_request);
    if (pool_) {
      starting_upstream_ = true;
      active_connection_ = pool_->Execute(*endpoint_, upstream_request,
                     [self](net::UpstreamResult result, http::HttpResponse response) {
                       self->HandleUpstream(result, std::move(response));
                     }, [self](net::UpstreamProgress progress) { self->HandleProgress(progress); });
      starting_upstream_ = false;
      return;
    }
    upstream_ = std::make_unique<net::UpstreamConnection>(
        loop_, upstream_port_, [self](net::UpstreamResult result, http::HttpResponse response) {
          self->HandleUpstream(result, std::move(response));
        });
    upstream_->SetProgressCallback([self](net::UpstreamProgress progress) {
      self->HandleProgress(progress);
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

void ProxyTransaction::HandleAdmissionRejected() {
  if (finished_) return;
  finished_ = true;
  CancelDeadlines();
  CompleteMetric(429, true);
  const auto client_lifetime = client_lifetime_.lock();
  if (!client_lifetime) return;
  const auto self = shared_from_this();
  try {
    client_->SendResponse(http::HttpResponse{429, "Too Many Requests", {}, ""});
  } catch (const std::logic_error &) {
  } catch (const std::system_error &) {
  }
}

void ProxyTransaction::HandleUpstream(net::UpstreamResult result, http::HttpResponse response) {
  if (finished_) return;
  active_connection_ = nullptr;
  if (RetryableFailure(result)) {
    // This attempt failed inside the safe retry window; account it once
    // before starting the replacement.  Both UpstreamConnection::Finish and
    // UpstreamPool::Complete are still executing their callback stack, so
    // the replacement starts after this epoll batch.
    if (client_lifetime_.lock()) AccountFailure();
    const auto self = shared_from_this();
    loop_.QueueAfterCurrentBatch([self] {
      if (self->finished_) return;
      if (!self->StartRetry()) self->FinishFailure();
    });
    return;
  }
  if (result != net::UpstreamResult::kSuccess) {
    FinishFailure();
    return;
  }
  finished_ = true;
  CancelDeadlines();
  reservation_.reset();
  // UpstreamConnection can complete synchronously from Start().  Do not
  // destroy that object while its Start()/Finish() stack frame is active.
  if (!starting_upstream_) upstream_.reset();

  CompleteMetric(response.status);
  const auto client_lifetime = client_lifetime_.lock();
  if (!client_lifetime) {
    return;
  }
  // A 5xx answer is an endpoint failure for the breaker; 4xx and success
  // responses are healthy answers.
  if (response.status >= 500) {
    AccountFailure();
  } else {
    AccountSuccess();
  }
  const auto self = shared_from_this();

  try {
    StripHopByHopHeaders(response);
    client_->SendResponse(response);
  } catch (const std::logic_error &) {
  } catch (const std::system_error &) {
  }
}

void ProxyTransaction::AccountSuccess() noexcept {
  if (!breaker_link_.has_value()) return;
  try {
    breaker_link_->breaker->RecordSuccess(resilience::CircuitBreaker::Clock::now(),
                                          breaker_link_->permit);
  } catch (...) {
    // Breaker accounting is best-effort; forwarding remains primary.
  }
}

void ProxyTransaction::AccountFailure() noexcept {
  if (!breaker_link_.has_value()) return;
  try {
    breaker_link_->breaker->RecordFailure(resilience::CircuitBreaker::Clock::now(),
                                          breaker_link_->permit);
  } catch (...) {
  }
}

void ProxyTransaction::FinishFailure() {
  if (finished_) return;
  finished_ = true;
  ++generation_;
  CancelDeadlines();
  reservation_.reset();
  // The upstream exchange already completed with a terminal failure, so its
  // descriptor is closed; only the owned object remains to be released.  When
  // Start() finished synchronously on its own stack frame, releasing here
  // would destroy that frame's owner, so it stays deferred.
  if (!starting_upstream_) upstream_.reset();
  CompleteMetric(502);
  if (!client_lifetime_.lock()) return;
  AccountFailure();
  const auto self = shared_from_this();
  try {
    client_->SendResponse(http::HttpResponse{502, "Bad Gateway", {}, ""});
  } catch (const std::logic_error &) {
  } catch (const std::system_error &) {
  }
}

void ProxyTransaction::HandleProgress(net::UpstreamProgress progress) {
  if (finished_) return;
  if (progress == net::UpstreamProgress::kConnected) {
    connected_ = true;
    if (timers_ && connect_timer_ != 0) (void)timers_->Cancel(connect_timer_);
    connect_timer_ = 0;
    return;
  }
  if (progress == net::UpstreamProgress::kRequestWritten) {
    ArmFirstByteDeadline();
    return;
  }
  if (progress == net::UpstreamProgress::kFirstByte) {
    if (timers_ && first_byte_timer_ != 0) (void)timers_->Cancel(first_byte_timer_);
    first_byte_timer_ = 0;
    return;
  }
  response_header_received_ = true;
}

void ProxyTransaction::ArmConnectDeadline() {
  if (!timers_) return;
  if (connect_timer_ != 0) (void)timers_->Cancel(connect_timer_);
  const auto generation = generation_;
  const std::weak_ptr<ProxyTransaction> weak = shared_from_this();
  connect_timer_ = timers_->ScheduleAfter(policy_.connect_timeout, [weak, generation] {
    if (const auto self = weak.lock()) self->HandleDeadline(generation);
  });
}

void ProxyTransaction::ArmFirstByteDeadline() {
  if (!timers_) return;
  const auto generation = generation_;
  const std::weak_ptr<ProxyTransaction> weak = shared_from_this();
  first_byte_timer_ = timers_->ScheduleAfter(policy_.first_byte_timeout, [weak, generation] {
    if (const auto self = weak.lock()) self->HandleDeadline(generation);
  });
}

void ProxyTransaction::ArmTotalDeadline() {
  if (!timers_) return;
  const std::weak_ptr<ProxyTransaction> weak = shared_from_this();
  total_timer_ = timers_->ScheduleAfter(policy_.total_timeout, [weak] {
    if (const auto self = weak.lock(); self && !self->finished_) self->FinishGatewayTimeout();
  });
}

void ProxyTransaction::CancelDeadlines() {
  if (!timers_) return;
  CancelAttemptDeadlines();
  if (total_timer_ != 0) (void)timers_->Cancel(total_timer_);
  total_timer_ = 0;
}

void ProxyTransaction::CancelAttemptDeadlines() {
  if (!timers_) return;
  for (auto *timer : {&connect_timer_, &first_byte_timer_}) {
    if (*timer != 0) (void)timers_->Cancel(*timer);
    *timer = 0;
  }
}

void ProxyTransaction::HandleDeadline(std::uint64_t generation) {
  if (finished_ || generation != generation_) return;
  FinishGatewayTimeout();
}

bool ProxyTransaction::RetryableFailure(net::UpstreamResult result) const noexcept {
  return !response_header_received_ && retries_ == 0 && policy_.retry_budget != 0 &&
         (request_.method == "GET" || request_.method == "HEAD") &&
         result != net::UpstreamResult::kSuccess && HasRetryAlternative();
}

bool ProxyTransaction::HasRetryAlternative() const noexcept {
  // Retrying only makes sense when a different endpoint can be attempted;
  // otherwise the request fails fast with a single terminal 502 instead of
  // re-entering the retry decision forever.
  if (!endpoint_) return false;
  return std::any_of(policy_.retry_endpoints.begin(), policy_.retry_endpoints.end(),
                     [this](const config::Endpoint &candidate) {
                       return candidate.address != endpoint_->address ||
                              candidate.port != endpoint_->port;
                     });
}

bool ProxyTransaction::StartRetry() {
  if (!endpoint_) return false;
  const auto next = std::find_if(policy_.retry_endpoints.begin(), policy_.retry_endpoints.end(),
                                 [this](const config::Endpoint &candidate) {
    return candidate.address != endpoint_->address || candidate.port != endpoint_->port;
  });
  if (next == policy_.retry_endpoints.end()) return false;
  ++retries_;
  *endpoint_ = *next;
  StartUpstream();
  return true;
}

void ProxyTransaction::FinishGatewayTimeout() {
  if (finished_) return;
  finished_ = true;
  ++generation_;
  CancelDeadlines();
  reservation_.reset();
  CompleteMetric(504);
  if (pool_ && active_connection_) (void)pool_->Cancel(active_connection_);
  active_connection_ = nullptr;
  if (upstream_) upstream_->Close();
  upstream_.reset();
  if (!client_lifetime_.lock()) return;
  AccountFailure();
  const auto self = shared_from_this();
  try { client_->SendResponse(http::HttpResponse{504, "Gateway Timeout", {}, ""}); }
  catch (const std::logic_error &) {} catch (const std::system_error &) {}
}

void ProxyTransaction::CompleteMetric(int status, bool rate_limited) noexcept {
  try {
    metric_request_.Complete(status, UpstreamLabel(), rate_limited);
  } catch (...) {
    // Metrics are best-effort; forwarding and lifetime cleanup remain primary.
  }
}

std::string ProxyTransaction::UpstreamLabel() const {
  if (!endpoint_) return {};
  return endpoint_->host + ":" + std::to_string(endpoint_->port);
}

} // namespace aegisgate::proxy
