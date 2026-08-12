#include "aegisgate/proxy/ProxyTransaction.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "aegisgate/health/Coordinator.h"
#include "aegisgate/net/ClientConnection.h"
#include "aegisgate/net/EventLoop.h"
#include "aegisgate/proxy/UpstreamPool.h"


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

void StripHopByHopHeaders(http::HttpResponseHead &head) {
  const auto names = HopByHopNames(head.headers);
  std::erase_if(head.headers, [&names](const auto &header) {
    return names.contains(LowerAscii(header.first));
  });
}

} // namespace

ProxyTransaction::ProxyTransaction(net::EventLoop &loop, net::ClientConnection &client,
                                   std::uint16_t upstream_port, http::HttpRequest request,
                                   std::optional<resilience::GlobalAdmission::Reservation> reservation)
    : loop_(loop), client_(&client), client_lifetime_(client.LifetimeToken()),
      upstream_port_(upstream_port), request_(std::move(request)),
      reservation_(std::move(reservation)) {}

ProxyTransaction::ProxyTransaction(net::EventLoop &loop, net::ClientConnection &client,
                                   config::Endpoint endpoint, http::HttpRequest request,
                                   std::shared_ptr<UpstreamPool> pool,
                                   std::optional<resilience::GlobalAdmission::Reservation> reservation,
                                   net::TimerQueue *timers, UpstreamPolicy policy,
                                   std::shared_ptr<observability::Metrics> metrics,
                                   std::string route_name, AttemptProvider attempt_provider,
                                   std::optional<std::weak_ptr<void>> gateway_lifetime)
    : loop_(loop), client_(&client), client_lifetime_(client.LifetimeToken()),
      gateway_lifetime_(std::move(gateway_lifetime)),
      endpoint_(std::move(endpoint)), request_(std::move(request)),
      reservation_(std::move(reservation)),
      metrics_(std::move(metrics)), route_name_(std::move(route_name)), pool_(std::move(pool)),
      timers_(timers), attempt_provider_(std::move(attempt_provider)),
      policy_(std::move(policy)) {}

std::shared_ptr<ProxyTransaction>
ProxyTransaction::Start(net::EventLoop &loop, net::ClientConnection &client,
                        std::uint16_t upstream_port, http::HttpRequest request,
                        std::optional<resilience::GlobalAdmission::Reservation> reservation) {
  const auto transaction = std::shared_ptr<ProxyTransaction>(
      new ProxyTransaction(loop, client, upstream_port, std::move(request), std::move(reservation)));
  transaction->Begin();
  return transaction;
}

std::shared_ptr<ProxyTransaction>
ProxyTransaction::Start(net::EventLoop &loop, net::ClientConnection &client,
                        config::Endpoint endpoint, http::HttpRequest request,
                        std::shared_ptr<UpstreamPool> pool,
                        std::optional<resilience::GlobalAdmission::Reservation> reservation,
                        net::TimerQueue *timers, UpstreamPolicy policy,
                        std::shared_ptr<observability::Metrics> metrics, std::string route_name,
                        AttemptProvider attempt_provider,
                        std::optional<std::weak_ptr<void>> gateway_lifetime) {
  if (!pool) throw std::invalid_argument("upstream pool is required");
  const auto transaction = std::shared_ptr<ProxyTransaction>(new ProxyTransaction(
      loop, client, std::move(endpoint), std::move(request), std::move(pool),
      std::move(reservation), timers, std::move(policy), std::move(metrics),
      std::move(route_name), std::move(attempt_provider), std::move(gateway_lifetime)));
  transaction->Begin();
  return transaction;
}

void ProxyTransaction::Begin() {
  // The gateway may already be down (e.g. an expired lifetime token passed to
  // Start): touch nothing of it — no metrics, no admission slot, no timers_,
  // no provider, no connect.  The transaction stays terminal and its RAII
  // members release nothing.
  if (GatewayDown()) {
    finished_ = true;
    return;
  }
  if (metrics_) {
    try {
      metric_request_ = metrics_->BeginRequest(route_name_);
    } catch (...) {
      // Observability must not turn a serviceable request into a failed one.
      metrics_.reset();
    }
  }
  // The global admission (in-flight slot plus one lease token) was acquired
  // by the worker data plane before this transaction started; the reservation
  // is released exactly once at the terminal path or by RAII.
  // Wire the downstream stream notifications.  Both callbacks hold only a
  // weak reference so a live connection cannot retain a finished transaction;
  // every terminal path clears them via ClearClientStreamCallbacks().
  const std::weak_ptr<ProxyTransaction> weak_self = shared_from_this();
  client_->SetWriteDrainedCallback([weak_self] {
    if (const auto self = weak_self.lock()) self->ResumeUpstreamReading();
  });
  client_->SetRequestAbortCallback([weak_self] {
    if (const auto self = weak_self.lock()) self->HandleClientAbort();
  });
  ArmTotalDeadline();
  if (!StartUpstream()) {
    FinishNoEndpoint();
  }
}

bool ProxyTransaction::StartUpstream() {
  // The owning gateway may have been destroyed while this attempt was queued
  // or retried; nothing of the gateway (timers, provider, pool wiring) may be
  // touched then.  Returning false reuses the "no candidate" terminal path.
  if (GatewayDown()) return false;
  CancelAttemptDeadlines();
  ++generation_;
  connected_ = false;
  response_header_received_ = false;
  // The provider chooses an eligible endpoint and issues the attempt permit
  // (freshly per retry).  No candidate means do not connect.
  breaker_link_.reset();
  coordinator_overloaded_ = false;
  if (attempt_provider_) {
    AttemptDecision decision = attempt_provider_();
    if (!decision.selection.has_value()) {
      // Record why there was no candidate so the terminal metric reason is
      // honest (coordinator overload vs no healthy endpoint).
      coordinator_overloaded_ = decision.coordinator_overloaded;
      // No candidate is not a new attempt: the previous attempt's guard and
      // accounting stay as they are (the old attempt already released its
      // active slot at its terminal point before the retry was queued).
      return false;
    }
    auto selection = std::move(decision.selection);
    endpoint_ = selection->endpoint;
    breaker_link_ = std::move(selection->link);
    // Bind the request snapshot on the first successful selection; retries use
    // the same provider (same snapshot), so this stays the request's own
    // configuration (R-054).
    request_snapshot_ = selection->snapshot;
    // Install the new attempt's active slot.  The previous slot is already
    // empty (released at the old attempt's terminal), so this move-assignment
    // releases nothing.
    active_reservation_ = std::move(selection->active);
    attempt_accounted_ = false;
  } else {
    attempt_accounted_ = false;
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
                     }, [self](net::UpstreamProgress progress) { self->HandleProgress(progress); },
                     [self](const http::HttpResponseHead &head) { self->HandleResponseHead(head); },
                     [self](std::string_view bytes) { return self->HandleResponseBody(bytes); });
      starting_upstream_ = false;
      return true;
    }
    upstream_ = std::make_unique<net::UpstreamConnection>(
        loop_, upstream_port_, [self](net::UpstreamResult result, http::HttpResponse response) {
          self->HandleUpstream(result, std::move(response));
        });
    upstream_->SetProgressCallback([self](net::UpstreamProgress progress) {
      self->HandleProgress(progress);
    });
    upstream_->SetStreamingCallbacks(
        [self](const http::HttpResponseHead &head) { self->HandleResponseHead(head); },
        [self](std::string_view bytes) { return self->HandleResponseBody(bytes); });
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
  return true;
}

void ProxyTransaction::HandleUpstream(net::UpstreamResult result, http::HttpResponse response) {
  if (finished_) return;
  active_connection_ = nullptr;
  if (RetryableFailure(result)) {
    // This attempt failed inside the safe retry window; account it once
    // before starting the replacement.  Both UpstreamConnection::Finish and
    // UpstreamPool::Complete are still executing their callback stack, so
    // the replacement starts after this epoll batch.  The ended attempt's
    // active slot returns before the replacement is queued.
    active_reservation_.Release();
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
  active_reservation_.Release();
  // UpstreamConnection can complete synchronously from Start().  Do not
  // destroy that object while its Start()/Finish() stack frame is active.
  if (!starting_upstream_) upstream_.reset();

  const auto client_lifetime = client_lifetime_.lock();
  if (!client_lifetime) {
    // The client is gone: nothing further may be written; breaker accounting
    // stays untouched (R-034: a client disconnect consumes no permit).
    CompleteMetric(downstream_response_committed_ ? response_head_.status
                                                  : response.status);
    return;
  }
  if (downstream_response_committed_) {
    // Streaming success: the full body was handed to the downstream queue;
    // FinishResponse closes the response and resumes request reading once the
    // queue drains.
    try {
      client_->FinishResponse();
    } catch (const std::logic_error &) {
    } catch (const std::system_error &) {
    }
    CompleteMetric(response_head_.status);
    if (response_head_.status >= 500) {
      AccountFailure();
    } else {
      AccountSuccess();
    }
    return;
  }
  // A 5xx answer is an endpoint failure for the breaker; 4xx and success
  // responses are healthy answers.
  CompleteMetric(response.status);
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

void ProxyTransaction::HandleResponseHead(const http::HttpResponseHead &head) {
  if (finished_) return;
  if (GatewayDown()) return;
  // Keep the transaction alive across this call: the client-dead branch below
  // may release the callback-held owners (and with them the last strong
  // reference to this transaction).
  const auto self = shared_from_this();
  response_head_ = head;
  StripHopByHopHeaders(response_head_);
  // The retry window closes the moment the head is handed downstream: the
  // output can no longer be replaced even before the kernel writes a byte.
  downstream_response_committed_ = true;
  const auto client_lifetime = client_lifetime_.lock();
  if (!client_lifetime) {
    // The client is gone before the head could be written: terminate with
    // the upstream cancel deferred (this call runs inside the upstream
    // callback stack, so nothing may destroy the upstream mid-callback).
    HandleClientAbort();
    return;
  }
  try {
    client_->BeginResponse(response_head_);
  } catch (const std::logic_error &) {
    HandleClientAbort();
  } catch (const std::system_error &) {
    HandleClientAbort();
  }
}

bool ProxyTransaction::HandleResponseBody(std::string_view bytes) {
  if (finished_) return false;
  if (GatewayDown()) return false;
  // Keep the transaction alive: HandleClientAbort below cancels the upstream
  // exchange, releasing every callback-held strong reference.
  const auto self = shared_from_this();
  bool at_high_watermark = false;
  try {
    at_high_watermark = client_->WriteResponseBody(bytes);
  } catch (const std::logic_error &) {
    HandleClientAbort();
    return false;
  } catch (const std::system_error &) {
    HandleClientAbort();
    return false;
  }
  if (at_high_watermark) {
    // The downstream queue is at or above the high watermark: pause the
    // upstream read.  The chunk itself was already consumed (appended to the
    // downstream queue), so this sink returns true: a false return would make
    // the parser retain the bytes in its input and re-deliver them on resume
    // (R-046).  The pause stops the streaming read loop and re-enables only
    // after a low-water drain notification.
    PauseUpstreamReading();
  }
  return true;
}

void ProxyTransaction::HandleClientAbort() {
  if (finished_) return;
  // Keep the transaction alive across this call: the deferred cancellation
  // may release the last callback-held strong reference at batch end.
  const auto self = shared_from_this();
  finished_ = true;
  ++generation_;
  CancelDeadlines();
  reservation_.reset();
  active_reservation_.Release();
  // A client abort consumes no breaker permit (R-034): cancel the outcome
  // reservation so its credit is returned without accounting.  The abort is
  // terminal, so no attempt will publish through this link.
  breaker_link_.reset();
  // The client connection is gone: account the started response status (or
  // 502 when nothing was written) and consume no breaker permit.
  CompleteMetric(downstream_response_committed_ ? response_head_.status : 502);
  if (client_lifetime_.lock()) {
    ClearClientStreamCallbacks();
    try {
      client_->AbortResponse();
    } catch (const std::logic_error &) {
    } catch (const std::system_error &) {
    }
  }
  // Cancel the upstream exchange at the end of this event batch: this method
  // runs inside the client's or the upstream's callback stack, and destroying
  // the upstream there would leave the calling stack touching its parser and
  // input buffer (R-042 pattern).  The pool owns its active connections, so
  // the deferred closure captures the pool; direct connections are owned by
  // this transaction, which the closure keeps alive through a weak ref.
  const auto pool = pool_;
  net::UpstreamConnection *connection = active_connection_;
  const std::weak_ptr<ProxyTransaction> weak_self = shared_from_this();
  loop_.QueueAfterCurrentBatch([pool, connection, weak_self] {
    if (pool && connection) {
      (void)pool->Cancel(connection);
      return;
    }
    if (const auto alive = weak_self.lock()) {
      if (alive->upstream_) {
        alive->upstream_->Close();
        alive->upstream_.reset();
      }
    }
  });
}

void ProxyTransaction::PauseUpstreamReading() noexcept {
  if (pool_ && active_connection_) active_connection_->PauseReading();
  if (upstream_) upstream_->PauseReading();
}

void ProxyTransaction::ResumeUpstreamReading() noexcept {
  if (finished_ || GatewayDown()) return;
  if (pool_ && active_connection_) active_connection_->ResumeReading();
  if (upstream_) upstream_->ResumeReading();
}

void ProxyTransaction::ClearClientStreamCallbacks() noexcept {
  if (!client_lifetime_.lock()) return;
  client_->ClearStreamCallbacks();
}

void ProxyTransaction::AccountSuccess() noexcept {
  if (!breaker_link_.has_value() || attempt_accounted_) return;
  attempt_accounted_ = true;
  BreakerLink &link = *breaker_link_;
  // Publish into the reservation's outcome channel.  The slot was reserved
  // before the attempt connected, so this is infallible by the capacity
  // invariant (R-053); the coordinator drains it and validates the permit.
  link.outcome_reservation.Publish(
      {link.route_index, link.endpoint_index, link.permit, true});
}

void ProxyTransaction::AccountFailure() noexcept {
  if (!breaker_link_.has_value() || attempt_accounted_) return;
  attempt_accounted_ = true;
  BreakerLink &link = *breaker_link_;
  link.outcome_reservation.Publish(
      {link.route_index, link.endpoint_index, link.permit, false});
}

void ProxyTransaction::FinishNoEndpoint() {
  if (finished_) return;
  finished_ = true;
  ++generation_;
  CancelDeadlines();
  reservation_.reset();
  // No upstream was ever connected, so the 503 carries no upstream label; the
  // reason distinguishes a route with no healthy candidate from one whose
  // outcome capacity is exhausted (R-053), so operators are not misled.
  try {
    metric_request_.Complete(503, {}, false,
                             coordinator_overloaded_ ? "coordinator_overloaded"
                                                     : "no_healthy_endpoint");
  } catch (...) {
  }
  if (!client_lifetime_.lock()) return;
  ClearClientStreamCallbacks();
  const auto self = shared_from_this();
  try {
    client_->SendResponse(http::HttpResponse{503, "Service Unavailable", {}, ""});
  } catch (const std::logic_error &) {
  } catch (const std::system_error &) {
  }
}

void ProxyTransaction::FinishFailure() {
  if (finished_) return;
  finished_ = true;
  ++generation_;
  CancelDeadlines();
  reservation_.reset();
  active_reservation_.Release();
  // The upstream exchange already completed with a terminal failure, so its
  // descriptor is closed; only the owned object remains to be released.  When
  // Start() finished synchronously on its own stack frame, releasing here
  // would destroy that frame's owner, so it stays deferred.
  if (!starting_upstream_) upstream_.reset();
  if (downstream_response_committed_) {
    // The head is already on the wire: truncate the client connection; a
    // second status line must never replace the committed response.  The
    // client may already be destroyed (late upstream failure after client
    // teardown, R-047): touch it only while its lifetime token is alive.
    CompleteMetric(502);
    AccountFailure();
    if (client_lifetime_.lock()) {
      ClearClientStreamCallbacks();
      try {
        client_->AbortResponse();
      } catch (const std::logic_error &) {
      } catch (const std::system_error &) {
      }
    }
    return;
  }
  CompleteMetric(502);
  if (!client_lifetime_.lock()) return;
  ClearClientStreamCallbacks();
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
  if (GatewayDown()) return;  // the gateway's TimerQueue may be gone
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
  if (GatewayDown()) return;  // the gateway's TimerQueue is destroyed
  if (!timers_) return;
  CancelAttemptDeadlines();
  if (total_timer_ != 0) (void)timers_->Cancel(total_timer_);
  total_timer_ = 0;
}

void ProxyTransaction::CancelAttemptDeadlines() {
  if (GatewayDown()) return;  // the gateway's TimerQueue is destroyed
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
  // The retry window closes when the response head is committed downstream;
  // only connection-level failures are retryable (kProtocolError and
  // kUnsupported are terminal 502 answers, per the design doc's "no response
  // header yet" retry rule).  With an attempt provider the candidate set is
  // decided at runtime (healthy and not open); without one the static
  // retry_endpoints apply.
  return !downstream_response_committed_ && retries_ == 0 &&
         policy_.retry_budget != 0 &&
         (request_.method == "GET" || request_.method == "HEAD") &&
         (result == net::UpstreamResult::kConnectError ||
          result == net::UpstreamResult::kWriteError ||
          result == net::UpstreamResult::kReadError ||
          result == net::UpstreamResult::kEof) &&
         (attempt_provider_ || HasRetryAlternative());
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
  if (attempt_provider_) {
    if (!StartUpstream()) return false;
    ++retries_;
    return true;
  }
  if (!endpoint_) return false;
  const auto next = std::find_if(policy_.retry_endpoints.begin(), policy_.retry_endpoints.end(),
                                 [this](const config::Endpoint &candidate) {
    return candidate.address != endpoint_->address || candidate.port != endpoint_->port;
  });
  if (next == policy_.retry_endpoints.end()) return false;
  *endpoint_ = *next;
  if (!StartUpstream()) return false;
  ++retries_;
  return true;
}

void ProxyTransaction::FinishGatewayTimeout() {
  if (finished_) return;
  finished_ = true;
  ++generation_;
  CancelDeadlines();
  reservation_.reset();
  active_reservation_.Release();
  if (pool_ && active_connection_) (void)pool_->Cancel(active_connection_);
  active_connection_ = nullptr;
  if (upstream_) upstream_->Close();
  upstream_.reset();
  if (downstream_response_committed_) {
    // The head is already on the wire: a 504 status line cannot replace it,
    // so the connection is truncated instead.  The client may already be
    // destroyed (R-047): touch it only while its lifetime token is alive.
    CompleteMetric(504);
    AccountFailure();
    if (client_lifetime_.lock()) {
      ClearClientStreamCallbacks();
      try {
        client_->AbortResponse();
      } catch (const std::logic_error &) {
      } catch (const std::system_error &) {
      }
    }
    return;
  }
  CompleteMetric(504);
  if (!client_lifetime_.lock()) return;
  ClearClientStreamCallbacks();
  AccountFailure();
  const auto self = shared_from_this();
  try { client_->SendResponse(http::HttpResponse{504, "Gateway Timeout", {}, ""}); }
  catch (const std::logic_error &) {} catch (const std::system_error &) {}
}

void ProxyTransaction::CompleteMetric(int status, bool rate_limited,
                                     std::string_view reason) noexcept {
  try {
    metric_request_.Complete(status, UpstreamLabel(), rate_limited, reason);
  } catch (...) {
    // Metrics are best-effort; forwarding and lifetime cleanup remain primary.
  }
}

std::string ProxyTransaction::UpstreamLabel() const {
  if (!endpoint_) return {};
  return endpoint_->host + ":" + std::to_string(endpoint_->port);
}

} // namespace aegisgate::proxy
