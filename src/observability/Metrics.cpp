#include "aegisgate/observability/Metrics.h"

#include <array>
#include <cstddef>
#include <iomanip>
#include <map>
#include <sstream>
#include <utility>

namespace aegisgate::observability {
namespace {

constexpr std::array<double, 8> kDurationBounds{0.001, 0.005, 0.01, 0.05,
                                                  0.1, 0.5, 1.0, 5.0};

struct RequestKey {
  std::string route;
  int status{};
  std::string upstream;
  std::string reason;

  [[nodiscard]] bool operator<(const RequestKey &other) const noexcept {
    if (route != other.route) return route < other.route;
    if (status != other.status) return status < other.status;
    if (upstream != other.upstream) return upstream < other.upstream;
    return reason < other.reason;
  }
};

struct Histogram {
  std::array<std::uint64_t, kDurationBounds.size()> buckets{};
  std::uint64_t count{};
  double sum{};
};

std::string EscapeLabel(std::string_view value) {
  std::string escaped;
  escaped.reserve(value.size());
  for (const char character : value) {
    switch (character) {
    case '\\': escaped.append("\\\\"); break;
    case '"': escaped.append("\\\""); break;
    case '\n': escaped.append("\\n"); break;
    default: escaped.push_back(character); break;
    }
  }
  return escaped;
}

void Record(Metrics::State &state, std::string_view route, int status,
            std::string_view upstream, bool rate_limited, double seconds,
            std::string_view reason = {});

} // namespace

struct Metrics::State {
  std::map<RequestKey, std::uint64_t> requests;
  std::map<std::string, std::uint64_t> rate_limited;
  std::map<std::string, Histogram> duration;
  std::size_t active_connections{};
  std::size_t inflight{};
};

namespace {

void Record(Metrics::State &state, std::string_view route, int status,
            std::string_view upstream, bool rate_limited, double seconds,
            std::string_view reason) {
  ++state.requests[RequestKey{std::string(route), status, std::string(upstream),
                              std::string(reason)}];
  if (rate_limited) ++state.rate_limited[std::string(route)];
  Histogram &histogram = state.duration[std::string(route)];
  ++histogram.count;
  histogram.sum += seconds;
  for (std::size_t index = 0; index < kDurationBounds.size(); ++index) {
    if (seconds <= kDurationBounds[index]) {
      ++histogram.buckets[index];
      break;
    }
  }
}

} // namespace

Metrics::Metrics() : state_(std::make_shared<State>()) {}
Metrics::~Metrics() = default;

Metrics::RequestHandle Metrics::BeginRequest(std::string_view route) {
  ++state_->inflight;
  return RequestHandle(state_, std::string(route), std::chrono::steady_clock::now());
}

void Metrics::RecordImmediate(std::string_view route, int status, std::string_view upstream,
                              bool rate_limited, std::string_view reason) {
  Record(*state_, route, status, upstream, rate_limited, 0.0, reason);
}

void Metrics::SetActiveConnections(std::size_t count) noexcept { state_->active_connections = count; }

std::string Metrics::RenderPrometheus() const {
  std::ostringstream output;
  output << "# TYPE aegisgate_requests_total counter\n";
  for (const auto &[key, count] : state_->requests) {
    output << "aegisgate_requests_total{route=\"" << EscapeLabel(key.route)
           << "\",status=\"" << key.status << "\",upstream=\""
           << EscapeLabel(key.upstream) << "\"";
    if (!key.reason.empty()) {
      output << ",reason=\"" << EscapeLabel(key.reason) << "\"";
    }
    output << "} " << count << '\n';
  }
  output << "# TYPE aegisgate_request_duration_seconds histogram\n";
  output << std::setprecision(17);
  for (const auto &[route, histogram] : state_->duration) {
    std::uint64_t cumulative = 0;
    for (std::size_t index = 0; index < kDurationBounds.size(); ++index) {
      cumulative += histogram.buckets[index];
      output << "aegisgate_request_duration_seconds_bucket{route=\""
             << EscapeLabel(route) << "\",le=\"" << kDurationBounds[index] << "\"} "
             << cumulative << '\n';
    }
    output << "aegisgate_request_duration_seconds_bucket{route=\"" << EscapeLabel(route)
           << "\",le=\"+Inf\"} " << histogram.count << '\n';
    output << "aegisgate_request_duration_seconds_sum{route=\"" << EscapeLabel(route)
           << "\"} " << histogram.sum << '\n';
    output << "aegisgate_request_duration_seconds_count{route=\"" << EscapeLabel(route)
           << "\"} " << histogram.count << '\n';
  }
  output << "# TYPE aegisgate_rate_limited_total counter\n";
  for (const auto &[route, count] : state_->rate_limited) {
    output << "aegisgate_rate_limited_total{route=\"" << EscapeLabel(route) << "\"} "
           << count << '\n';
  }
  output << "# TYPE aegisgate_active_connections gauge\n"
         << "aegisgate_active_connections " << state_->active_connections << '\n'
         << "# TYPE aegisgate_inflight_requests gauge\n"
         << "aegisgate_inflight_requests " << state_->inflight << '\n';
  return output.str();
}

Metrics::RequestHandle::RequestHandle(std::shared_ptr<State> state, std::string route,
                                      std::chrono::steady_clock::time_point started) noexcept
    : state_(std::move(state)), route_(std::move(route)), started_(started) {}

Metrics::RequestHandle::~RequestHandle() { Release(); }

Metrics::RequestHandle::RequestHandle(RequestHandle &&other) noexcept
    : state_(std::move(other.state_)), route_(std::move(other.route_)), started_(other.started_) {}

Metrics::RequestHandle &Metrics::RequestHandle::operator=(RequestHandle &&other) noexcept {
  if (this != &other) {
    Release();
    state_ = std::move(other.state_);
    route_ = std::move(other.route_);
    started_ = other.started_;
  }
  return *this;
}

void Metrics::RequestHandle::Complete(int status, std::string_view upstream,
                                      bool rate_limited, std::string_view reason) {
  if (!state_) return;
  const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started_).count();
  Record(*state_, route_, status, upstream, rate_limited, seconds < 0.0 ? 0.0 : seconds,
         reason);
  Release();
}

void Metrics::RequestHandle::Release() noexcept {
  if (!state_) return;
  if (state_->inflight != 0) --state_->inflight;
  state_.reset();
}

} // namespace aegisgate::observability
