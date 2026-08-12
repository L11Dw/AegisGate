#include "aegisgate/observability/Metrics.h"

#include <iomanip>
#include <mutex>
#include <sstream>
#include <utility>

namespace aegisgate::observability {
namespace {

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
  std::map<std::pair<std::string, std::string>, std::string> circuit_states;
  std::map<std::pair<std::string, std::string>, bool> upstream_health;
  std::size_t active_connections{};
  std::size_t inflight{};
  mutable std::mutex mutex;
};

namespace {

void Record(Metrics::State &state, std::string_view route, int status,
            std::string_view upstream, bool rate_limited, double seconds,
            std::string_view reason) {
  ++state.requests[Metrics::RequestKey{std::string(route), status, std::string(upstream),
                                              std::string(reason)}];
  if (rate_limited) ++state.rate_limited[std::string(route)];
  Metrics::Histogram &histogram = state.duration[std::string(route)];
  ++histogram.count;
  histogram.sum += seconds;
  for (std::size_t index = 0; index < Metrics::kDurationBounds.size(); ++index) {
    if (seconds <= Metrics::kDurationBounds[index]) {
      ++histogram.buckets[index];
      break;
    }
  }
}

} // namespace

Metrics::Metrics() : state_(std::make_shared<State>()) {}
Metrics::~Metrics() = default;

Metrics::RequestHandle Metrics::BeginRequest(std::string_view route) {
  std::lock_guard<std::mutex> guard(state_->mutex);
  ++state_->inflight;
  return RequestHandle(state_, std::string(route), std::chrono::steady_clock::now());
}

void Metrics::RecordImmediate(std::string_view route, int status, std::string_view upstream,
                              bool rate_limited, std::string_view reason) {
  std::lock_guard<std::mutex> guard(state_->mutex);
  Record(*state_, route, status, upstream, rate_limited, 0.0, reason);
}

void Metrics::SetActiveConnections(std::size_t count) noexcept {
  std::lock_guard<std::mutex> guard(state_->mutex);
  state_->active_connections = count;
}

void Metrics::SetCircuitState(std::string_view route, std::string_view upstream,
                              std::string_view state) {
  std::lock_guard<std::mutex> guard(state_->mutex);
  state_->circuit_states[{std::string(route), std::string(upstream)}] = std::string(state);
}

void Metrics::SetUpstreamHealth(std::string_view route, std::string_view upstream,
                                bool healthy) {
  std::lock_guard<std::mutex> guard(state_->mutex);
  state_->upstream_health[{std::string(route), std::string(upstream)}] = healthy;
}

Metrics::Data Metrics::Snapshot() const {
  std::lock_guard<std::mutex> guard(state_->mutex);
  Data data;
  data.requests = state_->requests;
  data.rate_limited = state_->rate_limited;
  data.duration = state_->duration;
  data.active_connections = state_->active_connections;
  data.inflight = state_->inflight;
  return data;
}

void Metrics::MergeInto(Data &target, const Data &source) noexcept {
  for (const auto &[key, count] : source.requests) {
    target.requests[key] += count;
  }
  for (const auto &[route, count] : source.rate_limited) {
    target.rate_limited[route] += count;
  }
  for (const auto &[route, histogram] : source.duration) {
    Histogram &merged = target.duration[route];
    merged.count += histogram.count;
    merged.sum += histogram.sum;
    for (std::size_t index = 0; index < histogram.buckets.size(); ++index) {
      merged.buckets[index] += histogram.buckets[index];
    }
  }
  target.active_connections += source.active_connections;
  target.inflight += source.inflight;
}

std::string Metrics::RenderPrometheus() const {
  std::lock_guard<std::mutex> guard(state_->mutex);
  Data data;
  data.requests = state_->requests;
  data.rate_limited = state_->rate_limited;
  data.duration = state_->duration;
  data.active_connections = state_->active_connections;
  data.inflight = state_->inflight;
  std::vector<ProtectionSample> protection;
  for (const auto &[key, state] : state_->circuit_states) {
    ProtectionSample sample;
    sample.route = key.first;
    sample.upstream = key.second;
    sample.state = state;
    const auto health = state_->upstream_health.find(key);
    sample.healthy = health == state_->upstream_health.end() || health->second;
    protection.push_back(std::move(sample));
  }
  return RenderPrometheus(data, protection);
}

std::string Metrics::RenderPrometheus(const Data &aggregate,
                                      const std::vector<ProtectionSample> &protection) {
  std::ostringstream output;
  output << "# TYPE aegisgate_requests_total counter\n";
  for (const auto &[key, count] : aggregate.requests) {
    output << "aegisgate_requests_total{route=\"" << EscapeLabel(key.route)
           << "\",status=\"" << key.status << "\",upstream=\""
           << EscapeLabel(key.upstream) << "\"";
    if (!key.reason.empty()) {
      output << ",reason=\"" << EscapeLabel(key.reason) << "\"";
    }
    output << "} " << count << '\n';
  }
  output << "# TYPE aegisgate_circuit_state gauge\n";
  for (const ProtectionSample &sample : protection) {
    for (const char *candidate : {"closed", "open", "half_open"}) {
      output << "aegisgate_circuit_state{route=\"" << EscapeLabel(sample.route)
             << "\",upstream=\"" << EscapeLabel(sample.upstream) << "\",state=\"" << candidate
             << "\"} " << (sample.state == candidate ? "1" : "0") << '\n';
    }
  }
  output << "# TYPE aegisgate_upstream_health gauge\n";
  for (const ProtectionSample &sample : protection) {
    output << "aegisgate_upstream_health{route=\"" << EscapeLabel(sample.route)
           << "\",upstream=\"" << EscapeLabel(sample.upstream) << "\"} "
           << (sample.healthy ? "1" : "0") << '\n';
  }
  output << "# TYPE aegisgate_request_duration_seconds histogram\n";
  output << std::setprecision(17);
  for (const auto &[route, histogram] : aggregate.duration) {
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
  for (const auto &[route, count] : aggregate.rate_limited) {
    output << "aegisgate_rate_limited_total{route=\"" << EscapeLabel(route) << "\"} "
           << count << '\n';
  }
  output << "# TYPE aegisgate_active_connections gauge\n"
         << "aegisgate_active_connections " << aggregate.active_connections << '\n'
         << "# TYPE aegisgate_inflight_requests gauge\n"
         << "aegisgate_inflight_requests " << aggregate.inflight << '\n';
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
  const double seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - started_).count();
  std::lock_guard<std::mutex> guard(state_->mutex);
  Record(*state_, route_, status, upstream, rate_limited, seconds < 0.0 ? 0.0 : seconds,
         reason);
  if (state_->inflight != 0) --state_->inflight;
  state_.reset();
}

void Metrics::RequestHandle::Release() noexcept {
  if (!state_) return;
  std::lock_guard<std::mutex> guard(state_->mutex);
  if (state_->inflight != 0) --state_->inflight;
  state_.reset();
}

} // namespace aegisgate::observability
