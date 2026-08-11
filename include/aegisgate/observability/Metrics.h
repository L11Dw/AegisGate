#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace aegisgate::observability {

// Metrics for one I/O worker, plus the aggregate rendering used by the
// control plane (/metrics and the gateway test view).  Instances are
// internally synchronized so the aggregating thread can snapshot them while
// workers record.  RequestHandle is intentionally move-only: ownership of one
// admitted request maps to exactly one in-flight decrement and at most one
// completed-request observation.
class Metrics {
public:
  static constexpr std::array<double, 8> kDurationBounds{0.001, 0.005, 0.01, 0.05,
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

  // A copyable snapshot of one instance's counters, used for aggregation.
  struct Data {
    std::map<RequestKey, std::uint64_t> requests;
    std::map<std::string, std::uint64_t> rate_limited;
    std::map<std::string, Histogram> duration;
    std::size_t active_connections{};
    std::size_t inflight{};
  };

  // One route x endpoint protection sample rendered as one-hot gauges.
  struct ProtectionSample {
    std::string route;
    std::string upstream;
    std::string state;  // closed / open / half_open
    bool healthy{};
  };

  class RequestHandle;
  struct State;

  Metrics();
  ~Metrics();

  Metrics(const Metrics &) = delete;
  Metrics &operator=(const Metrics &) = delete;
  Metrics(Metrics &&) noexcept = default;
  Metrics &operator=(Metrics &&) noexcept = default;

  [[nodiscard]] RequestHandle BeginRequest(std::string_view route);
  void RecordImmediate(std::string_view route, int status, std::string_view upstream = {},
                       bool rate_limited = false, std::string_view reason = {});
  // Route x endpoint protection state.  state is a fixed vocabulary string
  // (closed/open/half_open); rendering emits one-hot gauges with escaped
  // labels so exposition stays valid.
  void SetCircuitState(std::string_view route, std::string_view upstream,
                       std::string_view state);
  void SetUpstreamHealth(std::string_view route, std::string_view upstream,
                         bool healthy);
  void SetActiveConnections(std::size_t count) noexcept;

  // Thread-safe copy of the counters.
  [[nodiscard]] Data Snapshot() const;
  // Renders this instance's own counters and protection state.
  [[nodiscard]] std::string RenderPrometheus() const;

  // Sums source into target (request/rate-limited/duration/active/inflight).
  static void MergeInto(Data &target, const Data &source) noexcept;
  // Renders an aggregated snapshot plus control-plane protection samples.
  // All label escaping is centralized here.
  [[nodiscard]] static std::string
  RenderPrometheus(const Data &aggregate, const std::vector<ProtectionSample> &protection);

private:
  std::shared_ptr<State> state_;
};

class Metrics::RequestHandle {
public:
  RequestHandle() = default;
  ~RequestHandle();

  RequestHandle(const RequestHandle &) = delete;
  RequestHandle &operator=(const RequestHandle &) = delete;
  RequestHandle(RequestHandle &&other) noexcept;
  RequestHandle &operator=(RequestHandle &&other) noexcept;

  // Only the first Complete call is observed.  Later calls are ignored so a
  // retry/timeout race cannot double count a request or underflow in-flight.
  void Complete(int status, std::string_view upstream = {}, bool rate_limited = false,
               std::string_view reason = {});

private:
  friend class Metrics;
  RequestHandle(std::shared_ptr<State> state, std::string route,
                std::chrono::steady_clock::time_point started) noexcept;
  void Release() noexcept;

  std::shared_ptr<State> state_;
  std::string route_;
  std::chrono::steady_clock::time_point started_{};
};

} // namespace aegisgate::observability
