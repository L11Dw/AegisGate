#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <string_view>

namespace aegisgate::observability {

// Process-local metrics for the single event-loop gateway.  RequestHandle is
// intentionally move-only: ownership of one admitted request maps to exactly
// one in-flight decrement and at most one completed-request observation.
class Metrics {
public:
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
  [[nodiscard]] std::string RenderPrometheus() const;

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
