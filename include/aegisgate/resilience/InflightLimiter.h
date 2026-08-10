#pragma once

#include <cstddef>
#include <memory>
#include <utility>

namespace aegisgate::resilience {

// A single-event-loop concurrency limiter.  Reservation ownership is the
// lifetime of one admitted transaction; it must outlive asynchronous work.
class InflightLimiter {
  struct State;

public:
  class Reservation {
  public:
    Reservation() = default;
    Reservation(const Reservation &) = delete;
    Reservation &operator=(const Reservation &) = delete;
    Reservation(Reservation &&other) noexcept;
    Reservation &operator=(Reservation &&other) noexcept;
    ~Reservation();

    explicit operator bool() const noexcept { return !state_.expired(); }
    void Release() noexcept;

  private:
    friend class InflightLimiter;
    explicit Reservation(std::weak_ptr<State> state) noexcept : state_(std::move(state)) {}

    std::weak_ptr<State> state_;
  };

  explicit InflightLimiter(std::size_t maximum);
  InflightLimiter(const InflightLimiter &) = delete;
  InflightLimiter &operator=(const InflightLimiter &) = delete;
  InflightLimiter(InflightLimiter &&) = delete;
  InflightLimiter &operator=(InflightLimiter &&) = delete;

  [[nodiscard]] Reservation Acquire() noexcept;
  [[nodiscard]] std::size_t inflight() const noexcept;
  [[nodiscard]] std::size_t maximum() const noexcept;

private:
  std::shared_ptr<State> state_;
};

} // namespace aegisgate::resilience
