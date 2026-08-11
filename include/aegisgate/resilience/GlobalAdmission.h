#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>

#include "aegisgate/config/Config.h"

namespace aegisgate::resilience {

// Global route admission shared by every I/O worker.  The token budget is a
// single atomic credit pool refilled by the coordinator loop; workers draw
// small leases (never the full quota) and spend tokens locally, so the total
// admitted rate is never multiplied by the worker count.  The in-flight
// counter is a global atomic that increments exactly once on admission and
// decrements exactly once at every terminal outcome (success, failure,
// timeout, client abort, streaming truncation, gateway shutdown).
//
// A worker crash at most loses the lease batch it was holding: the global
// rate can only under-deliver, never exceed its configured budget.  A lease
// is not a breaker permit; the two are never interchangeable.
class GlobalAdmission {
  struct State;

public:
  using Clock = std::chrono::steady_clock;

  // Move-only RAII guard over one global in-flight slot.  Release is
  // idempotent and safe after the admission's shared state was destroyed.
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
    friend class GlobalAdmission;
    explicit Reservation(std::weak_ptr<State> state) noexcept : state_(std::move(state)) {}

    std::weak_ptr<State> state_;
  };

  explicit GlobalAdmission(const config::Route &route, Clock::time_point now);

  GlobalAdmission(const GlobalAdmission &) = delete;
  GlobalAdmission &operator=(const GlobalAdmission &) = delete;

  // Coordinator-loop refill (single writer, but atomic and callable from any
  // thread): accrues rate tokens per elapsed second, capped at burst.
  void Refill(Clock::time_point now) noexcept;
  // Worker-side lease draw: takes up to `want` whole tokens from the global
  // credit.  Safe from any thread.
  [[nodiscard]] std::uint32_t Draw(std::uint32_t want) noexcept;
  // Returns unspent lease tokens to the global credit (capped at burst).
  void Return(std::uint32_t tokens) noexcept;
  // Reserves one global in-flight slot; nullopt when the maximum is reached.
  [[nodiscard]] std::optional<Reservation> TryAcquireInflight() noexcept;

  [[nodiscard]] std::uint32_t inflight() const noexcept;
  [[nodiscard]] std::int64_t credit() const noexcept;
  [[nodiscard]] std::uint32_t MaxInflight() const noexcept;
  [[nodiscard]] std::uint32_t rate() const noexcept;
  [[nodiscard]] std::uint32_t burst() const noexcept;
  // L = clamp(ceil(rate / workers), 1, burst): the largest lease one worker
  // may hold at a time.
  [[nodiscard]] static std::uint32_t LeaseBatch(std::uint32_t rate, std::uint32_t workers,
                                                std::uint32_t burst) noexcept;

private:
  struct State {
    std::atomic<std::uint32_t> inflight{0};
    std::uint32_t maximum{};
    [[nodiscard]] bool TryAcquire() noexcept;
    void ReleaseOne() noexcept;
  };

  static constexpr std::int64_t kCreditScale = 1'000'000'000;

  std::uint32_t rate_;
  std::uint32_t burst_;
  std::int64_t capacity_credit_;
  std::atomic<std::int64_t> credit_;
  // Written only by the coordinator loop's Refill (single writer).
  Clock::time_point last_refill_;
  std::shared_ptr<State> state_;
};

} // namespace aegisgate::resilience
