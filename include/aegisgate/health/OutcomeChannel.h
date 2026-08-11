#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>

#include "aegisgate/health/CoordinatorState.h"

namespace aegisgate::health {

// Bounded, reliable result conduit for one route's breaker accounting.  The
// capacity is max_inflight x (1 + retry_budget): a request can hold one active
// attempt while its retry of a published-but-undrained attempt waits, so a
// retry never finds the ring full.  A worker reserves one slot before it may
// connect an accounted attempt; the terminal outcome is either Publish()
// (enqueue + wake, infallible by the capacity invariant) or Cancel() (return
// the credit without accounting).  The coordinator loop is the single consumer;
// each consumed outcome restores one credit.  Published outcomes are woken via
// an eventfd used strictly as a control wake (never for result data).
class OutcomeChannel {
  struct State;  // defined below (private); shared with outstanding reservations

public:
  // Safety ceiling for the MPSC ring allocation; a route whose capacity would
  // exceed it is rejected at configuration time.
  static constexpr std::uint32_t kMaxOutcomeCapacity = 1'048'576;

  class Reservation {
  public:
    Reservation() = default;
    Reservation(const Reservation &) = delete;
    Reservation &operator=(const Reservation &) = delete;
    Reservation(Reservation &&other) noexcept;
    Reservation &operator=(Reservation &&other) noexcept;
    ~Reservation();

    // Submits the terminal outcome.  Exactly once per reservation (later calls
    // and calls on an empty reservation are no-ops); infallible by the
    // capacity invariant.  The credit is restored when the coordinator drains
    // the outcome.
    void Publish(AttemptResult result) noexcept;
    // Returns the credit without enqueuing anything (client abort / gateway
    // shutdown / an attempt that never started); no breaker accounting.
    // Exactly once; idempotent.
    void Cancel() noexcept;
    // False when default-constructed or after the slot was moved away.
    explicit operator bool() const noexcept { return state_ != nullptr; }

  private:
    friend class OutcomeChannel;
    explicit Reservation(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}
    void Finalize() noexcept;

    std::shared_ptr<State> state_;
    bool finalized_ = false;
  };

  explicit OutcomeChannel(std::uint32_t capacity);
  ~OutcomeChannel() = default;

  OutcomeChannel(const OutcomeChannel &) = delete;
  OutcomeChannel &operator=(const OutcomeChannel &) = delete;

  // The route's channel capacity, computed in uint64:
  //   capacity = max_inflight x (1 + retry_budget)
  // Throws std::invalid_argument when the product is zero or exceeds
  // kMaxOutcomeCapacity (a config-level guard against absurd ring sizes).
  [[nodiscard]] static std::size_t CapacityForRoute(std::uint32_t max_inflight,
                                                    std::uint32_t retry_budget);

  // Worker side: claims one result slot.  nullopt when the channel is stopping
  // or the credit is exhausted — the route cannot safely start another
  // breaker-accounted attempt.  A rejection is counted for
  // outcome_reservation_rejected_total.
  [[nodiscard]] std::optional<Reservation> TryReserve() noexcept;
  // Coordinator-loop single consumer: drains every queued outcome, calling
  // `consume` per result (the coordinator records it and restores one credit).
  // Returns the number consumed.
  [[nodiscard]] std::size_t
  DrainOnCoordinatorLoop(const std::function<void(const AttemptResult &)> &consume) noexcept;
  // Rejects all future reservations; already-published outcomes stay
  // drainable.  Called on shutdown before workers stop their clients.
  void BeginStopping() noexcept;

  // Observation views.
  [[nodiscard]] std::size_t pending() const noexcept;   // queued outcomes
  [[nodiscard]] std::uint64_t rejected() const noexcept;  // reservation rejections
  // The control wake descriptor (a Publish writes 1).  Owned by the channel
  // and closed when its state is destroyed.
  [[nodiscard]] int WakeFd() const noexcept;

private:
  // One ring slot.  The payload is a plain (non-atomic) AttemptResult guarded
  // by the ready flag: the producer writes value then ready.store(true,
  // release); the consumer observes ready.load(acquire) then reads value, so
  // the release/acquire pair makes the write happen-before the read.  A slot
  // is only reused after the consumer drained it and released its credit
  // (available_.fetch_add(release)); the next producer's reservation CAS
  // (acquire) orders that reuse.  This keeps the ring lock-free without a
  // 32-byte libatomic load/store.
  struct Slot {
    AttemptResult value{};
    std::atomic<bool> ready{false};
  };

  // Shared between the channel and outstanding reservations so a late Publish/
  // Cancel after the channel handle is gone is still safe.  The ring slots,
  // the credit, the accepting flag and the wake descriptor all live here.
  struct State {
    std::atomic<bool> accepting{true};
    std::atomic<std::uint32_t> available{0};
    std::atomic<std::uint32_t> head{0};  // next claimed enqueue slot
    std::atomic<std::uint32_t> tail{0};  // next dequeue slot (consumer only)
    std::atomic<std::uint64_t> rejected{0};
    std::uint32_t capacity{};
    int wake_fd{};
    std::unique_ptr<Slot[]> slots;

    void Enqueue(AttemptResult result) noexcept;
    ~State();
  };

  std::shared_ptr<State> state_;
};

} // namespace aegisgate::health
