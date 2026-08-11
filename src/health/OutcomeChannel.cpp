#include "aegisgate/health/OutcomeChannel.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <sys/eventfd.h>
#include <unistd.h>

namespace aegisgate::health {

std::size_t OutcomeChannel::CapacityForRoute(std::uint32_t max_inflight,
                                             std::uint32_t retry_budget) {
  // uint64 intermediate: max_inflight x (1 + retry_budget) must not be
  // truncated to 32 bits before the ceiling check.
  const std::uint64_t attempts =
      static_cast<std::uint64_t>(max_inflight) * (static_cast<std::uint64_t>(retry_budget) + 1);
  if (attempts == 0 || attempts > kMaxOutcomeCapacity) {
    throw std::invalid_argument(
        "outcome channel capacity (max_inflight x (1 + retry_budget)) exceeds the safety limit");
  }
  return static_cast<std::size_t>(attempts);
}

OutcomeChannel::OutcomeChannel(std::uint32_t capacity) : state_(std::make_shared<State>()) {
  if (capacity == 0 || capacity > kMaxOutcomeCapacity) {
    throw std::invalid_argument("outcome channel capacity must be positive and bounded");
  }
  state_->capacity = capacity;
  state_->available.store(capacity, std::memory_order_release);
  state_->wake_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (state_->wake_fd < 0) {
    throw std::system_error(errno, std::generic_category(), "eventfd");
  }
  state_->slots = std::make_unique<Slot[]>(capacity);
}

std::optional<OutcomeChannel::Reservation> OutcomeChannel::TryReserve() noexcept {
  if (!state_->accepting.load(std::memory_order_acquire)) {
    state_->rejected.fetch_add(1, std::memory_order_relaxed);
    return std::nullopt;
  }
  std::uint32_t current = state_->available.load(std::memory_order_acquire);
  while (current > 0) {
    if (state_->available.compare_exchange_weak(current, current - 1,
                                                std::memory_order_acq_rel,
                                                std::memory_order_acquire)) {
      return std::optional<Reservation>(Reservation(state_));
    }
  }
  state_->rejected.fetch_add(1, std::memory_order_relaxed);
  return std::nullopt;
}

std::size_t OutcomeChannel::DrainOnCoordinatorLoop(
    const std::function<void(const AttemptResult &)> &consume) noexcept {
  std::size_t consumed = 0;
  const std::uint32_t capacity = state_->capacity;
  for (;;) {
    const std::uint32_t index = state_->tail.load(std::memory_order_relaxed) % capacity;
    if (!state_->slots[index].ready.load(std::memory_order_acquire)) break;
    AttemptResult result = state_->slots[index].value;
    state_->slots[index].ready.store(false, std::memory_order_relaxed);
    state_->slots[index].value = AttemptResult{};
    state_->tail.fetch_add(1, std::memory_order_relaxed);
    if (consume) consume(result);
    // Every consumed outcome restores one credit so a later attempt can
    // reserve it again.
    state_->available.fetch_add(1, std::memory_order_release);
    ++consumed;
  }
  return consumed;
}

void OutcomeChannel::BeginStopping() noexcept {
  state_->accepting.store(false, std::memory_order_release);
}

std::size_t OutcomeChannel::pending() const noexcept {
  const std::uint32_t head = state_->head.load(std::memory_order_acquire);
  const std::uint32_t tail = state_->tail.load(std::memory_order_acquire);
  return static_cast<std::size_t>(head - tail);
}

std::uint64_t OutcomeChannel::rejected() const noexcept {
  return state_->rejected.load(std::memory_order_acquire);
}

int OutcomeChannel::WakeFd() const noexcept { return state_->wake_fd; }

void OutcomeChannel::State::Enqueue(AttemptResult result) noexcept {
  // The reservation accounting guarantees head - tail never reaches capacity:
  // a published-but-undrained outcome still occupies the credit its attempt
  // reserved.  A violation is an internal invariant break; do not silently
  // swallow the outcome — revert the claim and record a fatal.
  const std::uint32_t claimed = head.fetch_add(1, std::memory_order_relaxed);
  const std::uint32_t tail_now = tail.load(std::memory_order_acquire);
  if (claimed - tail_now >= capacity) {
    head.fetch_sub(1, std::memory_order_relaxed);
    std::fprintf(stderr, "fatal: OutcomeChannel ring overflow (capacity %u)\n", capacity);
    return;
  }
  const std::uint32_t slot = claimed % capacity;
  slots[slot].value = result;
  // Release/acquire pair on `ready`: the consumer reading true observes the
  // value assignment above (see Slot in the header).
  slots[slot].ready.store(true, std::memory_order_release);
  // Control wake: tell the coordinator loop a result is waiting.  EINTR is
  // retried; EAGAIN means a wake is already pending (no loss).
  const std::uint64_t counter = 1;
  for (;;) {
    const ssize_t count = ::write(wake_fd, &counter, sizeof(counter));
    if (count == static_cast<ssize_t>(sizeof(counter))) break;
    if (count < 0 && errno == EINTR) continue;
    break;
  }
}

OutcomeChannel::State::~State() {
  if (wake_fd >= 0) (void)::close(wake_fd);
}

OutcomeChannel::Reservation::Reservation(Reservation &&other) noexcept
    : state_(std::move(other.state_)), finalized_(other.finalized_) {
  other.state_.reset();
  other.finalized_ = true;
}

OutcomeChannel::Reservation &OutcomeChannel::Reservation::operator=(Reservation &&other) noexcept {
  if (this == &other) return *this;
  Finalize();
  state_ = std::move(other.state_);
  finalized_ = other.finalized_;
  other.state_.reset();
  other.finalized_ = true;
  return *this;
}

OutcomeChannel::Reservation::~Reservation() { Finalize(); }

void OutcomeChannel::Reservation::Finalize() noexcept {
  if (state_ && !finalized_) {
    // Unpublished: return the credit.  Published outcomes restore their credit
    // when the coordinator drains them instead.
    state_->available.fetch_add(1, std::memory_order_release);
  }
  state_.reset();
  finalized_ = true;
}

void OutcomeChannel::Reservation::Publish(AttemptResult result) noexcept {
  if (!state_ || finalized_) return;
  finalized_ = true;
  state_->Enqueue(result);
}

void OutcomeChannel::Reservation::Cancel() noexcept { Finalize(); }

} // namespace aegisgate::health
