#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace aegisgate::net {

// A byte buffer with a logical read cursor.  Consuming input advances the
// cursor first; compaction is deferred until a later append needs the space.
class Buffer {
public:
  void Append(std::string_view bytes);
  [[nodiscard]] std::size_t ReadableBytes() const noexcept;
  // The returned view is invalidated by any non-const Buffer operation.
  [[nodiscard]] std::string_view ReadableView() const noexcept;
  // Returns an offset relative to ReadableView(), not to the backing storage.
  [[nodiscard]] std::size_t FindCrlf() const noexcept;
  void Retrieve(std::size_t length);
  void RetrieveAll() noexcept;

private:
  std::string storage_;
  // Bytes before this cursor have been consumed by the protocol layer.
  std::size_t read_index_ = 0;
};

} // namespace aegisgate::net
