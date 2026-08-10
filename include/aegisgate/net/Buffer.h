#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace aegisgate::net {

class Buffer {
public:
  void Append(std::string_view bytes);
  [[nodiscard]] std::size_t ReadableBytes() const noexcept;
  [[nodiscard]] std::string_view ReadableView() const noexcept;
  [[nodiscard]] std::size_t FindCrlf() const noexcept;
  void Retrieve(std::size_t length);
  void RetrieveAll() noexcept;

private:
  std::string storage_;
  std::size_t read_index_ = 0;
};

} // namespace aegisgate::net
