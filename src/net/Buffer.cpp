#include "aegisgate/net/Buffer.h"

#include <stdexcept>

namespace aegisgate::net {

void Buffer::Append(std::string_view bytes) {
  if (bytes.empty()) {
    return;
  }

  if (read_index_ > ReadableBytes()) {
    storage_.erase(0, read_index_);
    read_index_ = 0;
  }

  storage_.append(bytes.data(), bytes.size());
}

std::size_t Buffer::ReadableBytes() const noexcept {
  return storage_.size() - read_index_;
}

std::string_view Buffer::ReadableView() const noexcept {
  return std::string_view(storage_).substr(read_index_);
}

std::size_t Buffer::FindCrlf() const noexcept {
  return ReadableView().find("\r\n");
}

void Buffer::Retrieve(std::size_t length) {
  if (length > ReadableBytes()) {
    throw std::out_of_range("retrieve length exceeds readable bytes");
  }

  read_index_ += length;
}

void Buffer::RetrieveAll() noexcept {
  storage_.clear();
  read_index_ = 0;
}

} // namespace aegisgate::net
