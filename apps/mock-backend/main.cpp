#include <charconv>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

#include "aegisgate/mock/MockBackend.h"
#include "aegisgate/net/EventLoop.h"

namespace {

template <typename Integer>
Integer ParseUnsigned(std::string_view value, std::string_view name, unsigned long long maximum) {
  unsigned long long parsed = 0;
  const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size() || parsed > maximum) {
    throw std::invalid_argument(std::string(name) + " is out of range");
  }
  return static_cast<Integer>(parsed);
}

} // namespace

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cerr << "usage: aegisgate_mock_backend <listen-port> [--status N] [--delay-ms N] [--reset] [--max-inflight N] [--body-bytes N]\n";
    return 2;
  }
  try {
    const auto port = ParseUnsigned<std::uint16_t>(argv[1], "listen port", 65535);
    if (port == 0) throw std::invalid_argument("listen port must be positive");
    aegisgate::mock::MockBackendOptions options;
    for (int index = 2; index < argc; ++index) {
      const std::string_view option(argv[index]);
      if (option == "--reset") {
        options.reset = true;
        continue;
      }
      if (index + 1 >= argc) throw std::invalid_argument("option value is missing");
      const std::string_view value(argv[++index]);
      if (option == "--status") {
        options.status = ParseUnsigned<int>(value, "status", 599);
      } else if (option == "--delay-ms") {
        options.delay = std::chrono::milliseconds(ParseUnsigned<long long>(value, "delay-ms", 600000));
      } else if (option == "--max-inflight") {
        options.max_inflight = ParseUnsigned<std::size_t>(value, "max-inflight", 1000000);
      } else if (option == "--body-bytes") {
        options.body_bytes = ParseUnsigned<std::size_t>(value, "body-bytes", 16 * 1024 * 1024);
      } else {
        throw std::invalid_argument("unknown option");
      }
    }
    aegisgate::net::EventLoop loop;
    aegisgate::mock::MockBackend backend(loop, options, "0.0.0.0", port);
    backend.Start();
    loop.Loop();
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "aegisgate_mock_backend: " << error.what() << '\n';
    return 1;
  }
}
