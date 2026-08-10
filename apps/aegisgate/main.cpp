#include <charconv>
#include <cstdint>
#include <exception>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>

#include "aegisgate/config/Config.h"
#include "aegisgate/gateway/Gateway.h"
#include "aegisgate/net/EventLoop.h"

namespace {

std::uint16_t ParsePort(std::string_view value) {
  unsigned int port = 0;
  const auto [position, error] = std::from_chars(value.data(), value.data() + value.size(), port);
  if (error != std::errc{} || position != value.data() + value.size() || port == 0 || port > 65535) {
    throw std::invalid_argument("listen port must be an integer in [1, 65535]");
  }
  return static_cast<std::uint16_t>(port);
}

std::string ReadFile(const char *path) {
  std::ifstream input(path);
  if (!input) throw std::runtime_error("failed to open configuration file");
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

} // namespace

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "usage: aegisgate_server <config.yaml> <listen-port>\n";
    return 2;
  }
  try {
    aegisgate::net::EventLoop loop;
    aegisgate::gateway::Gateway gateway(
        loop, aegisgate::config::LoadFromYaml(ReadFile(argv[1])), "0.0.0.0", ParsePort(argv[2]));
    gateway.Start();
    loop.Loop();
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "aegisgate_server: " << error.what() << '\n';
    return 1;
  }
}
