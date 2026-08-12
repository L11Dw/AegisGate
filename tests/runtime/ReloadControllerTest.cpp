#include <array>
#include <cerrno>
#include <chrono>
#include <string>
#include <string_view>
#include <thread>

#include <gtest/gtest.h>

#include <poll.h>
#include <unistd.h>

#include "aegisgate/runtime/ReloadController.h"

namespace aegisgate::runtime {
namespace {

TEST(ReloadControllerTest, ParsesCandidateOffControlThreadAndSignalsItsWakeFd) {
  char path[] = "/tmp/aegisgate-reload-XXXXXX";
  const int fd = ::mkstemp(path);
  ASSERT_GE(fd, 0);
  constexpr std::string_view yaml = R"(workers: 1
routes:
  - name: api
    host: reload.test
    path_prefix: /
    endpoints:
      - host: 127.0.0.1
        port: 18081
        weight: 1
    rate_limit: 10
    burst: 10
    max_inflight: 4
)";
  ASSERT_EQ(::write(fd, yaml.data(), yaml.size()), static_cast<ssize_t>(yaml.size()));
  ASSERT_EQ(::close(fd), 0);

  ReloadController controller(path);
  ASSERT_TRUE(controller.Request());
  pollfd descriptor{controller.wake_fd(), POLLIN, 0};
  ASSERT_GT(::poll(&descriptor, 1, 5000), 0);
  const auto results = controller.Drain();
  ASSERT_EQ(results.size(), 1U);
  ASSERT_TRUE(results.front().candidate.has_value()) << results.front().error;
  EXPECT_EQ(results.front().candidate->routes.front().endpoints.front().port, 18081);
  EXPECT_NE(results.front().parser_thread, std::this_thread::get_id());

  controller.Stop();
  EXPECT_EQ(::unlink(path), 0);
}

} // namespace
} // namespace aegisgate::runtime
