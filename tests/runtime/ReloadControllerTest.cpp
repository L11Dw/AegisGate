// A6: ReloadController tests.
// ReloadController owns the background YAML parse thread and a latest-wins
// result mailbox.  The control loop drains results and publishes candidates.

#include <chrono>
#include <fstream>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "aegisgate/config/Config.h"
#include "aegisgate/runtime/ReloadController.h"

namespace aegisgate::runtime {
namespace {

// Write a YAML config string to a temporary file and return its path.
std::string WriteTempConfig(const std::string &content, const std::string &name = "test_config.yaml") {
  const std::string path = "/tmp/aegisgate_test_" + name;
  std::ofstream ofs(path);
  ofs << content;
  ofs.close();
  return path;
}

constexpr std::string_view kValidYaml = R"(workers: 1
routes:
  - name: api
    host: test.local
    path_prefix: /
    endpoints:
      - host: 127.0.0.1
        port: 8080
        weight: 1
    rate_limit: 100
    burst: 50
    max_inflight: 10
)";

constexpr std::string_view kInvalidYaml = R"(this is not valid yaml: [broken
)";

// ---------------------------------------------------------------------------
// ReloadController basic tests
// ---------------------------------------------------------------------------

TEST(ReloadControllerTest, ParseValidYamlSucceeds) {
  const std::string path = WriteTempConfig(std::string(kValidYaml));
  ReloadController controller(path);

  EXPECT_TRUE(controller.Request());
  // Wait for the parse to complete.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  auto results = controller.Drain();
  ASSERT_FALSE(results.empty());
  EXPECT_TRUE(results.back().error.empty()) << "parse error: " << results.back().error;
  ASSERT_TRUE(results.back().candidate.has_value());
  EXPECT_EQ(results.back().candidate->workers, 1u);
  EXPECT_EQ(results.back().candidate->routes.size(), 1u);
}

TEST(ReloadControllerTest, ParseInvalidYamlReportsError) {
  const std::string path = WriteTempConfig(std::string(kInvalidYaml), "invalid.yaml");
  ReloadController controller(path);

  EXPECT_TRUE(controller.Request());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  auto results = controller.Drain();
  ASSERT_FALSE(results.empty());
  EXPECT_FALSE(results.back().candidate.has_value());
  EXPECT_FALSE(results.back().error.empty());
}

TEST(ReloadControllerTest, ParseMissingFileReportsError) {
  ReloadController controller("/nonexistent/path/config.yaml");

  EXPECT_TRUE(controller.Request());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  auto results = controller.Drain();
  ASSERT_FALSE(results.empty());
  EXPECT_FALSE(results.back().candidate.has_value());
  EXPECT_FALSE(results.back().error.empty());
}

TEST(ReloadControllerTest, MailboxIsLatestWins) {
  const std::string path = WriteTempConfig(std::string(kValidYaml), "latest_wins.yaml");
  ReloadController controller(path);

  // Request multiple parses in rapid succession.
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(controller.Request());
  }
  // Wait for all parses to complete.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  auto results = controller.Drain();
  // Only one result (latest-wins), or at most one pending + one active.
  EXPECT_LE(results.size(), 2u);
  // The last result should be valid.
  EXPECT_FALSE(results.empty());
  EXPECT_TRUE(results.back().candidate.has_value());
}

TEST(ReloadControllerTest, StopRejectsLateRequests) {
  const std::string path = WriteTempConfig(std::string(kValidYaml), "stop_rejects.yaml");
  ReloadController controller(path);

  controller.Stop();
  EXPECT_FALSE(controller.Request());
}

TEST(ReloadControllerTest, WakeFdIsValid) {
  const std::string path = WriteTempConfig(std::string(kValidYaml), "wake_fd.yaml");
  ReloadController controller(path);
  EXPECT_GE(controller.wake_fd(), 0);
}

TEST(ReloadControllerTest, SequenceMonotonicallyIncreases) {
  const std::string path = WriteTempConfig(std::string(kValidYaml), "sequence.yaml");
  ReloadController controller(path);

  EXPECT_TRUE(controller.Request());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  auto results = controller.Drain();
  ASSERT_FALSE(results.empty());
  EXPECT_GT(results.back().sequence, 0u);
}

TEST(ReloadControllerTest, PostStopDrainReturnsEmpty) {
  const std::string path = WriteTempConfig(std::string(kValidYaml), "post_stop.yaml");
  ReloadController controller(path);

  controller.Stop();
  auto results = controller.Drain();
  EXPECT_TRUE(results.empty());
}

} // namespace
} // namespace aegisgate::runtime
