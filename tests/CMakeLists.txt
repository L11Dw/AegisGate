find_package(GTest CONFIG REQUIRED)

add_executable(aegisgate_tests
    net/BufferTest.cpp
    net/AcceptorTest.cpp
    net/EventLoopTest.cpp
    net/TimerQueueTest.cpp
    net/SocketTest.cpp
    net/ClientConnectionTest.cpp
    net/UpstreamConnectionTest.cpp
    proxy/ProxyTransactionTest.cpp
    proxy/UpstreamPoolTest.cpp
    runtime/WorkerRuntimeTest.cpp
    runtime/WorkerSetTest.cpp
    runtime/WorkerDataTest.cpp
    runtime/SelectionStateTest.cpp
    runtime/AttemptSelectorTest.cpp
    runtime/RuntimeGenerationTest.cpp
    runtime/ReloadControllerTest.cpp
    gateway/GatewayTest.cpp
    config/ConfigTest.cpp
    routing/ActiveReservationTest.cpp
    routing/RouteTableTest.cpp
    routing/WeightedRoundRobinTest.cpp
    resilience/TokenBucketTest.cpp
    resilience/InflightLimiterTest.cpp
    resilience/GlobalAdmissionTest.cpp
    resilience/CircuitBreakerTest.cpp
    health/EndpointHealthTest.cpp
    health/HealthCheckerTest.cpp
    health/CoordinatorTest.cpp
    health/OutcomeChannelTest.cpp
    observability/MetricsTest.cpp
    mock/MockBackendTest.cpp
    integration/EndToEndTest.cpp
    gateway/ReloadTest.cpp
    http/HttpResponseTest.cpp
    http/HttpResponseParserTest.cpp
    http/HttpRequestParserTest.cpp
    http/HttpRequestSerializerTest.cpp
)

target_link_libraries(aegisgate_tests PRIVATE aegisgate GTest::gtest_main)
target_compile_definitions(aegisgate_tests PRIVATE AEGISGATE_SOURCE_DIR="${PROJECT_SOURCE_DIR}")
target_include_directories(aegisgate_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

include(GoogleTest)
gtest_add_tests(TARGET aegisgate_tests TEST_LIST aegisgate_tests_TESTS)
set_tests_properties(${aegisgate_tests_TESTS} PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=detect_leaks=0"
)

aegisgate_enable_warnings(aegisgate_tests)
