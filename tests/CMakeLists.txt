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
    gateway/GatewayTest.cpp
    config/ConfigTest.cpp
    routing/RouteTableTest.cpp
    routing/WeightedRoundRobinTest.cpp
    resilience/TokenBucketTest.cpp
    resilience/InflightLimiterTest.cpp
    observability/MetricsTest.cpp
    http/HttpResponseTest.cpp
    http/HttpResponseParserTest.cpp
    http/HttpRequestParserTest.cpp
    http/HttpRequestSerializerTest.cpp
)

target_link_libraries(aegisgate_tests PRIVATE aegisgate GTest::gtest_main)

include(GoogleTest)
gtest_add_tests(TARGET aegisgate_tests TEST_LIST aegisgate_tests_TESTS)
set_tests_properties(${aegisgate_tests_TESTS} PROPERTIES
    ENVIRONMENT "LSAN_OPTIONS=detect_leaks=0"
)

aegisgate_enable_warnings(aegisgate_tests)
