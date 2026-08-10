find_package(GTest CONFIG REQUIRED)

add_executable(aegisgate_tests
    net/BufferTest.cpp
    http/HttpRequestParserTest.cpp
)

target_link_libraries(aegisgate_tests PRIVATE aegisgate GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(aegisgate_tests DISCOVERY_MODE PRE_TEST)

aegisgate_enable_warnings(aegisgate_tests)
