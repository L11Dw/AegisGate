cmake_minimum_required(VERSION 3.20)

project(AegisGate LANGUAGES CXX)

option(AEGISGATE_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

find_package(yaml-cpp CONFIG REQUIRED)

add_library(aegisgate STATIC
    src/config/Config.cpp
    src/gateway/Gateway.cpp
    src/mock/MockBackend.cpp
    src/observability/Metrics.cpp
    src/net/Buffer.cpp
    src/net/Acceptor.cpp
    src/net/Channel.cpp
    src/net/ClientConnection.cpp
    src/net/EventLoop.cpp
    src/net/TimerQueue.cpp
    src/net/Socket.cpp
    src/net/UpstreamConnection.cpp
    src/proxy/ProxyTransaction.cpp
    src/proxy/UpstreamPool.cpp
    src/routing/ActiveReservation.cpp
    src/routing/RouteTable.cpp
    src/routing/WeightedRoundRobin.cpp
    src/resilience/InflightLimiter.cpp
    src/resilience/GlobalAdmission.cpp
    src/resilience/RouteAdmission.cpp
    src/runtime/WorkerRuntime.cpp
    src/runtime/WorkerSet.cpp
    src/runtime/WorkerData.cpp
    src/runtime/SelectionState.cpp
    src/runtime/AttemptSelector.cpp
    src/health/HealthChecker.cpp
    src/health/CoordinatorState.cpp
    src/health/Coordinator.cpp
    src/health/OutcomeChannel.cpp
    src/resilience/CircuitBreaker.cpp
    src/resilience/TokenBucket.cpp
    src/http/HttpResponse.cpp
    src/http/HttpResponseParser.cpp
    src/http/HttpRequestParser.cpp
    src/http/HttpRequestSerializer.cpp
)

target_link_libraries(aegisgate PUBLIC yaml-cpp)

target_include_directories(aegisgate PUBLIC include)
target_compile_features(aegisgate PUBLIC cxx_std_20)

include(cmake/CompilerWarnings.cmake)
aegisgate_enable_warnings(aegisgate)

add_executable(aegisgate_server apps/aegisgate/main.cpp)
target_link_libraries(aegisgate_server PRIVATE aegisgate)
aegisgate_enable_warnings(aegisgate_server)

add_executable(aegisgate_mock_backend apps/mock-backend/main.cpp)
target_link_libraries(aegisgate_mock_backend PRIVATE aegisgate)
aegisgate_enable_warnings(aegisgate_mock_backend)


include(CTest)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
