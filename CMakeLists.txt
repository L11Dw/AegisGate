cmake_minimum_required(VERSION 3.20)

project(AegisGate LANGUAGES CXX)

option(AEGISGATE_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

add_library(aegisgate STATIC
    src/net/Buffer.cpp
    src/net/Acceptor.cpp
    src/net/Channel.cpp
    src/net/ClientConnection.cpp
    src/net/EventLoop.cpp
    src/net/Socket.cpp
    src/http/HttpResponse.cpp
    src/http/HttpRequestParser.cpp
)

target_include_directories(aegisgate PUBLIC include)
target_compile_features(aegisgate PUBLIC cxx_std_20)

include(cmake/CompilerWarnings.cmake)
aegisgate_enable_warnings(aegisgate)

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
