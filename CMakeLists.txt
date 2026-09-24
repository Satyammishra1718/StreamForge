cmake_minimum_required(VERSION 3.16)
project(StreamForge LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MINGW)
    add_compile_options(-Wall -Wextra -Wpedantic)
    add_link_options(-static)
elseif(MSVC)
    add_compile_options(/W4)
endif()

include_directories(include)

# Core library containing shared Winsock, networking, protocol, and storage engine code
add_library(streamforge_core STATIC
    src/WinsockRuntime.cpp
    src/Socket.cpp
    src/FrameCodec.cpp
    src/FrameAssembler.cpp
    src/WakeupChannel.cpp
    src/ThreadPool.cpp
    src/Connection.cpp
    src/MessageHandler.cpp
    src/TcpServer.cpp
    src/Logger.cpp
    src/Crc32.cpp
    src/RecordCodec.cpp
    src/OffsetIndex.cpp
    src/LogSegment.cpp
    src/Partition.cpp
    src/Partitioner.cpp
    src/Topic.cpp
    src/TopicManager.cpp
)

target_include_directories(streamforge_core PUBLIC include)
target_link_libraries(streamforge_core PUBLIC ws2_32)

# Broker Server executable
add_executable(streamforge_server src/server_main.cpp)
target_link_libraries(streamforge_server PRIVATE streamforge_core)

# Test CLI executable
add_executable(streamforge_cli src/cli_main.cpp)
target_link_libraries(streamforge_cli PRIVATE streamforge_core)

# Storage Tool executable
add_executable(streamforge_storage src/storage_main.cpp)
target_link_libraries(streamforge_storage PRIVATE streamforge_core)

# Storage Unit Tests executable
add_executable(streamforge_storage_tests tests/storage_tests.cpp)
target_link_libraries(streamforge_storage_tests PRIVATE streamforge_core)

# Protocol Unit Tests executable
add_executable(streamforge_protocol_tests tests/protocol_tests.cpp)
target_link_libraries(streamforge_protocol_tests PRIVATE streamforge_core)

# Network Unit Tests executable
add_executable(streamforge_net_tests tests/net_tests.cpp)
target_link_libraries(streamforge_net_tests PRIVATE streamforge_core)
