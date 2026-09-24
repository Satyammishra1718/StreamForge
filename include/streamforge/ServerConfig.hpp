#ifndef STREAMFORGE_SERVER_CONFIG_HPP
#define STREAMFORGE_SERVER_CONFIG_HPP

#include <string>
#include <cstdint>
#include <cstddef>
#include <thread>
#include <algorithm>

namespace streamforge {

struct ServerConfig {
    std::string host{"127.0.0.1"};
    uint16_t port{9092};
    size_t workers{std::max(2u, std::thread::hardware_concurrency())};
    size_t max_connections{1024};
    uint32_t read_stall_timeout_sec{30};
    size_t max_output_buffer_bytes{8 * 1024 * 1024}; // 8 MiB
    size_t max_input_buffer_bytes{2 * 1024 * 1024};   // 2 MiB
};

} // namespace streamforge

#endif // STREAMFORGE_SERVER_CONFIG_HPP
