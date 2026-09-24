#ifndef STREAMFORGE_CONNECTION_STATE_HPP
#define STREAMFORGE_CONNECTION_STATE_HPP

#include "streamforge/Socket.hpp"
#include "streamforge/FrameAssembler.hpp"
#include <queue>
#include <vector>
#include <string>
#include <chrono>
#include <cstdint>

namespace streamforge {

struct ConnectionState {
    uint64_t conn_id{0};
    Socket socket;
    std::string peer_ip;
    uint16_t peer_port{0};

    FrameAssembler assembler;
    std::queue<Frame> pending_requests;
    bool in_flight{false};

    std::vector<uint8_t> output_buffer;
    size_t output_offset{0};
    bool close_after_send{false};

    std::chrono::steady_clock::time_point last_read_time;
    bool has_active_read_stall{false};

    std::string peer_address_string() const {
        return peer_ip + ":" + std::to_string(peer_port);
    }
};

} // namespace streamforge

#endif // STREAMFORGE_CONNECTION_STATE_HPP
