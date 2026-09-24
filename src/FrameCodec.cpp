#include "streamforge/FrameCodec.hpp"
#include <cstring>

namespace streamforge {

uint32_t FrameCodec::read_u32(const uint8_t* buffer) {
    uint32_t net_val;
    std::memcpy(&net_val, buffer, sizeof(net_val));
    return ntohl(net_val);
}

void FrameCodec::write_u32(uint8_t* buffer, uint32_t value) {
    uint32_t net_val = htonl(value);
    std::memcpy(buffer, &net_val, sizeof(net_val));
}

uint16_t FrameCodec::read_u16(const uint8_t* buffer) {
    uint16_t net_val;
    std::memcpy(&net_val, buffer, sizeof(net_val));
    return ntohs(net_val);
}

void FrameCodec::write_u16(uint8_t* buffer, uint16_t value) {
    uint16_t net_val = htons(value);
    std::memcpy(buffer, &net_val, sizeof(net_val));
}

std::vector<uint8_t> FrameCodec::encode(const Frame& frame) {
    uint32_t payload_len = HEADER_SIZE + static_cast<uint32_t>(frame.body.size());
    std::vector<uint8_t> out(4 + payload_len);

    write_u32(out.data(), payload_len);
    out[4] = frame.type;
    write_u32(out.data() + 5, frame.request_id);

    if (!frame.body.empty()) {
        std::memcpy(out.data() + 9, frame.body.data(), frame.body.size());
    }

    return out;
}

Frame FrameCodec::create_error_frame(uint32_t request_id, uint16_t error_code, const std::string& message) {
    Frame f;
    f.type = MessageType::MSG_ERROR;
    f.request_id = request_id;

    f.body.resize(2 + message.size());
    write_u16(f.body.data(), error_code);
    if (!message.empty()) {
        std::memcpy(f.body.data() + 2, message.data(), message.size());
    }
    f.length = HEADER_SIZE + static_cast<uint32_t>(f.body.size());
    return f;
}

Frame FrameCodec::create_pong_frame(uint32_t request_id) {
    Frame f;
    f.type = MessageType::PONG;
    f.request_id = request_id;
    f.length = HEADER_SIZE;
    return f;
}

Frame FrameCodec::create_echo_reply_frame(uint32_t request_id, const std::vector<uint8_t>& body) {
    Frame f;
    f.type = MessageType::ECHO_REPLY;
    f.request_id = request_id;
    f.body = body;
    f.length = HEADER_SIZE + static_cast<uint32_t>(body.size());
    return f;
}

bool FrameCodec::parse_error_frame(const Frame& frame, uint16_t& out_code, std::string& out_message) {
    if (frame.type != MessageType::MSG_ERROR || frame.body.size() < 2) {
        return false;
    }
    out_code = read_u16(frame.body.data());
    out_message = std::string(reinterpret_cast<const char*>(frame.body.data() + 2), frame.body.size() - 2);
    return true;
}

} // namespace streamforge
