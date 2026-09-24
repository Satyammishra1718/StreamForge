#ifndef STREAMFORGE_FRAME_CODEC_HPP
#define STREAMFORGE_FRAME_CODEC_HPP

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <winsock2.h>
#include <ws2tcpip.h>

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>

namespace streamforge {

constexpr uint32_t MAX_FRAME_LENGTH = 1024 * 1024; // 1 MiB
constexpr uint32_t HEADER_SIZE = 5; // 1 byte type + 4 bytes request_id

namespace MessageType {
    // M1 message types
    constexpr uint8_t PING = 0x01;
    constexpr uint8_t ECHO = 0x02;
    constexpr uint8_t SHUTDOWN = 0x05;

    // M3 Request types
    constexpr uint8_t CREATE_TOPIC   = 0x10;
    constexpr uint8_t PRODUCE        = 0x11;
    constexpr uint8_t FETCH          = 0x12;
    constexpr uint8_t LIST_TOPICS    = 0x13;
    constexpr uint8_t DESCRIBE_TOPIC = 0x14;

    // M1 Response types
    constexpr uint8_t PONG        = 0x81;
    constexpr uint8_t ECHO_REPLY  = 0x82;
    constexpr uint8_t SHUTDOWN_OK = 0x85;

    // M3 Response types
    constexpr uint8_t CREATE_TOPIC_OK   = 0x90;
    constexpr uint8_t PRODUCE_OK        = 0x91;
    constexpr uint8_t FETCH_OK          = 0x92;
    constexpr uint8_t TOPICS            = 0x93;
    constexpr uint8_t TOPIC_DESCRIPTION = 0x94;

    // Error frame
    constexpr uint8_t MSG_ERROR = 0xFF;
}

namespace ErrorCode {
    // M1 error codes
    constexpr uint16_t UNKNOWN_TYPE     = 1;
    constexpr uint16_t FRAME_TOO_LARGE  = 2;
    constexpr uint16_t MALFORMED_FRAME  = 3;

    // M3 error codes
    constexpr uint16_t UNKNOWN_TOPIC        = 4;
    constexpr uint16_t TOPIC_ALREADY_EXISTS = 5;
    constexpr uint16_t INVALID_TOPIC_NAME   = 6;
    constexpr uint16_t INVALID_PARTITION    = 7;
    constexpr uint16_t OFFSET_OUT_OF_RANGE  = 8;
    constexpr uint16_t RECORD_TOO_LARGE     = 9;
    constexpr uint16_t MALFORMED_BODY       = 10;
    constexpr uint16_t CORRUPT_DATA         = 11;
    constexpr uint16_t INTERNAL_ERROR       = 12;
    constexpr uint16_t INVALID_ARGUMENT     = 13;
}

struct Frame {
    uint32_t length{0}; // Bytes that follow length field
    uint8_t type{0};
    uint32_t request_id{0};
    std::vector<uint8_t> body;
};

class FrameCodec {
public:
    static uint32_t read_u32(const uint8_t* buffer);
    static void write_u32(uint8_t* buffer, uint32_t value);
    static uint16_t read_u16(const uint8_t* buffer);
    static void write_u16(uint8_t* buffer, uint16_t value);

    static std::vector<uint8_t> encode(const Frame& frame);
    static Frame create_error_frame(uint32_t request_id, uint16_t error_code, const std::string& message);
    static Frame create_pong_frame(uint32_t request_id);
    static Frame create_echo_reply_frame(uint32_t request_id, const std::vector<uint8_t>& body);

    static bool parse_error_frame(const Frame& frame, uint16_t& out_code, std::string& out_message);
};

} // namespace streamforge

#endif // STREAMFORGE_FRAME_CODEC_HPP
