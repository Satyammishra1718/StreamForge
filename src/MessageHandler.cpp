#include "streamforge/MessageHandler.hpp"

namespace streamforge {

Frame MessageHandler::handle_request(const Frame& request) {
    switch (request.type) {
        case MessageType::PING:
            return FrameCodec::create_pong_frame(request.request_id);

        case MessageType::ECHO:
            return FrameCodec::create_echo_reply_frame(request.request_id, request.body);

        default:
            return FrameCodec::create_error_frame(
                request.request_id,
                ErrorCode::UNKNOWN_TYPE,
                "Unknown message type: 0x" + std::to_string(static_cast<int>(request.type))
            );
    }
}

} // namespace streamforge
