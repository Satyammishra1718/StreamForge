#include "streamforge/Connection.hpp"
#include "streamforge/Logger.hpp"
#include "streamforge/FrameCodec.hpp"
#include <vector>

namespace streamforge {

Connection::Connection(std::shared_ptr<Socket> socket, std::string peer_ip, uint16_t peer_port)
    : m_socket(std::move(socket)), m_peer_ip(std::move(peer_ip)), m_peer_port(peer_port) {}

void Connection::stop() {
    if (m_socket) {
        m_socket->shutdown(SD_BOTH);
    }
}

void Connection::run(MessageHandler& handler, const std::atomic<bool>& shutdown_requested) {
    std::string addr_str = peer_address_string();
    Logger::instance().info("Client connected: " + addr_str);

    while (!shutdown_requested) {
        // Step 1: Read 4-byte length header
        uint8_t length_buf[4];
        RecvResult res = m_socket->recv_exact(length_buf, 4);
        if (res == RecvResult::Disconnected) {
            Logger::instance().info("Client disconnected gracefully: " + addr_str);
            break;
        }
        if (res == RecvResult::Error) {
            Logger::instance().info("Client disconnected abruptly or socket error: " + addr_str);
            break;
        }

        uint32_t length = FrameCodec::read_u32(length_buf);

        // Step 2: Validate length
        if (length < HEADER_SIZE) {
            Logger::instance().warning("Malformed frame received from " + addr_str + " (length: " + std::to_string(length) + " < " + std::to_string(HEADER_SIZE) + ")");
            Frame err_frame = FrameCodec::create_error_frame(0, ErrorCode::MALFORMED_FRAME, "Malformed frame: length less than header size");
            std::vector<uint8_t> encoded_err = FrameCodec::encode(err_frame);
            m_socket->send_all(encoded_err.data(), encoded_err.size());
            break; // Close connection on malformed frame
        }

        if (length > MAX_FRAME_LENGTH) {
            Logger::instance().warning("Frame too large received from " + addr_str + " (length: " + std::to_string(length) + " > " + std::to_string(MAX_FRAME_LENGTH) + ")");
            Frame err_frame = FrameCodec::create_error_frame(0, ErrorCode::FRAME_TOO_LARGE, "Frame size exceeds maximum limit of 1 MiB");
            std::vector<uint8_t> encoded_err = FrameCodec::encode(err_frame);
            m_socket->send_all(encoded_err.data(), encoded_err.size());
            break; // Close connection on oversized frame
        }

        // Step 3: Read remaining payload (length bytes)
        std::vector<uint8_t> payload(length);
        res = m_socket->recv_exact(payload.data(), length);
        if (res != RecvResult::Success) {
            Logger::instance().info("Client disconnected during frame payload read: " + addr_str);
            break;
        }

        // Parse header fields from payload
        Frame req_frame;
        req_frame.length = length;
        req_frame.type = payload[0];
        req_frame.request_id = FrameCodec::read_u32(payload.data() + 1);
        if (length > HEADER_SIZE) {
            req_frame.body.assign(payload.begin() + HEADER_SIZE, payload.end());
        }

        // Step 4: Dispatch to MessageHandler
        Frame resp_frame = handler.handle_request(req_frame);

        if (resp_frame.type == MessageType::MSG_ERROR) {
            uint16_t code = 0;
            std::string msg;
            FrameCodec::parse_error_frame(resp_frame, code, msg);
            Logger::instance().warning("Protocol error for " + addr_str + " (code " + std::to_string(code) + "): " + msg);
        }

        // Step 5: Send reply
        std::vector<uint8_t> encoded_resp = FrameCodec::encode(resp_frame);
        SendResult s_res = m_socket->send_all(encoded_resp.data(), encoded_resp.size());
        if (s_res != SendResult::Success) {
            Logger::instance().info("Failed to send response to " + addr_str + ", closing connection");
            break;
        }
    }
}

} // namespace streamforge
