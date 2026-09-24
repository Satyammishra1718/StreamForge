#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include "streamforge/WinsockRuntime.hpp"
#include "streamforge/Socket.hpp"
#include "streamforge/FrameCodec.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <cstdlib>
#include <cstring>

using namespace streamforge;

static bool connect_socket(Socket& sock, const std::string& host, uint16_t port) {
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) {
        std::cerr << "Failed to create socket\n";
        return false;
    }
    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        std::cerr << "Invalid IP address: " << host << "\n";
        closesocket(s);
        return false;
    }
    if (connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cerr << "Connect failed to " << host << ":" << port << ". Error: " << WSAGetLastError() << "\n";
        closesocket(s);
        return false;
    }
    sock = Socket(s);
    return true;
}

static bool read_response_frame(Socket& sock, Frame& out_frame) {
    uint8_t len_buf[4];
    RecvResult res = sock.recv_exact(len_buf, 4);
    if (res != RecvResult::Success) {
        std::cerr << "Failed to read response length header\n";
        return false;
    }
    uint32_t len = FrameCodec::read_u32(len_buf);
    if (len < HEADER_SIZE) {
        std::cerr << "Response frame length " << len << " < " << HEADER_SIZE << "\n";
        return false;
    }
    std::vector<uint8_t> payload(len);
    res = sock.recv_exact(payload.data(), len);
    if (res != RecvResult::Success) {
        std::cerr << "Failed to read response frame payload\n";
        return false;
    }
    out_frame.length = len;
    out_frame.type = payload[0];
    out_frame.request_id = FrameCodec::read_u32(payload.data() + 1);
    if (len > HEADER_SIZE) {
        out_frame.body.assign(payload.begin() + HEADER_SIZE, payload.end());
    }
    return true;
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = 9092;
    std::string command;
    std::string cmd_param;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (command.empty()) {
            command = arg;
        } else if (cmd_param.empty()) {
            cmd_param = arg;
        }
    }

    if (command.empty()) {
        std::cerr << "Usage: streamforge_cli [--host <ip>] [--port <port>] <command> [param]\n";
        return 1;
    }

    try {
        WinsockRuntime runtime;
        Socket sock;
        if (!connect_socket(sock, host, port)) {
            return 1;
        }

        if (command == "ping") {
            Frame req;
            req.type = MessageType::PING;
            req.request_id = 1001;
            req.length = HEADER_SIZE;

            std::vector<uint8_t> data = FrameCodec::encode(req);
            if (sock.send_all(data.data(), data.size()) != SendResult::Success) {
                std::cerr << "Failed to send PING\n";
                return 1;
            }

            Frame resp;
            if (!read_response_frame(sock, resp)) return 1;

            if (resp.type != MessageType::PONG || resp.request_id != 1001) {
                std::cerr << "PING test failed. Unexpected response type: " << static_cast<int>(resp.type) << "\n";
                return 1;
            }
            std::cout << "PONG received (req_id=" << resp.request_id << ")\n";
            return 0;

        } else if (command == "echo") {
            std::string msg = cmd_param.empty() ? "hello world" : cmd_param;
            Frame req;
            req.type = MessageType::ECHO;
            req.request_id = 1002;
            req.body.assign(msg.begin(), msg.end());
            req.length = HEADER_SIZE + static_cast<uint32_t>(req.body.size());

            std::vector<uint8_t> data = FrameCodec::encode(req);
            if (sock.send_all(data.data(), data.size()) != SendResult::Success) {
                std::cerr << "Failed to send ECHO\n";
                return 1;
            }

            Frame resp;
            if (!read_response_frame(sock, resp)) return 1;

            std::string resp_str(resp.body.begin(), resp.body.end());
            if (resp.type != MessageType::ECHO_REPLY || resp.request_id != 1002 || resp_str != msg) {
                std::cerr << "ECHO test failed. Got reply: " << resp_str << "\n";
                return 1;
            }
            std::cout << "ECHO_REPLY received: " << resp_str << "\n";
            return 0;

        } else if (command == "slow-echo") {
            std::string msg = cmd_param.empty() ? "hello" : cmd_param;
            Frame req;
            req.type = MessageType::ECHO;
            req.request_id = 1003;
            req.body.assign(msg.begin(), msg.end());
            req.length = HEADER_SIZE + static_cast<uint32_t>(req.body.size());

            std::vector<uint8_t> data = FrameCodec::encode(req);
            for (size_t i = 0; i < data.size(); ++i) {
                if (sock.send_all(&data[i], 1) != SendResult::Success) {
                    std::cerr << "Failed to send byte " << i << " in slow-echo\n";
                    return 1;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }

            Frame resp;
            if (!read_response_frame(sock, resp)) return 1;

            std::string resp_str(resp.body.begin(), resp.body.end());
            if (resp.type != MessageType::ECHO_REPLY || resp.request_id != 1003 || resp_str != msg) {
                std::cerr << "SLOW-ECHO test failed. Got reply: " << resp_str << "\n";
                return 1;
            }
            std::cout << "SLOW-ECHO success: " << resp_str << "\n";
            return 0;

        } else if (command == "big-frame") {
            // Declare length = 2 MiB (2097152 bytes)
            uint32_t oversized = 2 * 1024 * 1024;
            uint8_t len_buf[4];
            FrameCodec::write_u32(len_buf, oversized);

            if (sock.send_all(len_buf, 4) != SendResult::Success) {
                std::cerr << "Failed to send big-frame length header\n";
                return 1;
            }

            Frame resp;
            if (!read_response_frame(sock, resp)) return 1;

            uint16_t code = 0;
            std::string err_msg;
            if (!FrameCodec::parse_error_frame(resp, code, err_msg) || code != ErrorCode::FRAME_TOO_LARGE) {
                std::cerr << "BIG-FRAME test failed. Expected error code 2, got code: " << code << "\n";
                return 1;
            }

            // Connection should be closed by server
            uint8_t dummy[1];
            RecvResult close_check = sock.recv_exact(dummy, 1);
            if (close_check == RecvResult::Success) {
                std::cerr << "BIG-FRAME test failed: connection remained open!\n";
                return 1;
            }

            std::cout << "BIG-FRAME rejected with ERROR code 2 as expected (" << err_msg << ")\n";
            return 0;

        } else if (command == "garbage") {
            // Declare length = 2 (malformed, < 5)
            uint8_t len_buf[4];
            FrameCodec::write_u32(len_buf, 2);
            uint8_t garbage_payload[2] = {0xAA, 0xBB};

            if (sock.send_all(len_buf, 4) != SendResult::Success ||
                sock.send_all(garbage_payload, 2) != SendResult::Success) {
                std::cerr << "Failed to send garbage frame\n";
                return 1;
            }

            Frame resp;
            if (!read_response_frame(sock, resp)) return 1;

            uint16_t code = 0;
            std::string err_msg;
            if (!FrameCodec::parse_error_frame(resp, code, err_msg) || code != ErrorCode::MALFORMED_FRAME) {
                std::cerr << "GARBAGE test failed. Expected error code 3, got code: " << code << "\n";
                return 1;
            }

            // Connection should be closed by server
            uint8_t dummy[1];
            RecvResult close_check = sock.recv_exact(dummy, 1);
            if (close_check == RecvResult::Success) {
                std::cerr << "GARBAGE test failed: connection remained open!\n";
                return 1;
            }

            std::cout << "GARBAGE rejected with ERROR code 3 as expected (" << err_msg << ")\n";
            return 0;

        } else if (command == "unknown-type") {
            Frame req;
            req.type = 0x7F; // Unknown message type
            req.request_id = 1004;
            req.length = HEADER_SIZE;

            std::vector<uint8_t> data = FrameCodec::encode(req);
            if (sock.send_all(data.data(), data.size()) != SendResult::Success) {
                std::cerr << "Failed to send unknown-type frame\n";
                return 1;
            }

            Frame resp;
            if (!read_response_frame(sock, resp)) return 1;

            uint16_t code = 0;
            std::string err_msg;
            if (!FrameCodec::parse_error_frame(resp, code, err_msg) || code != ErrorCode::UNKNOWN_TYPE) {
                std::cerr << "UNKNOWN-TYPE test failed. Expected error code 1, got code: " << code << "\n";
                return 1;
            }

            // Connection MUST stay open! Send a PING frame over the same socket
            Frame ping_req;
            ping_req.type = MessageType::PING;
            ping_req.request_id = 1005;
            ping_req.length = HEADER_SIZE;

            std::vector<uint8_t> ping_data = FrameCodec::encode(ping_req);
            if (sock.send_all(ping_data.data(), ping_data.size()) != SendResult::Success) {
                std::cerr << "Failed to send follow-up PING on open connection\n";
                return 1;
            }

            Frame ping_resp;
            if (!read_response_frame(sock, ping_resp)) return 1;

            if (ping_resp.type != MessageType::PONG || ping_resp.request_id != 1005) {
                std::cerr << "Follow-up PING failed after unknown type error\n";
                return 1;
            }

            std::cout << "UNKNOWN-TYPE received ERROR code 1, subsequent PING succeeded\n";
            return 0;

        } else {
            std::cerr << "Unknown command: " << command << "\n";
            return 1;
        }

    } catch (const std::exception& ex) {
        std::cerr << "CLI error: " << ex.what() << "\n";
        return 1;
    }
}
