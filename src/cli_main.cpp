#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include "streamforge/WinsockRuntime.hpp"
#include "streamforge/Socket.hpp"
#include "streamforge/FrameCodec.hpp"
#include "streamforge/ProtocolMessages.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <atomic>
#include <map>
#include <sstream>

using namespace streamforge;

static std::atomic<bool> g_stop_requested{false};

BOOL WINAPI cli_ctrl_handler(DWORD ctrl_type) {
    switch (ctrl_type) {
        case CTRL_C_EVENT:
        case CTRL_BREAK_EVENT:
        case CTRL_CLOSE_EVENT:
            g_stop_requested = true;
            return TRUE;
        default:
            return FALSE;
    }
}

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

static bool send_and_receive(Socket& sock, const Frame& req_frame, Frame& out_resp_frame) {
    std::vector<uint8_t> encoded = FrameCodec::encode(req_frame);
    if (sock.send_all(encoded.data(), encoded.size()) != SendResult::Success) {
        std::cerr << "Failed to send request frame\n";
        return false;
    }
    if (!read_response_frame(sock, out_resp_frame)) {
        return false;
    }
    if (out_resp_frame.type == MessageType::MSG_ERROR) {
        uint16_t code = 0;
        std::string msg;
        FrameCodec::parse_error_frame(out_resp_frame, code, msg);
        std::cerr << "Error (code " << code << "): " << msg << "\n";
        return false;
    }
    return true;
}

static std::string escape_bytes(const std::vector<uint8_t>& bytes) {
    std::string s;
    for (uint8_t b : bytes) {
        if (b >= 32 && b <= 126 && b != '\\') {
            s.push_back(static_cast<char>(b));
        } else {
            char buf[8];
            std::snprintf(buf, sizeof(buf), "\\x%02x", b);
            s.append(buf);
        }
    }
    return s;
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = 9092;
    std::string command;
    std::vector<std::string> args;

    std::string key_arg;
    int32_t partition_arg = -1;
    std::string key_prefix_arg;
    size_t size_arg = 100;
    size_t batch_arg = 100;
    uint32_t max_messages_arg = 50;
    uint32_t max_bytes_arg = 1048576;
    uint64_t from_offset_arg = 0;
    bool follow_arg = false;

    // M5 flags
    std::string member_arg = "";
    uint32_t session_timeout_ms_arg = 30000;
    std::string strategy_arg = "range";
    uint32_t generation_arg = 0xFFFFFFFF;
    std::string partitions_arg = "";
    size_t commit_every_arg = 100;
    std::string reset_arg = "earliest";
    uint32_t max_messages_per_fetch_arg = 50;
    uint32_t process_delay_ms_arg = 0;
    size_t max_records_arg = 0;
    uint64_t idle_exit_ms_arg = 0;
    size_t crash_after_arg = 0;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (arg == "--key" && i + 1 < argc) {
            key_arg = argv[++i];
        } else if (arg == "--partition" && i + 1 < argc) {
            partition_arg = std::atoi(argv[++i]);
        } else if (arg == "--key-prefix" && i + 1 < argc) {
            key_prefix_arg = argv[++i];
        } else if (arg == "--size" && i + 1 < argc) {
            size_arg = static_cast<size_t>(std::stoul(argv[++i]));
        } else if (arg == "--batch" && i + 1 < argc) {
            batch_arg = static_cast<size_t>(std::stoul(argv[++i]));
        } else if (arg == "--max-messages" && i + 1 < argc) {
            max_messages_arg = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--max-bytes" && i + 1 < argc) {
            max_bytes_arg = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--from" && i + 1 < argc) {
            from_offset_arg = std::stoull(argv[++i]);
        } else if (arg == "--follow") {
            follow_arg = true;
        } else if (arg == "--member" && i + 1 < argc) {
            member_arg = argv[++i];
        } else if (arg == "--session-timeout-ms" && i + 1 < argc) {
            session_timeout_ms_arg = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--strategy" && i + 1 < argc) {
            strategy_arg = argv[++i];
        } else if (arg == "--generation" && i + 1 < argc) {
            generation_arg = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--partitions" && i + 1 < argc) {
            partitions_arg = argv[++i];
        } else if (arg == "--commit-every" && i + 1 < argc) {
            commit_every_arg = static_cast<size_t>(std::stoul(argv[++i]));
        } else if (arg == "--reset" && i + 1 < argc) {
            reset_arg = argv[++i];
        } else if (arg == "--max-messages-per-fetch" && i + 1 < argc) {
            max_messages_per_fetch_arg = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--process-delay-ms" && i + 1 < argc) {
            process_delay_ms_arg = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--max-records" && i + 1 < argc) {
            max_records_arg = static_cast<size_t>(std::stoul(argv[++i]));
        } else if (arg == "--idle-exit-ms" && i + 1 < argc) {
            idle_exit_ms_arg = static_cast<uint64_t>(std::stoull(argv[++i]));
        } else if (arg == "--crash-after" && i + 1 < argc) {
            crash_after_arg = static_cast<size_t>(std::stoul(argv[++i]));
        } else if (command.empty() && arg.rfind("--", 0) != 0) {
            command = arg;
        } else {
            args.push_back(arg);
        }
    }

    if (command.empty()) {
        std::cerr << "Usage: streamforge_cli [--host H] [--port P] <command> [args...]\n";
        return 1;
    }

    try {
        WinsockRuntime runtime;

        if (command == "hold-connections") {
            size_t count = args.empty() ? 10 : static_cast<size_t>(std::stoul(args[0]));
            uint32_t seconds = args.size() > 1 ? static_cast<uint32_t>(std::stoul(args[1])) : 5;
            std::vector<Socket> sockets;
            sockets.reserve(count);
            size_t connected = 0;
            for (size_t i = 0; i < count; ++i) {
                Socket s;
                if (connect_socket(s, host, port)) {
                    sockets.push_back(std::move(s));
                    connected++;
                }
            }
            std::cout << "Connected " << connected << " of " << count << " sockets, holding for " << seconds << " seconds...\n";
            std::this_thread::sleep_for(std::chrono::seconds(seconds));
            std::cout << "Released " << connected << " sockets.\n";
            return 0;
        }

        Socket sock;
        if (!connect_socket(sock, host, port)) {
            return 1;
        }

        uint32_t req_id = 1000;

        if (command == "stall-frame") {
            uint8_t partial[2] = { 0x00, 0x00 };
            if (sock.send_all(partial, 2) != SendResult::Success) {
                std::cerr << "Failed to send stalled 2 bytes\n";
                return 1;
            }
            std::cout << "Sent 2-byte stalled frame header, waiting for server timeout...\n";
            uint8_t dummy[1];
            RecvResult res = sock.recv_exact(dummy, 1);
            if (res == RecvResult::Disconnected || res == RecvResult::Error) {
                std::cout << "Server closed connection as expected (stall timeout triggered)\n";
                return 0;
            }
            return 0;

        } else if (command == "slow-reader") {
            if (args.size() < 2) {
                std::cerr << "Usage: streamforge_cli slow-reader TOPIC PARTITION\n";
                return 1;
            }
            std::string topic_name = args[0];
            uint16_t partition = static_cast<uint16_t>(std::atoi(args[1].c_str()));
            std::cout << "Starting slow-reader on " << topic_name << " P" << partition
                      << ", sending rapid FETCHes without reading responses...\n";

            size_t sent_count = 0;
            while (true) {
                FetchRequest req_msg{ topic_name, partition, 0, 1024 * 1024, 100 };
                BodyWriter writer;
                req_msg.encode(writer);
                std::vector<uint8_t> body = writer.take_buffer();
                Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::FETCH, req_id++, body };
                std::vector<uint8_t> encoded = FrameCodec::encode(req);

                if (sock.send_all(encoded.data(), encoded.size()) != SendResult::Success) {
                    std::cout << "Connection closed by server after " << sent_count
                              << " FETCH requests (slow consumer limit enforced)\n";
                    return 0;
                }
                sent_count++;
                if (sent_count % 100 == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }
            }
            return 0;

        } else if (command == "pipeline") {
            size_t count = args.empty() ? 100 : static_cast<size_t>(std::stoul(args[0]));
            std::cout << "Pipelining " << count << " PING requests back-to-back...\n";
            std::vector<uint32_t> sent_req_ids;
            sent_req_ids.reserve(count);
            std::vector<uint8_t> all_bytes;

            for (size_t i = 0; i < count; ++i) {
                uint32_t id = req_id++;
                sent_req_ids.push_back(id);
                Frame req{ HEADER_SIZE, MessageType::PING, id, {} };
                std::vector<uint8_t> enc = FrameCodec::encode(req);
                all_bytes.insert(all_bytes.end(), enc.begin(), enc.end());
            }

            if (sock.send_all(all_bytes.data(), all_bytes.size()) != SendResult::Success) {
                std::cerr << "Pipeline send failed\n";
                return 1;
            }

            for (size_t i = 0; i < count; ++i) {
                Frame resp;
                if (!read_response_frame(sock, resp)) {
                    std::cerr << "Pipeline recv failed at index " << i << "\n";
                    return 1;
                }
                if (resp.type != MessageType::PONG || resp.request_id != sent_req_ids[i]) {
                    std::cerr << "Pipeline ordering violation: expected req_id=" << sent_req_ids[i]
                              << ", got " << resp.request_id << "\n";
                    return 1;
                }
            }
            std::cout << "SUCCESS: All " << count << " pipelined responses received in strict sequential order with matching request_ids!\n";
            return 0;

        } else if (command == "shutdown") {
            Frame req{ HEADER_SIZE, MessageType::SHUTDOWN, req_id++, {} };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) {
                std::cout << "Server shutdown acknowledged\n";
                return 0;
            }
            std::cout << "Server shutdown acknowledged (req_id=" << resp.request_id << ")\n";
            return 0;

        } else if (command == "ping") {
            Frame req{ HEADER_SIZE, MessageType::PING, req_id++, {} };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;
            if (resp.type == MessageType::PONG) {
                std::cout << "PONG received (req_id=" << resp.request_id << ")\n";
                return 0;
            }
            return 1;

        } else if (command == "echo") {
            std::string payload = args.empty() ? "hello" : args[0];
            std::vector<uint8_t> body(payload.begin(), payload.end());
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::ECHO, req_id++, body };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;
            std::string echoed(resp.body.begin(), resp.body.end());
            std::cout << "ECHO_REPLY received: " << echoed << "\n";
            return 0;

        } else if (command == "slow-echo") {
            std::string payload = args.empty() ? "slow" : args[0];
            std::vector<uint8_t> body(payload.begin(), payload.end());
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::ECHO, req_id++, body };
            std::vector<uint8_t> encoded = FrameCodec::encode(req);
            for (uint8_t byte : encoded) {
                sock.send_all(&byte, 1);
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            Frame resp;
            if (!read_response_frame(sock, resp)) return 1;
            std::string echoed(resp.body.begin(), resp.body.end());
            std::cout << echoed << "\n";
            return 0;

        } else if (command == "big-frame") {
            size_t size = args.empty() ? 2 * 1024 * 1024 : static_cast<size_t>(std::stoul(args[0]));
            uint8_t len_bytes[4];
            FrameCodec::write_u32(len_bytes, static_cast<uint32_t>(size));
            if (sock.send_all(len_bytes, 4) != SendResult::Success) {
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
            std::cout << "BIG-FRAME rejected with ERROR code 2 as expected (" << err_msg << ")\n";
            return 0;

        } else if (command == "garbage") {
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
            std::cout << "GARBAGE rejected with ERROR code 3 as expected (" << err_msg << ")\n";
            return 0;

        } else if (command == "malformed-produce") {
            // Hand-crafted PRODUCE body: lying key length (error 10), then PING on same connection.
            BodyWriter w;
            w.write_string("orders");
            w.write_i32(0);
            w.write_u16(1);
            w.write_u32(10000);
            w.write_u8('A');

            std::vector<uint8_t> body = w.take_buffer();
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::PRODUCE, req_id++, body };
            std::vector<uint8_t> data = FrameCodec::encode(req);
            if (sock.send_all(data.data(), data.size()) != SendResult::Success) {
                std::cerr << "Failed to send malformed PRODUCE frame\n";
                return 1;
            }

            Frame resp;
            if (!read_response_frame(sock, resp)) return 1;
            uint16_t code = 0;
            std::string err_msg;
            if (!FrameCodec::parse_error_frame(resp, code, err_msg) || code != ErrorCode::MALFORMED_BODY) {
                std::cerr << "MALFORMED-PRODUCE test failed. Expected error code 10, got code: " << code << "\n";
                return 1;
            }

            Frame ping_req;
            ping_req.type = MessageType::PING;
            ping_req.request_id = req_id++;
            ping_req.length = HEADER_SIZE;
            if (!send_and_receive(sock, ping_req, resp)) return 1;
            std::cout << "MALFORMED-PRODUCE received ERROR code 10, subsequent PING succeeded\n";
            return 0;

        } else if (command == "produce-oversized") {
            if (args.empty()) {
                std::cerr << "Usage: streamforge_cli produce-oversized TOPIC\n";
                return 1;
            }
            std::string topic_name = args[0];
            // Encoded on-disk record must exceed 1 MiB; keep the PRODUCE frame at or under 1 MiB.
            const size_t value_size = 1048549;

            ProduceRequest req_msg;
            req_msg.topic = topic_name;
            req_msg.partition = 0;
            req_msg.record_count = 1;
            req_msg.records.push_back({ {}, std::vector<uint8_t>(value_size, 'X') });

            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();
            if (HEADER_SIZE + body.size() > MAX_FRAME_LENGTH) {
                std::cerr << "Internal test payload exceeds max frame size\n";
                return 1;
            }

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::PRODUCE, req_id++, body };
            std::vector<uint8_t> encoded = FrameCodec::encode(req);
            if (sock.send_all(encoded.data(), encoded.size()) != SendResult::Success) {
                std::cerr << "Failed to send produce-oversized frame\n";
                return 1;
            }

            Frame resp;
            if (!read_response_frame(sock, resp)) return 1;
            uint16_t err_code = 0;
            std::string err_msg;
            if (resp.type != MessageType::MSG_ERROR ||
                !FrameCodec::parse_error_frame(resp, err_code, err_msg) ||
                err_code != ErrorCode::RECORD_TOO_LARGE) {
                std::cerr << "PRODUCE-OVERSIZED test failed. Expected error code 9, got type=0x"
                          << static_cast<int>(resp.type) << " code=" << err_code << "\n";
                return 1;
            }
            std::cout << "PRODUCE-OVERSIZED rejected with ERROR code 9 as expected (" << err_msg << ")\n";
            return 0;

        } else if (command == "unknown-type") {
            Frame req{ HEADER_SIZE, 0x7E, req_id++, {} };
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
            Frame ping_req{ HEADER_SIZE, MessageType::PING, req_id++, {} };
            std::vector<uint8_t> ping_data = FrameCodec::encode(ping_req);
            if (sock.send_all(ping_data.data(), ping_data.size()) != SendResult::Success) {
                std::cerr << "Failed to send follow-up PING on open connection\n";
                return 1;
            }
            Frame ping_resp;
            if (!read_response_frame(sock, ping_resp)) return 1;
            if (ping_resp.type != MessageType::PONG) {
                std::cerr << "Follow-up PING failed after unknown type error\n";
                return 1;
            }
            std::cout << "UNKNOWN-TYPE received ERROR code 1, subsequent PING succeeded\n";
            return 0;

        } else if (command == "create-topic") {
            if (args.empty()) {
                std::cerr << "Usage: streamforge_cli create-topic TOPIC [PARTITIONS]\n";
                return 1;
            }
            std::string topic_name = args[0];
            uint16_t num_partitions = (args.size() > 1) ? static_cast<uint16_t>(std::atoi(args[1].c_str())) : 1;

            CreateTopicRequest req_msg{ topic_name, num_partitions };
            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::CREATE_TOPIC, req_id++, body };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;

            std::cout << "Successfully created topic '" << topic_name << "' with " << num_partitions << " partition(s)\n";
            return 0;

        } else if (command == "produce") {
            if (args.empty()) {
                std::cerr << "Usage: streamforge_cli produce TOPIC [VALUE] [--key KEY] [--partition P]\n";
                return 1;
            }
            std::string topic_name = args[0];
            std::string value_str = (args.size() > 1) ? args[1] : "";

            ProduceRequest req_msg;
            req_msg.topic = topic_name;
            req_msg.partition = partition_arg;
            req_msg.record_count = 1;

            std::vector<uint8_t> k_bytes(key_arg.begin(), key_arg.end());
            std::vector<uint8_t> v_bytes(value_str.begin(), value_str.end());
            req_msg.records.push_back({ k_bytes, v_bytes });

            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::PRODUCE, req_id++, body };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;

            BodyReader reader(resp.body);
            ProduceResponse resp_msg;
            if (!resp_msg.decode(reader)) {
                std::cerr << "Failed to decode PRODUCE_OK response\n";
                return 1;
            }

            std::cout << "Produced 1 record to topic '" << topic_name << "' partition " << resp_msg.partition
                      << " base_offset=" << resp_msg.base_offset << "\n";
            return 0;

        } else if (command == "produce-many" || command == "produce-batch") {
            if (args.empty()) {
                std::cerr << "Usage: streamforge_cli produce-many TOPIC [COUNT] [--size BYTES] [--batch N] [--key-prefix P] [--partition P]\n";
                return 1;
            }
            std::string topic_name = args[0];
            size_t total_count = (args.size() > 1) ? static_cast<size_t>(std::stoul(args[1])) : size_arg;
            size_t batch_sz = (batch_arg > 0) ? batch_arg : 100;
            size_t val_bytes = (size_arg > 0) ? size_arg : 64;

            auto start_time = std::chrono::high_resolution_clock::now();

            size_t produced = 0;
            while (produced < total_count) {
                size_t current_batch_size = std::min(batch_sz, total_count - produced);

                ProduceRequest req_msg;
                req_msg.topic = topic_name;
                req_msg.partition = partition_arg;
                req_msg.record_count = static_cast<uint16_t>(current_batch_size);

                std::vector<uint8_t> val_payload(val_bytes, 'X');

                for (size_t i = 0; i < current_batch_size; ++i) {
                    ProduceRecordPayload rec;
                    if (!key_prefix_arg.empty()) {
                        std::string k = key_prefix_arg + std::to_string(produced + i);
                        rec.key.assign(k.begin(), k.end());
                    } else if (!key_arg.empty()) {
                        std::string k = key_arg + "_" + std::to_string(produced + i);
                        rec.key.assign(k.begin(), k.end());
                    }
                    rec.value = val_payload;
                    req_msg.records.push_back(std::move(rec));
                }

                BodyWriter writer;
                req_msg.encode(writer);
                std::vector<uint8_t> body = writer.take_buffer();

                Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::PRODUCE, req_id++, body };
                Frame resp;
                if (!send_and_receive(sock, req, resp)) return 1;

                produced += current_batch_size;
            }

            auto end_time = std::chrono::high_resolution_clock::now();
            double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
            double throughput = elapsed_sec > 0.0 ? (static_cast<double>(total_count) / elapsed_sec) : total_count;

            std::cout << "Produced " << total_count << " records to '" << topic_name << "' in batches of "
                      << batch_sz << " (elapsed: " << elapsed_sec << "s, throughput: "
                      << static_cast<uint64_t>(throughput) << " records/sec)\n";
            return 0;

        } else if (command == "fetch") {
            if (args.size() < 3) {
                std::cerr << "Usage: streamforge_cli fetch TOPIC PARTITION OFFSET [--max-messages M] [--max-bytes B]\n";
                return 1;
            }
            std::string topic_name = args[0];
            uint16_t partition = static_cast<uint16_t>(std::atoi(args[1].c_str()));
            uint64_t offset = std::stoull(args[2]);

            FetchRequest req_msg{ topic_name, partition, offset, max_bytes_arg, max_messages_arg };
            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::FETCH, req_id++, body };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;

            BodyReader reader(resp.body);
            FetchResponse resp_msg;
            if (!resp_msg.decode(reader)) {
                std::cerr << "Failed to decode FETCH_OK response\n";
                return 1;
            }

            std::cout << "Fetched " << resp_msg.records.size() << " records from '" << topic_name
                      << "' P" << partition << " (next_offset=" << resp_msg.next_offset
                      << ", high_watermark=" << resp_msg.high_watermark
                      << ", earliest=" << resp_msg.earliest_offset << "):\n";

            for (const auto& rec : resp_msg.records) {
                std::string k(rec.key.begin(), rec.key.end());
                std::string v(rec.value.begin(), rec.value.end());
                std::cout << "  [offset " << rec.offset << "] ts=" << rec.timestamp_ms
                          << " key=" << (k.empty() ? "<none>" : k)
                          << " val=" << (v.size() > 50 ? (v.substr(0, 50) + "...") : v) << "\n";
            }
            return 0;

        } else if (command == "consume") {
            if (args.size() < 2) {
                std::cerr << "Usage: streamforge_cli consume TOPIC PARTITION [--from OFFSET] [--follow]\n";
                return 1;
            }
            std::string topic_name = args[0];
            uint16_t partition = static_cast<uint16_t>(std::atoi(args[1].c_str()));
            uint64_t cur_offset = from_offset_arg;

            std::cout << "Consuming from topic '" << topic_name << "' P" << partition
                      << " starting at offset " << cur_offset << " (follow=" << (follow_arg ? "true" : "false") << ")...\n";

            while (true) {
                FetchRequest req_msg{ topic_name, partition, cur_offset, max_bytes_arg, max_messages_arg };
                BodyWriter writer;
                req_msg.encode(writer);
                std::vector<uint8_t> body = writer.take_buffer();

                Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::FETCH, req_id++, body };
                Frame resp;
                if (!send_and_receive(sock, req, resp)) {
                    break;
                }

                BodyReader reader(resp.body);
                FetchResponse resp_msg;
                if (!resp_msg.decode(reader)) {
                    std::cerr << "Failed to decode FETCH_OK response\n";
                    return 1;
                }

                for (const auto& rec : resp_msg.records) {
                    std::string k(rec.key.begin(), rec.key.end());
                    std::string v(rec.value.begin(), rec.value.end());
                    std::cout << "[" << rec.offset << "] key=" << (k.empty() ? "<none>" : k)
                              << " val=" << v << "\n";
                    std::cout.flush();
                }

                if (!resp_msg.records.empty()) {
                    cur_offset = resp_msg.next_offset;
                } else {
                    if (follow_arg) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(200));
                    } else {
                        break;
                    }
                }
            }
            return 0;

        } else if (command == "topics") {
            ListTopicsRequest req_msg;
            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::LIST_TOPICS, req_id++, body };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;

            BodyReader reader(resp.body);
            ListTopicsResponse resp_msg;
            if (!resp_msg.decode(reader)) {
                std::cerr << "Failed to decode TOPICS response\n";
                return 1;
            }

            std::cout << "Topics (" << resp_msg.topics.size() << "):\n";
            for (const auto& t : resp_msg.topics) {
                std::cout << "  - " << t.name << " (" << t.partitions << " partitions)\n";
            }
            return 0;

        } else if (command == "describe") {
            if (args.empty()) {
                std::cerr << "Usage: streamforge_cli describe TOPIC\n";
                return 1;
            }
            std::string topic_name = args[0];

            DescribeTopicRequest req_msg{ topic_name };
            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::DESCRIBE_TOPIC, req_id++, body };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;

            BodyReader reader(resp.body);
            DescribeTopicResponse resp_msg;
            if (!resp_msg.decode(reader)) {
                std::cerr << "Failed to decode TOPIC_DESCRIPTION response\n";
                return 1;
            }

            std::cout << "Topic '" << resp_msg.name << "' (" << resp_msg.partitions << " partitions):\n";
            for (uint16_t p = 0; p < resp_msg.partitions; ++p) {
                std::cout << "  Partition " << p << ": earliest=" << resp_msg.partition_offsets[p].earliest
                          << ", next_offset=" << resp_msg.partition_offsets[p].next_offset << "\n";
            }
            return 0;

        // ─────────────────────────────────────────────────────────────────────
        // M5 Commands
        // ─────────────────────────────────────────────────────────────────────
        } else if (command == "join-group") {
            if (args.size() < 2) {
                std::cerr << "Usage: streamforge_cli join-group GROUP TOPIC[,TOPIC2] [--member ID] [--session-timeout-ms N] [--strategy range|roundrobin]\n";
                return 1;
            }
            std::string group_id = args[0];
            std::string topics_raw = args[1];

            std::vector<std::string> topics;
            std::stringstream ss(topics_raw);
            std::string item;
            while (std::getline(ss, item, ',')) {
                if (!item.empty()) topics.push_back(item);
            }

            JoinGroupRequest req_msg;
            req_msg.group_id = group_id;
            req_msg.member_id = member_arg;
            req_msg.session_timeout_ms = session_timeout_ms_arg;
            req_msg.strategy = (strategy_arg == "roundrobin" ? 1 : 0);
            req_msg.topic_count = static_cast<uint16_t>(topics.size());
            req_msg.topics = topics;

            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::JOIN_GROUP, req_id++, body };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;

            BodyReader reader(resp.body);
            JoinGroupResponse resp_msg;
            if (!resp_msg.decode(reader)) {
                std::cerr << "Failed to decode JOIN_GROUP_OK response\n";
                return 1;
            }

            std::cout << "Joined group '" << group_id << "' successfully:\n";
            std::cout << "  Member ID: " << resp_msg.member_id << "\n";
            std::cout << "  Generation: " << resp_msg.generation << "\n";
            std::cout << "  Heartbeat interval: " << resp_msg.heartbeat_interval_ms << " ms\n";
            std::cout << "  Assigned partitions (" << resp_msg.assignments.size() << "):\n";
            for (const auto& a : resp_msg.assignments) {
                std::cout << "    - " << a.topic << ":" << a.partition << "\n";
            }
            return 0;

        } else if (command == "heartbeat") {
            if (args.size() < 3) {
                std::cerr << "Usage: streamforge_cli heartbeat GROUP MEMBER GENERATION\n";
                return 1;
            }
            std::string group_id = args[0];
            std::string member_id = args[1];
            uint32_t gen = static_cast<uint32_t>(std::stoul(args[2]));

            HeartbeatRequest req_msg{ group_id, member_id, gen };
            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::HEARTBEAT, req_id++, body };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;

            std::cout << "Heartbeat OK\n";
            return 0;

        } else if (command == "leave-group") {
            if (args.size() < 2) {
                std::cerr << "Usage: streamforge_cli leave-group GROUP MEMBER\n";
                return 1;
            }
            std::string group_id = args[0];
            std::string member_id = args[1];

            LeaveGroupRequest req_msg{ group_id, member_id };
            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::LEAVE_GROUP, req_id++, body };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;

            std::cout << "Leave group OK\n";
            return 0;

        } else if (command == "commit-offset") {
            if (args.size() < 4) {
                std::cerr << "Usage: streamforge_cli commit-offset GROUP TOPIC PARTITION OFFSET [--member ID] [--generation N]\n";
                return 1;
            }
            std::string group_id = args[0];
            std::string topic_name = args[1];
            uint16_t partition = static_cast<uint16_t>(std::stoul(args[2]));
            uint64_t offset = std::stoull(args[3]);

            CommitOffsetRequest req_msg;
            req_msg.group_id = group_id;
            req_msg.member_id = member_arg;
            req_msg.generation = generation_arg;
            req_msg.count = 1;
            req_msg.entries.push_back({ topic_name, partition, offset });

            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::COMMIT_OFFSET, req_id++, body };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;

            std::cout << "Commit offset OK\n";
            return 0;

        } else if (command == "fetch-offsets") {
            if (args.size() < 2) {
                std::cerr << "Usage: streamforge_cli fetch-offsets GROUP TOPIC [--partitions 0,1,2]\n";
                return 1;
            }
            std::string group_id = args[0];
            std::string topic_name = args[1];

            std::vector<uint16_t> part_list;
            if (!partitions_arg.empty()) {
                std::stringstream ss(partitions_arg);
                std::string item;
                while (std::getline(ss, item, ',')) {
                    if (!item.empty()) part_list.push_back(static_cast<uint16_t>(std::stoul(item)));
                }
            } else {
                // Discover partition count via describe topic
                DescribeTopicRequest dt_req{ topic_name };
                BodyWriter dtw;
                dt_req.encode(dtw);
                std::vector<uint8_t> dt_body = dtw.take_buffer();

                Frame dt_frame{ HEADER_SIZE + static_cast<uint32_t>(dt_body.size()), MessageType::DESCRIBE_TOPIC, req_id++, dt_body };
                Frame dt_resp;
                if (!send_and_receive(sock, dt_frame, dt_resp)) return 1;

                BodyReader dtr(dt_resp.body);
                DescribeTopicResponse dt_msg;
                if (!dt_msg.decode(dtr)) return 1;

                for (uint16_t p = 0; p < dt_msg.partitions; ++p) {
                    part_list.push_back(p);
                }
            }

            FetchOffsetRequest req_msg;
            req_msg.group_id = group_id;
            req_msg.count = static_cast<uint16_t>(part_list.size());
            for (uint16_t p : part_list) {
                req_msg.queries.push_back({ topic_name, p });
            }

            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::FETCH_OFFSET, req_id++, body };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;

            BodyReader reader(resp.body);
            FetchOffsetResponse resp_msg;
            if (!resp_msg.decode(reader)) {
                std::cerr << "Failed to decode OFFSETS response\n";
                return 1;
            }

            std::cout << "Committed offsets for group '" << group_id << "' topic '" << topic_name << "':\n";
            for (const auto& o : resp_msg.offsets) {
                std::cout << "  Partition " << o.partition << ": " << o.offset << "\n";
            }
            return 0;

        } else if (command == "describe-group") {
            if (args.empty()) {
                std::cerr << "Usage: streamforge_cli describe-group GROUP\n";
                return 1;
            }
            std::string group_id = args[0];

            DescribeGroupRequest req_msg{ group_id };
            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::DESCRIBE_GROUP, req_id++, body };
            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;

            BodyReader reader(resp.body);
            DescribeGroupResponse resp_msg;
            if (!resp_msg.decode(reader)) {
                std::cerr << "Failed to decode GROUP_DESCRIPTION response\n";
                return 1;
            }

            std::cout << "Group: " << resp_msg.group_id << "\n";
            std::cout << "Generation: " << resp_msg.generation << "\n";
            std::cout << "State: " << resp_msg.state << "\n";
            std::cout << "Strategy: " << (resp_msg.strategy == 0 ? "range" : "roundrobin") << "\n";
            std::cout << "Members (" << resp_msg.members.size() << "):\n";
            for (const auto& m : resp_msg.members) {
                std::cout << "  Member ID: " << m.member_id << "\n";
                std::cout << "    Session timeout: " << m.session_timeout_ms << " ms\n";
                std::cout << "    Time since last heartbeat: " << m.ms_since_heartbeat << " ms\n";
                std::cout << "    Assigned partitions (" << m.assignment.size() << "):\n";
                for (const auto& a : m.assignment) {
                    std::cout << "      - " << a.topic << ":" << a.partition << "\n";
                }
            }
            return 0;

        } else if (command == "group-lag") {
            if (args.size() < 2) {
                std::cerr << "Usage: streamforge_cli group-lag GROUP TOPIC\n";
                return 1;
            }
            std::string group_id = args[0];
            std::string topic_name = args[1];

            // 1. Describe topic to get high watermark for each partition
            DescribeTopicRequest dt_req{ topic_name };
            BodyWriter dtw;
            dt_req.encode(dtw);
            std::vector<uint8_t> dt_body = dtw.take_buffer();

            Frame dt_frame{ HEADER_SIZE + static_cast<uint32_t>(dt_body.size()), MessageType::DESCRIBE_TOPIC, req_id++, dt_body };
            Frame dt_resp;
            if (!send_and_receive(sock, dt_frame, dt_resp)) return 1;

            BodyReader dtr(dt_resp.body);
            DescribeTopicResponse dt_msg;
            if (!dt_msg.decode(dtr)) return 1;

            // 2. Fetch committed offsets for each partition
            FetchOffsetRequest fo_req;
            fo_req.group_id = group_id;
            fo_req.count = dt_msg.partitions;
            for (uint16_t p = 0; p < dt_msg.partitions; ++p) {
                fo_req.queries.push_back({ topic_name, p });
            }

            BodyWriter fow;
            fo_req.encode(fow);
            std::vector<uint8_t> fo_body = fow.take_buffer();

            Frame fo_frame{ HEADER_SIZE + static_cast<uint32_t>(fo_body.size()), MessageType::FETCH_OFFSET, req_id++, fo_body };
            Frame fo_resp;
            if (!send_and_receive(sock, fo_frame, fo_resp)) return 1;

            BodyReader for_reader(fo_resp.body);
            FetchOffsetResponse fo_msg;
            if (!fo_msg.decode(for_reader)) return 1;

            std::map<uint16_t, int64_t> committed_map;
            for (const auto& o : fo_msg.offsets) {
                committed_map[o.partition] = o.offset;
            }

            uint64_t total_lag = 0;
            std::cout << "Group '" << group_id << "' lag on topic '" << topic_name << "':\n";
            for (uint16_t p = 0; p < dt_msg.partitions; ++p) {
                uint64_t hw = dt_msg.partition_offsets[p].next_offset;
                int64_t c = (committed_map.count(p) ? committed_map[p] : -1);
                uint64_t lag = (c < 0 ? hw : (hw >= static_cast<uint64_t>(c) ? (hw - static_cast<uint64_t>(c)) : 0));
                total_lag += lag;
                std::cout << "  Partition " << p << ": committed=" << c
                          << ", high_watermark=" << hw
                          << ", lag=" << lag << "\n";
            }
            std::cout << "Total lag: " << total_lag << "\n";
            return 0;

        } else if (command == "consume-group") {
            if (args.size() < 2) {
                std::cerr << "Usage: streamforge_cli consume-group GROUP TOPIC [options...]\n";
                return 1;
            }
            std::string group_id = args[0];
            std::string topic_name = args[1];

            if (!SetConsoleCtrlHandler(cli_ctrl_handler, TRUE)) {
                std::cerr << "Failed to set console control handler\n";
            }

            std::string member_id = "";
            uint32_t generation = 0;
            uint32_t heartbeat_interval_ms = session_timeout_ms_arg / 3;
            std::vector<TopicPartitionWire> assignments;
            std::map<uint16_t, uint64_t> partition_positions;
            std::map<uint16_t, uint64_t> uncommitted_offsets;
            size_t records_since_commit = 0;
            size_t total_records_processed = 0;
            auto last_heartbeat_time = std::chrono::steady_clock::now();
            auto last_record_time = std::chrono::steady_clock::now();

            auto do_join = [&](bool is_rebalance) -> bool {
                JoinGroupRequest join_req;
                join_req.group_id = group_id;
                join_req.member_id = member_id;
                join_req.session_timeout_ms = session_timeout_ms_arg;
                join_req.strategy = (strategy_arg == "roundrobin" ? 1 : 0);
                join_req.topic_count = 1;
                join_req.topics = { topic_name };

                BodyWriter w;
                join_req.encode(w);
                std::vector<uint8_t> body = w.take_buffer();
                Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::JOIN_GROUP, req_id++, body };
                Frame resp;

                if (!send_and_receive(sock, req, resp)) {
                    // Check if error is 14 (unknown group or member); if so, retry with empty member_id
                    if (resp.type == MessageType::MSG_ERROR) {
                        uint16_t code = 0; std::string msg;
                        FrameCodec::parse_error_frame(resp, code, msg);
                        if (code == ErrorCode::UNKNOWN_GROUP_OR_MEMBER) {
                            member_id.clear();
                            join_req.member_id.clear();
                            BodyWriter w2;
                            join_req.encode(w2);
                            body = w2.take_buffer();
                            req.body = body;
                            req.length = HEADER_SIZE + static_cast<uint32_t>(body.size());
                            req.request_id = req_id++;
                            if (!send_and_receive(sock, req, resp)) return false;
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }

                BodyReader r(resp.body);
                JoinGroupResponse join_resp;
                if (!join_resp.decode(r)) return false;

                member_id = join_resp.member_id;
                generation = join_resp.generation;
                heartbeat_interval_ms = join_resp.heartbeat_interval_ms;
                assignments = join_resp.assignments;

                std::string assign_str;
                for (size_t i = 0; i < assignments.size(); ++i) {
                    if (i > 0) assign_str += ",";
                    assign_str += assignments[i].topic + ":" + std::to_string(assignments[i].partition);
                }

                if (!is_rebalance) {
                    std::cerr << "EVENT joined member=" << member_id << " generation=" << generation
                              << " assignment=" << assign_str << "\n";
                } else {
                    std::cerr << "EVENT rebalance generation=" << generation
                              << " assignment=" << assign_str << "\n";
                }

                partition_positions.clear();
                uncommitted_offsets.clear();
                records_since_commit = 0;

                if (!assignments.empty()) {
                    FetchOffsetRequest fo_req;
                    fo_req.group_id = group_id;
                    fo_req.count = static_cast<uint16_t>(assignments.size());
                    for (const auto& a : assignments) {
                        fo_req.queries.push_back({ a.topic, a.partition });
                    }

                    BodyWriter fow;
                    fo_req.encode(fow);
                    std::vector<uint8_t> fo_body = fow.take_buffer();
                    Frame fo_frame{ HEADER_SIZE + static_cast<uint32_t>(fo_body.size()), MessageType::FETCH_OFFSET, req_id++, fo_body };
                    Frame fo_resp;
                    if (!send_and_receive(sock, fo_frame, fo_resp)) return false;

                    BodyReader for_reader(fo_resp.body);
                    FetchOffsetResponse fo_resp_msg;
                    if (!fo_resp_msg.decode(for_reader)) return false;

                    for (const auto& off_item : fo_resp_msg.offsets) {
                        if (off_item.offset >= 0) {
                            partition_positions[off_item.partition] = static_cast<uint64_t>(off_item.offset);
                        } else {
                            if (reset_arg == "latest") {
                                DescribeTopicRequest dt_req{ topic_name };
                                BodyWriter dtw;
                                dt_req.encode(dtw);
                                std::vector<uint8_t> dt_body = dtw.take_buffer();
                                Frame dt_frame{ HEADER_SIZE + static_cast<uint32_t>(dt_body.size()), MessageType::DESCRIBE_TOPIC, req_id++, dt_body };
                                Frame dt_resp;
                                if (send_and_receive(sock, dt_frame, dt_resp)) {
                                    BodyReader dtr(dt_resp.body);
                                    DescribeTopicResponse dt_msg;
                                    if (dt_msg.decode(dtr) && off_item.partition < dt_msg.partition_offsets.size()) {
                                        partition_positions[off_item.partition] = dt_msg.partition_offsets[off_item.partition].next_offset;
                                    } else {
                                        partition_positions[off_item.partition] = 0;
                                    }
                                } else {
                                    partition_positions[off_item.partition] = 0;
                                }
                            } else {
                                partition_positions[off_item.partition] = 0;
                            }
                        }
                    }
                }

                last_heartbeat_time = std::chrono::steady_clock::now();
                last_record_time = std::chrono::steady_clock::now();
                return true;
            };

            auto do_commit = [&]() -> int {
                if (uncommitted_offsets.empty()) return 0;

                CommitOffsetRequest co_req;
                co_req.group_id = group_id;
                co_req.member_id = member_id;
                co_req.generation = generation;
                co_req.count = static_cast<uint16_t>(uncommitted_offsets.size());
                for (const auto& pair : uncommitted_offsets) {
                    co_req.entries.push_back({ topic_name, pair.first, pair.second });
                }

                BodyWriter cow;
                co_req.encode(cow);
                std::vector<uint8_t> co_body = cow.take_buffer();
                Frame co_frame{ HEADER_SIZE + static_cast<uint32_t>(co_body.size()), MessageType::COMMIT_OFFSET, req_id++, co_body };
                Frame co_resp;

                std::vector<uint8_t> encoded = FrameCodec::encode(co_frame);
                if (sock.send_all(encoded.data(), encoded.size()) != SendResult::Success) return -1;
                if (!read_response_frame(sock, co_resp)) return -1;

                if (co_resp.type == MessageType::MSG_ERROR) {
                    uint16_t code = 0; std::string msg;
                    FrameCodec::parse_error_frame(co_resp, code, msg);
                    if (code == ErrorCode::ILLEGAL_GENERATION || code == ErrorCode::UNKNOWN_GROUP_OR_MEMBER) {
                        return 15;
                    }
                    return -1;
                }

                std::cerr << "EVENT committed count=" << records_since_commit << "\n";
                records_since_commit = 0;
                uncommitted_offsets.clear();
                return 0;
            };

            auto do_heartbeat = [&]() -> int {
                HeartbeatRequest hb_req{ group_id, member_id, generation };
                BodyWriter hbw;
                hb_req.encode(hbw);
                std::vector<uint8_t> hb_body = hbw.take_buffer();
                Frame hb_frame{ HEADER_SIZE + static_cast<uint32_t>(hb_body.size()), MessageType::HEARTBEAT, req_id++, hb_body };
                Frame hb_resp;

                std::vector<uint8_t> encoded = FrameCodec::encode(hb_frame);
                if (sock.send_all(encoded.data(), encoded.size()) != SendResult::Success) return -1;
                if (!read_response_frame(sock, hb_resp)) return -1;

                if (hb_resp.type == MessageType::MSG_ERROR) {
                    uint16_t code = 0; std::string msg;
                    FrameCodec::parse_error_frame(hb_resp, code, msg);
                    if (code == ErrorCode::ILLEGAL_GENERATION || code == ErrorCode::UNKNOWN_GROUP_OR_MEMBER) {
                        return 15;
                    }
                    return -1;
                }
                last_heartbeat_time = std::chrono::steady_clock::now();
                return 0;
            };

            if (!do_join(false)) {
                return 1;
            }

            while (!g_stop_requested.load()) {
                if (max_records_arg > 0 && total_records_processed >= max_records_arg) {
                    break;
                }

                auto now = std::chrono::steady_clock::now();
                if (idle_exit_ms_arg > 0 && total_records_processed > 0) {
                    auto idle_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_record_time).count();
                    if (static_cast<uint64_t>(idle_ms) >= idle_exit_ms_arg) {
                        break;
                    }
                }

                // Heartbeat check
                auto hb_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat_time).count();
                if (static_cast<uint32_t>(hb_elapsed) >= heartbeat_interval_ms) {
                    int hb_res = do_heartbeat();
                    if (hb_res == 15) {
                        if (!do_join(true)) break;
                        continue;
                    } else if (hb_res < 0) {
                        break;
                    }
                }

                if (assignments.empty()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    continue;
                }

                bool any_records_in_round = false;
                for (const auto& a : assignments) {
                    if (g_stop_requested.load()) break;

                    uint64_t cur_pos = partition_positions[a.partition];
                    FetchRequest req_msg{ a.topic, a.partition, cur_pos, max_bytes_arg, max_messages_per_fetch_arg };
                    BodyWriter writer;
                    req_msg.encode(writer);
                    std::vector<uint8_t> body = writer.take_buffer();

                    Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::FETCH, req_id++, body };
                    Frame resp;

                    std::vector<uint8_t> enc = FrameCodec::encode(req);
                    if (sock.send_all(enc.data(), enc.size()) != SendResult::Success) break;
                    if (!read_response_frame(sock, resp)) break;

                    if (resp.type == MessageType::MSG_ERROR) {
                        uint16_t code = 0; std::string msg;
                        FrameCodec::parse_error_frame(resp, code, msg);
                        if (code == ErrorCode::ILLEGAL_GENERATION || code == ErrorCode::UNKNOWN_GROUP_OR_MEMBER) {
                            do_join(true);
                            break;
                        }
                    }

                    if (resp.type == MessageType::FETCH_OK) {
                        BodyReader reader(resp.body);
                        FetchResponse resp_msg;
                        if (!resp_msg.decode(reader)) break;

                        if (!resp_msg.records.empty()) {
                            any_records_in_round = true;
                            last_record_time = std::chrono::steady_clock::now();

                            for (const auto& rec : resp_msg.records) {
                                std::cout << "RECORD topic=" << a.topic
                                          << " partition=" << a.partition
                                          << " offset=" << rec.offset
                                          << " key=" << escape_bytes(rec.key)
                                          << " value=" << escape_bytes(rec.value) << "\n";
                                std::cout.flush();

                                total_records_processed++;
                                records_since_commit++;
                                uncommitted_offsets[a.partition] = rec.offset + 1;
                                partition_positions[a.partition] = rec.offset + 1;

                                if (crash_after_arg > 0 && total_records_processed >= crash_after_arg) {
                                    // Immediate crash simulation: NO commit, NO leave!
                                    std::_Exit(0);
                                }

                                if (max_records_arg > 0 && total_records_processed >= max_records_arg) {
                                    break;
                                }

                                if (process_delay_ms_arg > 0) {
                                    std::this_thread::sleep_for(std::chrono::milliseconds(process_delay_ms_arg));
                                }
                            }

                            if (records_since_commit >= commit_every_arg) {
                                int cr = do_commit();
                                if (cr == 15) {
                                    do_join(true);
                                    break;
                                }
                            }
                        }
                    }

                    if (max_records_arg > 0 && total_records_processed >= max_records_arg) {
                        break;
                    }
                }

                if (!any_records_in_round) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                }
            }

            // Graceful exit
            do_commit();

            if (!member_id.empty()) {
                LeaveGroupRequest lg_req{ group_id, member_id };
                BodyWriter lgw;
                lg_req.encode(lgw);
                std::vector<uint8_t> lg_body = lgw.take_buffer();
                Frame lg_frame{ HEADER_SIZE + static_cast<uint32_t>(lg_body.size()), MessageType::LEAVE_GROUP, req_id++, lg_body };
                Frame lg_resp;
                send_and_receive(sock, lg_frame, lg_resp);
                std::cerr << "EVENT left\n";
            }

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
