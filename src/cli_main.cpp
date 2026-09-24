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
        Socket sock;
        if (!connect_socket(sock, host, port)) {
            return 1;
        }

        uint32_t req_id = 1000;

        if (command == "ping") {
            Frame req;
            req.type = MessageType::PING;
            req.request_id = req_id++;
            req.length = HEADER_SIZE;

            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;
            std::cout << "PONG received (req_id=" << resp.request_id << ")\n";
            return 0;

        } else if (command == "echo") {
            std::string msg = args.empty() ? "hello world" : args[0];
            Frame req;
            req.type = MessageType::ECHO;
            req.request_id = req_id++;
            req.body.assign(msg.begin(), msg.end());
            req.length = HEADER_SIZE + static_cast<uint32_t>(req.body.size());

            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;
            std::string resp_str(resp.body.begin(), resp.body.end());
            std::cout << "ECHO_REPLY received: " << resp_str << "\n";
            return 0;

        } else if (command == "slow-echo") {
            std::string msg = args.empty() ? "hello" : args[0];
            Frame req;
            req.type = MessageType::ECHO;
            req.request_id = req_id++;
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
            std::cout << "SLOW-ECHO success: " << resp_str << "\n";
            return 0;

        } else if (command == "big-frame") {
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
            Frame req;
            req.type = 0x7F;
            req.request_id = req_id++;
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

            Frame ping_req;
            ping_req.type = MessageType::PING;
            ping_req.request_id = req_id++;
            ping_req.length = HEADER_SIZE;

            std::vector<uint8_t> ping_data = FrameCodec::encode(ping_req);
            if (sock.send_all(ping_data.data(), ping_data.size()) != SendResult::Success) {
                std::cerr << "Failed to send follow-up PING\n";
                return 1;
            }

            Frame ping_resp;
            if (!read_response_frame(sock, ping_resp)) return 1;
            std::cout << "UNKNOWN-TYPE received ERROR code 1, subsequent PING succeeded\n";
            return 0;

        } else if (command == "create-topic") {
            if (args.size() < 2) {
                std::cerr << "Usage: streamforge_cli create-topic NAME PARTITIONS\n";
                return 1;
            }
            std::string topic_name = args[0];
            uint16_t partitions = static_cast<uint16_t>(std::atoi(args[1].c_str()));

            CreateTopicRequest req_msg{ topic_name, partitions };
            BodyWriter writer;
            req_msg.encode(writer);

            std::vector<uint8_t> body = writer.take_buffer();
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::CREATE_TOPIC, req_id++, body };

            Frame resp;
            if (!send_and_receive(sock, req, resp)) return 1;

            std::cout << "Successfully created topic '" << topic_name << "' with " << partitions << " partitions.\n";
            return 0;

        } else if (command == "produce") {
            if (args.size() < 2) {
                std::cerr << "Usage: streamforge_cli produce TOPIC VALUE [--key K] [--partition P]\n";
                return 1;
            }
            std::string topic_name = args[0];
            std::string value_str = args[1];

            ProduceRequest req_msg;
            req_msg.topic = topic_name;
            req_msg.partition = partition_arg;
            req_msg.record_count = 1;

            ProduceRecordPayload rec;
            rec.key.assign(key_arg.begin(), key_arg.end());
            rec.value.assign(value_str.begin(), value_str.end());
            req_msg.records.push_back(std::move(rec));

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

        } else if (command == "produce-many") {
            if (args.size() < 2) {
                std::cerr << "Usage: streamforge_cli produce-many TOPIC COUNT [--key-prefix P] [--size BYTES] [--batch N]\n";
                return 1;
            }
            std::string topic_name = args[0];
            size_t total_count = static_cast<size_t>(std::stoul(args[1]));

            auto start_time = std::chrono::high_resolution_clock::now();

            size_t produced = 0;
            while (produced < total_count) {
                size_t current_batch_size = std::min(batch_arg, total_count - produced);

                ProduceRequest req_msg;
                req_msg.topic = topic_name;
                req_msg.partition = partition_arg;
                req_msg.record_count = static_cast<uint16_t>(current_batch_size);

                std::vector<uint8_t> val_payload(size_arg, 'X');

                for (size_t i = 0; i < current_batch_size; ++i) {
                    ProduceRecordPayload rec;
                    if (!key_prefix_arg.empty()) {
                        std::string k = key_prefix_arg + std::to_string(produced + i);
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
            double throughput = elapsed_sec > 0 ? (total_count / elapsed_sec) : total_count;

            std::cout << "Produced " << total_count << " records to '" << topic_name << "' in batches of "
                      << batch_arg << " (elapsed: " << elapsed_sec << "s, throughput: "
                      << static_cast<uint64_t>(throughput) << " records/sec)\n";
            return 0;

        } else if (command == "fetch") {
            if (args.size() < 3) {
                std::cerr << "Usage: streamforge_cli fetch TOPIC PARTITION START_OFFSET [--max-messages N] [--max-bytes N]\n";
                return 1;
            }
            std::string topic_name = args[0];
            uint16_t partition = static_cast<uint16_t>(std::atoi(args[1].c_str()));
            uint64_t start_offset = std::stoull(args[2]);

            FetchRequest req_msg{ topic_name, partition, start_offset, max_bytes_arg, max_messages_arg };
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

        } else {
            std::cerr << "Unknown command: " << command << "\n";
            return 1;
        }

    } catch (const std::exception& ex) {
        std::cerr << "CLI error: " << ex.what() << "\n";
        return 1;
    }
}
