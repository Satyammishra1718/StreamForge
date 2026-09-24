#include "TestFramework.hpp"
#include "streamforge/BodyReaderWriter.hpp"
#include "streamforge/ProtocolMessages.hpp"
#include "streamforge/MessageHandler.hpp"
#include "streamforge/TopicManager.hpp"
#include <filesystem>
#include <vector>
#include <string>

using namespace streamforge;

static std::filesystem::path create_temp_dir(const std::string& name) {
    std::filesystem::path p = std::filesystem::current_path() / ("tmp_proto_" + name);
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
    std::filesystem::create_directories(p);
    return p;
}

static void cleanup_temp_dir(const std::filesystem::path& p) {
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. BodyReader / BodyWriter Roundtrip for all message types
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(body_reader_writer_primitives) {
    BodyWriter writer;
    writer.write_u8(0x42);
    writer.write_u16(12345);
    writer.write_u32(3000000000u);
    writer.write_i32(-99999);
    writer.write_u64(9000000000000000000ull);
    writer.write_i64(-123456789012345ll);
    writer.write_string("hello streamforge");
    writer.write_bytes({'B', 'Y', 'T', 'E', 'S'});

    BodyReader reader(writer.buffer());
    uint8_t u8_val = 0;
    uint16_t u16_val = 0;
    uint32_t u32_val = 0;
    int32_t i32_val = 0;
    uint64_t u64_val = 0;
    int64_t i64_val = 0;
    std::string str_val;
    std::vector<uint8_t> bytes_val;

    CHECK_TRUE(reader.read_u8(u8_val));
    CHECK_EQ(u8_val, 0x42);

    CHECK_TRUE(reader.read_u16(u16_val));
    CHECK_EQ(u16_val, 12345u);

    CHECK_TRUE(reader.read_u32(u32_val));
    CHECK_EQ(u32_val, 3000000000u);

    CHECK_TRUE(reader.read_i32(i32_val));
    CHECK_EQ(i32_val, -99999);

    CHECK_TRUE(reader.read_u64(u64_val));
    CHECK_EQ(u64_val, 9000000000000000000ull);

    CHECK_TRUE(reader.read_i64(i64_val));
    CHECK_EQ(i64_val, -123456789012345ll);

    CHECK_TRUE(reader.read_string(str_val));
    CHECK_EQ(str_val, std::string("hello streamforge"));

    CHECK_TRUE(reader.read_bytes(bytes_val));
    CHECK_EQ(bytes_val.size(), 5u);
    CHECK_EQ(bytes_val[0], 'B');

    CHECK_TRUE(reader.require_empty());
}

TEST_CASE(message_structs_roundtrip) {
    // CreateTopicRequest
    {
        CreateTopicRequest req{"orders", 3};
        BodyWriter w;
        req.encode(w);
        BodyReader r(w.buffer());
        CreateTopicRequest dec;
        CHECK_TRUE(dec.decode(r));
        CHECK_EQ(dec.topic, "orders");
        CHECK_EQ(dec.partitions, 3u);
    }

    // ProduceRequest
    {
        ProduceRequest req;
        req.topic = "sensor_data";
        req.partition = 1;
        req.record_count = 2;
        req.records.push_back({ {'k', '1'}, {'v', '1'} });
        req.records.push_back({ {}, {'v', '2'} });

        BodyWriter w;
        req.encode(w);
        BodyReader r(w.buffer());
        ProduceRequest dec;
        CHECK_TRUE(dec.decode(r));
        CHECK_EQ(dec.topic, "sensor_data");
        CHECK_EQ(dec.partition, 1);
        CHECK_EQ(dec.record_count, 2u);
        CHECK_EQ(dec.records.size(), 2u);
        CHECK_EQ(dec.records[0].key.size(), 2u);
        CHECK_EQ(dec.records[1].key.size(), 0u);
    }

    // FetchRequest
    {
        FetchRequest req{"events", 0, 100, 65536, 10};
        BodyWriter w;
        req.encode(w);
        BodyReader r(w.buffer());
        FetchRequest dec;
        CHECK_TRUE(dec.decode(r));
        CHECK_EQ(dec.topic, "events");
        CHECK_EQ(dec.partition, 0u);
        CHECK_EQ(dec.start_offset, 100ull);
        CHECK_EQ(dec.max_bytes, 65536u);
        CHECK_EQ(dec.max_messages, 10u);
    }

    // DescribeTopicResponse
    {
        DescribeTopicResponse resp;
        resp.name = "logs";
        resp.partitions = 2;
        resp.partition_offsets.push_back({0, 150});
        resp.partition_offsets.push_back({0, 300});

        BodyWriter w;
        resp.encode(w);
        BodyReader r(w.buffer());
        DescribeTopicResponse dec;
        CHECK_TRUE(dec.decode(r));
        CHECK_EQ(dec.name, "logs");
        CHECK_EQ(dec.partitions, 2u);
        CHECK_EQ(dec.partition_offsets[0].next_offset, 150ull);
        CHECK_EQ(dec.partition_offsets[1].next_offset, 300ull);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Truncated bodies, lying length fields, trailing garbage -> Error 10
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(malformed_body_validation_no_crash) {
    auto dir = create_temp_dir("malformed");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false;

        TopicManager topic_mgr(config);
        CHECK(topic_mgr.open_and_recover_all().ok());
        CHECK(topic_mgr.create_topic("test_topic", 2).ok());

        MessageHandler handler(topic_mgr);

        // a) Truncated CREATE_TOPIC body (only topic string, missing partitions u16)
        {
            BodyWriter w;
            w.write_string("test_topic");
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::CREATE_TOPIC, 1, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // a2) Lying string length in CREATE_TOPIC
        {
            BodyWriter w;
            w.write_u16(500);
            w.write_u8('x');
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::CREATE_TOPIC, 11, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // a3) Trailing garbage in CREATE_TOPIC
        {
            BodyWriter w;
            w.write_string("test_topic_new");
            w.write_u16(1);
            w.write_u32(0xDEADBEEF); // trailing garbage
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::CREATE_TOPIC, 12, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // b) Lying length field in PRODUCE (key length declared as 10000 bytes, buffer ends)
        {
            BodyWriter w;
            w.write_string("test_topic");
            w.write_i32(0);
            w.write_u16(1); // 1 record
            w.write_u32(10000); // lying key_len
            w.write_u8('A');   // only 1 byte present!

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::PRODUCE, 2, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // b2) Trailing garbage in PRODUCE
        {
            BodyWriter w;
            w.write_string("test_topic");
            w.write_i32(0);
            w.write_u16(1); // 1 record
            w.write_bytes({'k'});
            w.write_bytes({'v'});
            w.write_u32(0xBAADF00D); // trailing garbage

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::PRODUCE, 21, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // c) Trailing garbage in FETCH (valid request + 4 trailing bytes)
        {
            BodyWriter w;
            w.write_string("test_topic");
            w.write_u16(0);
            w.write_u64(0);
            w.write_u32(1024);
            w.write_u32(10);
            w.write_u32(0xDEADBEEF); // trailing garbage!

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::FETCH, 3, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // c2) Truncated FETCH body
        {
            BodyWriter w;
            w.write_string("test_topic");
            w.write_u16(0);
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::FETCH, 31, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // c3) Lying string length in FETCH
        {
            BodyWriter w;
            w.write_u16(500); // lying topic string length
            w.write_u8('x');
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::FETCH, 32, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // d) Trailing garbage in DESCRIBE_TOPIC
        {
            BodyWriter w;
            w.write_string("test_topic");
            w.write_u8(0xFF); // garbage!

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::DESCRIBE_TOPIC, 4, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // d2) Truncated DESCRIBE_TOPIC (length prefix only)
        {
            BodyWriter w;
            w.write_u16(10);
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::DESCRIBE_TOPIC, 41, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // d3) Lying string length in DESCRIBE_TOPIC
        {
            BodyWriter w;
            w.write_u16(500); // lying topic string length
            w.write_u8('z');
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::DESCRIBE_TOPIC, 42, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // e) Trailing garbage in LIST_TOPICS (non-empty body)
        {
            BodyWriter w;
            w.write_u8(0x01);
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::LIST_TOPICS, 5, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // e2) Truncated / unexpected payload in LIST_TOPICS
        {
            BodyWriter w;
            w.write_u8(0x00);
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::LIST_TOPICS, 51, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // e3) Lying length prefix in LIST_TOPICS (e.g. 2 bytes specifying a length but zero or partial payload)
        {
            BodyWriter w;
            w.write_u16(500);
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::LIST_TOPICS, 52, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }

        // f) Record count mismatch in PRODUCE (declared 2 records, only 1 present)
        {
            BodyWriter w;
            w.write_string("test_topic");
            w.write_i32(0);
            w.write_u16(2);
            w.write_bytes({'k'});
            w.write_bytes({'v'});

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::PRODUCE, 6, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::MALFORMED_BODY);
        }
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Max-size boundaries (record at limit succeeds, over limit fails)
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(max_size_boundary_limits) {
    auto dir = create_temp_dir("max_size");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false;

        TopicManager topic_mgr(config);
        CHECK(topic_mgr.open_and_recover_all().ok());
        CHECK(topic_mgr.create_topic("limit_topic", 1).ok());

        MessageHandler handler(topic_mgr);

        // Max record size is 1 MiB (1,048,576 bytes).
        // Record size = 4 + 24 + key_size + val_size.
        // Record at 1,000,000 bytes payload succeeds.
        {
            ProduceRequest req_msg;
            req_msg.topic = "limit_topic";
            req_msg.partition = 0;
            req_msg.record_count = 1;
            req_msg.records.push_back({ {}, std::vector<uint8_t>(1000000, 'X') });

            BodyWriter w;
            req_msg.encode(w);
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::PRODUCE, 10, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::PRODUCE_OK);
        }

        // Record over limit (1,048,576 payload bytes + header > 1 MiB limit) fails with RECORD_TOO_LARGE (code 9)
        {
            ProduceRequest req_msg;
            req_msg.topic = "limit_topic";
            req_msg.partition = 0;
            req_msg.record_count = 1;
            req_msg.records.push_back({ {}, std::vector<uint8_t>(1048576, 'X') });

            BodyWriter w;
            req_msg.encode(w);
            Frame req{ HEADER_SIZE + static_cast<uint32_t>(w.buffer().size()), MessageType::PRODUCE, 11, w.take_buffer() };
            Frame resp = handler.handle_request(req);
            CHECK_EQ(resp.type, MessageType::MSG_ERROR);
            uint16_t code = 0; std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            CHECK_EQ(code, ErrorCode::RECORD_TOO_LARGE);
        }
    }
    cleanup_temp_dir(dir);
}

int main() {
    ::streamforge::test::TestRegistry::instance().set_suite_title("StreamForge Protocol Unit Tests");
    return ::streamforge::test::TestRegistry::instance().run_all();
}
