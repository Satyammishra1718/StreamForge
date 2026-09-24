#include "TestFramework.hpp"
#include "streamforge/FrameAssembler.hpp"
#include "streamforge/FrameCodec.hpp"
#include <random>
#include <vector>
#include <cstdint>

using namespace streamforge;
using namespace streamforge::test;

TEST_CASE(byte_at_a_time_feeding) {
    Frame req;
    req.type = MessageType::ECHO;
    req.request_id = 42;
    std::string test_str = "Testing byte-at-a-time streaming assembly!";
    req.body.assign(test_str.begin(), test_str.end());
    req.length = HEADER_SIZE + static_cast<uint32_t>(req.body.size());

    std::vector<uint8_t> encoded = FrameCodec::encode(req);

    FrameAssembler assembler;
    Frame out;

    for (size_t i = 0; i < encoded.size() - 1; ++i) {
        assembler.push_bytes(&encoded[i], 1);
        FrameExtractResult res = assembler.extract_next_frame(out);
        CHECK_EQ(static_cast<int>(res), static_cast<int>(FrameExtractResult::NeedMoreData));
        CHECK(assembler.has_partial_frame());
    }

    // Push the very last byte
    assembler.push_bytes(&encoded.back(), 1);
    FrameExtractResult res = assembler.extract_next_frame(out);
    CHECK_EQ(static_cast<int>(res), static_cast<int>(FrameExtractResult::FrameReady));
    CHECK_EQ(out.type, req.type);
    CHECK_EQ(out.request_id, req.request_id);
    CHECK_EQ(out.length, req.length);
    CHECK(out.body == req.body);
    CHECK(!assembler.has_partial_frame());
}

TEST_CASE(several_frames_in_one_chunk) {
    Frame f1;
    f1.type = MessageType::PING;
    f1.request_id = 101;
    f1.length = HEADER_SIZE;

    Frame f2;
    f2.type = MessageType::ECHO;
    f2.request_id = 102;
    std::string s2 = "Frame Two Payload";
    f2.body.assign(s2.begin(), s2.end());
    f2.length = HEADER_SIZE + static_cast<uint32_t>(f2.body.size());

    Frame f3;
    f3.type = MessageType::LIST_TOPICS;
    f3.request_id = 103;
    f3.length = HEADER_SIZE;

    std::vector<uint8_t> chunk;
    auto e1 = FrameCodec::encode(f1);
    auto e2 = FrameCodec::encode(f2);
    auto e3 = FrameCodec::encode(f3);

    chunk.insert(chunk.end(), e1.begin(), e1.end());
    chunk.insert(chunk.end(), e2.begin(), e2.end());
    chunk.insert(chunk.end(), e3.begin(), e3.end());

    FrameAssembler assembler;
    assembler.push_bytes(chunk.data(), chunk.size());

    Frame out;
    CHECK_EQ(static_cast<int>(assembler.extract_next_frame(out)), static_cast<int>(FrameExtractResult::FrameReady));
    CHECK_EQ(out.type, f1.type);
    CHECK_EQ(out.request_id, f1.request_id);

    CHECK_EQ(static_cast<int>(assembler.extract_next_frame(out)), static_cast<int>(FrameExtractResult::FrameReady));
    CHECK_EQ(out.type, f2.type);
    CHECK_EQ(out.request_id, f2.request_id);
    CHECK(out.body == f2.body);

    CHECK_EQ(static_cast<int>(assembler.extract_next_frame(out)), static_cast<int>(FrameExtractResult::FrameReady));
    CHECK_EQ(out.type, f3.type);
    CHECK_EQ(out.request_id, f3.request_id);

    CHECK_EQ(static_cast<int>(assembler.extract_next_frame(out)), static_cast<int>(FrameExtractResult::NeedMoreData));
    CHECK(!assembler.has_partial_frame());
}

TEST_CASE(random_1000_frames_reassemble_exactly) {
    std::mt19937 rng(1337);
    std::uniform_int_distribution<uint32_t> body_size_dist(0, 2048);
    std::uniform_int_distribution<int> type_dist(0, 3);
    uint8_t sample_types[] = { MessageType::PING, MessageType::ECHO, MessageType::PRODUCE, MessageType::FETCH };

    std::vector<Frame> expected;
    expected.reserve(1000);
    std::vector<uint8_t> total_stream;

    for (uint32_t i = 0; i < 1000; ++i) {
        Frame f;
        f.type = sample_types[type_dist(rng)];
        f.request_id = i + 1;
        uint32_t bsz = body_size_dist(rng);
        f.body.resize(bsz);
        for (uint32_t b = 0; b < bsz; ++b) {
            f.body[b] = static_cast<uint8_t>((i + b) & 0xFF);
        }
        f.length = HEADER_SIZE + bsz;
        expected.push_back(f);

        auto enc = FrameCodec::encode(f);
        total_stream.insert(total_stream.end(), enc.begin(), enc.end());
    }

    // Split stream into random chunks and feed to assembler
    FrameAssembler assembler;
    std::vector<Frame> actual;
    actual.reserve(1000);

    size_t stream_pos = 0;
    std::uniform_int_distribution<size_t> chunk_dist(1, 1024);

    while (stream_pos < total_stream.size()) {
        size_t chunk_len = std::min(chunk_dist(rng), total_stream.size() - stream_pos);
        assembler.push_bytes(total_stream.data() + stream_pos, chunk_len);
        stream_pos += chunk_len;

        Frame out;
        while (assembler.extract_next_frame(out) == FrameExtractResult::FrameReady) {
            actual.push_back(std::move(out));
        }
    }

    CHECK_EQ(actual.size(), expected.size());
    CHECK(!assembler.has_partial_frame());

    for (size_t i = 0; i < expected.size(); ++i) {
        CHECK_EQ(actual[i].type, expected[i].type);
        CHECK_EQ(actual[i].request_id, expected[i].request_id);
        CHECK_EQ(actual[i].length, expected[i].length);
        CHECK(actual[i].body == expected[i].body);
    }
}

TEST_CASE(oversized_and_small_length_errors) {
    // 1. Length too small (length = 4, but HEADER_SIZE is 5)
    {
        FrameAssembler assembler;
        uint8_t bad_len[4];
        FrameCodec::write_u32(bad_len, 4);
        assembler.push_bytes(bad_len, 4);

        Frame out;
        FrameExtractResult res = assembler.extract_next_frame(out);
        CHECK_EQ(static_cast<int>(res), static_cast<int>(FrameExtractResult::ErrorLengthTooSmall));
    }

    // 2. Length too large (length = MAX_FRAME_LENGTH + 1)
    {
        FrameAssembler assembler;
        uint8_t bad_len[4];
        FrameCodec::write_u32(bad_len, MAX_FRAME_LENGTH + 1);
        assembler.push_bytes(bad_len, 4);

        Frame out;
        FrameExtractResult res = assembler.extract_next_frame(out);
        CHECK_EQ(static_cast<int>(res), static_cast<int>(FrameExtractResult::ErrorLengthTooLarge));
    }
}

TEST_CASE(max_size_bodies) {
    Frame f;
    f.type = MessageType::ECHO;
    f.request_id = 9999;
    f.length = MAX_FRAME_LENGTH; // exactly 1 MiB
    uint32_t body_size = MAX_FRAME_LENGTH - HEADER_SIZE;
    f.body.resize(body_size, 'A');

    std::vector<uint8_t> encoded = FrameCodec::encode(f);
    CHECK_EQ(encoded.size(), static_cast<size_t>(4 + MAX_FRAME_LENGTH));

    FrameAssembler assembler;
    // Feed in 64 KiB chunks
    size_t chunk_size = 64 * 1024;
    for (size_t pos = 0; pos < encoded.size(); pos += chunk_size) {
        size_t n = std::min(chunk_size, encoded.size() - pos);
        assembler.push_bytes(encoded.data() + pos, n);
    }

    Frame out;
    FrameExtractResult res = assembler.extract_next_frame(out);
    CHECK_EQ(static_cast<int>(res), static_cast<int>(FrameExtractResult::FrameReady));
    CHECK_EQ(out.length, MAX_FRAME_LENGTH);
    CHECK_EQ(out.body.size(), static_cast<size_t>(body_size));
    CHECK_EQ(out.body.front(), 'A');
    CHECK_EQ(out.body.back(), 'A');
    CHECK(!assembler.has_partial_frame());
}

int main() {
    ::streamforge::test::TestRegistry::instance().set_suite_title("StreamForge Network Unit Tests");
    return ::streamforge::test::TestRegistry::instance().run_all();
}
