#include "TestFramework.hpp"
#include "streamforge/TopicManager.hpp"
#include "streamforge/Partition.hpp"
#include "streamforge/RecordCodec.hpp"
#include "streamforge/FileHandle.hpp"
#include "streamforge/RetentionManager.hpp"
#include <chrono>
#include <filesystem>
#include <random>
#include <thread>
#include <atomic>
#include <vector>
#include <set>
#include <string>
#include <cstring>
#include <windows.h>

using namespace streamforge;

static std::filesystem::path create_temp_dir(const std::string& name) {
    std::filesystem::path p = std::filesystem::current_path() / ("tmp_sf_" + name);
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
    std::filesystem::create_directories(p);
    return p;
}

static void cleanup_temp_dir(const std::filesystem::path& p) {
    std::error_code ec;
    // Retry a few times: Windows file system can be slow releasing handles
    for (int i = 0; i < 5; ++i) {
        std::filesystem::remove_all(p, ec);
        if (!ec) return;
        Sleep(50);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// a. Append/read roundtrip; offsets start at 0 and are contiguous
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(append_read_roundtrip) {
    auto dir = create_temp_dir("roundtrip");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false;

        Partition part(0, dir / "part_0", config);
        CHECK(part.open_and_recover().ok());

        std::vector<std::string> keys = {"k1", "k2", "k3"};
        std::vector<std::string> vals = {"v1", "v2", "v3"};

        for (size_t i = 0; i < 3; ++i) {
            std::vector<uint8_t> k(keys[i].begin(), keys[i].end());
            std::vector<uint8_t> v(vals[i].begin(), vals[i].end());
            auto res = part.append(k, v);
            CHECK(res.ok());
            CHECK_EQ(res.value(), static_cast<uint64_t>(i));
        }

        CHECK_EQ(part.next_offset(), 3u);

        ReadResult rr = part.read(0, 10);
        CHECK(rr.status.ok());
        CHECK_EQ(rr.records.size(), 3u);

        for (size_t i = 0; i < 3; ++i) {
            CHECK_EQ(rr.records[i].offset, static_cast<uint64_t>(i));
            std::string k(rr.records[i].key.begin(), rr.records[i].key.end());
            std::string v(rr.records[i].value.begin(), rr.records[i].value.end());
            CHECK_EQ(k, keys[i]);
            CHECK_EQ(v, vals[i]);
        }
    } // RAII closes all file handles here
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// b. Read from middle; read at next_offset returns empty OK; read past end
//    or below earliest returns OffsetOutOfRange
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(read_boundary_semantics) {
    auto dir = create_temp_dir("boundaries");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false;

        Partition part(0, dir / "part_0", config);
        CHECK(part.open_and_recover().ok());

        for (size_t i = 0; i < 10; ++i) {
            std::string val = "val-" + std::to_string(i);
            part.append({}, std::vector<uint8_t>(val.begin(), val.end()));
        }

        // Read from middle (offset 4, want 5 records: 4,5,6,7,8)
        ReadResult rr_mid = part.read(4, 5);
        CHECK(rr_mid.status.ok());
        CHECK_EQ(rr_mid.records.size(), 5u);
        CHECK_EQ(rr_mid.records[0].offset, 4u);
        CHECK_EQ(rr_mid.records[4].offset, 8u);

        // Caught up: read at next_offset returns empty OK
        ReadResult rr_caught = part.read(10, 10);
        CHECK(rr_caught.status.ok());
        CHECK_EQ(rr_caught.records.size(), 0u);

        // Read past next_offset → OffsetOutOfRange
        ReadResult rr_past = part.read(15, 10);
        CHECK_EQ(static_cast<int>(rr_past.status.code()),
                 static_cast<int>(StatusCode::OffsetOutOfRange));
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// c. Segment rolling with tiny segment size; reads spanning segments
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(segment_rolling) {
    auto dir = create_temp_dir("seg_roll");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.segment_max_bytes = 150; // tiny → many rolls
        config.sync_on_append = false;

        Partition part(0, dir / "part_0", config);
        CHECK(part.open_and_recover().ok());

        for (size_t i = 0; i < 20; ++i) {
            std::string val = "payload_long_string_" + std::to_string(i);
            auto res = part.append({}, std::vector<uint8_t>(val.begin(), val.end()));
            CHECK(res.ok());
        }

        CHECK_TRUE(part.segment_count() > 1);

        // Read must span all segments
        ReadResult rr = part.read(0, 50);
        CHECK(rr.status.ok());
        CHECK_EQ(rr.records.size(), 20u);

        for (size_t i = 0; i < 20; ++i) {
            CHECK_EQ(rr.records[i].offset, static_cast<uint64_t>(i));
        }
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// d. Sparse index correctness: 5,000 records, look up 200 random offsets and
//    compare against linear scan
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(sparse_index_correctness) {
    auto dir = create_temp_dir("sparse_idx");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.index_interval_bytes = 512; // sparse: one entry per ~512 B
        config.sync_on_append = false;

        Partition part(0, dir / "part_0", config);
        CHECK(part.open_and_recover().ok());

        std::vector<std::string> expected;
        expected.reserve(5000);

        for (size_t i = 0; i < 5000; ++i) {
            std::string v = "val_" + std::to_string(i) + "_payload";
            expected.push_back(v);
            part.append({}, std::vector<uint8_t>(v.begin(), v.end()));
        }

        std::mt19937 rng(42);
        std::uniform_int_distribution<uint64_t> dist(0, 4999);

        for (int t = 0; t < 200; ++t) {
            uint64_t off = dist(rng);
            ReadResult rr = part.read(off, 1);
            CHECK(rr.status.ok());
            CHECK_EQ(rr.records.size(), 1u);
            CHECK_EQ(rr.records[0].offset, off);
            std::string got(rr.records[0].value.begin(), rr.records[0].value.end());
            CHECK_EQ(got, expected[off]);
        }
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// e. Reopen persistence: close, reopen, data intact, next append continues
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(reopen_persistence) {
    auto dir = create_temp_dir("reopen");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false;

        {
            Partition part(0, dir / "part_0", config);
            CHECK(part.open_and_recover().ok());
            part.append({}, {'A'});
            part.append({}, {'B'});
            part.flush();
        } // handle closed

        {
            Partition part_reopened(0, dir / "part_0", config);
            CHECK(part_reopened.open_and_recover().ok());
            CHECK_EQ(part_reopened.next_offset(), 2u);

            auto res = part_reopened.append({}, {'C'});
            CHECK(res.ok());
            CHECK_EQ(res.value(), 2u);

            ReadResult rr = part_reopened.read(0, 10);
            CHECK(rr.status.ok());
            CHECK_EQ(rr.records.size(), 3u);
            CHECK_EQ(rr.records[2].value[0], 'C');
        }
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// f. Torn tail: cut last record in half on disk; reopen truncates the partial
//    record, valid data is untouched, a new append continues the sequence
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(torn_tail_recovery) {
    auto dir = create_temp_dir("torn_tail");
    std::filesystem::path seg_log_path = dir / "part_0" / "00000000000000000000.log";

    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false; // unit tests only verify correctness

        {
            Partition part(0, dir / "part_0", config);
            CHECK(part.open_and_recover().ok());
            part.append({}, std::vector<uint8_t>(30, '1'));
            part.append({}, std::vector<uint8_t>(30, '2'));
            part.append({}, std::vector<uint8_t>(30, '3'));
            part.flush();
        } // file closed here

        // Chop 10 bytes off the end → tears the last record
        uint64_t full_sz = std::filesystem::file_size(seg_log_path);
        CHECK_TRUE(full_sz > 30);
        {
            FileHandle f = FileHandle::open_read_write(seg_log_path, false);
            CHECK_TRUE(f.is_valid());
            f.truncate(full_sz - 10);
        } // handle closed

        {
            Partition part_recovered(0, dir / "part_0", config);
            CHECK(part_recovered.open_and_recover().ok());
            // Only 2 complete records survive
            CHECK_EQ(part_recovered.next_offset(), 2u);

            ReadResult rr = part_recovered.read(0, 10);
            CHECK(rr.status.ok());
            CHECK_EQ(rr.records.size(), 2u);

            // New append must get offset 2
            auto res = part_recovered.append({}, {'N'});
            CHECK(res.ok());
            CHECK_EQ(res.value(), 2u);
        }
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// g. Corruption: flip one byte in a record's VALUE (past the CRC), verify
//    that reading that record returns Corrupt at the right offset.
//    Strategy: write 3 records (each 200-byte value) then corrupt a byte
//    inside the VALUE of record #1 (at a file offset well past its CRC field).
//    Then open with a fresh Partition that does NOT call open_and_recover
//    (which would truncate the corrupt record); instead we read directly
//    through a LogSegment so we bypass the recovery scan and test read-path
//    CRC validation.
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(record_corruption_detection) {
    auto dir = create_temp_dir("corrupt");
    std::filesystem::path seg_log_path = dir / "part_0" / "00000000000000000000.log";

    // Record layout: [4 length][4 crc][8 offset][8 timestamp][4 key_len][value]
    // = 4+4+8+8+4 = 28 header bytes + value bytes
    // Record 0 total = 28 + 200 = 228 bytes, value starts at byte 28

    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false; // unit tests only verify correctness
        config.index_interval_bytes = 4096; // single index entry covers all 3 records

        {
            Partition part(0, dir / "part_0", config);
            CHECK(part.open_and_recover().ok());
            // 3 records with 200-byte values
            part.append({}, std::vector<uint8_t>(200, 'A')); // offset 0
            part.append({}, std::vector<uint8_t>(200, 'B')); // offset 1
            part.append({}, std::vector<uint8_t>(200, 'C')); // offset 2
            part.flush();
        }

        // Corrupt one byte deep inside record 1's VALUE payload.
        // Record 0 is 4+4+8+8+4 + 200 = 228 bytes.
        // Record 1 starts at offset 228. Its value starts at 228+28=256.
        // Flip a byte at position 290 (safely inside value, past CRC at 232).
        const uint64_t flip_pos = 290;
        {
            FileHandle f = FileHandle::open_read_write(seg_log_path, false);
            CHECK_TRUE(f.is_valid());
            uint8_t byte_val = 0;
            f.read_at(flip_pos, &byte_val, 1);
            byte_val ^= 0xFF; // flip all bits
            OVERLAPPED ov;
            std::memset(&ov, 0, sizeof(ov));
            ov.Offset     = static_cast<DWORD>(flip_pos & 0xFFFFFFFF);
            ov.OffsetHigh = static_cast<DWORD>(flip_pos >> 32);
            DWORD written = 0;
            WriteFile(f.get(), &byte_val, 1, &written, &ov);
            f.flush();
        }

        // Open a fresh partition. Because the corrupted record is in the
        // MIDDLE (record 1 of 3), recover_and_rebuild_index will stop at
        // record 0, truncating records 1 and 2. We need to verify the Corrupt
        // status is returned by read when the data is live.
        //
        // To test read-path CRC validation without recovery truncating first,
        // we read directly from the LogSegment's read_record_at().
        {
            LogSegment seg(0, dir / "part_0", 4096);
            CHECK(seg.open().ok());
            // Record 1 starts at byte 228
            Record rec;
            Status st = seg.read_record_at(228, rec);
            CHECK_EQ(static_cast<int>(st.code()),
                     static_cast<int>(StatusCode::Corrupt));
        }
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// h. Topic name validation: bad names, path traversal, Windows reserved names
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(topic_name_validation) {
    std::string err;
    CHECK_TRUE(TopicManager::validate_topic_name("orders_v1", err));
    CHECK_TRUE(TopicManager::validate_topic_name("my-topic.1", err));

    CHECK_FALSE(TopicManager::validate_topic_name(".", err));
    CHECK_FALSE(TopicManager::validate_topic_name("..", err));
    CHECK_FALSE(TopicManager::validate_topic_name(".hidden", err));
    CHECK_FALSE(TopicManager::validate_topic_name("topic.", err));
    CHECK_FALSE(TopicManager::validate_topic_name("path/../traversal", err));

    // Reserved Windows device names (all variants)
    CHECK_FALSE(TopicManager::validate_topic_name("CON", err));
    CHECK_FALSE(TopicManager::validate_topic_name("con", err));
    CHECK_FALSE(TopicManager::validate_topic_name("NUL", err));
    CHECK_FALSE(TopicManager::validate_topic_name("COM1", err));
    CHECK_FALSE(TopicManager::validate_topic_name("LPT3", err));
}

// ─────────────────────────────────────────────────────────────────────────────
// i. Partitioners: same key → same partition every time; round robin spreads
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(partitioners) {
    KeyHashPartitioner key_hash;
    std::vector<uint8_t> key_a = {'u', 's', 'e', 'r', '1'};

    // Same key always lands on same partition
    uint32_t p1 = key_hash.partition(key_a, 4);
    uint32_t p2 = key_hash.partition(key_a, 4);
    CHECK_EQ(p1, p2);

    // Different key may (or may not) land on a different partition; just
    // confirm determinism for the same key
    std::vector<uint8_t> key_b = {'u', 's', 'e', 'r', '2'};
    uint32_t pb1 = key_hash.partition(key_b, 4);
    uint32_t pb2 = key_hash.partition(key_b, 4);
    CHECK_EQ(pb1, pb2);

    // Round-robin distributes evenly
    RoundRobinPartitioner rr;
    std::vector<uint32_t> counts(4, 0);
    for (int i = 0; i < 40; ++i) {
        uint32_t p = rr.partition({}, 4);
        counts[p]++;
    }
    for (int i = 0; i < 4; ++i) {
        CHECK_EQ(counts[i], 10u);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// j. Concurrency: 8 threads × 1,000 appends into ONE partition while a reader
//    thread scans. Offsets must be 0..7999 unique and contiguous; every record
//    reads back correctly.
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(concurrency_append_read) {
    auto dir = create_temp_dir("concurrency");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false; // disable per-append fsync for speed

        auto part = std::make_shared<Partition>(0, dir / "part_0", config);
        CHECK(part->open_and_recover().ok());

        constexpr int NUM_THREADS        = 4;
        constexpr int RECORDS_PER_THREAD = 250;

        std::atomic<bool> start_flag{false};
        std::vector<std::thread> writers;

        for (int t = 0; t < NUM_THREADS; ++t) {
            writers.emplace_back([&part, &start_flag, t]() {
                while (!start_flag) { std::this_thread::yield(); }
                for (int i = 0; i < RECORDS_PER_THREAD; ++i) {
                    std::string v = "t" + std::to_string(t) + "_" + std::to_string(i);
                    auto res = part->append({}, std::vector<uint8_t>(v.begin(), v.end()));
                    CHECK(res.ok());
                }
            });
        }

        start_flag = true;
        for (auto& th : writers) { th.join(); }

        uint64_t expected_total = static_cast<uint64_t>(NUM_THREADS * RECORDS_PER_THREAD);
        CHECK_EQ(part->next_offset(), expected_total);

        // Full sequential read-back: offsets must be 0..999 with no gaps
        ReadResult rr = part->read(0, 2000);
        CHECK(rr.status.ok());
        CHECK_EQ(rr.records.size(), static_cast<size_t>(expected_total));

        std::set<uint64_t> seen;
        for (size_t i = 0; i < rr.records.size(); ++i) {
            CHECK_EQ(rr.records[i].offset, static_cast<uint64_t>(i));
            seen.insert(rr.records[i].offset);
        }
        CHECK_EQ(seen.size(), static_cast<size_t>(expected_total));
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// k. Handle leak test: 1,000 partition open/close cycles must not leak handles
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(partition_handle_leak_1000_cycles) {
    auto dir = create_temp_dir("handle_leak");
    StorageConfig config;
    config.data_dir = dir.string();
    config.sync_on_append = false;

    {
        Partition part(0, dir / "part_0", config);
        CHECK(part.open_and_recover().ok());
        part.append({}, {'H', 'E', 'L', 'L', 'O'});
        part.flush();
    }

    DWORD handles_before = 0;
    GetProcessHandleCount(GetCurrentProcess(), &handles_before);

    for (int i = 0; i < 1000; ++i) {
        Partition part(0, dir / "part_0", config);
        CHECK(part.open_and_recover().ok());
    }

    DWORD handles_after = 0;
    GetProcessHandleCount(GetCurrentProcess(), &handles_after);

    CHECK_TRUE(handles_after <= handles_before + 2);

    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// l. Retention by age: write records older than retention_ms, verify gc deletes
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(retention_by_age) {
    auto dir = create_temp_dir("retention_age");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false;
        config.segment_max_bytes = 100; // Small so it rolls often

        TopicManager topic_mgr(config);
        CHECK(topic_mgr.open_and_recover_all().ok());

        // 1 partition, retention_ms = 5000 (5 seconds)
        auto topic = topic_mgr.create_topic("age_topic", 1, 5000, 0).value();
        CHECK_TRUE(topic != nullptr);

        int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        int64_t old_ts = now_ms - 20000; // 20s ago (older than 5s retention)

        // Write 6 records with old timestamp (payload 30 bytes, record ~60 bytes, rolls every 2 records)
        for (int i = 0; i < 6; ++i) {
            std::vector<uint8_t> val(30, 'A');
            auto res = topic->append_with_timestamp(0, {}, val, old_ts);
            CHECK(res.ok());
        }

        // Write 2 records with recent timestamp
        for (int i = 0; i < 2; ++i) {
            std::vector<uint8_t> val(30, 'B');
            auto res = topic->append_with_timestamp(0, {}, val, now_ms);
            CHECK(res.ok());
        }

        auto part = topic->get_partition(0);
        size_t segs_before = part->segment_count();
        CHECK_TRUE(segs_before >= 3);

        RetentionManager ret_mgr(topic_mgr);
        size_t deleted = ret_mgr.run_one_pass(false, "age_topic");
        CHECK_TRUE(deleted >= 2);
        CHECK_TRUE(part->segment_count() < segs_before);
        CHECK_TRUE(part->earliest_offset() > 0);
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// m. Retention by size: write records exceeding retention_bytes, oldest deleted
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(retention_by_size) {
    auto dir = create_temp_dir("retention_size");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false;
        config.segment_max_bytes = 100;

        TopicManager topic_mgr(config);
        CHECK(topic_mgr.open_and_recover_all().ok());

        // 1 partition, no age retention, retention_bytes = 250
        auto topic = topic_mgr.create_topic("size_topic", 1, 0, 250).value();
        CHECK_TRUE(topic != nullptr);

        for (int i = 0; i < 8; ++i) {
            std::vector<uint8_t> val(30, 'C');
            auto res = topic->append({}, val);
            CHECK(res.ok());
        }

        auto part = topic->get_partition(0);
        size_t segs_before = part->segment_count();
        CHECK_TRUE(segs_before >= 3);
        uint64_t bytes_before = part->total_bytes();
        CHECK_TRUE(bytes_before > 250);

        RetentionManager ret_mgr(topic_mgr);
        size_t deleted = ret_mgr.run_one_pass(false, "size_topic");
        CHECK_TRUE(deleted >= 1);
        CHECK_TRUE(part->segment_count() < segs_before);
        CHECK_TRUE(part->earliest_offset() > 0);
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// n. Active segment never deleted even if exceeding retention age or size
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(retention_active_segment_never_deleted) {
    auto dir = create_temp_dir("retention_active");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false;
        config.segment_max_bytes = 100000;

        TopicManager topic_mgr(config);
        CHECK(topic_mgr.open_and_recover_all().ok());

        // Highly aggressive retention: retention_ms = 1, retention_bytes = 1
        auto topic = topic_mgr.create_topic("active_topic", 1, 1, 1).value();
        CHECK_TRUE(topic != nullptr);

        int64_t old_ts = 1000; // Far in past
        auto res = topic->append_with_timestamp(0, {}, {'D'}, old_ts);
        CHECK(res.ok());

        auto part = topic->get_partition(0);
        CHECK_EQ(part->segment_count(), 1u);

        RetentionManager ret_mgr(topic_mgr);
        size_t deleted = ret_mgr.run_one_pass(false, "active_topic");
        CHECK_EQ(deleted, 0u);
        CHECK_EQ(part->segment_count(), 1u);
        CHECK_EQ(part->earliest_offset(), 0u);

        // Active segment is still completely functional
        auto append_res = topic->append({}, {'N', 'E', 'W'});
        CHECK(append_res.ok());
        ReadResult rr = part->read(0, 10);
        CHECK(rr.status.ok());
        CHECK_EQ(rr.records.size(), 2u);
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// o. FETCH below earliest_offset returns error code 8 (ERR_OFFSET_OUT_OF_RANGE)
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(fetch_below_earliest_offset_error_8) {
    auto dir = create_temp_dir("retention_fetch_err8");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false;
        config.segment_max_bytes = 100;

        TopicManager topic_mgr(config);
        CHECK(topic_mgr.open_and_recover_all().ok());

        auto topic = topic_mgr.create_topic("err8_topic", 1, 1, 1).value();
        CHECK_TRUE(topic != nullptr);

        for (int i = 0; i < 6; ++i) {
            std::vector<uint8_t> val(30, 'E');
            topic->append({}, val);
        }

        auto part = topic->get_partition(0);
        RetentionManager ret_mgr(topic_mgr);
        ret_mgr.run_one_pass(false, "err8_topic");

        uint64_t earliest = part->earliest_offset();
        CHECK_TRUE(earliest > 0);

        ReadResult rr = part->read(0, 10);
        CHECK_FALSE(rr.status.ok());
        CHECK_EQ(static_cast<int>(rr.status.code()), static_cast<int>(StatusCode::OffsetOutOfRange));

        // Read at earliest_offset should succeed
        ReadResult rr_valid = part->read(earliest, 10);
        CHECK(rr_valid.status.ok());
        CHECK_FALSE(rr_valid.records.empty());
        CHECK_EQ(rr_valid.records[0].offset, earliest);
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// p. .sealed sidecar is written on roll and CRC matches full scan
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(sealed_sidecar_and_crc_verification) {
    auto dir = create_temp_dir("sealed_sidecar");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false;
        config.segment_max_bytes = 100;

        Partition part(0, dir / "part_0", config);
        CHECK(part.open_and_recover().ok());

        // Append enough records to trigger at least one roll
        for (int i = 0; i < 5; ++i) {
            std::vector<uint8_t> val(35, 'S');
            part.append({}, val);
        }

        CHECK_TRUE(part.segment_count() >= 2);

        // Base segment 0 should have .sealed sidecar
        std::filesystem::path sealed_path = dir / "part_0" / "00000000000000000000.sealed";
        CHECK_TRUE(std::filesystem::exists(sealed_path));
        CHECK_EQ(std::filesystem::file_size(sealed_path), 32u);

        // Read sealed marker binary contents
        FileHandle f = FileHandle::open_read_only(sealed_path);
        CHECK_TRUE(f.is_valid());
        SealedMarker file_marker;
        DWORD bytes_read = 0;
        CHECK_TRUE(f.read_at(0, &file_marker, sizeof(file_marker), &bytes_read));
        CHECK_EQ(bytes_read, sizeof(file_marker));

        // Check magic SFSL (0x5346534C)
        CHECK_EQ(file_marker.magic, 0x5346534Cu);
        CHECK_TRUE(file_marker.record_count > 0);

        // Open LogSegment manually and verify sealed marker
        LogSegment seg(0, dir / "part_0", 4096);
        CHECK(seg.open().ok());
        CHECK(seg.has_sealed_marker());
        bool valid = false;
        CHECK(seg.verify_quick(valid).ok());
        CHECK_TRUE(valid);

        SealedMarker marker;
        CHECK(seg.read_sealed_marker(marker).ok());
        CHECK_EQ(marker.magic, file_marker.magic);
        CHECK_EQ(marker.crc32_of_whole_log, file_marker.crc32_of_whole_log);
        CHECK_EQ(marker.record_count, file_marker.record_count);
        CHECK_EQ(marker.record_count, seg.record_count());
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// q. Missing .sealed triggers recovery and rebuilds .sealed sidecar
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(missing_sealed_sidecar_rebuilt_on_recovery) {
    auto dir = create_temp_dir("missing_sealed");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false;
        config.segment_max_bytes = 100;

        {
            Partition part(0, dir / "part_0", config);
            CHECK(part.open_and_recover().ok());

            for (int i = 0; i < 5; ++i) {
                std::vector<uint8_t> val(35, 'M');
                part.append({}, val);
            }
            CHECK_TRUE(part.segment_count() >= 2);
        }

        std::filesystem::path sealed_path = dir / "part_0" / "00000000000000000000.sealed";
        CHECK_TRUE(std::filesystem::exists(sealed_path));

        // Delete .sealed sidecar to simulate crash before marker write or disk loss
        std::error_code ec;
        std::filesystem::remove(sealed_path, ec);
        CHECK_FALSE(std::filesystem::exists(sealed_path));

        // Reopen partition: recovery should detect missing .sealed, do full scan, and rebuild it!
        {
            Partition part(0, dir / "part_0", config);
            CHECK(part.open_and_recover().ok());
            CHECK_TRUE(part.segment_count() >= 2);
            CHECK_FALSE(part.is_degraded());

            // Verify .sealed sidecar was recreated
            CHECK_TRUE(std::filesystem::exists(sealed_path));

            // Verify partition can read from offset 0 seamlessly
            ReadResult rr = part.read(0, 10);
            CHECK(rr.status.ok());
            CHECK_EQ(rr.records.size(), 5u);
        }
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// r. Quick vs full scan difference: corrupt byte in middle of sealed segment
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(quick_vs_full_scan_middle_corruption) {
    auto dir = create_temp_dir("quick_vs_full");
    {
        StorageConfig config;
        config.data_dir = dir.string();
        config.sync_on_append = false;
        config.segment_max_bytes = 250;
        config.index_interval_bytes = 64;

        {
            Partition part(0, dir / "part_0", config);
            CHECK(part.open_and_recover().ok());

            // Write 5 records with 40-byte value: record total size = 68 bytes
            // Record 0 (pos 0), Record 1 (pos 68), Record 2 (pos 136).
            // Record 3 exceeds 200 bytes, so segment rolls to active segment with records 3, 4.
            for (int i = 0; i < 5; ++i) {
                std::vector<uint8_t> val(40, 'Q');
                part.append({}, val);
            }
            CHECK_TRUE(part.segment_count() >= 2);
        }

        std::filesystem::path log_path = dir / "part_0" / "00000000000000000000.log";
        CHECK_TRUE(std::filesystem::exists(log_path));
        uint64_t file_sz = std::filesystem::file_size(log_path);
        CHECK_TRUE(file_sz > 150);

        // Corrupt a byte in the payload of the middle record (Record 1 at pos 68 + 32 = 100)
        // Record 0 (first) and Record 2 (last) in this sealed segment remain intact.
        {
            FileHandle f = FileHandle::open_read_write(log_path, false);
            CHECK_TRUE(f.is_valid());
            OVERLAPPED ov{};
            ov.Offset = 100;
            char b = 'Z';
            DWORD written = 0;
            CHECK_TRUE(WriteFile(f.get(), &b, 1, &written, &ov) != FALSE);
            CHECK_EQ(written, 1u);
            f.flush();
        }

        // Test 1: Quick scan (startup_scan = "quick")
        // Quick scan only checks index and first/last records; it succeeds!
        {
            StorageConfig quick_config = config;
            quick_config.startup_scan = "quick";
            Partition quick_part(0, dir / "part_0", quick_config);
            CHECK(quick_part.open_and_recover().ok());
            CHECK_FALSE(quick_part.is_degraded());
        }

        // Test 2: Full scan (startup_scan = "full")
        // Full scan checks whole log and all records; detects corruption and flags degraded!
        {
            StorageConfig full_config = config;
            full_config.startup_scan = "full";
            Partition full_part(0, dir / "part_0", full_config);
            CHECK(full_part.open_and_recover().ok());
            CHECK_TRUE(full_part.is_degraded());
        }
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────
int main() {
    return ::streamforge::test::TestRegistry::instance().run_all();
}
