#ifndef STREAMFORGE_PROTOCOL_MESSAGES_HPP
#define STREAMFORGE_PROTOCOL_MESSAGES_HPP

#include "streamforge/BodyReaderWriter.hpp"
#include <string>
#include <vector>
#include <cstdint>

namespace streamforge {

// Request Structures
struct CreateTopicRequest {
    std::string topic;
    uint16_t partitions{0};

    bool decode(BodyReader& reader) {
        return reader.read_string(topic) &&
               reader.read_u16(partitions) &&
               reader.require_empty();
    }

    void encode(BodyWriter& writer) const {
        writer.write_string(topic);
        writer.write_u16(partitions);
    }
};

struct ProduceRecordPayload {
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
};

struct ProduceRequest {
    std::string topic;
    int32_t partition{-1};
    uint16_t record_count{0};
    std::vector<ProduceRecordPayload> records;

    bool decode(BodyReader& reader) {
        if (!reader.read_string(topic)) return false;
        if (!reader.read_i32(partition)) return false;
        if (!reader.read_u16(record_count)) return false;
        records.resize(record_count);
        for (uint16_t i = 0; i < record_count; ++i) {
            if (!reader.read_bytes(records[i].key)) return false;
            if (!reader.read_bytes(records[i].value)) return false;
        }
        return reader.require_empty();
    }

    void encode(BodyWriter& writer) const {
        writer.write_string(topic);
        writer.write_i32(partition);
        writer.write_u16(static_cast<uint16_t>(records.size()));
        for (const auto& rec : records) {
            writer.write_bytes(rec.key);
            writer.write_bytes(rec.value);
        }
    }
};

struct FetchRequest {
    std::string topic;
    uint16_t partition{0};
    uint64_t start_offset{0};
    uint32_t max_bytes{0};
    uint32_t max_messages{0};

    bool decode(BodyReader& reader) {
        return reader.read_string(topic) &&
               reader.read_u16(partition) &&
               reader.read_u64(start_offset) &&
               reader.read_u32(max_bytes) &&
               reader.read_u32(max_messages) &&
               reader.require_empty();
    }

    void encode(BodyWriter& writer) const {
        writer.write_string(topic);
        writer.write_u16(partition);
        writer.write_u64(start_offset);
        writer.write_u32(max_bytes);
        writer.write_u32(max_messages);
    }
};

struct ListTopicsRequest {
    bool decode(BodyReader& reader) {
        return reader.require_empty();
    }

    void encode(BodyWriter& writer) const {
        (void)writer;
    }
};

struct DescribeTopicRequest {
    std::string topic;

    bool decode(BodyReader& reader) {
        return reader.read_string(topic) &&
               reader.require_empty();
    }

    void encode(BodyWriter& writer) const {
        writer.write_string(topic);
    }
};

// Response Structures
struct CreateTopicResponse {
    void encode(BodyWriter& writer) const {
        (void)writer;
    }

    bool decode(BodyReader& reader) {
        return reader.require_empty();
    }
};

struct ProduceResponse {
    uint16_t partition{0};
    uint64_t base_offset{0};
    uint16_t count{0};

    void encode(BodyWriter& writer) const {
        writer.write_u16(partition);
        writer.write_u64(base_offset);
        writer.write_u16(count);
    }

    bool decode(BodyReader& reader) {
        return reader.read_u16(partition) &&
               reader.read_u64(base_offset) &&
               reader.read_u16(count) &&
               reader.require_empty();
    }
};

struct FetchRecordWire {
    uint64_t offset{0};
    int64_t timestamp_ms{0};
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
};

struct FetchResponse {
    uint64_t next_offset{0};
    uint64_t high_watermark{0};
    uint64_t earliest_offset{0};
    uint16_t record_count{0};
    std::vector<FetchRecordWire> records;

    void encode(BodyWriter& writer) const {
        writer.write_u64(next_offset);
        writer.write_u64(high_watermark);
        writer.write_u64(earliest_offset);
        writer.write_u16(static_cast<uint16_t>(records.size()));
        for (const auto& rec : records) {
            writer.write_u64(rec.offset);
            writer.write_i64(rec.timestamp_ms);
            writer.write_bytes(rec.key);
            writer.write_bytes(rec.value);
        }
    }

    bool decode(BodyReader& reader) {
        if (!reader.read_u64(next_offset)) return false;
        if (!reader.read_u64(high_watermark)) return false;
        if (!reader.read_u64(earliest_offset)) return false;
        if (!reader.read_u16(record_count)) return false;
        records.resize(record_count);
        for (uint16_t i = 0; i < record_count; ++i) {
            if (!reader.read_u64(records[i].offset)) return false;
            if (!reader.read_i64(records[i].timestamp_ms)) return false;
            if (!reader.read_bytes(records[i].key)) return false;
            if (!reader.read_bytes(records[i].value)) return false;
        }
        return reader.require_empty();
    }
};

struct TopicInfoWire {
    std::string name;
    uint16_t partitions{0};
};

struct ListTopicsResponse {
    uint16_t count{0};
    std::vector<TopicInfoWire> topics;

    void encode(BodyWriter& writer) const {
        writer.write_u16(static_cast<uint16_t>(topics.size()));
        for (const auto& t : topics) {
            writer.write_string(t.name);
            writer.write_u16(t.partitions);
        }
    }

    bool decode(BodyReader& reader) {
        if (!reader.read_u16(count)) return false;
        topics.resize(count);
        for (uint16_t i = 0; i < count; ++i) {
            if (!reader.read_string(topics[i].name)) return false;
            if (!reader.read_u16(topics[i].partitions)) return false;
        }
        return reader.require_empty();
    }
};

struct PartitionOffsetWire {
    uint64_t earliest{0};
    uint64_t next_offset{0};
};

struct DescribeTopicResponse {
    std::string name;
    uint16_t partitions{0};
    std::vector<PartitionOffsetWire> partition_offsets;

    void encode(BodyWriter& writer) const {
        writer.write_string(name);
        writer.write_u16(partitions);
        for (const auto& po : partition_offsets) {
            writer.write_u64(po.earliest);
            writer.write_u64(po.next_offset);
        }
    }

    bool decode(BodyReader& reader) {
        if (!reader.read_string(name)) return false;
        if (!reader.read_u16(partitions)) return false;
        partition_offsets.resize(partitions);
        for (uint16_t i = 0; i < partitions; ++i) {
            if (!reader.read_u64(partition_offsets[i].earliest)) return false;
            if (!reader.read_u64(partition_offsets[i].next_offset)) return false;
        }
        return reader.require_empty();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// M5 Consumer Group Request Structures
// ─────────────────────────────────────────────────────────────────────────────

struct JoinGroupRequest {
    std::string group_id;
    std::string member_id;
    uint32_t session_timeout_ms{0};
    uint8_t strategy{0}; // 0 = Range, 1 = RoundRobin
    uint16_t topic_count{0};
    std::vector<std::string> topics;

    bool decode(BodyReader& reader) {
        if (!reader.read_string(group_id)) return false;
        if (!reader.read_string(member_id)) return false;
        if (!reader.read_u32(session_timeout_ms)) return false;
        if (!reader.read_u8(strategy)) return false;
        if (!reader.read_u16(topic_count)) return false;
        if (reader.remaining() < static_cast<size_t>(topic_count) * 2) return false;
        topics.resize(topic_count);
        for (uint16_t i = 0; i < topic_count; ++i) {
            if (!reader.read_string(topics[i])) return false;
        }
        return reader.require_empty();
    }

    void encode(BodyWriter& writer) const {
        writer.write_string(group_id);
        writer.write_string(member_id);
        writer.write_u32(session_timeout_ms);
        writer.write_u8(strategy);
        writer.write_u16(static_cast<uint16_t>(topics.size()));
        for (const auto& t : topics) {
            writer.write_string(t);
        }
    }
};

struct HeartbeatRequest {
    std::string group_id;
    std::string member_id;
    uint32_t generation{0};

    bool decode(BodyReader& reader) {
        return reader.read_string(group_id) &&
               reader.read_string(member_id) &&
               reader.read_u32(generation) &&
               reader.require_empty();
    }

    void encode(BodyWriter& writer) const {
        writer.write_string(group_id);
        writer.write_string(member_id);
        writer.write_u32(generation);
    }
};

struct LeaveGroupRequest {
    std::string group_id;
    std::string member_id;

    bool decode(BodyReader& reader) {
        return reader.read_string(group_id) &&
               reader.read_string(member_id) &&
               reader.require_empty();
    }

    void encode(BodyWriter& writer) const {
        writer.write_string(group_id);
        writer.write_string(member_id);
    }
};

struct CommitOffsetItemWire {
    std::string topic;
    uint16_t partition{0};
    uint64_t offset{0};
};

struct CommitOffsetRequest {
    std::string group_id;
    std::string member_id;
    uint32_t generation{0};
    uint16_t count{0};
    std::vector<CommitOffsetItemWire> entries;

    bool decode(BodyReader& reader) {
        if (!reader.read_string(group_id)) return false;
        if (!reader.read_string(member_id)) return false;
        if (!reader.read_u32(generation)) return false;
        if (!reader.read_u16(count)) return false;
        if (reader.remaining() < static_cast<size_t>(count) * 12) return false;
        entries.resize(count);
        for (uint16_t i = 0; i < count; ++i) {
            if (!reader.read_string(entries[i].topic)) return false;
            if (!reader.read_u16(entries[i].partition)) return false;
            if (!reader.read_u64(entries[i].offset)) return false;
        }
        return reader.require_empty();
    }

    void encode(BodyWriter& writer) const {
        writer.write_string(group_id);
        writer.write_string(member_id);
        writer.write_u32(generation);
        writer.write_u16(static_cast<uint16_t>(entries.size()));
        for (const auto& e : entries) {
            writer.write_string(e.topic);
            writer.write_u16(e.partition);
            writer.write_u64(e.offset);
        }
    }
};

struct FetchOffsetQueryWire {
    std::string topic;
    uint16_t partition{0};
};

struct FetchOffsetRequest {
    std::string group_id;
    uint16_t count{0};
    std::vector<FetchOffsetQueryWire> queries;

    bool decode(BodyReader& reader) {
        if (!reader.read_string(group_id)) return false;
        if (!reader.read_u16(count)) return false;
        if (reader.remaining() < static_cast<size_t>(count) * 4) return false;
        queries.resize(count);
        for (uint16_t i = 0; i < count; ++i) {
            if (!reader.read_string(queries[i].topic)) return false;
            if (!reader.read_u16(queries[i].partition)) return false;
        }
        return reader.require_empty();
    }

    void encode(BodyWriter& writer) const {
        writer.write_string(group_id);
        writer.write_u16(static_cast<uint16_t>(queries.size()));
        for (const auto& q : queries) {
            writer.write_string(q.topic);
            writer.write_u16(q.partition);
        }
    }
};

struct DescribeGroupRequest {
    std::string group_id;

    bool decode(BodyReader& reader) {
        return reader.read_string(group_id) &&
               reader.require_empty();
    }

    void encode(BodyWriter& writer) const {
        writer.write_string(group_id);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// M5 Consumer Group Response Structures
// ─────────────────────────────────────────────────────────────────────────────

struct TopicPartitionWire {
    std::string topic;
    uint16_t partition{0};
};

struct JoinGroupResponse {
    std::string member_id;
    uint32_t generation{0};
    uint32_t heartbeat_interval_ms{0};
    uint16_t count{0};
    std::vector<TopicPartitionWire> assignments;

    void encode(BodyWriter& writer) const {
        writer.write_string(member_id);
        writer.write_u32(generation);
        writer.write_u32(heartbeat_interval_ms);
        writer.write_u16(static_cast<uint16_t>(assignments.size()));
        for (const auto& a : assignments) {
            writer.write_string(a.topic);
            writer.write_u16(a.partition);
        }
    }

    bool decode(BodyReader& reader) {
        if (!reader.read_string(member_id)) return false;
        if (!reader.read_u32(generation)) return false;
        if (!reader.read_u32(heartbeat_interval_ms)) return false;
        if (!reader.read_u16(count)) return false;
        if (reader.remaining() < static_cast<size_t>(count) * 4) return false;
        assignments.resize(count);
        for (uint16_t i = 0; i < count; ++i) {
            if (!reader.read_string(assignments[i].topic)) return false;
            if (!reader.read_u16(assignments[i].partition)) return false;
        }
        return reader.require_empty();
    }
};

struct HeartbeatResponse {
    void encode(BodyWriter& writer) const {
        (void)writer;
    }

    bool decode(BodyReader& reader) {
        return reader.require_empty();
    }
};

struct LeaveGroupResponse {
    void encode(BodyWriter& writer) const {
        (void)writer;
    }

    bool decode(BodyReader& reader) {
        return reader.require_empty();
    }
};

struct CommitOffsetResponse {
    void encode(BodyWriter& writer) const {
        (void)writer;
    }

    bool decode(BodyReader& reader) {
        return reader.require_empty();
    }
};

struct OffsetEntryWire {
    std::string topic;
    uint16_t partition{0};
    int64_t offset{-1};
};

struct FetchOffsetResponse {
    uint16_t count{0};
    std::vector<OffsetEntryWire> offsets;

    void encode(BodyWriter& writer) const {
        writer.write_u16(static_cast<uint16_t>(offsets.size()));
        for (const auto& o : offsets) {
            writer.write_string(o.topic);
            writer.write_u16(o.partition);
            writer.write_i64(o.offset);
        }
    }

    bool decode(BodyReader& reader) {
        if (!reader.read_u16(count)) return false;
        if (reader.remaining() < static_cast<size_t>(count) * 12) return false;
        offsets.resize(count);
        for (uint16_t i = 0; i < count; ++i) {
            if (!reader.read_string(offsets[i].topic)) return false;
            if (!reader.read_u16(offsets[i].partition)) return false;
            if (!reader.read_i64(offsets[i].offset)) return false;
        }
        return reader.require_empty();
    }
};

struct MemberDescriptionWire {
    std::string member_id;
    uint32_t session_timeout_ms{0};
    uint32_t ms_since_heartbeat{0};
    uint16_t count{0};
    std::vector<TopicPartitionWire> assignment;
};

struct DescribeGroupResponse {
    std::string group_id;
    uint32_t generation{0};
    std::string state;
    uint8_t strategy{0};
    uint16_t member_count{0};
    std::vector<MemberDescriptionWire> members;

    void encode(BodyWriter& writer) const {
        writer.write_string(group_id);
        writer.write_u32(generation);
        writer.write_string(state);
        writer.write_u8(strategy);
        writer.write_u16(static_cast<uint16_t>(members.size()));
        for (const auto& m : members) {
            writer.write_string(m.member_id);
            writer.write_u32(m.session_timeout_ms);
            writer.write_u32(m.ms_since_heartbeat);
            writer.write_u16(static_cast<uint16_t>(m.assignment.size()));
            for (const auto& a : m.assignment) {
                writer.write_string(a.topic);
                writer.write_u16(a.partition);
            }
        }
    }

    bool decode(BodyReader& reader) {
        if (!reader.read_string(group_id)) return false;
        if (!reader.read_u32(generation)) return false;
        if (!reader.read_string(state)) return false;
        if (!reader.read_u8(strategy)) return false;
        if (!reader.read_u16(member_count)) return false;
        members.resize(member_count);
        for (uint16_t i = 0; i < member_count; ++i) {
            if (!reader.read_string(members[i].member_id)) return false;
            if (!reader.read_u32(members[i].session_timeout_ms)) return false;
            if (!reader.read_u32(members[i].ms_since_heartbeat)) return false;
            if (!reader.read_u16(members[i].count)) return false;
            if (reader.remaining() < static_cast<size_t>(members[i].count) * 4) return false;
            members[i].assignment.resize(members[i].count);
            for (uint16_t j = 0; j < members[i].count; ++j) {
                if (!reader.read_string(members[i].assignment[j].topic)) return false;
                if (!reader.read_u16(members[i].assignment[j].partition)) return false;
            }
        }
        return reader.require_empty();
    }
};

} // namespace streamforge

#endif // STREAMFORGE_PROTOCOL_MESSAGES_HPP
