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

} // namespace streamforge

#endif // STREAMFORGE_PROTOCOL_MESSAGES_HPP
