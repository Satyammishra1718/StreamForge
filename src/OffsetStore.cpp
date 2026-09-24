#include "streamforge/OffsetStore.hpp"
#include "streamforge/FrameCodec.hpp"
#include "streamforge/Logger.hpp"
#include <sstream>

namespace streamforge {

const std::string OffsetStore::INTERNAL_OFFSETS_TOPIC = "__consumer_offsets";

std::string OffsetStore::make_key(const std::string& group_id, const std::string& topic, uint16_t partition) {
    return group_id + "|" + topic + "|" + std::to_string(partition);
}

bool OffsetStore::parse_key(const std::string& key_str, std::string& out_group, std::string& out_topic, uint16_t& out_partition) {
    size_t first_pipe = key_str.find('|');
    if (first_pipe == std::string::npos) return false;
    size_t second_pipe = key_str.find('|', first_pipe + 1);
    if (second_pipe == std::string::npos) return false;

    out_group = key_str.substr(0, first_pipe);
    out_topic = key_str.substr(first_pipe + 1, second_pipe - (first_pipe + 1));
    std::string part_str = key_str.substr(second_pipe + 1);

    if (out_group.empty() || out_topic.empty() || part_str.empty()) return false;

    try {
        size_t idx = 0;
        unsigned long p = std::stoul(part_str, &idx);
        if (idx != part_str.size() || p > 0xFFFF) return false;
        out_partition = static_cast<uint16_t>(p);
    } catch (...) {
        return false;
    }
    return true;
}

Status OffsetStore::open_and_recover(TopicManager& topic_mgr) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_cache.clear();

    auto topic = topic_mgr.get_topic(INTERNAL_OFFSETS_TOPIC);
    if (!topic) {
        auto create_res = topic_mgr.create_topic(INTERNAL_OFFSETS_TOPIC, 1);
        if (!create_res.ok()) {
            return Status::IoError("Failed to create " + INTERNAL_OFFSETS_TOPIC + ": " + create_res.status().message());
        }
        topic = create_res.value();
    }

    m_offsets_partition = topic->get_partition(0);
    if (!m_offsets_partition) {
        return Status::IoError("Failed to access partition 0 of " + INTERNAL_OFFSETS_TOPIC);
    }

    uint64_t high_watermark = m_offsets_partition->next_offset();
    uint64_t curr_offset = m_offsets_partition->earliest_offset();

    while (curr_offset < high_watermark) {
        ReadResult res = m_offsets_partition->read(curr_offset, 1000);
        if (!res.status.ok()) {
            return Status::Corrupt("Failed to read " + INTERNAL_OFFSETS_TOPIC + " at offset " + std::to_string(curr_offset) + ": " + res.status.message());
        }
        if (res.records.empty()) {
            break;
        }

        for (const auto& rec : res.records) {
            std::string key_str(rec.key.begin(), rec.key.end());
            std::string group, top;
            uint16_t part = 0;
            if (!parse_key(key_str, group, top, part)) {
                return Status::Corrupt("Corrupt key in " + INTERNAL_OFFSETS_TOPIC + " at offset " + std::to_string(rec.offset));
            }

            if (rec.value.size() != 16) {
                return Status::Corrupt("Corrupt value size (" + std::to_string(rec.value.size()) + " != 16) in " + INTERNAL_OFFSETS_TOPIC + " at offset " + std::to_string(rec.offset));
            }

            const uint8_t* p = rec.value.data();
            uint64_t off_high = FrameCodec::read_u32(p);
            uint64_t off_low  = FrameCodec::read_u32(p + 4);
            uint64_t offset_val = (off_high << 32) | off_low;

            uint64_t ts_high = FrameCodec::read_u32(p + 8);
            uint64_t ts_low  = FrameCodec::read_u32(p + 12);
            int64_t ts_val = static_cast<int64_t>((ts_high << 32) | ts_low);

            m_cache[key_str] = OffsetEntry{static_cast<int64_t>(offset_val), ts_val};
            curr_offset = rec.offset + 1;
        }
    }

    Logger::instance().info("OffsetStore loaded " + std::to_string(m_cache.size()) + " committed offset keys from " + INTERNAL_OFFSETS_TOPIC);
    return Status::OK();
}

Status OffsetStore::commit(const std::string& group_id, const std::vector<OffsetCommitItem>& items) {
    if (items.empty()) {
        return Status::OK();
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_offsets_partition) {
        return Status::IoError("OffsetStore partition not initialized");
    }

    std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> batch;
    batch.reserve(items.size());

    for (const auto& item : items) {
        std::string key_str = make_key(group_id, item.topic, item.partition);
        std::vector<uint8_t> key_bytes(key_str.begin(), key_str.end());

        std::vector<uint8_t> val_bytes(16);
        uint8_t* p = val_bytes.data();

        uint32_t off_high = static_cast<uint32_t>(item.offset >> 32);
        uint32_t off_low  = static_cast<uint32_t>(item.offset & 0xFFFFFFFF);
        FrameCodec::write_u32(p, off_high);
        FrameCodec::write_u32(p + 4, off_low);

        uint64_t ts_u = static_cast<uint64_t>(item.timestamp_ms);
        uint32_t ts_high = static_cast<uint32_t>(ts_u >> 32);
        uint32_t ts_low  = static_cast<uint32_t>(ts_u & 0xFFFFFFFF);
        FrameCodec::write_u32(p + 8, ts_high);
        FrameCodec::write_u32(p + 12, ts_low);

        batch.push_back({std::move(key_bytes), std::move(val_bytes)});
    }

    auto append_res = m_offsets_partition->append_batch(batch);
    if (!append_res.ok()) {
        return append_res.status();
    }

    // Update in-memory cache
    for (const auto& item : items) {
        std::string key_str = make_key(group_id, item.topic, item.partition);
        m_cache[key_str] = OffsetEntry{static_cast<int64_t>(item.offset), item.timestamp_ms};
    }

    return Status::OK();
}

int64_t OffsetStore::get(const std::string& group_id, const std::string& topic, uint16_t partition) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::string key_str = make_key(group_id, topic, partition);
    auto it = m_cache.find(key_str);
    if (it != m_cache.end()) {
        return it->second.offset;
    }
    return -1;
}

size_t OffsetStore::loaded_keys_count() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_cache.size();
}

} // namespace streamforge
