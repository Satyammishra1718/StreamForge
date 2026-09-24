#ifndef STREAMFORGE_OFFSET_STORE_HPP
#define STREAMFORGE_OFFSET_STORE_HPP

#include "streamforge/TopicManager.hpp"
#include "streamforge/Status.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <cstdint>
#include <memory>

namespace streamforge {

struct OffsetCommitItem {
    std::string topic;
    uint16_t partition{0};
    uint64_t offset{0};
    int64_t timestamp_ms{0};
};

struct OffsetEntry {
    int64_t offset{-1};
    int64_t timestamp_ms{0};
};

class OffsetStore {
public:
    OffsetStore() = default;
    ~OffsetStore() = default;

    OffsetStore(const OffsetStore&) = delete;
    OffsetStore& operator=(const OffsetStore&) = delete;

    Status open_and_recover(TopicManager& topic_mgr);

    Status commit(const std::string& group_id, const std::vector<OffsetCommitItem>& items);

    int64_t get(const std::string& group_id, const std::string& topic, uint16_t partition) const;

    size_t loaded_keys_count() const;

    static std::string make_key(const std::string& group_id, const std::string& topic, uint16_t partition);
    static bool parse_key(const std::string& key_str, std::string& out_group, std::string& out_topic, uint16_t& out_partition);

    static const std::string INTERNAL_OFFSETS_TOPIC;

private:
    mutable std::mutex m_mutex;
    std::shared_ptr<Partition> m_offsets_partition;
    std::unordered_map<std::string, OffsetEntry> m_cache;
};

} // namespace streamforge

#endif // STREAMFORGE_OFFSET_STORE_HPP
