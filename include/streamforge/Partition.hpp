#ifndef STREAMFORGE_PARTITION_HPP
#define STREAMFORGE_PARTITION_HPP

#include "streamforge/LogSegment.hpp"
#include "streamforge/Status.hpp"
#include "streamforge/StorageConfig.hpp"
#include <mutex>
#include <vector>
#include <memory>
#include <filesystem>
#include <cstdint>

namespace streamforge {

struct ReadResult {
    std::vector<Record> records;
    Status status;
};

class Partition {
public:
    Partition(uint32_t partition_id, std::filesystem::path partition_dir, StorageConfig config);
    ~Partition() = default;

    Partition(const Partition&) = delete;
    Partition& operator=(const Partition&) = delete;

    Status open_and_recover();

    Result<uint64_t> append(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value);
    ReadResult read(uint64_t start_offset, size_t max_messages = 1000, size_t max_bytes = 1024 * 1024);

    uint64_t earliest_offset() const;
    uint64_t next_offset() const;
    void flush();

    uint32_t partition_id() const { return m_partition_id; }
    size_t segment_count() const;
    uint64_t total_bytes() const;

private:
    LogSegment* active_segment_unlocked();
    Status roll_segment_unlocked();

    mutable std::mutex m_mutex;
    uint32_t m_partition_id;
    std::filesystem::path m_partition_dir;
    StorageConfig m_config;

    uint64_t m_next_offset{0};
    std::vector<std::unique_ptr<LogSegment>> m_segments;
};

} // namespace streamforge

#endif // STREAMFORGE_PARTITION_HPP
