#ifndef STREAMFORGE_TOPIC_HPP
#define STREAMFORGE_TOPIC_HPP

#include "streamforge/Partition.hpp"
#include "streamforge/Partitioner.hpp"
#include "streamforge/Status.hpp"
#include "streamforge/StorageConfig.hpp"
#include "streamforge/ProtocolMessages.hpp"
#include <string>
#include <vector>
#include <memory>
#include <filesystem>
#include <mutex>

namespace streamforge {

class Topic {
public:
    Topic(std::string name, uint32_t num_partitions, std::filesystem::path topic_dir, StorageConfig config,
          uint64_t retention_ms = DEFAULT_RETENTION_MS, uint64_t retention_bytes = DEFAULT_RETENTION_BYTES);
    ~Topic() = default;

    Topic(const Topic&) = delete;
    Topic& operator=(const Topic&) = delete;

    Status open_and_recover();

    Result<uint64_t> append(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value);
    Result<uint64_t> append_to(uint32_t partition_id, const std::vector<uint8_t>& key, const std::vector<uint8_t>& value);
    Result<uint64_t> append_with_timestamp(uint32_t partition_id, const std::vector<uint8_t>& key, const std::vector<uint8_t>& value, int64_t timestamp_ms);

    Result<uint64_t> produce_batch(int32_t partition,
                                  const std::vector<ProduceRecordPayload>& records,
                                  uint32_t& out_chosen_partition);

    std::shared_ptr<Partition> get_partition(uint32_t partition_id) const;
    const std::string& name() const { return m_name; }
    uint32_t num_partitions() const { return static_cast<uint32_t>(m_partitions.size()); }
    void flush();

    const std::filesystem::path& topic_dir() const { return m_topic_dir; }

    uint64_t retention_ms() const { return m_retention_ms; }
    uint64_t retention_bytes() const { return m_retention_bytes; }
    void set_retention_ms(uint64_t ms) { m_retention_ms = ms; }
    void set_retention_bytes(uint64_t b) { m_retention_bytes = b; }

    bool is_degraded() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        for (const auto& p : m_partitions) {
            if (p && p->is_degraded()) return true;
        }
        return false;
    }

private:
    std::string m_name;
    std::filesystem::path m_topic_dir;
    StorageConfig m_config;
    uint64_t m_retention_ms{DEFAULT_RETENTION_MS};
    uint64_t m_retention_bytes{DEFAULT_RETENTION_BYTES};

    mutable std::mutex m_mutex;
    KeyHashPartitioner m_key_partitioner;
    RoundRobinPartitioner m_rr_partitioner;
    std::vector<std::shared_ptr<Partition>> m_partitions;
};

} // namespace streamforge

#endif // STREAMFORGE_TOPIC_HPP
