#ifndef STREAMFORGE_TOPIC_HPP
#define STREAMFORGE_TOPIC_HPP

#include "streamforge/Partition.hpp"
#include "streamforge/Partitioner.hpp"
#include "streamforge/Status.hpp"
#include "streamforge/StorageConfig.hpp"
#include <string>
#include <vector>
#include <memory>
#include <filesystem>
#include <mutex>

namespace streamforge {

class Topic {
public:
    Topic(std::string name, uint32_t num_partitions, std::filesystem::path topic_dir, StorageConfig config);
    ~Topic() = default;

    Topic(const Topic&) = delete;
    Topic& operator=(const Topic&) = delete;

    Status open_and_recover();

    Result<uint64_t> append(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value);
    Result<uint64_t> append_to(uint32_t partition_id, const std::vector<uint8_t>& key, const std::vector<uint8_t>& value);

    std::shared_ptr<Partition> get_partition(uint32_t partition_id) const;
    const std::string& name() const { return m_name; }
    uint32_t num_partitions() const { return static_cast<uint32_t>(m_partitions.size()); }
    void flush();

    const std::filesystem::path& topic_dir() const { return m_topic_dir; }

private:
    std::string m_name;
    std::filesystem::path m_topic_dir;
    StorageConfig m_config;

    mutable std::mutex m_mutex;
    std::unique_ptr<Partitioner> m_partitioner;
    std::vector<std::shared_ptr<Partition>> m_partitions;
};

} // namespace streamforge

#endif // STREAMFORGE_TOPIC_HPP
