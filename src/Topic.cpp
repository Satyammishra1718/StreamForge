#include "streamforge/Topic.hpp"

namespace streamforge {

Topic::Topic(std::string name, uint32_t num_partitions, std::filesystem::path topic_dir, StorageConfig config)
    : m_name(std::move(name)),
      m_topic_dir(std::move(topic_dir)),
      m_config(std::move(config)),
      m_partitioner(std::make_unique<KeyHashPartitioner>()) {
    m_partitions.reserve(num_partitions);
    for (uint32_t i = 0; i < num_partitions; ++i) {
        std::filesystem::path part_dir = m_topic_dir / std::to_string(i);
        m_partitions.push_back(std::make_shared<Partition>(i, part_dir, m_config));
    }
}

Status Topic::open_and_recover() {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::filesystem::create_directories(m_topic_dir);

    for (auto& partition : m_partitions) {
        Status st = partition->open_and_recover();
        if (!st.ok()) {
            return st;
        }
    }
    return Status::OK();
}

Result<uint64_t> Topic::append(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value) {
    uint32_t part_id = m_partitioner->partition(key, num_partitions());
    return append_to(part_id, key, value);
}

Result<uint64_t> Topic::append_to(uint32_t partition_id, const std::vector<uint8_t>& key, const std::vector<uint8_t>& value) {
    if (partition_id >= m_partitions.size()) {
        return Status::InvalidArgument("Invalid partition ID: " + std::to_string(partition_id));
    }
    return m_partitions[partition_id]->append(key, value);
}

std::shared_ptr<Partition> Topic::get_partition(uint32_t partition_id) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (partition_id >= m_partitions.size()) {
        return nullptr;
    }
    return m_partitions[partition_id];
}

void Topic::flush() {
    std::lock_guard<std::mutex> lock(m_mutex);
    for (auto& p : m_partitions) {
        p->flush();
    }
}

} // namespace streamforge
