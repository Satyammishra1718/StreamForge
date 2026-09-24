#include "streamforge/Topic.hpp"

namespace streamforge {

Topic::Topic(std::string name, uint32_t num_partitions, std::filesystem::path topic_dir, StorageConfig config)
    : m_name(std::move(name)),
      m_topic_dir(std::move(topic_dir)),
      m_config(std::move(config)) {
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
    uint32_t part_id = m_key_partitioner.partition(key, num_partitions());
    return append_to(part_id, key, value);
}

Result<uint64_t> Topic::append_to(uint32_t partition_id, const std::vector<uint8_t>& key, const std::vector<uint8_t>& value) {
    if (partition_id >= m_partitions.size()) {
        return Status::InvalidArgument("Invalid partition ID: " + std::to_string(partition_id));
    }
    return m_partitions[partition_id]->append(key, value);
}

Result<uint64_t> Topic::produce_batch(int32_t partition,
                                      const std::vector<ProduceRecordPayload>& records,
                                      uint32_t& out_chosen_partition) {
    if (records.empty()) {
        return Status::InvalidArgument("Cannot produce empty record batch");
    }

    uint32_t chosen_p = 0;
    if (partition < -1 || partition >= static_cast<int32_t>(m_partitions.size())) {
        return Status::InvalidArgument("Invalid partition ID " + std::to_string(partition));
    }

    if (partition == -1) {
        // Requirement 2: partition == -1: server picks partition using topic's Partitioner
        // (KeyHash if FIRST record has non-empty key, else RoundRobin)
        if (!records[0].key.empty()) {
            chosen_p = m_key_partitioner.partition(records[0].key, num_partitions());
        } else {
            chosen_p = m_rr_partitioner.partition(records[0].key, num_partitions());
        }
    } else {
        chosen_p = static_cast<uint32_t>(partition);
    }

    out_chosen_partition = chosen_p;

    std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> batch;
    batch.reserve(records.size());
    for (const auto& r : records) {
        batch.push_back({r.key, r.value});
    }

    return m_partitions[chosen_p]->append_batch(batch);
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
