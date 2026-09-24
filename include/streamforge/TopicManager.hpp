#ifndef STREAMFORGE_TOPIC_MANAGER_HPP
#define STREAMFORGE_TOPIC_MANAGER_HPP

#include "streamforge/Topic.hpp"
#include "streamforge/StorageConfig.hpp"
#include "streamforge/Status.hpp"
#include "streamforge/SharedMutex.hpp"
#include <string>
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <mutex>
#include <vector>
#include <filesystem>

namespace streamforge {

// Lock Order Hierarchy:
// 1. TopicManager::m_mutex (shared_mutex: shared lock for lookups/list, exclusive lock for create/recover)
// 2. Topic::m_mutex (guards partition array access)
// 3. Partition::m_mutex (shared_mutex: shared lock for reads/status, exclusive lock for append/roll/delete-prefix)

class TopicManager {
public:
    explicit TopicManager(StorageConfig config = StorageConfig{});
    ~TopicManager() = default;

    TopicManager(const TopicManager&) = delete;
    TopicManager& operator=(const TopicManager&) = delete;

    Status open_and_recover_all();

    Result<std::shared_ptr<Topic>> create_topic(const std::string& name, uint32_t partitions,
                                                uint64_t retention_ms = 0, uint64_t retention_bytes = 0);
    std::shared_ptr<Topic> get_topic(const std::string& name) const;
    std::vector<std::shared_ptr<Topic>> list_topics() const;

    static bool validate_topic_name(const std::string& name, std::string& out_error);

    const StorageConfig& config() const { return m_config; }

private:
    Status save_topic_metadata(const Topic& topic);
    Status load_topic_metadata(const std::filesystem::path& topic_dir, uint32_t& out_partitions,
                               uint64_t& out_retention_ms, uint64_t& out_retention_bytes);

    StorageConfig m_config;
    mutable SharedMutex m_mutex;
    std::unordered_map<std::string, std::shared_ptr<Topic>> m_topics;
};

} // namespace streamforge

#endif // STREAMFORGE_TOPIC_MANAGER_HPP
