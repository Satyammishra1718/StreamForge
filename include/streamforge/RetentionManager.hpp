#ifndef STREAMFORGE_RETENTION_MANAGER_HPP
#define STREAMFORGE_RETENTION_MANAGER_HPP

#include "streamforge/TopicManager.hpp"
#include <thread>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>

namespace streamforge {

/**
 * RetentionManager runs in a background thread and periodically scans all topics
 * and partitions, deleting sealed log segments that are older than the per-topic
 * retention policy (retention_ms) or cause the partition size to exceed the
 * configured byte limit (retention_bytes).
 *
 * Deletion is performed under the same shared_mutex used by normal reads and
 * appends - we acquire the exclusive lock only for the quick pointer-list
 * update (removing the segment objects) and then close/delete the underlying
 * files outside the lock. Existing open handles for concurrent reads remain valid
 * on Windows due to FILE_SHARE_DELETE.
 */
class RetentionManager {
public:
    explicit RetentionManager(TopicManager& topic_mgr, uint64_t check_interval_ms = 60000);
    ~RetentionManager();

    RetentionManager(const RetentionManager&) = delete;
    RetentionManager& operator=(const RetentionManager&) = delete;

    // Start/stop the background retention thread
    void start();
    void stop();

    // Runs one retention pass across topics.
    // If dry_run is true, computes and reports what would be deleted without deleting files.
    // Returns the total number of segments deleted (or identified for deletion in dry_run).
    size_t run_one_pass(bool dry_run = false, const std::string& specific_topic = "");

private:
    void thread_loop();

    TopicManager& m_topic_mgr;
    uint64_t m_interval_ms;
    std::atomic<bool> m_running{false};
    std::thread m_thread;
    std::mutex m_control_mutex;
};

} // namespace streamforge

#endif // STREAMFORGE_RETENTION_MANAGER_HPP
