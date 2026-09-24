#ifndef STREAMFORGE_RETENTION_MANAGER_HPP
#define STREAMFORGE_RETENTION_MANAGER_HPP

#include <thread>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <mutex>

namespace streamforge {

class Topic; // forward declaration
class Partition; // forward declaration

/**
 * RetentionManager runs in a background thread and periodically scans all topics
 * and partitions, deleting sealed log segments that are older than the per‑topic
 * retention policy or cause the partition size to exceed the configured byte
 * limit.
 *
 * Deletion is performed under the same `std::shared_mutex` used by normal reads
 * and appends – we acquire the lock exclusively only for the quick pointer-list
 * update (removing the segment objects) and then close/delete the underlying
 * files outside the lock. This mirrors the design of the M5 reaper thread.
 */
class RetentionManager {
public:
    RetentionManager(uint64_t check_interval_ms = 60000);
    ~RetentionManager();

    // start/stop the background thread (called from server start/shutdown)
    void start();
    void stop();

    // for testing: run a single pass synchronously
    void run_one_pass();

private:
    void thread_loop();
    void scan_partition(Partition& part);

    uint64_t m_interval_ms;
    std::atomic<bool> m_running{false};
    std::thread m_thread;
    std::mutex m_control_mutex; // protects start/stop race
};

} // namespace streamforge

#endif // STREAMFORGE_RETENTION_MANAGER_HPP
