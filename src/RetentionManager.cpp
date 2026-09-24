#include "streamforge/RetentionManager.hpp"
#include "streamforge/Logger.hpp"
#include <iostream>

namespace streamforge {

RetentionManager::RetentionManager(TopicManager& topic_mgr, uint64_t check_interval_ms)
    : m_topic_mgr(topic_mgr),
      m_interval_ms(check_interval_ms) {}

RetentionManager::~RetentionManager() {
    stop();
}

void RetentionManager::start() {
    std::lock_guard<std::mutex> lock(m_control_mutex);
    if (m_running.load()) {
        return;
    }
    m_running.store(true);
    m_thread = std::thread(&RetentionManager::thread_loop, this);
    Logger::instance().info("RetentionManager background thread started (interval=" +
                           std::to_string(m_interval_ms) + " ms)");
}

void RetentionManager::stop() {
    {
        std::lock_guard<std::mutex> lock(m_control_mutex);
        if (!m_running.load()) {
            return;
        }
        m_running.store(false);
    }
    if (m_thread.joinable()) {
        m_thread.join();
    }
    Logger::instance().info("RetentionManager background thread stopped cleanly");
}

void RetentionManager::thread_loop() {
    while (m_running.load()) {
        // Sleep in small increments up to m_interval_ms for responsive shutdown
        uint64_t elapsed = 0;
        while (elapsed < m_interval_ms && m_running.load()) {
            uint64_t step = std::min<uint64_t>(100, m_interval_ms - elapsed);
            std::this_thread::sleep_for(std::chrono::milliseconds(step));
            elapsed += step;
        }

        if (!m_running.load()) {
            break;
        }

        run_one_pass(false);
    }
}

size_t RetentionManager::run_one_pass(bool dry_run, const std::string& specific_topic) {
    std::vector<std::shared_ptr<Topic>> topics_to_scan;
    if (!specific_topic.empty()) {
        auto t = m_topic_mgr.get_topic(specific_topic);
        if (t) {
            topics_to_scan.push_back(t);
        }
    } else {
        topics_to_scan = m_topic_mgr.list_topics();
    }

    size_t total_deleted = 0;

    for (const auto& topic : topics_to_scan) {
        if (!topic) continue;
        if (topic->name().rfind("__", 0) == 0) {
            continue; // Skip internal/reserved topics
        }

        uint64_t retention_ms = topic->retention_ms();
        uint64_t retention_bytes = topic->retention_bytes();

        if (retention_ms == 0 && retention_bytes == 0) {
            continue; // Both disabled
        }

        for (uint32_t p = 0; p < topic->num_partitions(); ++p) {
            auto part = topic->get_partition(p);
            if (!part) continue;

            std::vector<DeletionCandidate> candidates;
            Status st = part->scan_and_delete_segments(retention_ms, retention_bytes, dry_run, candidates);
            if (!st.ok()) {
                Logger::instance().warning("Retention scan failed for " + topic->name() +
                                          " partition " + std::to_string(p) + ": " + st.message());
                continue;
            }

            for (const auto& cand : candidates) {
                total_deleted++;
                if (dry_run) {
                    std::cout << "[GC Dry-Run] Topic=" << topic->name()
                              << " Partition=" << p
                              << " BaseOffset=" << cand.base_offset
                              << " Records=" << cand.records
                              << " Bytes=" << cand.bytes
                              << " would be deleted\n";
                } else {
                    Logger::instance().info("[Retention] Deleted segment topic=" + topic->name() +
                                           " partition=" + std::to_string(p) +
                                           " base_offset=" + std::to_string(cand.base_offset) +
                                           " records=" + std::to_string(cand.records) +
                                           " bytes=" + std::to_string(cand.bytes));
                    std::cout << "[GC] Deleted segment topic=" << topic->name()
                              << " partition=" << p
                              << " base_offset=" << cand.base_offset
                              << " records=" << cand.records
                              << " bytes=" << cand.bytes << "\n";
                }
            }
        }
    }

    return total_deleted;
}

} // namespace streamforge
