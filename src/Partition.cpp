#include "streamforge/Partition.hpp"
#include "streamforge/Logger.hpp"
#include "streamforge/CrashPoint.hpp"
#include <algorithm>
#include <chrono>
#include <fstream>

namespace streamforge {

Partition::Partition(uint32_t partition_id, std::filesystem::path partition_dir, StorageConfig config)
    : m_partition_id(partition_id),
      m_partition_dir(std::move(partition_dir)),
      m_config(std::move(config)) {}

Status Partition::open_and_recover() {
    std::unique_lock<SharedMutex> lock(m_mutex);
    std::filesystem::create_directories(m_partition_dir);

    std::vector<uint64_t> base_offsets;
    for (const auto& entry : std::filesystem::directory_iterator(m_partition_dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".log") {
            std::string filename = entry.path().stem().string();
            try {
                uint64_t base_off = std::stoull(filename);
                base_offsets.push_back(base_off);
            } catch (...) {
                // Ignore non-matching filenames
            }
        }
    }

    std::sort(base_offsets.begin(), base_offsets.end());

    m_segments.clear();
    if (base_offsets.empty()) {
        base_offsets.push_back(0);
    }

    bool full_scan = (m_config.startup_scan == "full");
    uint64_t total_bytes_scanned = 0;
    uint64_t total_records_validated = 0;
    uint64_t total_corruptions_found = 0;
    auto scan_start_time = std::chrono::steady_clock::now();

    for (size_t i = 0; i < base_offsets.size(); ++i) {
        uint64_t base_off = base_offsets[i];
        auto seg = std::make_unique<LogSegment>(base_off, m_partition_dir, m_config.index_interval_bytes);
        Status st = seg->open();
        if (!st.ok()) {
            return st;
        }

        if (i < base_offsets.size() - 1) {
            // Sealed segment
            if (!seg->has_sealed_marker()) {
                // Edge case: sealed segment missing its marker
                Logger::instance().warning("Sealed segment " + seg->log_path().string() +
                                          " missing .sealed sidecar. Fully recovering and creating marker.");
                uint64_t dummy_next = 0;
                st = seg->recover_and_rebuild_index(dummy_next);
                if (!st.ok()) {
                    return st;
                }
                seg->seal();
                st = seg->write_sealed_marker();
                if (!st.ok()) {
                    Logger::instance().warning("Failed to write recovered sealed marker: " + st.message());
                }
                seg->flush();
                FileHandle::flush_directory(m_partition_dir);
            } else {
                // Has .sealed marker
                if (full_scan) {
                    uint64_t seg_bytes = 0, seg_records = 0, seg_corrupt = 0;
                    st = seg->verify_and_rebuild_full(seg_bytes, seg_records, seg_corrupt);
                    total_bytes_scanned += seg_bytes;
                    total_records_validated += seg_records;
                    total_corruptions_found += seg_corrupt;
                    if (seg_corrupt > 0) {
                        m_is_degraded = true;
                    }
                } else {
                    // Quick scan: spot-check first and last record and index positions
                    bool valid = true;
                    st = seg->verify_quick(valid);
                    if (!valid) {
                        Logger::instance().warning("Spot-check verification failed for sealed segment " +
                                                  seg->log_path().string() + ". Marking partition degraded.");
                        m_is_degraded = true;
                    }
                }
                seg->seal();
            }
        } else {
            // Active segment: always recover torn tail and rebuild index
            st = seg->recover_and_rebuild_index(m_next_offset);
            if (!st.ok()) {
                return st;
            }
        }
        m_segments.push_back(std::move(seg));
    }

    if (full_scan) {
        auto scan_end_time = std::chrono::steady_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(scan_end_time - scan_start_time).count();
        Logger::instance().info("[Startup Scan: FULL] Partition " + std::to_string(m_partition_id) +
                               " scanned " + std::to_string(m_segments.size()) + " segments, " +
                               std::to_string(total_bytes_scanned) + " bytes, " +
                               std::to_string(total_records_validated) + " records, " +
                               std::to_string(total_corruptions_found) + " corruptions in " +
                               std::to_string(elapsed_ms) + " ms.");
    }

    // Set earliest & next offsets
    if (!m_segments.empty()) {
        uint64_t active_base = m_segments.back()->base_offset();
        if (m_next_offset < active_base) {
            m_next_offset = active_base;
        }
    }

    return Status::OK();
}

LogSegment* Partition::active_segment_unlocked() {
    if (m_segments.empty()) return nullptr;
    return m_segments.back().get();
}

Status Partition::roll_segment_unlocked() {
    uint64_t new_base_offset = m_next_offset;
    auto new_seg = std::make_unique<LogSegment>(new_base_offset, m_partition_dir, m_config.index_interval_bytes);
    Status st = new_seg->open();
    if (!st.ok()) {
        return st;
    }
    m_segments.push_back(std::move(new_seg));
    FileHandle::flush_directory(m_partition_dir);

    if (m_segments.size() > 1) {
        LogSegment* old_seg = m_segments[m_segments.size() - 2].get();
        old_seg->seal();

        // Crash injection point: between sealing and writing .sealed sidecar
        CrashPoint::maybe_die("between_seal_and_sealed");

        st = old_seg->write_sealed_marker();
        if (!st.ok()) {
            return st;
        }
        old_seg->flush();
        FileHandle::flush_directory(m_partition_dir);
    }

    return Status::OK();
}

Result<uint64_t> Partition::append(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value) {
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    return append_with_timestamp(key, value, now_ms);
}

Result<uint64_t> Partition::append_with_timestamp(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value, int64_t timestamp_ms) {
    std::unique_lock<SharedMutex> lock(m_mutex);

    uint32_t record_size = 4 + 24 + static_cast<uint32_t>(key.size() + value.size());
    if (record_size > MAX_RECORD_SIZE) {
        return Status::RecordTooLarge("Record size (" + std::to_string(record_size) + ") exceeds maximum limit of 1 MiB");
    }

    LogSegment* active = active_segment_unlocked();
    if (!active) {
        Status st = roll_segment_unlocked();
        if (!st.ok()) return st;
        active = active_segment_unlocked();
    }

    if (active->log_size_bytes() > 0 && active->log_size_bytes() + record_size > m_config.segment_max_bytes) {
        Status st = roll_segment_unlocked();
        if (!st.ok()) return st;
        active = active_segment_unlocked();
    }

    Record record;
    record.offset = m_next_offset;
    record.timestamp_ms = timestamp_ms;
    record.key = key;
    record.value = value;

    Status st = active->append_with_timestamp(record, timestamp_ms, m_config.sync_on_append);
    if (!st.ok()) {
        return st;
    }

    uint64_t assigned_offset = m_next_offset;
    m_next_offset++;
    return assigned_offset;
}

Result<uint64_t> Partition::append_batch(const std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>>& batch) {
    if (batch.empty()) {
        return Status::InvalidArgument("Cannot append empty batch");
    }

    std::unique_lock<SharedMutex> lock(m_mutex);

    // Validate size of every record in batch
    for (const auto& item : batch) {
        uint32_t record_size = 4 + 24 + static_cast<uint32_t>(item.first.size() + item.second.size());
        if (record_size > MAX_RECORD_SIZE) {
            return Status::RecordTooLarge("Record size (" + std::to_string(record_size) + ") exceeds maximum limit of 1 MiB");
        }
    }

    uint64_t base_assigned_offset = m_next_offset;
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    for (const auto& item : batch) {
        uint32_t record_size = 4 + 24 + static_cast<uint32_t>(item.first.size() + item.second.size());

        LogSegment* active = active_segment_unlocked();
        if (!active) {
            Status st = roll_segment_unlocked();
            if (!st.ok()) return st;
            active = active_segment_unlocked();
        }

        if (active->log_size_bytes() > 0 && active->log_size_bytes() + record_size > m_config.segment_max_bytes) {
            Status st = roll_segment_unlocked();
            if (!st.ok()) return st;
            active = active_segment_unlocked();
        }

        Record record;
        record.offset = m_next_offset;
        record.timestamp_ms = now_ms;
        record.key = item.first;
        record.value = item.second;

        // Pass false for sync_on_append per-record, sync once after batch
        Status st = active->append(record, false);
        if (!st.ok()) {
            return st;
        }

        m_next_offset++;
    }

    if (m_config.sync_on_append) {
        for (auto& seg : m_segments) {
            seg->flush();
        }
    }

    return base_assigned_offset;
}

ReadResult Partition::read(uint64_t start_offset, size_t max_messages, size_t max_bytes) {
    std::shared_lock<SharedMutex> lock(m_mutex);
    ReadResult res;

    if (start_offset == m_next_offset) {
        res.status = Status::OK();
        return res;
    }

    uint64_t earliest = earliest_offset();
    if (start_offset > m_next_offset || start_offset < earliest) {
        res.status = Status::OffsetOutOfRange(
            "Start offset " + std::to_string(start_offset) + " out of valid range [" +
            std::to_string(earliest) + ", " + std::to_string(m_next_offset) + ")"
        );
        return res;
    }

    size_t seg_idx = 0;
    for (size_t i = 0; i < m_segments.size(); ++i) {
        if (i + 1 < m_segments.size() && m_segments[i + 1]->base_offset() <= start_offset) {
            continue;
        }
        seg_idx = i;
        break;
    }

    size_t total_bytes_read = 0;
    size_t total_messages_read = 0;

    for (size_t i = seg_idx; i < m_segments.size(); ++i) {
        LogSegment* seg = m_segments[i].get();
        uint64_t pos = seg->lookup_file_position(start_offset);
        uint64_t seg_size = seg->log_size_bytes();

        while (pos < seg_size) {
            Record rec;
            Status st = seg->read_record_at(pos, rec);
            if (!st.ok()) {
                if (st.code() == StatusCode::OffsetOutOfRange) {
                    break;
                }
                res.status = st;
                return res;
            }

            uint32_t rec_bytes = 4 + 24 + static_cast<uint32_t>(rec.key.size() + rec.value.size());
            pos += rec_bytes;

            if (rec.offset < start_offset) {
                continue;
            }

            res.records.push_back(std::move(rec));
            total_bytes_read += rec_bytes;
            total_messages_read++;

            if (total_messages_read >= max_messages || total_bytes_read >= max_bytes) {
                res.status = Status::OK();
                return res;
            }
        }
    }

    res.status = Status::OK();
    return res;
}

uint64_t Partition::earliest_offset() const {
    if (m_segments.empty()) return 0;
    return m_segments.front()->base_offset();
}

uint64_t Partition::next_offset() const {
    return m_next_offset;
}

void Partition::flush() {
    std::shared_lock<SharedMutex> lock(m_mutex);
    for (auto& seg : m_segments) {
        seg->flush();
    }
}

size_t Partition::segment_count() const {
    std::shared_lock<SharedMutex> lock(m_mutex);
    return m_segments.size();
}

uint64_t Partition::total_bytes() const {
    std::shared_lock<SharedMutex> lock(m_mutex);
    uint64_t total = 0;
    for (const auto& seg : m_segments) {
        total += seg->log_size_bytes();
    }
    return total;
}

Status Partition::scan_and_delete_segments(uint64_t retention_ms,
                                          uint64_t retention_bytes,
                                          bool dry_run,
                                          std::vector<DeletionCandidate>& out_deleted) {
    std::unique_lock<SharedMutex> lock(m_mutex);

    // Deletion unit is a SEALED SEGMENT, never a partial segment, and NEVER the active segment.
    // If we have <= 1 segment, active segment is the only segment and cannot be deleted.
    if (m_segments.size() <= 1) {
        return Status::OK();
    }

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    size_t deletable_by_age = 0;
    if (retention_ms > 0) {
        for (size_t i = 0; i < m_segments.size() - 1; ++i) {
            int64_t seg_max_ts = m_segments[i]->max_timestamp_ms();
            if (seg_max_ts > 0 && (now_ms - seg_max_ts) > static_cast<int64_t>(retention_ms)) {
                deletable_by_age = i + 1;
            } else {
                break; // Prefix condition: stop at first non-expired segment
            }
        }
    }

    size_t deletable_by_size = 0;
    if (retention_bytes > 0) {
        uint64_t current_total = 0;
        for (const auto& seg : m_segments) {
            current_total += seg->log_size_bytes();
        }

        if (current_total > retention_bytes) {
            for (size_t i = 0; i < m_segments.size() - 1; ++i) {
                if (current_total > retention_bytes) {
                    current_total -= m_segments[i]->log_size_bytes();
                    deletable_by_size = i + 1;
                } else {
                    break;
                }
            }
        }
    }

    size_t deletable_count = std::max(deletable_by_age, deletable_by_size);
    if (deletable_count >= m_segments.size()) {
        deletable_count = m_segments.size() - 1; // Strict invariant: never delete active segment!
    }

    if (deletable_count == 0) {
        return Status::OK();
    }

    out_deleted.clear();
    for (size_t i = 0; i < deletable_count; ++i) {
        LogSegment* seg = m_segments[i].get();
        DeletionCandidate cand;
        cand.base_offset = seg->base_offset();
        cand.records = seg->record_count();
        cand.bytes = seg->log_size_bytes();
        cand.log_path = seg->log_path();
        cand.index_path = seg->index_path();
        cand.sealed_path = seg->sealed_path();
        out_deleted.push_back(cand);
    }

    if (dry_run) {
        return Status::OK();
    }

    // Move deletable segments out under the exclusive lock
    std::vector<std::unique_ptr<LogSegment>> to_delete;
    to_delete.reserve(deletable_count);
    for (size_t i = 0; i < deletable_count; ++i) {
        to_delete.push_back(std::move(m_segments[i]));
    }
    m_segments.erase(m_segments.begin(), m_segments.begin() + deletable_count);

    // Release lock BEFORE file deletion!
    lock.unlock();

    // Close open handles by clearing segment vector
    to_delete.clear();

    // Delete files outside the lock
    for (const auto& cand : out_deleted) {
        std::error_code ec;
        std::filesystem::remove(cand.log_path, ec);
        std::filesystem::remove(cand.index_path, ec);
        std::filesystem::remove(cand.sealed_path, ec);
    }
    FileHandle::flush_directory(m_partition_dir);

    return Status::OK();
}

} // namespace streamforge
