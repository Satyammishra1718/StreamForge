#include "streamforge/Partition.hpp"
#include "streamforge/Logger.hpp"
#include <algorithm>
#include <chrono>
#include <fstream>

namespace streamforge {

Partition::Partition(uint32_t partition_id, std::filesystem::path partition_dir, StorageConfig config)
    : m_partition_id(partition_id),
      m_partition_dir(std::move(partition_dir)),
      m_config(std::move(config)) {}

Status Partition::open_and_recover() {
    std::lock_guard<std::mutex> lock(m_mutex);
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

    for (size_t i = 0; i < base_offsets.size(); ++i) {
        uint64_t base_off = base_offsets[i];
        auto seg = std::make_unique<LogSegment>(base_off, m_partition_dir, m_config.index_interval_bytes);
        Status st = seg->open();
        if (!st.ok()) {
            return st;
        }

        if (i < base_offsets.size() - 1) {
            seg->seal();
        } else {
            // Active segment: recover torn tail if any and rebuild index
            st = seg->recover_and_rebuild_index(m_next_offset);
            if (!st.ok()) {
                return st;
            }
        }
        m_segments.push_back(std::move(seg));
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
    if (!m_segments.empty()) {
        m_segments.back()->seal();
    }
    uint64_t new_base_offset = m_next_offset;
    auto new_seg = std::make_unique<LogSegment>(new_base_offset, m_partition_dir, m_config.index_interval_bytes);
    Status st = new_seg->open();
    if (!st.ok()) {
        return st;
    }
    m_segments.push_back(std::move(new_seg));
    return Status::OK();
}

Result<uint64_t> Partition::append(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value) {
    std::lock_guard<std::mutex> lock(m_mutex);

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

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    Record record;
    record.offset = m_next_offset;
    record.timestamp_ms = now_ms;
    record.key = key;
    record.value = value;

    Status st = active->append(record, m_config.sync_on_append);
    if (!st.ok()) {
        return st;
    }

    uint64_t assigned_offset = m_next_offset;
    m_next_offset++;
    return assigned_offset;
}

ReadResult Partition::read(uint64_t start_offset, size_t max_messages, size_t max_bytes) {
    std::lock_guard<std::mutex> lock(m_mutex);
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
    std::lock_guard<std::mutex> lock(m_mutex);
    for (auto& seg : m_segments) {
        seg->flush();
    }
}

size_t Partition::segment_count() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_segments.size();
}

uint64_t Partition::total_bytes() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    uint64_t total = 0;
    for (const auto& seg : m_segments) {
        total += seg->log_size_bytes();
    }
    return total;
}

} // namespace streamforge
