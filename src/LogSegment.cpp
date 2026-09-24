#include "streamforge/LogSegment.hpp"
#include "streamforge/Logger.hpp"
#include "streamforge/CrashPoint.hpp"
#include <sstream>
#include <iomanip>
#include <vector>
#include <cstring>

namespace streamforge {

LogSegment::LogSegment(uint64_t base_offset, std::filesystem::path partition_dir, uint32_t index_interval_bytes)
    : m_base_offset(base_offset),
      m_partition_dir(std::move(partition_dir)),
      m_index_interval_bytes(index_interval_bytes) {
    std::string base_str = format_base_offset(m_base_offset);
    m_log_path = m_partition_dir / (base_str + ".log");
    m_index_path = m_partition_dir / (base_str + ".index");
    m_sealed_path = m_partition_dir / (base_str + ".sealed");
}

std::string LogSegment::format_base_offset(uint64_t base_offset) {
    std::ostringstream ss;
    ss << std::setfill('0') << std::setw(20) << base_offset;
    return ss.str();
}

Status LogSegment::open() {
    std::filesystem::create_directories(m_partition_dir);
    m_log_file = FileHandle::open_read_write(m_log_path, true);
    if (!m_log_file.is_valid()) {
        return Status::IoError("Failed to open log file: " + m_log_path.string());
    }

    m_index_file = FileHandle::open_read_write(m_index_path, true);
    if (!m_index_file.is_valid()) {
        return Status::IoError("Failed to open index file: " + m_index_path.string());
    }

    m_log_size_bytes = static_cast<uint32_t>(m_log_file.get_size());
    if (!m_index.load_from_file(m_index_file)) {
        return Status::IoError("Failed to load index file: " + m_index_path.string());
    }

    if (!m_index.entries().empty()) {
        uint32_t last_pos = m_index.entries().back().file_position;
        m_bytes_since_last_index = (m_log_size_bytes >= last_pos) ? (m_log_size_bytes - last_pos) : 0;
    } else {
        m_bytes_since_last_index = m_log_size_bytes;
    }

    // If .sealed file exists, read its metadata
    if (has_sealed_marker()) {
        SealedMarker marker;
        if (read_sealed_marker(marker).ok()) {
            m_record_count = marker.record_count;
            m_last_offset = marker.last_offset;
            m_max_timestamp_ms = marker.max_timestamp_ms;
            m_is_sealed = true;
        }
    }

    return Status::OK();
}

bool LogSegment::has_sealed_marker() const {
    std::error_code ec;
    return std::filesystem::exists(m_sealed_path, ec);
}

Status LogSegment::write_sealed_marker() {
    FileHandle marker_file = FileHandle::open_read_write(m_sealed_path, true);
    if (!marker_file.is_valid()) {
        return Status::IoError("Failed to open sealed marker file: " + m_sealed_path.string());
    }

    SealedMarker marker;
    marker.magic = 0x5346534C;
    marker.record_count = m_record_count;
    marker.last_offset = m_last_offset;
    marker.crc32_of_whole_log = running_crc();
    marker.max_timestamp_ms = m_max_timestamp_ms;

    if (!marker_file.write(&marker, sizeof(marker))) {
        return Status::IoError("Failed to write sealed marker to file");
    }
    marker_file.flush();
    marker_file.close();

    return Status::OK();
}

Status LogSegment::read_sealed_marker(SealedMarker& out_marker) const {
    FileHandle marker_file = FileHandle::open_read_only(m_sealed_path);
    if (!marker_file.is_valid()) {
        return Status::NotFound("Sealed marker file not found: " + m_sealed_path.string());
    }

    DWORD bytes_read = 0;
    if (!marker_file.read_at(0, &out_marker, sizeof(out_marker), &bytes_read) || bytes_read < 24) {
        return Status::Corrupt("Sealed marker file corrupted or incomplete: " + m_sealed_path.string());
    }

    if (out_marker.magic != 0x5346534C) {
        return Status::Corrupt("Invalid sealed marker magic header in " + m_sealed_path.string());
    }

    return Status::OK();
}

Status LogSegment::append(Record& record, bool sync_on_append) {
    return append_with_timestamp(record, record.timestamp_ms, sync_on_append);
}

Status LogSegment::append_with_timestamp(Record& record, int64_t custom_ts, bool sync_on_append) {
    if (m_is_sealed) {
        return Status::IoError("Cannot append to a sealed log segment");
    }

    record.timestamp_ms = custom_ts;
    std::vector<uint8_t> encoded = RecordCodec::encode(record);
    if (encoded.size() > MAX_RECORD_SIZE) {
        return Status::RecordTooLarge("Record size " + std::to_string(encoded.size()) + " exceeds 1 MiB limit");
    }

#ifdef STREAMFORGE_CRASH_TESTING
    char crash_buf[256];
    DWORD c_len = GetEnvironmentVariableA("STREAMFORGE_CRASH_AT", crash_buf, sizeof(crash_buf));
    const char* crash_target = std::getenv("STREAMFORGE_CRASH_AT");
    if ((c_len > 0 && std::strcmp(crash_buf, "mid_write_record") == 0) ||
        (crash_target && std::strcmp(crash_target, "mid_write_record") == 0)) {
        DWORD half = static_cast<DWORD>(encoded.size() / 2);
        if (half > 0) {
            m_log_file.write(encoded.data(), half);
            m_log_file.flush();
        }
        CrashPoint::maybe_die("mid_write_record");
    }
#endif

    uint32_t record_bytes = static_cast<uint32_t>(encoded.size());
    uint32_t record_pos = m_log_size_bytes;

    if (!m_log_file.write(encoded.data(), static_cast<DWORD>(record_bytes))) {
        return Status::IoError("Failed to write record to log file");
    }

    m_log_size_bytes += record_bytes;
    m_running_crc_raw = Crc32::update(m_running_crc_raw, encoded.data(), encoded.size());
    m_record_count++;
    m_last_offset = record.offset;
    if (custom_ts > m_max_timestamp_ms) {
        m_max_timestamp_ms = custom_ts;
    }

    // Crash point between writing .log and writing .index
    CrashPoint::maybe_die("between_log_and_index");

    uint32_t rel_offset = static_cast<uint32_t>(record.offset - m_base_offset);
    if (m_index.entries().empty() || m_bytes_since_last_index >= m_index_interval_bytes) {
        if (!m_index.append_to_file(m_index_file, rel_offset, record_pos)) {
            return Status::IoError("Failed to append index entry");
        }
        m_bytes_since_last_index = 0;
    } else {
        m_bytes_since_last_index += record_bytes;
    }

    if (sync_on_append) {
        flush();
    }

    return Status::OK();
}

Status LogSegment::read_record_at(uint64_t file_position, Record& out_record) const {
    if (file_position >= m_log_size_bytes) {
        return Status::OffsetOutOfRange("File position beyond log size");
    }

    uint8_t header[28];
    if (!m_log_file.read_at(file_position, header, 28)) {
        return Status::Corrupt("Failed to read record header at position " + std::to_string(file_position));
    }

    uint32_t length = RecordCodec::read_u32(header);
    if (length < 24 || length > MAX_RECORD_SIZE) {
        return Status::Corrupt("Invalid record length (" + std::to_string(length) + ") at position " + std::to_string(file_position));
    }

    uint32_t total_record_size = 4 + length;
    std::vector<uint8_t> buffer(total_record_size);
    if (!m_log_file.read_at(file_position, buffer.data(), total_record_size)) {
        return Status::Corrupt("Failed to read complete record buffer at position " + std::to_string(file_position));
    }

    return RecordCodec::decode(buffer.data(), buffer.size(), out_record);
}

uint32_t LogSegment::lookup_file_position(uint64_t target_offset) const {
    if (target_offset <= m_base_offset) {
        return 0;
    }
    uint32_t rel_offset = static_cast<uint32_t>(target_offset - m_base_offset);
    return m_index.lookup(rel_offset);
}

void LogSegment::seal() {
    m_is_sealed = true;
    flush();
}

void LogSegment::flush() {
    m_log_file.flush();
    m_index_file.flush();
}

Status LogSegment::recover_and_rebuild_index(uint64_t& out_next_offset) {
    m_index.clear();
    m_log_size_bytes = static_cast<uint32_t>(m_log_file.get_size());

    out_next_offset = m_base_offset;
    uint32_t current_pos = 0;
    uint32_t bytes_since_idx = 0;
    m_running_crc_raw = Crc32::INITIAL_CRC;
    m_record_count = 0;
    m_last_offset = m_base_offset;
    m_max_timestamp_ms = 0;

    while (current_pos < m_log_size_bytes) {
        if (current_pos + 28 > m_log_size_bytes) {
            break;
        }

        uint8_t header[28];
        if (!m_log_file.read_at(current_pos, header, 28)) {
            break;
        }

        uint32_t length = RecordCodec::read_u32(header);
        if (length < 24 || length > MAX_RECORD_SIZE) {
            break;
        }

        uint32_t total_size = 4 + length;
        if (current_pos + total_size > m_log_size_bytes) {
            break;
        }

        std::vector<uint8_t> buf(total_size);
        if (!m_log_file.read_at(current_pos, buf.data(), total_size)) {
            break;
        }

        Record record;
        Status st = RecordCodec::decode(buf.data(), buf.size(), record);
        if (!st.ok()) {
            break;
        }

        uint32_t rel_offset = static_cast<uint32_t>(record.offset - m_base_offset);
        if (m_index.entries().empty() || bytes_since_idx >= m_index_interval_bytes) {
            m_index.add_entry(rel_offset, current_pos);
            bytes_since_idx = 0;
        }

        m_running_crc_raw = Crc32::update(m_running_crc_raw, buf.data(), buf.size());
        m_record_count++;
        m_last_offset = record.offset;
        if (record.timestamp_ms > m_max_timestamp_ms) {
            m_max_timestamp_ms = record.timestamp_ms;
        }

        bytes_since_idx += total_size;
        out_next_offset = record.offset + 1;
        current_pos += total_size;
    }

    if (current_pos < m_log_size_bytes) {
        uint32_t truncated_bytes = m_log_size_bytes - current_pos;
        m_log_file.truncate(current_pos);
        m_log_size_bytes = current_pos;
        Logger::instance().warning("Torn or corrupted record at end of segment " + m_log_path.string() + ". Truncated " + std::to_string(truncated_bytes) + " bytes.");
    }

    m_index.rewrite_file(m_index_file);
    m_bytes_since_last_index = bytes_since_idx;
    return Status::OK();
}

Status LogSegment::verify_quick(bool& out_valid) const {
    out_valid = true;
    if (m_log_size_bytes == 0) {
        return Status::OK();
    }
    if (m_log_size_bytes < 28) {
        out_valid = false;
        return Status::OK();
    }

    // 1. Verify index file positions are within file size
    for (const auto& entry : m_index.entries()) {
        if (entry.file_position >= m_log_size_bytes) {
            out_valid = false;
            return Status::OK();
        }
    }

    // 2. Verify FIRST record CRC
    Record first_rec;
    Status st_first = read_record_at(0, first_rec);
    if (!st_first.ok()) {
        out_valid = false;
        return Status::OK();
    }

    // 3. Verify LAST record CRC
    // Locate last record: start from the last index entry
    uint32_t search_pos = 0;
    if (!m_index.entries().empty()) {
        search_pos = m_index.entries().back().file_position;
    }

    Record last_rec;
    bool found_last = false;
    uint32_t pos = search_pos;
    while (pos < m_log_size_bytes) {
        if (pos + 28 > m_log_size_bytes) {
            out_valid = false;
            return Status::OK();
        }
        uint8_t header[28];
        if (!m_log_file.read_at(pos, header, 28)) {
            out_valid = false;
            return Status::OK();
        }
        uint32_t len = RecordCodec::read_u32(header);
        if (len < 24 || len > MAX_RECORD_SIZE || pos + 4 + len > m_log_size_bytes) {
            out_valid = false;
            return Status::OK();
        }
        Status st = read_record_at(pos, last_rec);
        if (!st.ok()) {
            out_valid = false;
            return Status::OK();
        }
        found_last = true;
        pos += (4 + len);
    }

    if (!found_last) {
        out_valid = false;
    }

    return Status::OK();
}

Status LogSegment::verify_and_rebuild_full(uint64_t& out_bytes_scanned, uint64_t& out_records_validated, uint64_t& out_corruption_count) {
    m_index.clear();
    m_log_size_bytes = static_cast<uint32_t>(m_log_file.get_size());

    uint32_t current_pos = 0;
    uint32_t bytes_since_idx = 0;
    m_running_crc_raw = Crc32::INITIAL_CRC;
    m_record_count = 0;
    m_last_offset = m_base_offset;
    m_max_timestamp_ms = 0;

    while (current_pos < m_log_size_bytes) {
        if (current_pos + 28 > m_log_size_bytes) {
            out_corruption_count++;
            break;
        }

        uint8_t header[28];
        if (!m_log_file.read_at(current_pos, header, 28)) {
            out_corruption_count++;
            break;
        }

        uint32_t length = RecordCodec::read_u32(header);
        if (length < 24 || length > MAX_RECORD_SIZE) {
            out_corruption_count++;
            break;
        }

        uint32_t total_size = 4 + length;
        if (current_pos + total_size > m_log_size_bytes) {
            out_corruption_count++;
            break;
        }

        std::vector<uint8_t> buf(total_size);
        if (!m_log_file.read_at(current_pos, buf.data(), total_size)) {
            out_corruption_count++;
            break;
        }

        Record record;
        Status st = RecordCodec::decode(buf.data(), buf.size(), record);
        if (!st.ok()) {
            out_corruption_count++;
            break;
        }

        uint32_t rel_offset = static_cast<uint32_t>(record.offset - m_base_offset);
        if (m_index.entries().empty() || bytes_since_idx >= m_index_interval_bytes) {
            m_index.add_entry(rel_offset, current_pos);
            bytes_since_idx = 0;
        }

        m_running_crc_raw = Crc32::update(m_running_crc_raw, buf.data(), buf.size());
        m_record_count++;
        m_last_offset = record.offset;
        if (record.timestamp_ms > m_max_timestamp_ms) {
            m_max_timestamp_ms = record.timestamp_ms;
        }

        bytes_since_idx += total_size;
        out_records_validated++;
        out_bytes_scanned += total_size;
        current_pos += total_size;
    }

    if (current_pos < m_log_size_bytes) {
        uint32_t truncated_bytes = m_log_size_bytes - current_pos;
        m_log_file.truncate(current_pos);
        m_log_size_bytes = current_pos;
        Logger::instance().warning("Full scan found corrupt/torn tail in segment " + m_log_path.string() + ". Truncated " + std::to_string(truncated_bytes) + " bytes.");
    }

    m_index.rewrite_file(m_index_file);
    m_bytes_since_last_index = bytes_since_idx;
    return Status::OK();
}

} // namespace streamforge
