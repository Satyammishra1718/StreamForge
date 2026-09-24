#include "streamforge/LogSegment.hpp"
#include "streamforge/Logger.hpp"
#include <sstream>
#include <iomanip>
#include <vector>

namespace streamforge {

LogSegment::LogSegment(uint64_t base_offset, std::filesystem::path partition_dir, uint32_t index_interval_bytes)
    : m_base_offset(base_offset),
      m_partition_dir(std::move(partition_dir)),
      m_index_interval_bytes(index_interval_bytes) {
    std::string base_str = format_base_offset(m_base_offset);
    m_log_path = m_partition_dir / (base_str + ".log");
    m_index_path = m_partition_dir / (base_str + ".index");
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

    return Status::OK();
}

Status LogSegment::append(Record& record, bool sync_on_append) {
    if (m_is_sealed) {
        return Status::IoError("Cannot append to a sealed log segment");
    }

    std::vector<uint8_t> encoded = RecordCodec::encode(record);
    if (encoded.size() > MAX_RECORD_SIZE) {
        return Status::RecordTooLarge("Record size " + std::to_string(encoded.size()) + " exceeds 1 MiB limit");
    }

    uint32_t rel_offset = static_cast<uint32_t>(record.offset - m_base_offset);
    if (m_index.entries().empty() || m_bytes_since_last_index >= m_index_interval_bytes) {
        if (!m_index.append_to_file(m_index_file, rel_offset, m_log_size_bytes)) {
            return Status::IoError("Failed to append index entry");
        }
        m_bytes_since_last_index = 0;
    }

    if (!m_log_file.write(encoded.data(), static_cast<DWORD>(encoded.size()))) {
        return Status::IoError("Failed to write record to log file");
    }

    m_log_size_bytes += static_cast<uint32_t>(encoded.size());
    m_bytes_since_last_index += static_cast<uint32_t>(encoded.size());

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

} // namespace streamforge
