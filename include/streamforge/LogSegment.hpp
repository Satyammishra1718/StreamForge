#ifndef STREAMFORGE_LOG_SEGMENT_HPP
#define STREAMFORGE_LOG_SEGMENT_HPP

#include "streamforge/FileHandle.hpp"
#include "streamforge/OffsetIndex.hpp"
#include "streamforge/RecordCodec.hpp"
#include "streamforge/Status.hpp"
#include <filesystem>
#include <memory>
#include <string>

namespace streamforge {

class LogSegment {
public:
    LogSegment(uint64_t base_offset, std::filesystem::path partition_dir, uint32_t index_interval_bytes = 4096);
    ~LogSegment() = default;

    LogSegment(const LogSegment&) = delete;
    LogSegment& operator=(const LogSegment&) = delete;
    LogSegment(LogSegment&&) = default;
    LogSegment& operator=(LogSegment&&) = default;

    Status open();
    Status append(Record& record, bool sync_on_append);
    Status read_record_at(uint64_t file_position, Record& out_record) const;
    uint32_t lookup_file_position(uint64_t target_offset) const;

    void seal();
    void flush();

    Status recover_and_rebuild_index(uint64_t& out_next_offset);

    uint64_t base_offset() const { return m_base_offset; }
    uint64_t log_size_bytes() const { return m_log_size_bytes; }
    bool is_sealed() const { return m_is_sealed; }
    const OffsetIndex& index() const { return m_index; }
    const std::filesystem::path& log_path() const { return m_log_path; }

    static std::string format_base_offset(uint64_t base_offset);

private:
    uint64_t m_base_offset;
    std::filesystem::path m_partition_dir;
    std::filesystem::path m_log_path;
    std::filesystem::path m_index_path;

    FileHandle m_log_file;
    FileHandle m_index_file;
    OffsetIndex m_index;

    uint32_t m_log_size_bytes{0};
    uint32_t m_bytes_since_last_index{0};
    uint32_t m_index_interval_bytes{4096};
    bool m_is_sealed{false};
};

} // namespace streamforge

#endif // STREAMFORGE_LOG_SEGMENT_HPP
