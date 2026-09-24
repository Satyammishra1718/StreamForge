#ifndef STREAMFORGE_LOG_SEGMENT_HPP
#define STREAMFORGE_LOG_SEGMENT_HPP

#include "streamforge/FileHandle.hpp"
#include "streamforge/OffsetIndex.hpp"
#include "streamforge/RecordCodec.hpp"
#include "streamforge/Crc32.hpp"
#include "streamforge/CrashPoint.hpp"
#include "streamforge/Status.hpp"
#include <filesystem>
#include <memory>
#include <string>

namespace streamforge {

#pragma pack(push, 1)
struct SealedMarker {
    uint32_t magic{0x5346534C}; // "SFSL" (StreamForge Sealed Log)
    uint64_t record_count{0};
    uint64_t last_offset{0};
    uint32_t crc32_of_whole_log{0};
    int64_t max_timestamp_ms{0};
};
#pragma pack(pop)

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
    Status append_with_timestamp(Record& record, int64_t custom_timestamp_ms, bool sync_on_append);

    Status read_record_at(uint64_t file_position, Record& out_record) const;
    uint32_t lookup_file_position(uint64_t target_offset) const;

    void seal();
    void flush();

    bool has_sealed_marker() const;
    Status write_sealed_marker();
    Status read_sealed_marker(SealedMarker& out_marker) const;

    Status recover_and_rebuild_index(uint64_t& out_next_offset);
    Status verify_quick(bool& out_valid) const;
    Status verify_and_rebuild_full(uint64_t& out_bytes_scanned, uint64_t& out_records_validated, uint64_t& out_corruption_count);

    uint64_t base_offset() const { return m_base_offset; }
    uint64_t log_size_bytes() const { return m_log_size_bytes; }
    bool is_sealed() const { return m_is_sealed; }
    const OffsetIndex& index() const { return m_index; }
    const std::filesystem::path& log_path() const { return m_log_path; }
    const std::filesystem::path& index_path() const { return m_index_path; }
    const std::filesystem::path& sealed_path() const { return m_sealed_path; }

    uint64_t record_count() const { return m_record_count; }
    uint64_t last_offset() const { return m_last_offset; }
    int64_t max_timestamp_ms() const { return m_max_timestamp_ms; }
    uint32_t running_crc() const { return Crc32::finalize(m_running_crc_raw); }

    void set_record_count(uint64_t count) { m_record_count = count; }
    void set_last_offset(uint64_t off) { m_last_offset = off; }
    void set_max_timestamp_ms(int64_t ts) { m_max_timestamp_ms = ts; }
    void set_running_crc_raw(uint32_t raw) { m_running_crc_raw = raw; }

    static std::string format_base_offset(uint64_t base_offset);

private:
    uint64_t m_base_offset;
    std::filesystem::path m_partition_dir;
    std::filesystem::path m_log_path;
    std::filesystem::path m_index_path;
    std::filesystem::path m_sealed_path;

    FileHandle m_log_file;
    FileHandle m_index_file;
    OffsetIndex m_index;

    uint32_t m_log_size_bytes{0};
    uint32_t m_bytes_since_last_index{0};
    uint32_t m_index_interval_bytes{4096};
    bool m_is_sealed{false};

    uint32_t m_running_crc_raw{Crc32::INITIAL_CRC};
    uint64_t m_record_count{0};
    uint64_t m_last_offset{0};
    int64_t m_max_timestamp_ms{0};
};

} // namespace streamforge

#endif // STREAMFORGE_LOG_SEGMENT_HPP
