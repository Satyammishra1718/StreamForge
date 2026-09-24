#ifndef STREAMFORGE_STORAGE_CONFIG_HPP
#define STREAMFORGE_STORAGE_CONFIG_HPP

#include <string>
#include <cstdint>
#include <cstddef>

namespace streamforge {

constexpr uint32_t DEFAULT_SEGMENT_MAX_BYTES = 64 * 1024 * 1024; // 64 MiB
constexpr uint32_t DEFAULT_INDEX_INTERVAL_BYTES = 4096; // 4 KB
constexpr uint32_t MAX_RECORD_SIZE = 1024 * 1024; // 1 MiB

constexpr uint64_t DEFAULT_RETENTION_MS = 604800000ULL; // 7 days (ms)
constexpr uint64_t DEFAULT_RETENTION_BYTES = 0ULL; // 0 = disabled
constexpr uint64_t DEFAULT_RETENTION_CHECK_INTERVAL_MS = 60000ULL; // 60s

struct StorageConfig {
    std::string data_dir{"./data"};
    uint32_t segment_max_bytes{DEFAULT_SEGMENT_MAX_BYTES};
    uint32_t index_interval_bytes{DEFAULT_INDEX_INTERVAL_BYTES};
    bool sync_on_append{true};
    uint64_t default_retention_ms{DEFAULT_RETENTION_MS};
    uint64_t default_retention_bytes{DEFAULT_RETENTION_BYTES};
    uint64_t retention_check_interval_ms{DEFAULT_RETENTION_CHECK_INTERVAL_MS};
    std::string startup_scan{"quick"}; // "quick" or "full"
};

} // namespace streamforge

#endif // STREAMFORGE_STORAGE_CONFIG_HPP
