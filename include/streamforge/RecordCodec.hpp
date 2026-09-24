#ifndef STREAMFORGE_RECORD_CODEC_HPP
#define STREAMFORGE_RECORD_CODEC_HPP

#include "streamforge/Status.hpp"
#include "streamforge/StorageConfig.hpp"
#include <cstdint>
#include <vector>
#include <string>

namespace streamforge {

struct Record {
    uint64_t offset{0};
    int64_t timestamp_ms{0};
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
};

class RecordCodec {
public:
    static uint32_t read_u32(const uint8_t* buffer);
    static void write_u32(uint8_t* buffer, uint32_t value);
    static uint64_t read_u64(const uint8_t* buffer);
    static void write_u64(uint8_t* buffer, uint64_t value);

    static std::vector<uint8_t> encode(const Record& record);
    static Status decode(const uint8_t* buffer, size_t size, Record& out_record);
};

} // namespace streamforge

#endif // STREAMFORGE_RECORD_CODEC_HPP
