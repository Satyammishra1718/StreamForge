#include "streamforge/RecordCodec.hpp"
#include "streamforge/Crc32.hpp"
#include <cstring>

namespace streamforge {

uint32_t RecordCodec::read_u32(const uint8_t* buffer) {
    uint32_t val;
    std::memcpy(&val, buffer, sizeof(val));
    return ((val & 0x000000FFU) << 24) |
           ((val & 0x0000FF00U) << 8)  |
           ((val & 0x00FF0000U) >> 8)  |
           ((val & 0xFF000000U) >> 24);
}

void RecordCodec::write_u32(uint8_t* buffer, uint32_t value) {
    uint32_t be = ((value & 0x000000FFU) << 24) |
                  ((value & 0x0000FF00U) << 8)  |
                  ((value & 0x00FF0000U) >> 8)  |
                  ((value & 0xFF000000U) >> 24);
    std::memcpy(buffer, &be, sizeof(be));
}

uint64_t RecordCodec::read_u64(const uint8_t* buffer) {
    uint64_t val;
    std::memcpy(&val, buffer, sizeof(val));
    return ((val & 0x00000000000000FFULL) << 56) |
           ((val & 0x000000000000FF00ULL) << 40) |
           ((val & 0x0000000000FF0000ULL) << 24) |
           ((val & 0x00000000FF000000ULL) << 8)  |
           ((val & 0x000000FF00000000ULL) >> 8)  |
           ((val & 0x0000FF0000000000ULL) >> 24) |
           ((val & 0x00FF000000000000ULL) >> 40) |
           ((val & 0xFF00000000000000ULL) >> 56);
}

void RecordCodec::write_u64(uint8_t* buffer, uint64_t value) {
    uint64_t be = ((value & 0x00000000000000FFULL) << 56) |
                  ((value & 0x000000000000FF00ULL) << 40) |
                  ((value & 0x0000000000FF0000ULL) << 24) |
                  ((value & 0x00000000FF000000ULL) << 8)  |
                  ((value & 0x000000FF00000000ULL) >> 8)  |
                  ((value & 0x0000FF0000000000ULL) >> 24) |
                  ((value & 0x00FF000000000000ULL) >> 40) |
                  ((value & 0xFF00000000000000ULL) >> 56);
    std::memcpy(buffer, &be, sizeof(be));
}

std::vector<uint8_t> RecordCodec::encode(const Record& record) {
    uint32_t key_len = static_cast<uint32_t>(record.key.size());
    uint32_t value_len = static_cast<uint32_t>(record.value.size());
    uint32_t length = 24 + key_len + value_len;

    std::vector<uint8_t> buffer(4 + length);

    write_u32(buffer.data(), length);
    write_u64(buffer.data() + 8, record.offset);
    write_u64(buffer.data() + 16, static_cast<uint64_t>(record.timestamp_ms));
    write_u32(buffer.data() + 24, key_len);

    if (key_len > 0) {
        std::memcpy(buffer.data() + 28, record.key.data(), key_len);
    }
    if (value_len > 0) {
        std::memcpy(buffer.data() + 28 + key_len, record.value.data(), value_len);
    }

    uint32_t crc = Crc32::calculate(buffer.data() + 8, length - 4);
    write_u32(buffer.data() + 4, crc);

    return buffer;
}

Status RecordCodec::decode(const uint8_t* buffer, size_t size, Record& out_record) {
    if (size < 28) {
        return Status::Corrupt("Buffer size less than record header size (28 bytes)");
    }

    uint32_t length = read_u32(buffer);
    if (length < 24) {
        return Status::Corrupt("Invalid record length field: " + std::to_string(length));
    }
    if (length > MAX_RECORD_SIZE) {
        return Status::RecordTooLarge("Record length " + std::to_string(length) + " exceeds maximum limit of 1 MiB");
    }
    if (size < 4 + length) {
        return Status::Corrupt("Incomplete record payload buffer");
    }

    uint32_t stored_crc = read_u32(buffer + 4);
    uint32_t computed_crc = Crc32::calculate(buffer + 8, length - 4);

    if (stored_crc != computed_crc) {
        return Status::Corrupt("CRC mismatch. Stored: " + std::to_string(stored_crc) + ", computed: " + std::to_string(computed_crc));
    }

    out_record.offset = read_u64(buffer + 8);
    out_record.timestamp_ms = static_cast<int64_t>(read_u64(buffer + 16));
    uint32_t key_len = read_u32(buffer + 24);

    if (24 + key_len > length) {
        return Status::Corrupt("Invalid key length in record header");
    }

    uint32_t value_len = length - 24 - key_len;

    out_record.key.clear();
    if (key_len > 0) {
        out_record.key.assign(buffer + 28, buffer + 28 + key_len);
    }

    out_record.value.clear();
    if (value_len > 0) {
        out_record.value.assign(buffer + 28 + key_len, buffer + 28 + key_len + value_len);
    }

    return Status::OK();
}

} // namespace streamforge
