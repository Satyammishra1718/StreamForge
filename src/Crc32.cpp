#include "streamforge/Crc32.hpp"

namespace streamforge {

const std::array<uint32_t, 256>& Crc32::get_table() {
    static const auto table = []() {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t crc = i;
            for (int j = 0; j < 8; ++j) {
                if (crc & 1) {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc >>= 1;
                }
            }
            t[i] = crc;
        }
        return t;
    }();
    return table;
}

uint32_t Crc32::update(uint32_t raw_crc, const void* data, size_t size) {
    const auto& table = get_table();
    const auto* bytes = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < size; ++i) {
        uint8_t index = static_cast<uint8_t>((raw_crc ^ bytes[i]) & 0xFF);
        raw_crc = (raw_crc >> 8) ^ table[index];
    }
    return raw_crc;
}

uint32_t Crc32::calculate(const void* data, size_t size) {
    uint32_t raw = update(INITIAL_CRC, data, size);
    return finalize(raw);
}

} // namespace streamforge
