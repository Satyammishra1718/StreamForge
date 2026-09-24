#ifndef STREAMFORGE_CRC32_HPP
#define STREAMFORGE_CRC32_HPP

#include <cstdint>
#include <cstddef>
#include <array>

namespace streamforge {

class Crc32 {
public:
    static constexpr uint32_t INITIAL_CRC = 0xFFFFFFFF;

    static uint32_t calculate(const void* data, size_t size);
    static uint32_t update(uint32_t raw_crc, const void* data, size_t size);
    static uint32_t finalize(uint32_t raw_crc) { return raw_crc ^ 0xFFFFFFFF; }

private:
    static const std::array<uint32_t, 256>& get_table();
};

} // namespace streamforge

#endif // STREAMFORGE_CRC32_HPP
