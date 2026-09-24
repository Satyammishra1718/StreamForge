#ifndef STREAMFORGE_CRC32_HPP
#define STREAMFORGE_CRC32_HPP

#include <cstdint>
#include <cstddef>
#include <array>

namespace streamforge {

class Crc32 {
public:
    static uint32_t calculate(const void* data, size_t size);

private:
    static const std::array<uint32_t, 256>& get_table();
};

} // namespace streamforge

#endif // STREAMFORGE_CRC32_HPP
