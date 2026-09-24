#ifndef STREAMFORGE_FRAME_ASSEMBLER_HPP
#define STREAMFORGE_FRAME_ASSEMBLER_HPP

#include "streamforge/FrameCodec.hpp"
#include <vector>
#include <cstdint>
#include <cstddef>

namespace streamforge {

enum class FrameExtractResult {
    FrameReady,
    NeedMoreData,
    ErrorLengthTooSmall,
    ErrorLengthTooLarge
};

class FrameAssembler {
public:
    FrameAssembler() = default;

    void push_bytes(const uint8_t* data, size_t length);
    FrameExtractResult extract_next_frame(Frame& out_frame);

    bool has_partial_frame() const;
    size_t buffered_bytes() const;
    void clear();

private:
    void compact_if_needed();

    std::vector<uint8_t> m_buffer;
    size_t m_read_offset{0};
};

} // namespace streamforge

#endif // STREAMFORGE_FRAME_ASSEMBLER_HPP
