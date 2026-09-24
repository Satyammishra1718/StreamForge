#include "streamforge/FrameAssembler.hpp"
#include <algorithm>

namespace streamforge {

void FrameAssembler::push_bytes(const uint8_t* data, size_t length) {
    if (!data || length == 0) return;
    m_buffer.insert(m_buffer.end(), data, data + length);
}

FrameExtractResult FrameAssembler::extract_next_frame(Frame& out_frame) {
    size_t available = m_buffer.size() - m_read_offset;
    if (available < 4) {
        return FrameExtractResult::NeedMoreData;
    }

    uint32_t length = FrameCodec::read_u32(m_buffer.data() + m_read_offset);
    if (length < HEADER_SIZE) {
        return FrameExtractResult::ErrorLengthTooSmall;
    }
    if (length > MAX_FRAME_LENGTH) {
        return FrameExtractResult::ErrorLengthTooLarge;
    }

    size_t total_frame_bytes = 4 + static_cast<size_t>(length);
    if (available < total_frame_bytes) {
        return FrameExtractResult::NeedMoreData;
    }

    const uint8_t* frame_ptr = m_buffer.data() + m_read_offset + 4;
    out_frame.length = length;
    out_frame.type = frame_ptr[0];
    out_frame.request_id = FrameCodec::read_u32(frame_ptr + 1);

    size_t body_len = length - HEADER_SIZE;
    if (body_len > 0) {
        out_frame.body.assign(frame_ptr + HEADER_SIZE, frame_ptr + HEADER_SIZE + body_len);
    } else {
        out_frame.body.clear();
    }

    m_read_offset += total_frame_bytes;
    compact_if_needed();
    return FrameExtractResult::FrameReady;
}

bool FrameAssembler::has_partial_frame() const {
    return (m_buffer.size() - m_read_offset) > 0;
}

size_t FrameAssembler::buffered_bytes() const {
    return m_buffer.size() - m_read_offset;
}

void FrameAssembler::clear() {
    m_buffer.clear();
    m_read_offset = 0;
}

void FrameAssembler::compact_if_needed() {
    if (m_read_offset == m_buffer.size()) {
        m_buffer.clear();
        m_read_offset = 0;
    } else if (m_read_offset > 64 * 1024) {
        m_buffer.erase(m_buffer.begin(), m_buffer.begin() + m_read_offset);
        m_read_offset = 0;
    }
}

} // namespace streamforge
