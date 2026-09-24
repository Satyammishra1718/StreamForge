#ifndef STREAMFORGE_BODY_READER_WRITER_HPP
#define STREAMFORGE_BODY_READER_WRITER_HPP

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <winsock2.h>
#include <ws2tcpip.h>

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include "streamforge/FrameCodec.hpp"

namespace streamforge {

class BodyReader {
public:
    BodyReader(const uint8_t* data, size_t size) : m_data(data), m_size(size), m_cursor(0) {}
    explicit BodyReader(const std::vector<uint8_t>& vec) : m_data(vec.data()), m_size(vec.size()), m_cursor(0) {}

    size_t remaining() const { return (m_cursor < m_size) ? (m_size - m_cursor) : 0; }
    bool empty() const { return m_cursor >= m_size; }
    bool require_empty() const { return m_cursor == m_size; }

    bool read_u8(uint8_t& out_val) {
        if (remaining() < 1) return false;
        out_val = m_data[m_cursor++];
        return true;
    }

    bool read_u16(uint16_t& out_val) {
        if (remaining() < 2) return false;
        out_val = FrameCodec::read_u16(m_data + m_cursor);
        m_cursor += 2;
        return true;
    }

    bool read_u32(uint32_t& out_val) {
        if (remaining() < 4) return false;
        out_val = FrameCodec::read_u32(m_data + m_cursor);
        m_cursor += 4;
        return true;
    }

    bool read_i32(int32_t& out_val) {
        uint32_t u = 0;
        if (!read_u32(u)) return false;
        out_val = static_cast<int32_t>(u);
        return true;
    }

    bool read_u64(uint64_t& out_val) {
        if (remaining() < 8) return false;
        uint32_t high = FrameCodec::read_u32(m_data + m_cursor);
        uint32_t low = FrameCodec::read_u32(m_data + m_cursor + 4);
        out_val = (static_cast<uint64_t>(high) << 32) | low;
        m_cursor += 8;
        return true;
    }

    bool read_i64(int64_t& out_val) {
        uint64_t u = 0;
        if (!read_u64(u)) return false;
        out_val = static_cast<int64_t>(u);
        return true;
    }

    bool read_string(std::string& out_str) {
        uint16_t len = 0;
        if (!read_u16(len)) return false;
        if (remaining() < len) return false;
        out_str.assign(reinterpret_cast<const char*>(m_data + m_cursor), len);
        m_cursor += len;
        return true;
    }

    bool read_bytes(std::vector<uint8_t>& out_bytes) {
        uint32_t len = 0;
        if (!read_u32(len)) return false;
        if (len > MAX_FRAME_LENGTH || remaining() < len) return false;
        out_bytes.assign(m_data + m_cursor, m_data + m_cursor + len);
        m_cursor += len;
        return true;
    }

private:
    const uint8_t* m_data;
    size_t m_size;
    size_t m_cursor;
};

class BodyWriter {
public:
    BodyWriter() = default;

    void write_u8(uint8_t val) {
        m_buffer.push_back(val);
    }

    void write_u16(uint16_t val) {
        uint8_t tmp[2];
        FrameCodec::write_u16(tmp, val);
        m_buffer.insert(m_buffer.end(), tmp, tmp + 2);
    }

    void write_u32(uint32_t val) {
        uint8_t tmp[4];
        FrameCodec::write_u32(tmp, val);
        m_buffer.insert(m_buffer.end(), tmp, tmp + 4);
    }

    void write_i32(int32_t val) {
        write_u32(static_cast<uint32_t>(val));
    }

    void write_u64(uint64_t val) {
        uint32_t high = static_cast<uint32_t>(val >> 32);
        uint32_t low = static_cast<uint32_t>(val & 0xFFFFFFFF);
        write_u32(high);
        write_u32(low);
    }

    void write_i64(int64_t val) {
        write_u64(static_cast<uint64_t>(val));
    }

    void write_string(const std::string& str) {
        uint16_t len = static_cast<uint16_t>(str.size());
        write_u16(len);
        m_buffer.insert(m_buffer.end(), str.begin(), str.end());
    }

    void write_bytes(const std::vector<uint8_t>& bytes) {
        uint32_t len = static_cast<uint32_t>(bytes.size());
        write_u32(len);
        m_buffer.insert(m_buffer.end(), bytes.begin(), bytes.end());
    }

    const std::vector<uint8_t>& buffer() const { return m_buffer; }
    std::vector<uint8_t> take_buffer() { return std::move(m_buffer); }

private:
    std::vector<uint8_t> m_buffer;
};

} // namespace streamforge

#endif // STREAMFORGE_BODY_READER_WRITER_HPP
