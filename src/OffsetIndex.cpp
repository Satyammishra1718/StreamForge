#include "streamforge/OffsetIndex.hpp"
#include "streamforge/RecordCodec.hpp"
#include <algorithm>

namespace streamforge {

void OffsetIndex::add_entry(uint32_t relative_offset, uint32_t file_position) {
    m_entries.push_back({relative_offset, file_position});
}

uint32_t OffsetIndex::lookup(uint32_t target_relative_offset) const {
    if (m_entries.empty()) {
        return 0;
    }

    auto it = std::upper_bound(
        m_entries.begin(), m_entries.end(), target_relative_offset,
        [](uint32_t target, const IndexEntry& entry) {
            return target < entry.relative_offset;
        }
    );

    if (it == m_entries.begin()) {
        return m_entries.front().file_position;
    }

    --it;
    return it->file_position;
}

bool OffsetIndex::load_from_file(const FileHandle& file) {
    m_entries.clear();
    uint64_t file_size = file.get_size();
    if (file_size == 0) return true;

    size_t num_entries = static_cast<size_t>(file_size / 8);
    std::vector<uint8_t> buffer(file_size);
    if (!file.read_at(0, buffer.data(), static_cast<DWORD>(file_size))) {
        return false;
    }

    m_entries.reserve(num_entries);
    for (size_t i = 0; i < num_entries; ++i) {
        uint32_t rel_off = RecordCodec::read_u32(buffer.data() + i * 8);
        uint32_t file_pos = RecordCodec::read_u32(buffer.data() + i * 8 + 4);
        m_entries.push_back({rel_off, file_pos});
    }
    return true;
}

bool OffsetIndex::append_to_file(FileHandle& file, uint32_t relative_offset, uint32_t file_position) {
    uint8_t buf[8];
    RecordCodec::write_u32(buf, relative_offset);
    RecordCodec::write_u32(buf + 4, file_position);
    add_entry(relative_offset, file_position);
    return file.write(buf, 8);
}

bool OffsetIndex::rewrite_file(FileHandle& file) const {
    file.truncate(0);
    if (m_entries.empty()) return true;

    std::vector<uint8_t> buffer(m_entries.size() * 8);
    for (size_t i = 0; i < m_entries.size(); ++i) {
        RecordCodec::write_u32(buffer.data() + i * 8, m_entries[i].relative_offset);
        RecordCodec::write_u32(buffer.data() + i * 8 + 4, m_entries[i].file_position);
    }
    return file.write(buffer.data(), static_cast<DWORD>(buffer.size()));
}

} // namespace streamforge
