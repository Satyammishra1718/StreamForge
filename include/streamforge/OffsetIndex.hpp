#ifndef STREAMFORGE_OFFSET_INDEX_HPP
#define STREAMFORGE_OFFSET_INDEX_HPP

#include "streamforge/FileHandle.hpp"
#include <cstdint>
#include <vector>

namespace streamforge {

struct IndexEntry {
    uint32_t relative_offset{0};
    uint32_t file_position{0};
};

class OffsetIndex {
public:
    OffsetIndex() = default;

    void add_entry(uint32_t relative_offset, uint32_t file_position);
    uint32_t lookup(uint32_t target_relative_offset) const;

    bool load_from_file(const FileHandle& file);
    bool append_to_file(FileHandle& file, uint32_t relative_offset, uint32_t file_position);
    bool rewrite_file(FileHandle& file) const;

    const std::vector<IndexEntry>& entries() const { return m_entries; }
    void clear() { m_entries.clear(); }
    size_t size() const { return m_entries.size(); }

private:
    std::vector<IndexEntry> m_entries;
};

} // namespace streamforge

#endif // STREAMFORGE_OFFSET_INDEX_HPP
