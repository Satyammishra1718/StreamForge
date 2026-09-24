#include "streamforge/Partitioner.hpp"

namespace streamforge {

uint32_t RoundRobinPartitioner::partition(const std::vector<uint8_t>& /*key*/, uint32_t num_partitions) {
    if (num_partitions == 0) return 0;
    return (m_counter++) % num_partitions;
}

uint32_t KeyHashPartitioner::fnv1a_32(const uint8_t* data, size_t length) {
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < length; ++i) {
        hash ^= data[i];
        hash *= 16777619u;
    }
    return hash;
}

uint32_t KeyHashPartitioner::partition(const std::vector<uint8_t>& key, uint32_t num_partitions) {
    if (num_partitions == 0) return 0;
    if (key.empty()) {
        return m_fallback_rr.partition(key, num_partitions);
    }
    uint32_t hash = fnv1a_32(key.data(), key.size());
    return hash % num_partitions;
}

} // namespace streamforge
