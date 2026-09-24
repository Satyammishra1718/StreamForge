#ifndef STREAMFORGE_PARTITIONER_HPP
#define STREAMFORGE_PARTITIONER_HPP

#include <cstdint>
#include <vector>
#include <atomic>
#include <memory>

namespace streamforge {

class Partitioner {
public:
    virtual ~Partitioner() = default;
    virtual uint32_t partition(const std::vector<uint8_t>& key, uint32_t num_partitions) = 0;
};

class RoundRobinPartitioner : public Partitioner {
public:
    RoundRobinPartitioner() = default;
    uint32_t partition(const std::vector<uint8_t>& key, uint32_t num_partitions) override;

private:
    std::atomic<uint32_t> m_counter{0};
};

class KeyHashPartitioner : public Partitioner {
public:
    KeyHashPartitioner() = default;
    uint32_t partition(const std::vector<uint8_t>& key, uint32_t num_partitions) override;

private:
    RoundRobinPartitioner m_fallback_rr;
    static uint32_t fnv1a_32(const uint8_t* data, size_t length);
};

} // namespace streamforge

#endif // STREAMFORGE_PARTITIONER_HPP
