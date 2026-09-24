#ifndef STREAMFORGE_ASSIGNOR_HPP
#define STREAMFORGE_ASSIGNOR_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <tuple>

namespace streamforge {

struct TopicPartition {
    std::string topic;
    uint16_t partition{0};

    bool operator==(const TopicPartition& other) const {
        return partition == other.partition && topic == other.topic;
    }

    bool operator<(const TopicPartition& other) const {
        if (topic != other.topic) {
            return topic < other.topic;
        }
        return partition < other.partition;
    }
};

enum class AssignorStrategy : uint8_t {
    Range = 0,
    RoundRobin = 1
};

class Assignor {
public:
    virtual ~Assignor() = default;

    virtual std::unordered_map<std::string, std::vector<TopicPartition>> assign(
        const std::vector<std::string>& sorted_members,
        const std::vector<std::pair<std::string, uint16_t>>& topic_partitions) = 0;
};

class RangeAssignor : public Assignor {
public:
    std::unordered_map<std::string, std::vector<TopicPartition>> assign(
        const std::vector<std::string>& sorted_members,
        const std::vector<std::pair<std::string, uint16_t>>& topic_partitions) override;
};

class RoundRobinAssignor : public Assignor {
public:
    std::unordered_map<std::string, std::vector<TopicPartition>> assign(
        const std::vector<std::string>& sorted_members,
        const std::vector<std::pair<std::string, uint16_t>>& topic_partitions) override;
};

} // namespace streamforge

#endif // STREAMFORGE_ASSIGNOR_HPP
