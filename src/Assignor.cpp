#include "streamforge/Assignor.hpp"
#include <algorithm>

namespace streamforge {

std::unordered_map<std::string, std::vector<TopicPartition>> RangeAssignor::assign(
    const std::vector<std::string>& sorted_members,
    const std::vector<std::pair<std::string, uint16_t>>& topic_partitions)
{
    std::unordered_map<std::string, std::vector<TopicPartition>> result;
    for (const auto& m : sorted_members) {
        result[m] = {};
    }

    if (sorted_members.empty()) {
        return result;
    }

    const size_t num_members = sorted_members.size();

    for (const auto& tp : topic_partitions) {
        const std::string& topic = tp.first;
        const uint16_t num_parts = tp.second;

        const size_t base = num_parts / num_members;
        const size_t extra = num_parts % num_members;

        for (size_t i = 0; i < num_members; ++i) {
            const size_t count = base + (i < extra ? 1 : 0);
            const size_t start = i * base + std::min(i, extra);

            for (size_t p = 0; p < count; ++p) {
                result[sorted_members[i]].push_back({topic, static_cast<uint16_t>(start + p)});
            }
        }
    }

    return result;
}

std::unordered_map<std::string, std::vector<TopicPartition>> RoundRobinAssignor::assign(
    const std::vector<std::string>& sorted_members,
    const std::vector<std::pair<std::string, uint16_t>>& topic_partitions)
{
    std::unordered_map<std::string, std::vector<TopicPartition>> result;
    for (const auto& m : sorted_members) {
        result[m] = {};
    }

    if (sorted_members.empty()) {
        return result;
    }

    // Flatten all (topic, partition) pairs
    std::vector<TopicPartition> all_tp;
    for (const auto& tp : topic_partitions) {
        const std::string& topic = tp.first;
        const uint16_t num_parts = tp.second;
        for (uint16_t p = 0; p < num_parts; ++p) {
            all_tp.push_back({topic, p});
        }
    }

    // Sort flattened pairs: topic name ascending, then partition ascending
    std::sort(all_tp.begin(), all_tp.end());

    // Deal them out round-robin
    const size_t num_members = sorted_members.size();
    for (size_t i = 0; i < all_tp.size(); ++i) {
        const std::string& member = sorted_members[i % num_members];
        result[member].push_back(all_tp[i]);
    }

    return result;
}

} // namespace streamforge
