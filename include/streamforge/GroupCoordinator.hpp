#ifndef STREAMFORGE_GROUP_COORDINATOR_HPP
#define STREAMFORGE_GROUP_COORDINATOR_HPP

#include "streamforge/Assignor.hpp"
#include "streamforge/TopicManager.hpp"
#include "streamforge/OffsetStore.hpp"
#include "streamforge/Status.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <functional>
#include <atomic>
#include <cstdint>

namespace streamforge {

using Clock = std::function<std::chrono::steady_clock::time_point()>;

enum class GroupState : uint8_t {
    Empty = 0,
    Stable = 1
};

struct CoordinatorConfig {
    uint32_t min_session_timeout_ms{1000};
    uint32_t max_session_timeout_ms{60000};
    size_t max_members_per_group{64};
    size_t max_groups{1024};
};

struct MemberMetadata {
    std::string member_id;
    uint32_t session_timeout_ms{0};
    std::chrono::steady_clock::time_point last_heartbeat;
    std::vector<TopicPartition> assignment;
};

struct Group {
    std::string group_id;
    uint32_t generation{0};
    AssignorStrategy strategy{AssignorStrategy::Range};
    std::vector<std::string> subscribed_topics;
    GroupState state{GroupState::Empty};
    std::unordered_map<std::string, MemberMetadata> members;
    mutable std::mutex m_mutex;
};

struct JoinResult {
    Status status;
    uint16_t error_code{0};
    std::string member_id;
    uint32_t generation{0};
    uint32_t heartbeat_interval_ms{0};
    std::vector<TopicPartition> assignment;
};

struct HeartbeatResult {
    Status status;
    uint16_t error_code{0};
};

struct LeaveResult {
    Status status;
    uint16_t error_code{0};
};

struct OffsetCommitRequestEntry {
    std::string topic;
    uint16_t partition{0};
    uint64_t offset{0};
};

struct CommitResult {
    Status status;
    uint16_t error_code{0};
};

struct TopicPartitionQuery {
    std::string topic;
    uint16_t partition{0};
};

struct FetchOffsetsResult {
    Status status;
    uint16_t error_code{0};
    std::vector<int64_t> offsets;
};

struct MemberDescription {
    std::string member_id;
    uint32_t session_timeout_ms{0};
    uint32_t ms_since_heartbeat{0};
    std::vector<TopicPartition> assignment;
};

struct GroupDescription {
    Status status;
    uint16_t error_code{0};
    std::string group_id;
    uint32_t generation{0};
    std::string state; // "Empty" or "Stable"
    uint8_t strategy{0};
    std::vector<MemberDescription> members;
};

class GroupCoordinator {
public:
    GroupCoordinator(TopicManager& topic_mgr,
                     OffsetStore& offset_store,
                     CoordinatorConfig config = CoordinatorConfig{},
                     Clock clock = []() { return std::chrono::steady_clock::now(); });

    ~GroupCoordinator() = default;

    GroupCoordinator(const GroupCoordinator&) = delete;
    GroupCoordinator& operator=(const GroupCoordinator&) = delete;

    JoinResult join(const std::string& group_id,
                    const std::string& member_id,
                    uint32_t session_timeout_ms,
                    const std::vector<std::string>& topics,
                    AssignorStrategy strategy);

    HeartbeatResult heartbeat(const std::string& group_id,
                              const std::string& member_id,
                              uint32_t generation);

    LeaveResult leave(const std::string& group_id,
                      const std::string& member_id);

    size_t expire_members();

    CommitResult commit(const std::string& group_id,
                        const std::string& member_id,
                        uint32_t generation,
                        const std::vector<OffsetCommitRequestEntry>& entries);

    FetchOffsetsResult fetch_offsets(const std::string& group_id,
                                     const std::vector<TopicPartitionQuery>& queries);

    GroupDescription describe(const std::string& group_id);

    bool debug_check_invariants(const std::string& group_id);

    const CoordinatorConfig& config() const { return m_config; }

    static bool validate_group_id(const std::string& group_id);

private:
    std::string generate_member_id(const std::string& group_id);
    void rebalance_unlocked(Group& group);

    TopicManager& m_topic_mgr;
    OffsetStore& m_offset_store;
    CoordinatorConfig m_config;
    Clock m_clock;

    std::atomic<uint64_t> m_member_counter{1};
    mutable std::shared_mutex m_groups_mutex;
    std::unordered_map<std::string, std::shared_ptr<Group>> m_groups;

    RangeAssignor m_range_assignor;
    RoundRobinAssignor m_round_robin_assignor;
};

} // namespace streamforge

#endif // STREAMFORGE_GROUP_COORDINATOR_HPP
