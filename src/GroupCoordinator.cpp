#include "streamforge/GroupCoordinator.hpp"
#include "streamforge/FrameCodec.hpp"
#include "streamforge/Logger.hpp"
#include <algorithm>
#include <random>
#include <cstdio>
#include <set>

namespace streamforge {

GroupCoordinator::GroupCoordinator(TopicManager& topic_mgr,
                                   OffsetStore& offset_store,
                                   CoordinatorConfig config,
                                   Clock clock)
    : m_topic_mgr(topic_mgr),
      m_offset_store(offset_store),
      m_config(config),
      m_clock(std::move(clock))
{
}

bool GroupCoordinator::validate_group_id(const std::string& group_id) {
    if (group_id.empty() || group_id.size() > 64) {
        return false;
    }
    if (group_id.rfind("__", 0) == 0) {
        return false;
    }
    if (group_id.front() == '.' || group_id.back() == '.') {
        return false;
    }
    for (char c : group_id) {
        if (!((c >= 'a' && c <= 'z') ||
              (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') ||
              c == '.' || c == '_' || c == '-')) {
            return false;
        }
    }
    return true;
}

std::string GroupCoordinator::generate_member_id(const std::string& group_id) {
    uint64_t counter = m_member_counter.fetch_add(1, std::memory_order_relaxed);
    thread_local std::mt19937 rng(std::random_device{}());
    uint16_t rand_val = static_cast<uint16_t>(rng() & 0xFFFF);
    char buf[128];
    std::snprintf(buf, sizeof(buf), "%s-%08llu-%04x", group_id.c_str(), static_cast<unsigned long long>(counter), rand_val);
    return std::string(buf);
}

void GroupCoordinator::rebalance_unlocked(Group& group) {
    if (group.members.empty()) {
        group.state = GroupState::Empty;
        return;
    }

    std::vector<std::string> sorted_members;
    sorted_members.reserve(group.members.size());
    for (const auto& pair : group.members) {
        sorted_members.push_back(pair.first);
    }
    std::sort(sorted_members.begin(), sorted_members.end());

    std::vector<std::pair<std::string, uint16_t>> topic_partitions;
    for (const auto& topic_name : group.subscribed_topics) {
        auto topic = m_topic_mgr.get_topic(topic_name);
        if (topic) {
            topic_partitions.push_back({topic_name, static_cast<uint16_t>(topic->num_partitions())});
        }
    }

    std::unordered_map<std::string, std::vector<TopicPartition>> assignments;
    if (group.strategy == AssignorStrategy::Range) {
        assignments = m_range_assignor.assign(sorted_members, topic_partitions);
    } else {
        assignments = m_round_robin_assignor.assign(sorted_members, topic_partitions);
    }

    for (auto& pair : group.members) {
        pair.second.assignment = assignments[pair.first];
    }
    group.state = GroupState::Stable;
}

JoinResult GroupCoordinator::join(const std::string& group_id,
                                  const std::string& member_id,
                                  uint32_t session_timeout_ms,
                                  const std::vector<std::string>& topics,
                                  AssignorStrategy strategy)
{
    JoinResult res;

    if (!validate_group_id(group_id)) {
        res.status = Status::InvalidArgument("Invalid group ID");
        res.error_code = ErrorCode::INVALID_ARGUMENT;
        return res;
    }

    if (topics.empty()) {
        res.status = Status::InvalidArgument("Subscribed topics list cannot be empty");
        res.error_code = ErrorCode::INVALID_ARGUMENT;
        return res;
    }

    if (session_timeout_ms < m_config.min_session_timeout_ms ||
        session_timeout_ms > m_config.max_session_timeout_ms) {
        res.status = Status::InvalidArgument("Session timeout out of bounds");
        res.error_code = ErrorCode::INVALID_ARGUMENT;
        return res;
    }

    if (static_cast<uint8_t>(strategy) > 1) {
        res.status = Status::InvalidArgument("Invalid assignment strategy");
        res.error_code = ErrorCode::INVALID_ARGUMENT;
        return res;
    }

    // Verify all topics exist and are not reserved
    for (const auto& t : topics) {
        if (t.rfind("__", 0) == 0) {
            res.status = Status::InvalidArgument("Cannot subscribe to reserved topic: " + t);
            res.error_code = ErrorCode::INVALID_TOPIC_NAME;
            return res;
        }
        auto topic_obj = m_topic_mgr.get_topic(t);
        if (!topic_obj) {
            res.status = Status::NotFound("Subscribed topic not found: " + t);
            res.error_code = ErrorCode::UNKNOWN_TOPIC;
            return res;
        }
    }

    std::shared_ptr<Group> group;
    {
        std::unique_lock<std::shared_mutex> groups_lock(m_groups_mutex);
        auto it = m_groups.find(group_id);
        if (it == m_groups.end()) {
            if (m_groups.size() >= m_config.max_groups) {
                res.status = Status::InvalidArgument("Maximum groups limit reached");
                res.error_code = ErrorCode::INVALID_ARGUMENT;
                return res;
            }
            group = std::make_shared<Group>();
            group->group_id = group_id;
            group->strategy = strategy;
            group->subscribed_topics = topics;
            group->state = GroupState::Empty;
            m_groups[group_id] = group;
        } else {
            group = it->second;
        }
    }

    std::lock_guard<std::mutex> group_lock(group->m_mutex);

    // If group has no members, set initial strategy and subscriptions
    if (group->members.empty()) {
        group->strategy = strategy;
        group->subscribed_topics = topics;
    } else {
        if (group->strategy != strategy) {
            res.status = Status::InvalidArgument("Strategy mismatch for group");
            res.error_code = ErrorCode::INVALID_ARGUMENT;
            return res;
        }
        if (group->subscribed_topics != topics) {
            res.status = Status::InvalidArgument("Topic subscription mismatch for group");
            res.error_code = ErrorCode::INVALID_ARGUMENT;
            return res;
        }
    }

    if (member_id.empty()) {
        // New member joining
        if (group->members.size() >= m_config.max_members_per_group) {
            res.status = Status::InvalidArgument("Maximum members limit reached for group");
            res.error_code = ErrorCode::INVALID_ARGUMENT;
            return res;
        }

        std::string new_id = generate_member_id(group_id);
        MemberMetadata member;
        member.member_id = new_id;
        member.session_timeout_ms = session_timeout_ms;
        member.last_heartbeat = m_clock();

        group->members[new_id] = std::move(member);
        group->generation++;
        rebalance_unlocked(*group);

        res.status = Status::OK();
        res.error_code = 0;
        res.member_id = new_id;
        res.generation = group->generation;
        res.heartbeat_interval_ms = session_timeout_ms / 3;
        res.assignment = group->members[new_id].assignment;
        return res;
    } else {
        // Re-join with existing member_id
        auto it = group->members.find(member_id);
        if (it == group->members.end()) {
            res.status = Status::NotFound("Unknown member ID in group");
            res.error_code = ErrorCode::UNKNOWN_GROUP_OR_MEMBER;
            return res;
        }

        // Refresh heartbeat, update session timeout, return CURRENT assignment and generation
        it->second.last_heartbeat = m_clock();
        it->second.session_timeout_ms = session_timeout_ms;

        res.status = Status::OK();
        res.error_code = 0;
        res.member_id = member_id;
        res.generation = group->generation;
        res.heartbeat_interval_ms = session_timeout_ms / 3;
        res.assignment = it->second.assignment;
        return res;
    }
}

HeartbeatResult GroupCoordinator::heartbeat(const std::string& group_id,
                                            const std::string& member_id,
                                            uint32_t generation)
{
    HeartbeatResult res;
    std::shared_ptr<Group> group;
    {
        std::shared_lock<std::shared_mutex> groups_lock(m_groups_mutex);
        auto it = m_groups.find(group_id);
        if (it == m_groups.end()) {
            res.status = Status::NotFound("Unknown group");
            res.error_code = ErrorCode::UNKNOWN_GROUP_OR_MEMBER;
            return res;
        }
        group = it->second;
    }

    std::lock_guard<std::mutex> group_lock(group->m_mutex);
    auto it = group->members.find(member_id);
    if (it == group->members.end()) {
        res.status = Status::NotFound("Unknown member");
        res.error_code = ErrorCode::UNKNOWN_GROUP_OR_MEMBER;
        return res;
    }

    if (generation != group->generation) {
        res.status = Status::InvalidArgument("Illegal generation");
        res.error_code = ErrorCode::ILLEGAL_GENERATION;
        return res;
    }

    it->second.last_heartbeat = m_clock();
    res.status = Status::OK();
    res.error_code = 0;
    return res;
}

LeaveResult GroupCoordinator::leave(const std::string& group_id,
                                    const std::string& member_id)
{
    LeaveResult res;
    std::shared_ptr<Group> group;
    {
        std::shared_lock<std::shared_mutex> groups_lock(m_groups_mutex);
        auto it = m_groups.find(group_id);
        if (it == m_groups.end()) {
            res.status = Status::NotFound("Unknown group");
            res.error_code = ErrorCode::UNKNOWN_GROUP_OR_MEMBER;
            return res;
        }
        group = it->second;
    }

    std::lock_guard<std::mutex> group_lock(group->m_mutex);
    auto it = group->members.find(member_id);
    if (it == group->members.end()) {
        res.status = Status::NotFound("Unknown member");
        res.error_code = ErrorCode::UNKNOWN_GROUP_OR_MEMBER;
        return res;
    }

    group->members.erase(it);
    group->generation++;
    if (group->members.empty()) {
        group->state = GroupState::Empty;
    } else {
        rebalance_unlocked(*group);
    }

    res.status = Status::OK();
    res.error_code = 0;
    return res;
}

size_t GroupCoordinator::expire_members() {
    auto now = m_clock();
    std::vector<std::shared_ptr<Group>> all_groups;
    {
        std::shared_lock<std::shared_mutex> groups_lock(m_groups_mutex);
        all_groups.reserve(m_groups.size());
        for (const auto& pair : m_groups) {
            all_groups.push_back(pair.second);
        }
    }

    size_t total_expired = 0;
    for (const auto& group : all_groups) {
        std::lock_guard<std::mutex> group_lock(group->m_mutex);
        std::vector<std::string> expired_ids;
        for (const auto& pair : group->members) {
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - pair.second.last_heartbeat).count();
            if (elapsed_ms > static_cast<int64_t>(pair.second.session_timeout_ms)) {
                expired_ids.push_back(pair.first);
            }
        }

        if (!expired_ids.empty()) {
            for (const auto& id : expired_ids) {
                Logger::instance().warning("Expiring member '" + id + "' from group '" + group->group_id + "' (session timeout exceeded)");
                group->members.erase(id);
                total_expired++;
            }
            group->generation++;
            if (group->members.empty()) {
                group->state = GroupState::Empty;
            } else {
                rebalance_unlocked(*group);
            }
        }
    }

    return total_expired;
}

CommitResult GroupCoordinator::commit(const std::string& group_id,
                                      const std::string& member_id,
                                      uint32_t generation,
                                      const std::vector<OffsetCommitRequestEntry>& entries)
{
    CommitResult res;

    // Validate topics and partitions
    for (const auto& entry : entries) {
        if (entry.topic.rfind("__", 0) == 0) {
            res.status = Status::InvalidArgument("Cannot commit offsets for reserved topic: " + entry.topic);
            res.error_code = ErrorCode::INVALID_TOPIC_NAME;
            return res;
        }
        auto topic_obj = m_topic_mgr.get_topic(entry.topic);
        if (!topic_obj) {
            res.status = Status::NotFound("Topic not found: " + entry.topic);
            res.error_code = ErrorCode::UNKNOWN_TOPIC;
            return res;
        }
        auto partition_obj = topic_obj->get_partition(entry.partition);
        if (!partition_obj) {
            res.status = Status::InvalidArgument("Invalid partition: " + std::to_string(entry.partition));
            res.error_code = ErrorCode::INVALID_PARTITION;
            return res;
        }
    }

    bool is_standalone = (member_id.empty() && generation == 0xFFFFFFFF);
    std::shared_ptr<Group> group;
    {
        std::shared_lock<std::shared_mutex> groups_lock(m_groups_mutex);
        auto it = m_groups.find(group_id);
        if (it != m_groups.end()) {
            group = it->second;
        }
    }

    std::unique_ptr<std::lock_guard<std::mutex>> group_lock;
    if (group) {
        group_lock = std::make_unique<std::lock_guard<std::mutex>>(group->m_mutex);
    }

    if (is_standalone) {
        if (group && !group->members.empty()) {
            res.status = Status::InvalidArgument("Standalone commit rejected: group has active members");
            res.error_code = ErrorCode::ILLEGAL_GENERATION;
            return res;
        }
    } else {
        if (!group) {
            res.status = Status::NotFound("Unknown group: " + group_id);
            res.error_code = ErrorCode::UNKNOWN_GROUP_OR_MEMBER;
            return res;
        }
        auto it_mem = group->members.find(member_id);
        if (it_mem == group->members.end()) {
            res.status = Status::NotFound("Unknown member: " + member_id);
            res.error_code = ErrorCode::UNKNOWN_GROUP_OR_MEMBER;
            return res;
        }
        if (generation != group->generation) {
            res.status = Status::InvalidArgument("Illegal generation (expected " + std::to_string(group->generation) + ", got " + std::to_string(generation) + ")");
            res.error_code = ErrorCode::ILLEGAL_GENERATION;
            return res;
        }

        // Verify all partitions in entries are assigned to this member
        for (const auto& entry : entries) {
            bool assigned = false;
            for (const auto& tp : it_mem->second.assignment) {
                if (tp.topic == entry.topic && tp.partition == entry.partition) {
                    assigned = true;
                    break;
                }
            }
            if (!assigned) {
                res.status = Status::InvalidArgument("Partition " + entry.topic + ":" + std::to_string(entry.partition) + " is not assigned to member " + member_id);
                res.error_code = ErrorCode::PARTITION_NOT_ASSIGNED;
                return res;
            }
        }
    }

    // Validate offset upper bound (0 <= offset <= partition high watermark)
    for (const auto& entry : entries) {
        auto topic_obj = m_topic_mgr.get_topic(entry.topic);
        auto partition_obj = topic_obj->get_partition(entry.partition);
        if (entry.offset > partition_obj->next_offset()) {
            res.status = Status::OffsetOutOfRange("Commit offset (" + std::to_string(entry.offset) + ") exceeds high watermark (" + std::to_string(partition_obj->next_offset()) + ")");
            res.error_code = ErrorCode::OFFSET_OUT_OF_RANGE;
            return res;
        }
    }

    // Persist through OffsetStore while holding group lock (if group exists)
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(m_clock().time_since_epoch()).count();
    std::vector<OffsetCommitItem> items;
    items.reserve(entries.size());
    for (const auto& e : entries) {
        items.push_back({e.topic, e.partition, e.offset, now_ms});
    }

    Status persist_status = m_offset_store.commit(group_id, items);
    if (!persist_status.ok()) {
        res.status = persist_status;
        res.error_code = ErrorCode::INTERNAL_ERROR;
        return res;
    }

    res.status = Status::OK();
    res.error_code = 0;
    return res;
}

FetchOffsetsResult GroupCoordinator::fetch_offsets(const std::string& group_id,
                                                   const std::vector<TopicPartitionQuery>& queries)
{
    FetchOffsetsResult res;
    res.offsets.reserve(queries.size());
    for (const auto& q : queries) {
        int64_t offset = m_offset_store.get(group_id, q.topic, q.partition);
        res.offsets.push_back(offset);
    }
    res.status = Status::OK();
    res.error_code = 0;
    return res;
}

GroupDescription GroupCoordinator::describe(const std::string& group_id) {
    GroupDescription desc;
    std::shared_ptr<Group> group;
    {
        std::shared_lock<std::shared_mutex> groups_lock(m_groups_mutex);
        auto it = m_groups.find(group_id);
        if (it == m_groups.end()) {
            desc.status = Status::NotFound("Unknown group: " + group_id);
            desc.error_code = ErrorCode::UNKNOWN_GROUP_OR_MEMBER;
            return desc;
        }
        group = it->second;
    }

    std::lock_guard<std::mutex> group_lock(group->m_mutex);
    desc.status = Status::OK();
    desc.error_code = 0;
    desc.group_id = group->group_id;
    desc.generation = group->generation;
    desc.state = (group->state == GroupState::Empty ? "Empty" : "Stable");
    desc.strategy = static_cast<uint8_t>(group->strategy);

    auto now = m_clock();
    for (const auto& pair : group->members) {
        MemberDescription mem;
        mem.member_id = pair.first;
        mem.session_timeout_ms = pair.second.session_timeout_ms;
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - pair.second.last_heartbeat).count();
        mem.ms_since_heartbeat = static_cast<uint32_t>(elapsed >= 0 ? elapsed : 0);
        mem.assignment = pair.second.assignment;
        desc.members.push_back(std::move(mem));
    }

    return desc;
}

bool GroupCoordinator::debug_check_invariants(const std::string& group_id) {
    std::shared_ptr<Group> group;
    {
        std::shared_lock<std::shared_mutex> groups_lock(m_groups_mutex);
        auto it = m_groups.find(group_id);
        if (it == m_groups.end()) return true;
        group = it->second;
    }

    std::lock_guard<std::mutex> group_lock(group->m_mutex);
    if (group->members.empty()) {
        return group->state == GroupState::Empty;
    }

    // Every partition of every subscribed topic must be assigned to exactly one member
    for (const auto& topic_name : group->subscribed_topics) {
        auto topic = m_topic_mgr.get_topic(topic_name);
        if (!topic) return false;

        uint32_t num_parts = topic->num_partitions();
        for (uint16_t p = 0; p < num_parts; ++p) {
            size_t count = 0;
            for (const auto& pair : group->members) {
                for (const auto& tp : pair.second.assignment) {
                    if (tp.topic == topic_name && tp.partition == p) {
                        count++;
                    }
                }
            }
            if (count != 1) {
                return false;
            }
        }
    }

    // No member should have assignments for unknown topics or out-of-range partitions
    for (const auto& pair : group->members) {
        for (const auto& tp : pair.second.assignment) {
            auto topic = m_topic_mgr.get_topic(tp.topic);
            if (!topic) return false;
            if (tp.partition >= topic->num_partitions()) return false;
        }
    }

    return true;
}

} // namespace streamforge
