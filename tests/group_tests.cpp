#include "TestFramework.hpp"
#include "streamforge/Assignor.hpp"
#include "streamforge/GroupCoordinator.hpp"
#include "streamforge/OffsetStore.hpp"
#include "streamforge/TopicManager.hpp"
#include <filesystem>
#include <vector>
#include <string>
#include <chrono>
#include <thread>
#include <atomic>
#include <random>

using namespace streamforge;

static std::atomic<uint64_t> g_dir_seq{1};

static std::filesystem::path create_temp_dir(const std::string& name) {
    auto id = std::chrono::steady_clock::now().time_since_epoch().count();
    std::filesystem::path p = std::filesystem::current_path() / ("tmp_grp_" + name + "_" + std::to_string(id) + "_" + std::to_string(g_dir_seq++));
    std::error_code ec;
    std::filesystem::create_directories(p, ec);
    return p;
}

static void cleanup_temp_dir(const std::filesystem::path& p) {
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Assignor Tests
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(assignor_strategies_comprehensive) {
    RangeAssignor range_assignor;
    RoundRobinAssignor rr_assignor;

    struct TestCase {
        size_t num_members;
        std::vector<std::pair<std::string, uint16_t>> topics;
    };

    std::vector<TestCase> cases = {
        // More members than partitions
        { 5, { {"topicA", 3} } },
        // 1 member
        { 1, { {"topicA", 5} } },
        // Equal counts
        { 4, { {"topicA", 4} } },
        // 7 partitions / 3 members
        { 3, { {"topicA", 7} } },
        // Multiple topics
        { 3, { {"topicA", 3}, {"topicB", 4} } },
        // Zero partitions edge case
        { 2, { {"emptyTopic", 0} } }
    };

    for (const auto& tc : cases) {
        std::vector<std::string> members;
        for (size_t m = 0; m < tc.num_members; ++m) {
            members.push_back("member-" + std::to_string(m));
        }

        // Test RangeAssignor
        auto res1 = range_assignor.assign(members, tc.topics);
        auto res1_repeat = range_assignor.assign(members, tc.topics);
        CHECK_TRUE(res1 == res1_repeat); // Deterministic

        // Test RoundRobinAssignor
        auto res2 = rr_assignor.assign(members, tc.topics);
        auto res2_repeat = rr_assignor.assign(members, tc.topics);
        CHECK_TRUE(res2 == res2_repeat); // Deterministic

        // Verify every partition is assigned exactly once
        for (const auto& assign_map : {res1, res2}) {
            for (const auto& tp : tc.topics) {
                for (uint16_t p = 0; p < tp.second; ++p) {
                    size_t count = 0;
                    for (const auto& member_pair : assign_map) {
                        for (const auto& assigned_tp : member_pair.second) {
                            if (assigned_tp.topic == tp.first && assigned_tp.partition == p) {
                                count++;
                            }
                        }
                    }
                    CHECK_EQ(count, 1u);
                }
            }
        }

        // If single topic, verify load differs by at most 1 across members
        if (tc.topics.size() == 1) {
            size_t min_load = 999999, max_load = 0;
            for (const auto& pair : res1) {
                min_load = std::min(min_load, pair.second.size());
                max_load = std::max(max_load, pair.second.size());
            }
            CHECK_TRUE(max_load - min_load <= 1);

            min_load = 999999; max_load = 0;
            for (const auto& pair : res2) {
                min_load = std::min(min_load, pair.second.size());
                max_load = std::max(max_load, pair.second.size());
            }
            CHECK_TRUE(max_load - min_load <= 1);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Join / Leave / Re-join Generation Bumping
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(join_leave_generation_and_rejoin) {
    auto dir = create_temp_dir("join_leave");
    {
        StorageConfig sc;
        sc.data_dir = dir.string();
        TopicManager tm(sc);
        CHECK_TRUE(tm.open_and_recover_all().ok());
        CHECK_TRUE(tm.create_topic("topic_1", 4).ok());

        OffsetStore os;
        CHECK_TRUE(os.open_and_recover(tm).ok());

        auto simulated_now = std::chrono::steady_clock::time_point(std::chrono::milliseconds(1000000));
        Clock fake_clock = [&simulated_now]() { return simulated_now; };

        GroupCoordinator coord(tm, os, CoordinatorConfig{}, fake_clock);

        // Initial join with empty member ID creates member and sets gen=1
        auto j1 = coord.join("grpA", "", 3000, {"topic_1"}, AssignorStrategy::Range);
        CHECK_TRUE(j1.status.ok());
        CHECK_EQ(j1.generation, 1u);
        CHECK_FALSE(j1.member_id.empty());
        CHECK_EQ(j1.assignment.size(), 4u); // Owns all 4 partitions

        // Second member joins -> bumps gen to 2, rebalances 2 + 2
        auto j2 = coord.join("grpA", "", 3000, {"topic_1"}, AssignorStrategy::Range);
        CHECK_TRUE(j2.status.ok());
        CHECK_EQ(j2.generation, 2u);
        CHECK_EQ(j2.assignment.size(), 2u);

        // Re-join with existing known member ID -> does NOT bump gen, returns CURRENT gen=2
        auto j1_rejoin = coord.join("grpA", j1.member_id, 3000, {"topic_1"}, AssignorStrategy::Range);
        CHECK_TRUE(j1_rejoin.status.ok());
        CHECK_EQ(j1_rejoin.generation, 2u);
        CHECK_EQ(j1_rejoin.assignment.size(), 2u);

        // Leave -> bumps gen to 3, survivor gets all 4 partitions
        auto l2 = coord.leave("grpA", j2.member_id);
        CHECK_TRUE(l2.status.ok());

        auto j1_after_leave = coord.join("grpA", j1.member_id, 3000, {"topic_1"}, AssignorStrategy::Range);
        CHECK_EQ(j1_after_leave.generation, 3u);
        CHECK_EQ(j1_after_leave.assignment.size(), 4u);

        // Unknown member ID in join -> error 14
        auto j_bad = coord.join("grpA", "non-existent-member", 3000, {"topic_1"}, AssignorStrategy::Range);
        CHECK_FALSE(j_bad.status.ok());
        CHECK_EQ(j_bad.error_code, ErrorCode::UNKNOWN_GROUP_OR_MEMBER);
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Session Timeout & Member Expiration
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(session_timeout_and_expiration) {
    auto dir = create_temp_dir("expiration");
    {
        StorageConfig sc;
        sc.data_dir = dir.string();
        TopicManager tm(sc);
        CHECK_TRUE(tm.open_and_recover_all().ok());
        CHECK_TRUE(tm.create_topic("topic_2", 4).ok());

        OffsetStore os;
        CHECK_TRUE(os.open_and_recover(tm).ok());

        auto simulated_now = std::chrono::steady_clock::time_point(std::chrono::milliseconds(1000000));
        Clock fake_clock = [&simulated_now]() { return simulated_now; };

        GroupCoordinator coord(tm, os, CoordinatorConfig{}, fake_clock);

        auto m1 = coord.join("grp_exp", "", 2000, {"topic_2"}, AssignorStrategy::Range);
        auto m2 = coord.join("grp_exp", "", 4000, {"topic_2"}, AssignorStrategy::Range);
        CHECK_EQ(m2.generation, 2u);

        // Advance time by 2.5 seconds (m1 expires, m2 still active)
        simulated_now += std::chrono::milliseconds(2500);

        size_t expired = coord.expire_members();
        CHECK_EQ(expired, 1u);

        auto desc = coord.describe("grp_exp");
        CHECK_EQ(desc.generation, 3u);
        CHECK_EQ(desc.members.size(), 1u);
        CHECK_EQ(desc.members[0].member_id, m2.member_id);
        CHECK_EQ(desc.members[0].assignment.size(), 4u); // Rebalanced all 4 to survivor

        // Heartbeat resets timer
        simulated_now += std::chrono::milliseconds(3000);
        auto hb = coord.heartbeat("grp_exp", m2.member_id, 3);
        CHECK_TRUE(hb.status.ok());

        simulated_now += std::chrono::milliseconds(2000);
        // Even though 5 seconds passed since m2 joined, heartbeat kept it alive
        expired = coord.expire_members();
        CHECK_EQ(expired, 0u);

        // Now let it time out
        simulated_now += std::chrono::milliseconds(5000);
        expired = coord.expire_members();
        CHECK_EQ(expired, 1u);

        desc = coord.describe("grp_exp");
        CHECK_EQ(desc.state, "Empty");
        CHECK_EQ(desc.members.size(), 0u);
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Commit Fencing Rules
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(commit_fencing_semantics) {
    auto dir = create_temp_dir("commit_fencing");
    {
        StorageConfig sc;
        sc.data_dir = dir.string();
        TopicManager tm(sc);
        CHECK_TRUE(tm.open_and_recover_all().ok());
        auto top = tm.create_topic("topic_fence", 4).value();
        top->append_to(0, {'k'}, {'v'}); // Partition 0 gets high_watermark = 1

        OffsetStore os;
        CHECK_TRUE(os.open_and_recover(tm).ok());

        auto simulated_now = std::chrono::steady_clock::time_point(std::chrono::milliseconds(1000000));
        Clock fake_clock = [&simulated_now]() { return simulated_now; };

        GroupCoordinator coord(tm, os, CoordinatorConfig{}, fake_clock);

        // Standalone commit allowed when group is empty
        auto c_sa = coord.commit("grp_fence", "", 0xFFFFFFFF, { {"topic_fence", 0, 1} });
        CHECK_TRUE(c_sa.status.ok());

        auto j1 = coord.join("grp_fence", "", 5000, {"topic_fence"}, AssignorStrategy::Range);
        auto j2 = coord.join("grp_fence", "", 5000, {"topic_fence"}, AssignorStrategy::Range);
        // gen is now 2. j1 has partitions 0,1; j2 has partitions 2,3

        // Standalone commit rejected with error 15 when group has members
        auto c_sa_active = coord.commit("grp_fence", "", 0xFFFFFFFF, { {"topic_fence", 0, 1} });
        CHECK_FALSE(c_sa_active.status.ok());
        CHECK_EQ(c_sa_active.error_code, ErrorCode::ILLEGAL_GENERATION);

        // Stale generation commit (gen 1 on gen 2 group) -> error 15
        auto c_stale = coord.commit("grp_fence", j1.member_id, 1, { {"topic_fence", 0, 1} });
        CHECK_FALSE(c_stale.status.ok());
        CHECK_EQ(c_stale.error_code, ErrorCode::ILLEGAL_GENERATION);

        // Partition not assigned to member (j1 trying to commit P2 owned by j2) -> error 16
        auto c_wrong_part = coord.commit("grp_fence", j1.member_id, 2, { {"topic_fence", 2, 0} });
        CHECK_FALSE(c_wrong_part.status.ok());
        CHECK_EQ(c_wrong_part.error_code, ErrorCode::PARTITION_NOT_ASSIGNED);

        // Unknown member ID -> error 14
        auto c_unknown = coord.commit("grp_fence", "zombie-id", 2, { {"topic_fence", 0, 1} });
        CHECK_FALSE(c_unknown.status.ok());
        CHECK_EQ(c_unknown.error_code, ErrorCode::UNKNOWN_GROUP_OR_MEMBER);

        // Offset above high watermark -> error 8 (P0 next_offset is 1, trying to commit 99)
        auto c_oob = coord.commit("grp_fence", j1.member_id, 2, { {"topic_fence", 0, 99} });
        CHECK_FALSE(c_oob.status.ok());
        CHECK_EQ(c_oob.error_code, ErrorCode::OFFSET_OUT_OF_RANGE);

        // Valid commit -> OK
        auto c_ok = coord.commit("grp_fence", j1.member_id, 2, { {"topic_fence", 0, 1} });
        CHECK_TRUE(c_ok.status.ok());

        auto fo = coord.fetch_offsets("grp_fence", { {"topic_fence", 0}, {"topic_fence", 1} });
        CHECK_TRUE(fo.status.ok());
        CHECK_EQ(fo.offsets[0], 1);
        CHECK_EQ(fo.offsets[1], -1); // Nothing committed yet for P1
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. Validation Rules and Limits
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(validation_rules_and_limits) {
    auto dir = create_temp_dir("validation");
    {
        StorageConfig sc;
        sc.data_dir = dir.string();
        TopicManager tm(sc);
        CHECK_TRUE(tm.open_and_recover_all().ok());
        CHECK_TRUE(tm.create_topic("topic_val", 2).ok());
        CHECK_TRUE(tm.create_topic("topic_other", 2).ok());

        OffsetStore os;
        CHECK_TRUE(os.open_and_recover(tm).ok());

        CoordinatorConfig cfg;
        cfg.min_session_timeout_ms = 2000;
        cfg.max_session_timeout_ms = 10000;
        cfg.max_members_per_group = 2;
        cfg.max_groups = 1;

        GroupCoordinator coord(tm, os, cfg);

        // Session timeout out of bounds (< min) -> error 13
        auto j_low = coord.join("grp1", "", 1000, {"topic_val"}, AssignorStrategy::Range);
        CHECK_EQ(j_low.error_code, ErrorCode::INVALID_ARGUMENT);

        // Unknown topic -> error 4
        auto j_unknown_topic = coord.join("grp1", "", 3000, {"non_existent_topic"}, AssignorStrategy::Range);
        CHECK_EQ(j_unknown_topic.error_code, ErrorCode::UNKNOWN_TOPIC);

        // Reserved topic -> error 6
        auto j_reserved_topic = coord.join("grp1", "", 3000, {"__consumer_offsets"}, AssignorStrategy::Range);
        CHECK_EQ(j_reserved_topic.error_code, ErrorCode::INVALID_TOPIC_NAME);

        // First member establishes group settings
        auto j1 = coord.join("grp1", "", 3000, {"topic_val"}, AssignorStrategy::Range);
        CHECK_TRUE(j1.status.ok());

        // Subscription mismatch -> error 13
        auto j_sub_mismatch = coord.join("grp1", "", 3000, {"topic_other"}, AssignorStrategy::Range);
        CHECK_EQ(j_sub_mismatch.error_code, ErrorCode::INVALID_ARGUMENT);

        // Strategy mismatch -> error 13
        auto j_strat_mismatch = coord.join("grp1", "", 3000, {"topic_val"}, AssignorStrategy::RoundRobin);
        CHECK_EQ(j_strat_mismatch.error_code, ErrorCode::INVALID_ARGUMENT);

        // Join second member up to limit of 2
        auto j2 = coord.join("grp1", "", 3000, {"topic_val"}, AssignorStrategy::Range);
        CHECK_TRUE(j2.status.ok());

        // Third member exceeds max_members_per_group -> error 13
        auto j3 = coord.join("grp1", "", 3000, {"topic_val"}, AssignorStrategy::Range);
        CHECK_EQ(j3.error_code, ErrorCode::INVALID_ARGUMENT);

        // Second group exceeds max_groups limit of 1 -> error 13
        auto j_grp2 = coord.join("grp2", "", 3000, {"topic_val"}, AssignorStrategy::Range);
        CHECK_EQ(j_grp2.error_code, ErrorCode::INVALID_ARGUMENT);
    }
    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. OffsetStore Replay, Persistence, and Torn Tail Recovery
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(offset_store_persistence_and_replay) {
    auto dir = create_temp_dir("offset_store");
    StorageConfig sc;
    sc.data_dir = dir.string();
    sc.sync_on_append = true;

    {
        TopicManager tm(sc);
        CHECK_TRUE(tm.open_and_recover_all().ok());
        OffsetStore os;
        CHECK_TRUE(os.open_and_recover(tm).ok());

        // Commit multiple entries in one batch
        std::vector<OffsetCommitItem> batch = {
            {"orders", 0, 100, 12345},
            {"orders", 1, 200, 12345},
            {"orders", 2, 300, 12345}
        };
        CHECK_TRUE(os.commit("grpA", batch).ok());
        CHECK_EQ(os.get("grpA", "orders", 0), 100);
        CHECK_EQ(os.get("grpA", "orders", 1), 200);
        CHECK_EQ(os.get("grpA", "orders", 2), 300);

        // Last write wins
        CHECK_TRUE(os.commit("grpA", { {"orders", 0, 150, 12399} }).ok());
        CHECK_EQ(os.get("grpA", "orders", 0), 150);
    }

    // Reopen and recover
    {
        TopicManager tm(sc);
        CHECK_TRUE(tm.open_and_recover_all().ok());
        OffsetStore os;
        CHECK_TRUE(os.open_and_recover(tm).ok());

        CHECK_EQ(os.get("grpA", "orders", 0), 150);
        CHECK_EQ(os.get("grpA", "orders", 1), 200);
        CHECK_EQ(os.get("grpA", "orders", 2), 300);
        CHECK_EQ(os.get("grpA", "orders", 3), -1);
    }

    cleanup_temp_dir(dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. Concurrency Stress Test with Watchdog
// ─────────────────────────────────────────────────────────────────────────────
TEST_CASE(concurrency_stress_and_invariants) {
    auto dir = create_temp_dir("stress");
    {
        StorageConfig sc;
        sc.data_dir = dir.string();
        TopicManager tm(sc);
        CHECK_TRUE(tm.open_and_recover_all().ok());
        auto topic = tm.create_topic("stress_topic", 8).value();
        for (int i = 0; i < 100; ++i) {
            topic->append_to(static_cast<uint32_t>(i % 8), {'k'}, {'v'});
        }

        OffsetStore os;
        CHECK_TRUE(os.open_and_recover(tm).ok());

        GroupCoordinator coord(tm, os);

        std::atomic<bool> running{true};
        std::atomic<int> errors{0};

        // Watchdog thread: fail test if running > 20s
        std::thread watchdog([&]() {
            auto start = std::chrono::steady_clock::now();
            while (running.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start).count() >= 20) {
                    errors++;
                    running = false;
                    std::cerr << "Watchdog timeout: concurrency stress test exceeded 20 seconds!" << std::endl;
                    break;
                }
            }
        });

        constexpr int NUM_THREADS = 8;
        std::vector<std::thread> workers;

        for (int t = 0; t < NUM_THREADS; ++t) {
            workers.emplace_back([&, t]() {
                std::mt19937 rng(1337 + t);
                std::string member_id = "";
                uint32_t gen = 0;

                while (running.load()) {
                    int op = rng() % 5;
                    if (member_id.empty() || op == 0) {
                        auto j = coord.join("stress_group", member_id, 3000, {"stress_topic"}, AssignorStrategy::Range);
                        if (j.status.ok()) {
                            member_id = j.member_id;
                            gen = j.generation;
                        } else if (j.error_code == ErrorCode::UNKNOWN_GROUP_OR_MEMBER) {
                            member_id.clear();
                        }
                    } else if (op == 1) {
                        auto hb = coord.heartbeat("stress_group", member_id, gen);
                        if (hb.error_code == ErrorCode::ILLEGAL_GENERATION || hb.error_code == ErrorCode::UNKNOWN_GROUP_OR_MEMBER) {
                            // Stale -> re-join next loop
                            auto rj = coord.join("stress_group", member_id, 3000, {"stress_topic"}, AssignorStrategy::Range);
                            if (rj.status.ok()) {
                                gen = rj.generation;
                            } else {
                                member_id.clear();
                            }
                        }
                    } else if (op == 2) {
                        coord.describe("stress_group");
                    } else if (op == 3) {
                        coord.commit("stress_group", member_id, gen, { {"stress_topic", static_cast<uint16_t>(t % 8), 10} });
                    } else if (op == 4) {
                        coord.leave("stress_group", member_id);
                        member_id.clear();
                    }

                    // Verify group assignment invariants periodically
                    if (!coord.debug_check_invariants("stress_group")) {
                        errors++;
                    }

                    std::this_thread::yield();
                }
            });
        }

        // Run for 2 seconds
        std::this_thread::sleep_for(std::chrono::seconds(2));
        running = false;

        for (auto& th : workers) {
            if (th.joinable()) th.join();
        }
        if (watchdog.joinable()) watchdog.join();

        CHECK_EQ(errors.load(), 0);
    }
    cleanup_temp_dir(dir);
}

int main() {
    ::streamforge::test::TestRegistry::instance().set_suite_title("StreamForge Group Coordinator Unit Tests");
    return ::streamforge::test::TestRegistry::instance().run_all();
}
