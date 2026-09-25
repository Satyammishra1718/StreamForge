#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include "streamforge/WinsockRuntime.hpp"
#include "streamforge/Socket.hpp"
#include "streamforge/FrameCodec.hpp"
#include "streamforge/ProtocolMessages.hpp"

#include <windows.h>
#include <tlhelp32.h>
#include <winioctl.h>

#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <iomanip>
#include <numeric>
#include <sstream>
#include <cstdlib>
#include <cstring>

using namespace streamforge;

// ─────────────────────────────────────────────────────────────────────────────
// System Information Detection (Win32)
// ─────────────────────────────────────────────────────────────────────────────
struct SystemInfo {
    std::string cpu_model{"unknown"};
    uint32_t logical_cores{0};
    uint64_t ram_bytes{0};
    std::string disk_type{"unknown"};
};

static SystemInfo detect_system_info(const std::string& data_dir_hint = "") {
    SystemInfo info;

    // 1. CPU Model from Registry
    HKEY hKey = nullptr;
    if (RegOpenKeyExW(HKEY_LOCAL_MACHINE, L"HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0", 0, KEY_READ, &hKey) == ERROR_SUCCESS) {
        wchar_t buf[256]{};
        DWORD bufSize = sizeof(buf);
        DWORD type = REG_SZ;
        if (RegQueryValueExW(hKey, L"ProcessorNameString", nullptr, &type, reinterpret_cast<LPBYTE>(buf), &bufSize) == ERROR_SUCCESS) {
            int len = WideCharToMultiByte(CP_UTF8, 0, buf, -1, nullptr, 0, nullptr, nullptr);
            if (len > 0) {
                std::string s(len - 1, '\0');
                WideCharToMultiByte(CP_UTF8, 0, buf, -1, &s[0], len, nullptr, nullptr);
                // Trim whitespace
                size_t first = s.find_first_not_of(" \t\r\n");
                size_t last = s.find_last_not_of(" \t\r\n");
                if (first != std::string::npos && last != std::string::npos) {
                    info.cpu_model = s.substr(first, (last - first + 1));
                } else {
                    info.cpu_model = s;
                }
            }
        }
        RegCloseKey(hKey);
    }

    // 2. Logical Cores
    info.logical_cores = std::thread::hardware_concurrency();

    // 3. RAM
    MEMORYSTATUSEX memState{};
    memState.dwLength = sizeof(memState);
    if (GlobalMemoryStatusEx(&memState)) {
        info.ram_bytes = memState.ullTotalPhys;
    }

    // 4. Disk Type (SSD vs HDD)
    (void)data_dir_hint;
    // Inspect volume path
    HANDLE hDrive = CreateFileW(L"\\\\.\\PhysicalDrive0", 0, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING, 0, nullptr);
    if (hDrive != INVALID_HANDLE_VALUE) {
        STORAGE_PROPERTY_QUERY query{};
        query.PropertyId = StorageDeviceSeekPenaltyProperty;
        query.QueryType = PropertyStandardQuery;

        DEVICE_SEEK_PENALTY_DESCRIPTOR seekDesc{};
        DWORD bytesReturned = 0;
        if (DeviceIoControl(hDrive, IOCTL_STORAGE_QUERY_PROPERTY, &query, sizeof(query),
                            &seekDesc, sizeof(seekDesc), &bytesReturned, nullptr)) {
            if (seekDesc.IncursSeekPenalty == FALSE) {
                info.disk_type = "SSD (Non-rotational / NVMe)";
            } else {
                info.disk_type = "HDD (Rotational)";
            }
        }
        CloseHandle(hDrive);
    }

    if (info.disk_type == "unknown") {
        // Fallback: Check drive volume letter
        wchar_t volPath[MAX_PATH] = L"\\\\.\\C:";
        HANDLE hVol = CreateFileW(volPath, 0, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING, 0, nullptr);
        if (hVol != INVALID_HANDLE_VALUE) {
            STORAGE_PROPERTY_QUERY query{};
            query.PropertyId = StorageDeviceSeekPenaltyProperty;
            query.QueryType = PropertyStandardQuery;
            DEVICE_SEEK_PENALTY_DESCRIPTOR seekDesc{};
            DWORD bytesReturned = 0;
            if (DeviceIoControl(hVol, IOCTL_STORAGE_QUERY_PROPERTY, &query, sizeof(query),
                                &seekDesc, sizeof(seekDesc), &bytesReturned, nullptr)) {
                if (seekDesc.IncursSeekPenalty == FALSE) {
                    info.disk_type = "SSD (Non-rotational)";
                } else {
                    info.disk_type = "HDD (Rotational)";
                }
            }
            CloseHandle(hVol);
        }
    }

    return info;
}

static void print_system_info(const SystemInfo& info) {
    double ram_gb = static_cast<double>(info.ram_bytes) / (1024.0 * 1024.0 * 1024.0);
    std::cout << "=====================================================================\n";
    std::cout << " StreamForge Performance Benchmark Runner\n";
    std::cout << "---------------------------------------------------------------------\n";
    std::cout << " CPU Model:     " << info.cpu_model << "\n";
    std::cout << " Logical Cores: " << info.logical_cores << "\n";
    std::cout << " Total Memory:  " << std::fixed << std::setprecision(2) << ram_gb << " GiB\n";
    std::cout << " Storage Media: " << info.disk_type << "\n";
    std::cout << "=====================================================================\n" << std::endl;
}

// ─────────────────────────────────────────────────────────────────────────────
// Socket Connect & Helper Functions
// ─────────────────────────────────────────────────────────────────────────────
static bool connect_socket(Socket& sock, const std::string& host, uint16_t port) {
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) return false;

    // Enable TCP_NODELAY for accurate latency measurement
    int flag = 1;
    ::setsockopt(s, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&flag), sizeof(flag));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        closesocket(s);
        return false;
    }
    if (::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        closesocket(s);
        return false;
    }
    sock = Socket(s);
    return true;
}

static bool read_response_frame(Socket& sock, Frame& out_frame) {
    uint8_t len_buf[4];
    if (sock.recv_exact(len_buf, 4) != RecvResult::Success) return false;
    uint32_t len = FrameCodec::read_u32(len_buf);
    if (len < HEADER_SIZE) return false;

    std::vector<uint8_t> payload(len);
    if (sock.recv_exact(payload.data(), len) != RecvResult::Success) return false;

    out_frame.length = len;
    out_frame.type = payload[0];
    out_frame.request_id = FrameCodec::read_u32(payload.data() + 1);
    if (len > HEADER_SIZE) {
        out_frame.body.assign(payload.begin() + HEADER_SIZE, payload.end());
    }
    return true;
}

static bool send_and_receive(Socket& sock, const Frame& req, Frame& resp) {
    std::vector<uint8_t> enc = FrameCodec::encode(req);
    if (sock.send_all(enc.data(), enc.size()) != SendResult::Success) return false;
    return read_response_frame(sock, resp);
}

// ─────────────────────────────────────────────────────────────────────────────
// Metrics & Statistics
// ─────────────────────────────────────────────────────────────────────────────
struct LatencyStats {
    size_t count{0};
    double p50{0.0};
    double p95{0.0};
    double p99{0.0};
    double p99_9{0.0};
    double max_ms{0.0};
    double min_ms{0.0};
    double avg_ms{0.0};
};

static LatencyStats compute_latency_stats(std::vector<double>& latencies) {
    LatencyStats stats;
    if (latencies.empty()) return stats;

    std::sort(latencies.begin(), latencies.end());
    stats.count = latencies.size();
    stats.min_ms = latencies.front();
    stats.max_ms = latencies.back();

    double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
    stats.avg_ms = sum / static_cast<double>(latencies.size());

    auto get_pct = [&](double pct) -> double {
        size_t idx = static_cast<size_t>(pct * static_cast<double>(latencies.size() - 1));
        return latencies[idx];
    };

    stats.p50 = get_pct(0.50);
    stats.p95 = get_pct(0.95);
    stats.p99 = get_pct(0.99);
    stats.p99_9 = get_pct(0.999);
    return stats;
}

static void print_metrics_table(const std::string& scenario_name,
                                uint64_t total_records,
                                uint64_t total_bytes,
                                double duration_sec,
                                LatencyStats stats) {
    double rec_sec = (duration_sec > 0.0) ? (static_cast<double>(total_records) / duration_sec) : 0.0;
    double mb_sec = (duration_sec > 0.0) ? (static_cast<double>(total_bytes) / (1024.0 * 1024.0) / duration_sec) : 0.0;

    std::cout << "\n---------------------------------------------------------------------\n";
    std::cout << " RESULTS: " << scenario_name << "\n";
    std::cout << "---------------------------------------------------------------------\n";
    std::cout << " Total Records:      " << total_records << "\n";
    std::cout << " Measurement Time:   " << std::fixed << std::setprecision(2) << duration_sec << " s\n";
    std::cout << " Throughput:         " << std::fixed << std::setprecision(0) << rec_sec << " records/sec\n";
    std::cout << " Data Rate:          " << std::fixed << std::setprecision(2) << mb_sec << " MB/sec\n";
    std::cout << " Latency Percentiles (ms):\n";
    std::cout << "   p50:   " << std::fixed << std::setprecision(3) << stats.p50 << " ms\n";
    std::cout << "   p95:   " << std::fixed << std::setprecision(3) << stats.p95 << " ms\n";
    std::cout << "   p99:   " << std::fixed << std::setprecision(3) << stats.p99 << " ms\n";
    std::cout << "   p99.9: " << std::fixed << std::setprecision(3) << stats.p99_9 << " ms\n";
    std::cout << "   Max:   " << std::fixed << std::setprecision(3) << stats.max_ms << " ms\n";
    std::cout << "   Min:   " << std::fixed << std::setprecision(3) << stats.min_ms << " ms\n";
    std::cout << "   Avg:   " << std::fixed << std::setprecision(3) << stats.avg_ms << " ms\n";
    std::cout << "---------------------------------------------------------------------\n" << std::endl;
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 1 & 2: Produce Benchmark (Single or Multi-Connection)
// ─────────────────────────────────────────────────────────────────────────────
static int run_produce_bench(const std::string& host, uint16_t port,
                             const std::string& topic, uint16_t num_partitions,
                             size_t record_size, size_t batch_size,
                             size_t num_connections, uint32_t duration_sec,
                             uint32_t warmup_sec, const std::string& label) {
    std::cout << "Starting Produce Benchmark [" << label << "]:\n"
              << "  Connections: " << num_connections << " | Partitions: " << num_partitions
              << " | Record Size: " << record_size << " B | Batch Size: " << batch_size
              << " | Duration: " << duration_sec << "s (warmup " << warmup_sec << "s)...\n";

    std::atomic<bool> stop_flag{false};
    std::atomic<bool> measurement_started{false};

    std::vector<std::thread> workers;
    workers.reserve(num_connections);

    struct ThreadResult {
        uint64_t records{0};
        uint64_t bytes{0};
        std::vector<double> latencies;
    };
    std::vector<ThreadResult> thread_results(num_connections);

    std::vector<uint8_t> payload_data(record_size, 'B');

    std::chrono::steady_clock::time_point measure_start{};

    for (size_t i = 0; i < num_connections; ++i) {
        workers.emplace_back([&, i]() {
            Socket sock;
            if (!connect_socket(sock, host, port)) {
                std::cerr << "Thread " << i << " failed to connect to " << host << ":" << port << "\n";
                return;
            }

            uint32_t req_id = static_cast<uint32_t>(i * 1000000 + 1);
            uint16_t target_partition = static_cast<uint16_t>(i % num_partitions);

            std::vector<double> local_lats;
            local_lats.reserve(50000);
            uint64_t local_recs = 0;
            uint64_t local_bytes = 0;

            ProduceRequest req_msg;
            req_msg.topic = topic;
            req_msg.partition = target_partition;
            req_msg.record_count = static_cast<uint32_t>(batch_size);
            req_msg.records.reserve(batch_size);
            for (size_t b = 0; b < batch_size; ++b) {
                req_msg.records.push_back({ {}, payload_data });
            }

            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();
            uint32_t frame_len = HEADER_SIZE + static_cast<uint32_t>(body.size());

            while (!stop_flag.load(std::memory_order_relaxed)) {
                Frame req_frame{ frame_len, MessageType::PRODUCE, req_id++, body };
                Frame resp_frame;

                auto t0 = std::chrono::high_resolution_clock::now();
                bool ok = send_and_receive(sock, req_frame, resp_frame);
                auto t1 = std::chrono::high_resolution_clock::now();

                if (!ok || resp_frame.type != MessageType::PRODUCE_OK) {
                    break;
                }

                if (measurement_started.load(std::memory_order_relaxed)) {
                    double lat_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
                    local_lats.push_back(lat_ms);
                    local_recs += batch_size;
                    local_bytes += (batch_size * record_size);
                }
            }

            thread_results[i].records = local_recs;
            thread_results[i].bytes = local_bytes;
            thread_results[i].latencies = std::move(local_lats);
        });
    }

    // Warmup period
    std::this_thread::sleep_for(std::chrono::seconds(warmup_sec));
    measure_start = std::chrono::steady_clock::now();
    measurement_started.store(true, std::memory_order_release);

    // Measurement period
    std::this_thread::sleep_for(std::chrono::seconds(duration_sec));
    stop_flag.store(true, std::memory_order_release);

    for (auto& w : workers) {
        if (w.joinable()) w.join();
    }
    auto measure_end = std::chrono::steady_clock::now();
    double actual_measure_sec = std::chrono::duration<double>(measure_end - measure_start).count();

    uint64_t grand_records = 0;
    uint64_t grand_bytes = 0;
    std::vector<double> all_latencies;

    for (const auto& tr : thread_results) {
        grand_records += tr.records;
        grand_bytes += tr.bytes;
        all_latencies.insert(all_latencies.end(), tr.latencies.begin(), tr.latencies.end());
    }

    LatencyStats stats = compute_latency_stats(all_latencies);
    print_metrics_table(label, grand_records, grand_bytes, actual_measure_sec, stats);
    return 0;
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 3: Fetch Throughput Benchmark
// ─────────────────────────────────────────────────────────────────────────────
static int run_fetch_bench(const std::string& host, uint16_t port,
                           const std::string& topic, uint16_t partition,
                           uint32_t max_bytes, uint32_t max_messages,
                           uint32_t duration_sec, uint32_t warmup_sec,
                           const std::string& label) {
    std::cout << "Starting Fetch Benchmark [" << label << "]:\n"
              << "  Topic: " << topic << " P" << partition
              << " | Max Bytes: " << max_bytes << " | Max Messages: " << max_messages
              << " | Duration: " << duration_sec << "s (warmup " << warmup_sec << "s)...\n";

    Socket sock;
    if (!connect_socket(sock, host, port)) {
        std::cerr << "Failed to connect to " << host << ":" << port << "\n";
        return 1;
    }

    std::atomic<bool> stop_flag{false};
    std::atomic<bool> measurement_started{false};

    uint64_t total_records = 0;
    uint64_t total_bytes = 0;
    std::vector<double> latencies;
    latencies.reserve(50000);

    std::thread worker([&]() {
        uint32_t req_id = 1;
        uint64_t cur_offset = 0;

        while (!stop_flag.load(std::memory_order_relaxed)) {
            FetchRequest req_msg{ topic, partition, cur_offset, max_bytes, max_messages };
            BodyWriter writer;
            req_msg.encode(writer);
            std::vector<uint8_t> body = writer.take_buffer();

            Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::FETCH, req_id++, body };
            Frame resp;

            auto t0 = std::chrono::high_resolution_clock::now();
            bool ok = send_and_receive(sock, req, resp);
            auto t1 = std::chrono::high_resolution_clock::now();

            if (!ok || resp.type != MessageType::FETCH_OK) {
                break;
            }

            BodyReader reader(resp.body);
            FetchResponse resp_msg;
            if (!resp_msg.decode(reader)) break;

            if (resp_msg.records.empty()) {
                // Loop back to beginning to sustain continuous throughput load
                cur_offset = 0;
                continue;
            }

            cur_offset = resp_msg.next_offset;

            if (measurement_started.load(std::memory_order_relaxed)) {
                double lat_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
                latencies.push_back(lat_ms);
                total_records += resp_msg.records.size();
                for (const auto& rec : resp_msg.records) {
                    total_bytes += (rec.key.size() + rec.value.size());
                }
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(warmup_sec));
    auto measure_start = std::chrono::steady_clock::now();
    measurement_started.store(true, std::memory_order_release);

    std::this_thread::sleep_for(std::chrono::seconds(duration_sec));
    stop_flag.store(true, std::memory_order_release);

    if (worker.joinable()) worker.join();
    auto measure_end = std::chrono::steady_clock::now();
    double actual_measure_sec = std::chrono::duration<double>(measure_end - measure_start).count();

    LatencyStats stats = compute_latency_stats(latencies);
    print_metrics_table(label, total_records, total_bytes, actual_measure_sec, stats);
    return 0;
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 4: Mixed Workload (Producers + Consumers + End-to-End Latency)
// ─────────────────────────────────────────────────────────────────────────────
static int run_mixed_bench(const std::string& host, uint16_t port,
                           const std::string& topic, uint16_t num_partitions,
                           size_t num_producers, size_t num_consumers,
                           const std::string& group_id, uint32_t duration_sec,
                           uint32_t warmup_sec) {
    std::cout << "Starting Mixed Workload Benchmark:\n"
              << "  Producers: " << num_producers << " | Consumers: " << num_consumers
              << " | Group: " << group_id << " | Topic: " << topic
              << " | Duration: " << duration_sec << "s (warmup " << warmup_sec << "s)...\n";

    std::atomic<bool> stop_flag{false};
    std::atomic<bool> measurement_started{false};

    std::atomic<uint64_t> producer_records{0};
    std::atomic<uint64_t> producer_bytes{0};
    std::atomic<uint64_t> consumer_records{0};
    std::atomic<uint64_t> consumer_bytes{0};

    std::mutex e2e_mutex;
    std::vector<double> e2e_latencies;
    e2e_latencies.reserve(50000);

    std::vector<std::thread> prod_threads;
    std::vector<std::thread> cons_threads;

    // 1. Launch Producers
    for (size_t p = 0; p < num_producers; ++p) {
        prod_threads.emplace_back([&, p]() {
            Socket sock;
            if (!connect_socket(sock, host, port)) return;

            uint32_t req_id = static_cast<uint32_t>(100000 + p * 10000);
            uint16_t target_part = static_cast<uint16_t>(p % num_partitions);

            std::vector<uint8_t> val(100, 'M');

            while (!stop_flag.load(std::memory_order_relaxed)) {
                // Embed send timestamp in value payload (first 8 bytes)
                int64_t send_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                std::memcpy(val.data(), &send_time_ms, sizeof(send_time_ms));

                ProduceRequest preq;
                preq.topic = topic;
                preq.partition = target_part;
                preq.record_count = 10;
                for (int b = 0; b < 10; ++b) {
                    preq.records.push_back({ {}, val });
                }

                BodyWriter w;
                preq.encode(w);
                std::vector<uint8_t> body = w.take_buffer();

                Frame req{ HEADER_SIZE + static_cast<uint32_t>(body.size()), MessageType::PRODUCE, req_id++, body };
                Frame resp;
                if (!send_and_receive(sock, req, resp) || resp.type != MessageType::PRODUCE_OK) {
                    break;
                }

                if (measurement_started.load(std::memory_order_relaxed)) {
                    producer_records.fetch_add(10, std::memory_order_relaxed);
                    producer_bytes.fetch_add(10 * val.size(), std::memory_order_relaxed);
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
        });
    }

    // 2. Launch Consumers (using JoinGroup & Fetch)
    for (size_t c = 0; c < num_consumers; ++c) {
        cons_threads.emplace_back([&, c]() {
            Socket sock;
            if (!connect_socket(sock, host, port)) return;

            uint32_t req_id = static_cast<uint32_t>(200000 + c * 10000);

            // Join group
            JoinGroupRequest jreq{ group_id, "", 10000, 0, 1, { topic } };
            BodyWriter jw;
            jreq.encode(jw);
            std::vector<uint8_t> jbody = jw.take_buffer();

            Frame jframe{ HEADER_SIZE + static_cast<uint32_t>(jbody.size()), MessageType::JOIN_GROUP, req_id++, jbody };
            Frame jresp;
            if (!send_and_receive(sock, jframe, jresp) || jresp.type != MessageType::JOIN_GROUP_OK) {
                return;
            }

            BodyReader jr(jresp.body);
            JoinGroupResponse jmsg;
            if (!jmsg.decode(jr)) return;

            std::string member_id = jmsg.member_id;
            std::vector<TopicPartitionWire> assignments = jmsg.assignments;

            std::vector<double> local_e2e;
            local_e2e.reserve(10000);

            std::map<uint16_t, uint64_t> part_positions;
            for (const auto& a : assignments) {
                part_positions[a.partition] = 0;
            }

            while (!stop_flag.load(std::memory_order_relaxed)) {
                for (const auto& a : assignments) {
                    uint64_t cur_pos = part_positions[a.partition];
                    FetchRequest freq{ a.topic, a.partition, cur_pos, 65536, 50 };
                    BodyWriter fw;
                    freq.encode(fw);
                    std::vector<uint8_t> fbody = fw.take_buffer();

                    Frame freq_frame{ HEADER_SIZE + static_cast<uint32_t>(fbody.size()), MessageType::FETCH, req_id++, fbody };
                    Frame fresp;
                    if (!send_and_receive(sock, freq_frame, fresp) || fresp.type != MessageType::FETCH_OK) {
                        break;
                    }

                    BodyReader fr(fresp.body);
                    FetchResponse resp_msg;
                    if (!resp_msg.decode(fr)) break;

                    if (!resp_msg.records.empty()) {
                        int64_t recv_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count();

                        for (const auto& rec : resp_msg.records) {
                            part_positions[a.partition] = rec.offset + 1;
                            if (measurement_started.load(std::memory_order_relaxed)) {
                                consumer_records.fetch_add(1, std::memory_order_relaxed);
                                consumer_bytes.fetch_add(rec.value.size(), std::memory_order_relaxed);

                                if (rec.value.size() >= sizeof(int64_t)) {
                                    int64_t send_time_ms = 0;
                                    std::memcpy(&send_time_ms, rec.value.data(), sizeof(send_time_ms));
                                    double delta_ms = static_cast<double>(recv_time_ms - send_time_ms);
                                    if (delta_ms >= 0.0 && delta_ms < 10000.0) {
                                        local_e2e.push_back(delta_ms);
                                    }
                                }
                            }
                        }
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }

            {
                std::lock_guard<std::mutex> lock(e2e_mutex);
                e2e_latencies.insert(e2e_latencies.end(), local_e2e.begin(), local_e2e.end());
            }

            // Clean leave
            LeaveGroupRequest lreq{ group_id, member_id };
            BodyWriter lw;
            lreq.encode(lw);
            std::vector<uint8_t> lbody = lw.take_buffer();
            Frame lframe{ HEADER_SIZE + static_cast<uint32_t>(lbody.size()), MessageType::LEAVE_GROUP, req_id++, lbody };
            Frame lresp;
            send_and_receive(sock, lframe, lresp);
        });
    }

    std::this_thread::sleep_for(std::chrono::seconds(warmup_sec));
    auto measure_start = std::chrono::steady_clock::now();
    measurement_started.store(true, std::memory_order_release);

    std::this_thread::sleep_for(std::chrono::seconds(duration_sec));
    stop_flag.store(true, std::memory_order_release);

    for (auto& t : prod_threads) if (t.joinable()) t.join();
    for (auto& t : cons_threads) if (t.joinable()) t.join();

    auto measure_end = std::chrono::steady_clock::now();
    double actual_measure_sec = std::chrono::duration<double>(measure_end - measure_start).count();

    uint64_t p_rec = producer_records.load();
    uint64_t c_rec = consumer_records.load();
    double p_rate = (actual_measure_sec > 0.0) ? (static_cast<double>(p_rec) / actual_measure_sec) : 0.0;
    double c_rate = (actual_measure_sec > 0.0) ? (static_cast<double>(c_rec) / actual_measure_sec) : 0.0;

    LatencyStats e2e_stats = compute_latency_stats(e2e_latencies);

    std::cout << "\n---------------------------------------------------------------------\n";
    std::cout << " RESULTS: Mixed Workload (Producers + Consumers)\n";
    std::cout << "---------------------------------------------------------------------\n";
    std::cout << " Producer Rate:      " << std::fixed << std::setprecision(0) << p_rate << " records/sec (" << p_rec << " total)\n";
    std::cout << " Consumer Rate:      " << std::fixed << std::setprecision(0) << c_rate << " records/sec (" << c_rec << " total)\n";
    std::cout << " End-to-End Latency (Produce -> Receive):\n";
    std::cout << "   p50:   " << std::fixed << std::setprecision(3) << e2e_stats.p50 << " ms\n";
    std::cout << "   p95:   " << std::fixed << std::setprecision(3) << e2e_stats.p95 << " ms\n";
    std::cout << "   p99:   " << std::fixed << std::setprecision(3) << e2e_stats.p99 << " ms\n";
    std::cout << "   p99.9: " << std::fixed << std::setprecision(3) << e2e_stats.p99_9 << " ms\n";
    std::cout << "   Max:   " << std::fixed << std::setprecision(3) << e2e_stats.max_ms << " ms\n";
    std::cout << "   Min:   " << std::fixed << std::setprecision(3) << e2e_stats.min_ms << " ms\n";
    std::cout << "   Avg:   " << std::fixed << std::setprecision(3) << e2e_stats.avg_ms << " ms\n";
    std::cout << "---------------------------------------------------------------------\n" << std::endl;
    return 0;
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 5: Connection Scaling Benchmark (Thread Count & Ping Latency)
// ─────────────────────────────────────────────────────────────────────────────
static DWORD get_server_thread_count_by_pid(DWORD pid) {
    HANDLE hSnap = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
    if (hSnap == INVALID_HANDLE_VALUE) return 0;

    THREADENTRY32 te{};
    te.dwSize = sizeof(te);
    DWORD count = 0;
    if (Thread32First(hSnap, &te)) {
        do {
            if (te.th32OwnerProcessID == pid) {
                count++;
            }
        } while (Thread32Next(hSnap, &te));
    }
    CloseHandle(hSnap);
    return count;
}

static int run_connection_scaling_bench(const std::string& host, uint16_t port,
                                        size_t num_connections, uint32_t duration_sec,
                                        DWORD server_pid) {
    std::cout << "Running Connection Scaling Benchmark: " << num_connections << " connections...\n";

    std::vector<Socket> sockets;
    sockets.reserve(num_connections);

    for (size_t i = 0; i < num_connections; ++i) {
        Socket s;
        if (!connect_socket(s, host, port)) {
            std::cerr << "Failed to connect socket " << i << " of " << num_connections << "\n";
            return 1;
        }
        sockets.push_back(std::move(s));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    DWORD max_thread_count = (server_pid > 0) ? get_server_thread_count_by_pid(server_pid) : 0;

    // Send PING requests and measure p99 ping latency
    std::vector<double> ping_lats;
    ping_lats.reserve(num_connections * 2);

    auto start = std::chrono::steady_clock::now();
    uint32_t req_id = 1;

    while (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start).count() < duration_sec) {
        if (server_pid > 0) {
            DWORD cur_t = get_server_thread_count_by_pid(server_pid);
            if (cur_t > max_thread_count) max_thread_count = cur_t;
        }
        for (auto& s : sockets) {
            Frame req{ HEADER_SIZE, MessageType::PING, req_id++, {} };
            Frame resp;
            auto t0 = std::chrono::high_resolution_clock::now();
            bool ok = send_and_receive(s, req, resp);
            auto t1 = std::chrono::high_resolution_clock::now();
            if (ok && resp.type == MessageType::PONG) {
                double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
                ping_lats.push_back(ms);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    LatencyStats stats = compute_latency_stats(ping_lats);

    std::cout << "---------------------------------------------------------------------\n";
    std::cout << " RESULTS: Connection Scaling (N = " << num_connections << ")\n";
    std::cout << "---------------------------------------------------------------------\n";
    std::cout << " Connected Sockets:   " << sockets.size() << "\n";
    if (server_pid > 0) {
        std::cout << " Server Thread Count: " << max_thread_count << "\n";
    }
    std::cout << " PING Latency Percentiles (ms):\n";
    std::cout << "   p50:   " << std::fixed << std::setprecision(3) << stats.p50 << " ms\n";
    std::cout << "   p95:   " << std::fixed << std::setprecision(3) << stats.p95 << " ms\n";
    std::cout << "   p99:   " << std::fixed << std::setprecision(3) << stats.p99 << " ms\n";
    std::cout << "   Max:   " << std::fixed << std::setprecision(3) << stats.max_ms << " ms\n";
    std::cout << "   Avg:   " << std::fixed << std::setprecision(3) << stats.avg_ms << " ms\n";
    std::cout << "---------------------------------------------------------------------\n" << std::endl;

    for (auto& s : sockets) s.close();
    return 0;
}

// ─────────────────────────────────────────────────────────────────────────────
// Main Entry Point & Command Dispatch
// ─────────────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    WinsockRuntime ws_runtime;
    if (!ws_runtime.is_initialized()) {
        std::cerr << "Failed to initialize Winsock\n";
        return 1;
    }

    if (argc < 2) {
        SystemInfo sys = detect_system_info();
        print_system_info(sys);
        std::cout << "Usage: streamforge_bench <subcommand> [options]\n\n"
                  << "Subcommands:\n"
                  << "  system-info                                 Print host hardware configuration\n"
                  << "  produce-throughput [options]                Measure produce throughput & latency\n"
                  << "  produce-scaling [options]                   Measure multi-connection produce scaling\n"
                  << "  fetch-throughput [options]                  Measure fetch throughput & batching\n"
                  << "  mixed-workload [options]                    Measure concurrent producers & consumers\n"
                  << "  connection-scaling [options]                Measure idle connection scaling & p99 ping\n\n"
                  << "Options:\n"
                  << "  --host HOST             Broker IP (default: 127.0.0.1)\n"
                  << "  --port PORT             Broker port (default: 9092)\n"
                  << "  --topic TOPIC           Target topic (default: bench_topic)\n"
                  << "  --partitions N          Number of partitions (default: 1)\n"
                  << "  --record-size BYTES     Record payload size (default: 100)\n"
                  << "  --batch-size N          Batch produce count (default: 50)\n"
                  << "  --connections N         Concurrent client connections (default: 1)\n"
                  << "  --producers N           Producer count in mixed workload (default: 4)\n"
                  << "  --consumers N           Consumer count in mixed workload (default: 4)\n"
                  << "  --group ID              Consumer group ID (default: bench_cg)\n"
                  << "  --duration-sec N        Measurement duration in seconds (default: 5)\n"
                  << "  --warmup-sec N          Warmup duration in seconds (default: 2)\n"
                  << "  --server-pid PID        Server PID for thread inspection\n";
        return 1;
    }

    std::string subcommand = argv[1];
    std::string host = "127.0.0.1";
    uint16_t port = 9092;
    std::string topic = "bench_topic";
    uint16_t partitions = 1;
    size_t record_size = 100;
    size_t batch_size = 50;
    size_t connections = 1;
    size_t producers = 4;
    size_t consumers = 4;
    std::string group = "bench_cg";
    uint32_t duration_sec = 5;
    uint32_t warmup_sec = 2;
    uint32_t max_bytes = 1048500;
    uint32_t max_messages = 500;
    DWORD server_pid = 0;
    std::string label = subcommand;

    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) host = argv[++i];
        else if (arg == "--port" && i + 1 < argc) port = static_cast<uint16_t>(std::atoi(argv[++i]));
        else if (arg == "--topic" && i + 1 < argc) topic = argv[++i];
        else if (arg == "--partitions" && i + 1 < argc) partitions = static_cast<uint16_t>(std::atoi(argv[++i]));
        else if (arg == "--record-size" && i + 1 < argc) record_size = std::stoull(argv[++i]);
        else if (arg == "--batch-size" && i + 1 < argc) batch_size = std::stoull(argv[++i]);
        else if (arg == "--connections" && i + 1 < argc) connections = std::stoull(argv[++i]);
        else if (arg == "--producers" && i + 1 < argc) producers = std::stoull(argv[++i]);
        else if (arg == "--consumers" && i + 1 < argc) consumers = std::stoull(argv[++i]);
        else if (arg == "--group" && i + 1 < argc) group = argv[++i];
        else if (arg == "--duration-sec" && i + 1 < argc) duration_sec = static_cast<uint32_t>(std::stoul(argv[++i]));
        else if (arg == "--warmup-sec" && i + 1 < argc) warmup_sec = static_cast<uint32_t>(std::stoul(argv[++i]));
        else if (arg == "--max-bytes" && i + 1 < argc) max_bytes = static_cast<uint32_t>(std::stoul(argv[++i]));
        else if (arg == "--max-messages" && i + 1 < argc) max_messages = static_cast<uint32_t>(std::stoul(argv[++i]));
        else if (arg == "--server-pid" && i + 1 < argc) server_pid = static_cast<DWORD>(std::stoul(argv[++i]));
        else if (arg == "--label" && i + 1 < argc) label = argv[++i];
    }

    SystemInfo sys = detect_system_info();
    print_system_info(sys);

    if (subcommand == "system-info") {
        return 0;
    } else if (subcommand == "produce-throughput" || subcommand == "produce-scaling") {
        return run_produce_bench(host, port, topic, partitions, record_size, batch_size,
                                 connections, duration_sec, warmup_sec, label);
    } else if (subcommand == "fetch-throughput") {
        return run_fetch_bench(host, port, topic, 0, max_bytes, max_messages,
                               duration_sec, warmup_sec, label);
    } else if (subcommand == "mixed-workload") {
        return run_mixed_bench(host, port, topic, partitions, producers, consumers,
                               group, duration_sec, warmup_sec);
    } else if (subcommand == "connection-scaling") {
        return run_connection_scaling_bench(host, port, connections, duration_sec, server_pid);
    } else {
        std::cerr << "Unknown subcommand: " << subcommand << "\n";
        return 1;
    }
}
