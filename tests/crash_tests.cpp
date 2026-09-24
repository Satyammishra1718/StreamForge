#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include "streamforge/Partition.hpp"
#include "streamforge/TopicManager.hpp"
#include "streamforge/OffsetStore.hpp"
#include "streamforge/Logger.hpp"
#include <windows.h>
#include <iostream>
#include <vector>
#include <string>
#include <filesystem>
#include <cstdlib>

using namespace streamforge;

static std::string get_self_exe_path() {
    wchar_t buf[MAX_PATH];
    GetModuleFileNameW(nullptr, buf, MAX_PATH);
    std::filesystem::path p(buf);
    return p.string();
}

static bool run_child_process(const std::string& exe_path, const std::string& label, const std::filesystem::path& data_dir, DWORD& out_exit_code) {
    std::string cmd = "\"" + exe_path + "\" --child " + label + " \"" + data_dir.string() + "\"";

    STARTUPINFOA si;
    PROCESS_INFORMATION pi;
    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);
    ZeroMemory(&pi, sizeof(pi));

    std::vector<char> cmd_buf(cmd.begin(), cmd.end());
    cmd_buf.push_back('\0');

    BOOL res = CreateProcessA(
        nullptr,
        cmd_buf.data(),
        nullptr,
        nullptr,
        FALSE,
        0,
        nullptr,
        nullptr,
        &si,
        &pi
    );

    if (!res) {
        std::cerr << "Failed to spawn child process: " << GetLastError() << "\n";
        return false;
    }

    WaitForSingleObject(pi.hProcess, 10000);
    GetExitCodeProcess(pi.hProcess, &out_exit_code);

    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);

    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// Child process implementation for each crash scenario
// ─────────────────────────────────────────────────────────────────────────────

static int run_child(const std::string& label, const std::filesystem::path& data_dir) {
    SetEnvironmentVariableA("STREAMFORGE_CRASH_AT", nullptr);

    StorageConfig cfg;
    cfg.data_dir = data_dir.string();
    cfg.segment_max_bytes = 200; // Small segment size to force rolls
    cfg.index_interval_bytes = 50; // Small interval to force index appends
    cfg.sync_on_append = true;

    if (label == "mid_write_record") {
        Partition part(0, data_dir / "part0", cfg);
        part.open_and_recover();

        // Append 5 records that are acknowledged
        for (int i = 0; i < 5; ++i) {
            std::string val = "ack_rec_" + std::to_string(i);
            part.append({}, {val.begin(), val.end()});
        }

        // Arm crash trigger for the 6th record
        SetEnvironmentVariableA("STREAMFORGE_CRASH_AT", "mid_write_record");
        std::string crash_val = "crash_payload_mid_write_record_test";
        part.append({}, {crash_val.begin(), crash_val.end()});

        return 0; // Should never be reached
    }

    if (label == "between_log_and_index") {
        cfg.segment_max_bytes = 10000;
        Partition part(0, data_dir / "part0", cfg);
        part.open_and_recover();

        // Append 3 acknowledged records
        for (int i = 0; i < 3; ++i) {
            std::string val = "ack_rec_" + std::to_string(i);
            part.append({}, {val.begin(), val.end()});
        }

        // Arm crash trigger for 4th record
        SetEnvironmentVariableA("STREAMFORGE_CRASH_AT", "between_log_and_index");
        std::string val = "crash_rec_between_log_and_index";
        part.append({}, {val.begin(), val.end()});

        return 0;
    }

    if (label == "between_seal_and_sealed") {
        Partition part(0, data_dir / "part0", cfg);
        part.open_and_recover();

        // Append 1st record of 150 bytes (fits in 200 byte segment)
        std::vector<uint8_t> payload(150, 'A');
        part.append({}, payload);

        // Arm crash trigger for rolling
        SetEnvironmentVariableA("STREAMFORGE_CRASH_AT", "between_seal_and_sealed");
        // 2nd record of 150 bytes exceeds 200 bytes and forces roll
        part.append({}, payload);

        return 0;
    }

    if (label == "mid_write_offset_commit") {
        TopicManager tm(cfg);
        tm.open_and_recover_all();

        OffsetStore os;
        os.open_and_recover(tm);

        // Commit 1: acknowledged
        os.commit("grp1", {{"orders", 0, 100, 1000}});
        // Commit 2: acknowledged
        os.commit("grp2", {{"payments", 1, 200, 2000}});

        // Arm crash trigger for 3rd commit
        SetEnvironmentVariableA("STREAMFORGE_CRASH_AT", "mid_write_offset_commit");
        os.commit("grp3", {{"inventory", 0, 300, 3000}});

        return 0;
    }

    std::cerr << "Unknown crash label: " << label << "\n";
    return 2;
}

// ─────────────────────────────────────────────────────────────────────────────
// Parent verification tests
// ─────────────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    Logger::instance().set_level(LogLevel::WARNING);

    if (argc >= 4 && std::string(argv[1]) == "--child") {
        std::string label = argv[2];
        std::filesystem::path data_dir = argv[3];
        return run_child(label, data_dir);
    }

    std::string exe_path = get_self_exe_path();
    std::cout << "==========================================================\n";
    std::cout << "Starting StreamForge Crash Injection Recovery Test Suite\n";
    std::cout << "==========================================================\n";

    int passed = 0;
    int failed = 0;

    auto test_label = [&](const std::string& label, auto verify_func) {
        std::cout << "\n--- TEST: Crash Injection at '" << label << "' ---\n";
        std::filesystem::path test_dir = std::filesystem::current_path() / ("temp_crash_test_" + label);
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);

        DWORD exit_code = 0;
        bool ok = run_child_process(exe_path, label, test_dir, exit_code);
        if (!ok || exit_code != 1) {
            std::cout << "[FAIL] Child process did not exit with crash code 1 (got " << exit_code << ")\n";
            failed++;
            std::filesystem::remove_all(test_dir);
            return;
        }

        std::cout << "Child process terminated at crash point '" << label << "' as expected (exit code 1).\n";

        // Parent restarts and verifies data integrity
        bool pass = verify_func(test_dir);
        if (pass) {
            std::cout << "[PASS] Recovery verified cleanly for crash point '" << label << "'\n";
            passed++;
        } else {
            std::cout << "[FAIL] Verification failed for crash point '" << label << "'\n";
            failed++;
        }

        std::filesystem::remove_all(test_dir);
    };

    // 1. mid_write_record
    test_label("mid_write_record", [](const std::filesystem::path& dir) {
        StorageConfig cfg;
        cfg.data_dir = dir.string();
        cfg.segment_max_bytes = 200;
        cfg.sync_on_append = true;

        Partition part(0, dir / "part0", cfg);
        Status st = part.open_and_recover();
        if (!st.ok()) {
            std::cerr << "open_and_recover failed: " << st.message() << "\n";
            return false;
        }

        // All 5 acknowledged records must be present
        ReadResult rr = part.read(0, 100);
        if (!rr.status.ok() || rr.records.size() != 5) {
            std::cerr << "Expected 5 records, got " << rr.records.size() << "\n";
            return false;
        }

        for (uint64_t i = 0; i < 5; ++i) {
            if (rr.records[i].offset != i) {
                std::cerr << "Record offset mismatch at " << i << "\n";
                return false;
            }
        }

        // Partition must be immediately usable for new appends
        auto res = part.append({}, {'N', 'E', 'W'});
        if (!res.ok() || res.value() != 5) {
            std::cerr << "New append failed or assigned wrong offset: " << res.status().message() << "\n";
            return false;
        }

        return true;
    });

    // 2. between_log_and_index
    test_label("between_log_and_index", [](const std::filesystem::path& dir) {
        StorageConfig cfg;
        cfg.data_dir = dir.string();
        cfg.segment_max_bytes = 10000;
        cfg.index_interval_bytes = 50;

        Partition part(0, dir / "part0", cfg);
        Status st = part.open_and_recover();
        if (!st.ok()) {
            std::cerr << "open_and_recover failed: " << st.message() << "\n";
            return false;
        }

        // The 4 records are valid and recovered, index rebuilt
        ReadResult rr = part.read(0, 100);
        if (!rr.status.ok() || rr.records.size() != 4) {
            std::cerr << "Expected 4 records, got " << rr.records.size() << "\n";
            return false;
        }

        auto res = part.append({}, {'O', 'K'});
        return res.ok() && res.value() == 4;
    });

    // 3. between_seal_and_sealed
    test_label("between_seal_and_sealed", [](const std::filesystem::path& dir) {
        StorageConfig cfg;
        cfg.data_dir = dir.string();
        cfg.segment_max_bytes = 200;

        Partition part(0, dir / "part0", cfg);
        Status st = part.open_and_recover();
        if (!st.ok()) {
            std::cerr << "open_and_recover failed: " << st.message() << "\n";
            return false;
        }

        // Partition should have recovered the missing .sealed marker
        std::string base0_str = LogSegment::format_base_offset(0);
        std::filesystem::path sealed_marker = dir / "part0" / (base0_str + ".sealed");
        if (!std::filesystem::exists(sealed_marker)) {
            std::cerr << "Missing .sealed marker was not recreated during recovery!\n";
            return false;
        }

        ReadResult rr = part.read(0, 100);
        if (!rr.status.ok() || rr.records.empty()) {
            std::cerr << "Failed to read records after recovery: " << rr.status.message() << "\n";
            return false;
        }

        auto res = part.append({}, {'R', 'O', 'L', 'L'});
        return res.ok();
    });

    // 4. mid_write_offset_commit
    test_label("mid_write_offset_commit", [](const std::filesystem::path& dir) {
        StorageConfig cfg;
        cfg.data_dir = dir.string();

        TopicManager tm(cfg);
        Status st = tm.open_and_recover_all();
        if (!st.ok()) {
            std::cerr << "TopicManager recovery failed: " << st.message() << "\n";
            return false;
        }

        OffsetStore os;
        st = os.open_and_recover(tm);
        if (!st.ok()) {
            std::cerr << "OffsetStore recovery failed: " << st.message() << "\n";
            return false;
        }

        // Acknowledged commits must be intact
        int64_t off1 = os.get("grp1", "orders", 0);
        int64_t off2 = os.get("grp2", "payments", 1);
        if (off1 != 100 || off2 != 200) {
            std::cerr << "Offset mismatch: grp1=" << off1 << ", grp2=" << off2 << "\n";
            return false;
        }

        // New commits must succeed
        Status cst = os.commit("grp_new", {{"deliveries", 0, 50, 5000}});
        return cst.ok() && os.get("grp_new", "deliveries", 0) == 50;
    });

    std::cout << "\n==========================================================\n";
    std::cout << "CRASH TEST SUMMARY\n";
    std::cout << "Passed: " << passed << "\n";
    std::cout << "Failed: " << failed << "\n";
    std::cout << "==========================================================\n";

    return (failed == 0) ? 0 : 1;
}
