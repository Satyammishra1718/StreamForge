#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include "streamforge/TcpServer.hpp"
#include "streamforge/TopicManager.hpp"
#include "streamforge/OffsetStore.hpp"
#include "streamforge/GroupCoordinator.hpp"
#include "streamforge/RetentionManager.hpp"
#include "streamforge/Logger.hpp"
#include <windows.h>

#include <iostream>
#include <string>
#include <cstdlib>

static streamforge::TcpServer* g_server_instance = nullptr;

BOOL WINAPI console_ctrl_handler(DWORD ctrl_type) {
    switch (ctrl_type) {
        case CTRL_C_EVENT:
        case CTRL_BREAK_EVENT:
        case CTRL_CLOSE_EVENT:
            streamforge::Logger::instance().info("Shutdown signal received, stopping server...");
            if (g_server_instance) {
                g_server_instance->request_stop();
            }
            return TRUE;
        default:
            return FALSE;
    }
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = 9092;
    std::string data_dir = "./data";
    uint32_t segment_bytes = streamforge::DEFAULT_SEGMENT_MAX_BYTES;
    bool sync_on_append = true;
    std::string log_level_str = "INFO";
    size_t workers = std::max(2u, std::thread::hardware_concurrency());
    size_t max_connections = 1024;
    uint32_t read_stall_timeout_sec = 30;
    size_t max_output_buffer_bytes = 8 * 1024 * 1024; // 8 MiB
    uint32_t group_min_session_ms = 1000;
    uint32_t group_max_session_ms = 60000;
    uint32_t reaper_interval_ms = 500;
    uint64_t default_retention_ms = streamforge::DEFAULT_RETENTION_MS;
    uint64_t default_retention_bytes = streamforge::DEFAULT_RETENTION_BYTES;
    uint64_t retention_check_interval_ms = streamforge::DEFAULT_RETENTION_CHECK_INTERVAL_MS;
    std::string startup_scan = "quick";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (arg == "--data-dir" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg == "--segment-bytes" && i + 1 < argc) {
            segment_bytes = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--sync-on-append" && i + 1 < argc) {
            std::string val = argv[++i];
            sync_on_append = (val == "true" || val == "1");
        } else if (arg == "--log-level" && i + 1 < argc) {
            log_level_str = argv[++i];
        } else if (arg == "--workers" && i + 1 < argc) {
            workers = static_cast<size_t>(std::stoul(argv[++i]));
        } else if (arg == "--max-connections" && i + 1 < argc) {
            max_connections = static_cast<size_t>(std::stoul(argv[++i]));
        } else if (arg == "--read-stall-timeout-sec" && i + 1 < argc) {
            read_stall_timeout_sec = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--max-output-buffer-bytes" && i + 1 < argc) {
            max_output_buffer_bytes = static_cast<size_t>(std::stoull(argv[++i]));
        } else if (arg == "--group-min-session-ms" && i + 1 < argc) {
            group_min_session_ms = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--group-max-session-ms" && i + 1 < argc) {
            group_max_session_ms = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--reaper-interval-ms" && i + 1 < argc) {
            reaper_interval_ms = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--default-retention-ms" && i + 1 < argc) {
            default_retention_ms = std::stoull(argv[++i]);
        } else if (arg == "--default-retention-bytes" && i + 1 < argc) {
            default_retention_bytes = std::stoull(argv[++i]);
        } else if (arg == "--retention-check-interval-ms" && i + 1 < argc) {
            retention_check_interval_ms = std::stoull(argv[++i]);
        } else if (arg == "--startup-scan" && i + 1 < argc) {
            startup_scan = argv[++i];
        } else if (arg.rfind("--", 0) != 0) {
            port = static_cast<uint16_t>(std::atoi(argv[i]));
        }
    }

    if (log_level_str == "DEBUG") {
        streamforge::Logger::instance().set_level(streamforge::LogLevel::DEBUG);
    } else if (log_level_str == "WARN" || log_level_str == "WARNING") {
        streamforge::Logger::instance().set_level(streamforge::LogLevel::WARNING);
    } else if (log_level_str == "ERROR" || log_level_str == "ERR") {
        streamforge::Logger::instance().set_level(streamforge::LogLevel::ERR);
    } else {
        streamforge::Logger::instance().set_level(streamforge::LogLevel::INFO);
    }

    if (!SetConsoleCtrlHandler(console_ctrl_handler, TRUE)) {
        streamforge::Logger::instance().error("Failed to set console control handler.");
        return 1;
    }

    try {
        streamforge::StorageConfig storage_cfg;
        storage_cfg.data_dir = data_dir;
        storage_cfg.segment_max_bytes = segment_bytes;
        storage_cfg.sync_on_append = sync_on_append;
        storage_cfg.default_retention_ms = default_retention_ms;
        storage_cfg.default_retention_bytes = default_retention_bytes;
        storage_cfg.retention_check_interval_ms = retention_check_interval_ms;
        storage_cfg.startup_scan = startup_scan;

        streamforge::TopicManager topic_mgr(storage_cfg);
        streamforge::Status st = topic_mgr.open_and_recover_all();
        if (!st.ok()) {
            streamforge::Logger::instance().error("Failed to initialize storage engine: " + st.message());
            return 1;
        }

        // Initialize and recover OffsetStore
        streamforge::OffsetStore offset_store;
        streamforge::Status ost = offset_store.open_and_recover(topic_mgr);
        if (!ost.ok()) {
            streamforge::Logger::instance().error("Failed to initialize offset store: " + ost.message());
            return 1;
        }

        // Initialize GroupCoordinator
        streamforge::CoordinatorConfig coord_cfg;
        coord_cfg.min_session_timeout_ms = group_min_session_ms;
        coord_cfg.max_session_timeout_ms = group_max_session_ms;
        streamforge::GroupCoordinator coordinator(topic_mgr, offset_store, coord_cfg);

        // Start RetentionManager background thread
        streamforge::RetentionManager retention_mgr(topic_mgr, retention_check_interval_ms);
        retention_mgr.start();

        auto topics = topic_mgr.list_topics();
        streamforge::Logger::instance().info("Storage engine initialized (" + std::to_string(topics.size()) + " topics loaded from " + data_dir + ")");
        for (const auto& topic : topics) {
            if (topic->name().rfind("__", 0) == 0) continue; // Hide reserved topics from user display
            std::string info = "  Topic '" + topic->name() + "' (" + std::to_string(topic->num_partitions()) + " partitions): ";
            for (uint32_t p = 0; p < topic->num_partitions(); ++p) {
                auto part = topic->get_partition(p);
                info += "P" + std::to_string(p) + "[next=" + std::to_string(part->next_offset()) + "] ";
            }
            streamforge::Logger::instance().info(info);
        }

        streamforge::MessageHandler message_handler(topic_mgr, &coordinator);
        streamforge::ServerConfig server_cfg;
        server_cfg.host = host;
        server_cfg.port = port;
        server_cfg.workers = workers;
        server_cfg.max_connections = max_connections;
        server_cfg.read_stall_timeout_sec = read_stall_timeout_sec;
        server_cfg.max_output_buffer_bytes = max_output_buffer_bytes;
        server_cfg.group_min_session_ms = group_min_session_ms;
        server_cfg.group_max_session_ms = group_max_session_ms;
        server_cfg.reaper_interval_ms = reaper_interval_ms;

        streamforge::TcpServer server(server_cfg, message_handler, &coordinator);
        g_server_instance = &server;
        server.start();
        server.wait_until_stopped();
        retention_mgr.stop();
        g_server_instance = nullptr;
    } catch (const std::exception& ex) {
        streamforge::Logger::instance().error(std::string("Fatal server error: ") + ex.what());
        return 1;
    }

    return 0;
}
