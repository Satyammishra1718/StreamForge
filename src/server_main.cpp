#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include "streamforge/TcpServer.hpp"
#include "streamforge/TopicManager.hpp"
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

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (arg == "--data-dir" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg.rfind("--", 0) != 0) {
            port = static_cast<uint16_t>(std::atoi(argv[i]));
        }
    }

    if (!SetConsoleCtrlHandler(console_ctrl_handler, TRUE)) {
        streamforge::Logger::instance().error("Failed to set console control handler.");
        return 1;
    }

    try {
        streamforge::StorageConfig storage_cfg;
        storage_cfg.data_dir = data_dir;

        streamforge::TopicManager topic_mgr(storage_cfg);
        streamforge::Status st = topic_mgr.open_and_recover_all();
        if (!st.ok()) {
            streamforge::Logger::instance().error("Failed to initialize storage engine: " + st.message());
            return 1;
        }

        auto topics = topic_mgr.list_topics();
        streamforge::Logger::instance().info("Storage engine initialized (" + std::to_string(topics.size()) + " topics loaded from " + data_dir + ")");
        for (const auto& topic : topics) {
            std::string info = "  Topic '" + topic->name() + "' (" + std::to_string(topic->num_partitions()) + " partitions): ";
            for (uint32_t p = 0; p < topic->num_partitions(); ++p) {
                auto part = topic->get_partition(p);
                info += "P" + std::to_string(p) + "[next=" + std::to_string(part->next_offset()) + "] ";
            }
            streamforge::Logger::instance().info(info);
        }

        streamforge::TcpServer server(host, port);
        g_server_instance = &server;
        server.start();
        server.wait_until_stopped();
        g_server_instance = nullptr;
    } catch (const std::exception& ex) {
        streamforge::Logger::instance().error(std::string("Fatal server error: ") + ex.what());
        return 1;
    }

    return 0;
}
