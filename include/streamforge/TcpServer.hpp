#ifndef STREAMFORGE_TCP_SERVER_HPP
#define STREAMFORGE_TCP_SERVER_HPP

#include "streamforge/WinsockRuntime.hpp"
#include "streamforge/Socket.hpp"
#include "streamforge/MessageHandler.hpp"
#include "streamforge/ServerConfig.hpp"
#include "streamforge/ConnectionState.hpp"
#include "streamforge/WakeupChannel.hpp"
#include "streamforge/ThreadPool.hpp"
#include "streamforge/TaskQueue.hpp"
#include "streamforge/GroupCoordinator.hpp"
#include <atomic>
#include <vector>
#include <unordered_map>
#include <memory>
#include <string>
#include <cstdint>
#include <thread>
#include <condition_variable>
#include <mutex>

namespace streamforge {

class TcpServer {
public:
    TcpServer(ServerConfig config, MessageHandler handler, GroupCoordinator* coordinator = nullptr);
    // Backward-compatible constructor for M1-M3 scripts and tests
    TcpServer(std::string host, uint16_t port, MessageHandler handler);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void start();
    void request_stop();
    void wait_until_stopped();

    size_t active_connections() const;

private:
    void event_loop();
    void accept_new_connections();
    void process_completions();
    void check_read_stalls(std::chrono::steady_clock::time_point now);
    void close_connection(uint64_t conn_id);
    void queue_immediate_error_and_close(ConnectionState* conn, const Frame& err_frame);
    void reaper_loop();

    WinsockRuntime m_winsock;
    ServerConfig m_config;
    MessageHandler m_message_handler;
    GroupCoordinator* m_coordinator{nullptr};

    Socket m_listen_socket;
    WakeupChannel m_wakeup;
    CompletionQueue m_comp_queue;
    std::unique_ptr<ThreadPool> m_thread_pool;

    std::atomic<bool> m_shutdown_requested{false};
    std::atomic<bool> m_running{false};

    uint64_t m_next_conn_id{1};
    std::unordered_map<uint64_t, std::unique_ptr<ConnectionState>> m_connections;

    std::thread m_reaper_thread;
    std::mutex m_reaper_mutex;
    std::condition_variable m_reaper_cv;
};

} // namespace streamforge

#endif // STREAMFORGE_TCP_SERVER_HPP
