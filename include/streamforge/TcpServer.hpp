#ifndef STREAMFORGE_TCP_SERVER_HPP
#define STREAMFORGE_TCP_SERVER_HPP

#include "streamforge/WinsockRuntime.hpp"
#include "streamforge/Socket.hpp"
#include "streamforge/Connection.hpp"
#include "streamforge/MessageHandler.hpp"
#include <atomic>
#include <mutex>
#include <thread>
#include <vector>
#include <memory>
#include <string>

namespace streamforge {

constexpr size_t MAX_CONCURRENT_CONNECTIONS = 256;

class TcpServer {
public:
    TcpServer(std::string host, uint16_t port);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void start();
    void request_stop();
    void wait_until_stopped();

private:
    void accept_loop();
    void reap_finished_connections();

    struct ClientSession {
        std::thread thread;
        std::shared_ptr<Connection> connection;
        std::shared_ptr<Socket> socket;
        std::shared_ptr<std::atomic<bool>> finished;
    };

    WinsockRuntime m_winsock;
    std::string m_host;
    uint16_t m_port;

    Socket m_listen_socket;
    std::atomic<bool> m_shutdown_requested{false};
    std::atomic<bool> m_running{false};

    MessageHandler m_message_handler;

    std::mutex m_sessions_mutex;
    std::vector<std::shared_ptr<ClientSession>> m_sessions;
};

} // namespace streamforge

#endif // STREAMFORGE_TCP_SERVER_HPP
