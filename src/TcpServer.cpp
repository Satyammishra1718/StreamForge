#include "streamforge/TcpServer.hpp"
#include "streamforge/Logger.hpp"
#include <algorithm>
#include <chrono>

namespace streamforge {

TcpServer::TcpServer(std::string host, uint16_t port, MessageHandler handler)
    : m_host(std::move(host)), m_port(port), m_message_handler(std::move(handler)) {}

TcpServer::~TcpServer() {
    request_stop();
    wait_until_stopped();
}

void TcpServer::start() {
    m_listen_socket = Socket::create_listener(m_host, m_port);
    m_running = true;
    m_shutdown_requested = false;

    Logger::instance().info("StreamForge server started on " + m_host + ":" + std::to_string(m_port));
    accept_loop();
}

void TcpServer::request_stop() {
    m_shutdown_requested = true;
}

void TcpServer::wait_until_stopped() {
    if (!m_running && m_sessions.empty()) {
        return;
    }

    // Close listen socket to unblock any accept state
    m_listen_socket.close();

    // Copy sessions to avoid deadlocks while shutting down
    std::vector<std::shared_ptr<ClientSession>> sessions_to_join;
    {
        std::lock_guard<std::mutex> lock(m_sessions_mutex);
        sessions_to_join = m_sessions;
    }

    // Unblock all worker threads by calling shutdown on sockets
    for (auto& session : sessions_to_join) {
        if (session->connection) {
            session->connection->stop();
        }
    }

    // Join all worker threads
    for (auto& session : sessions_to_join) {
        if (session->thread.joinable()) {
            session->thread.join();
        }
    }

    {
        std::lock_guard<std::mutex> lock(m_sessions_mutex);
        m_sessions.clear();
    }

    m_running = false;
    Logger::instance().info("StreamForge server shutdown complete.");
}

void TcpServer::reap_finished_connections() {
    std::lock_guard<std::mutex> lock(m_sessions_mutex);
    auto it = std::remove_if(m_sessions.begin(), m_sessions.end(), [](const std::shared_ptr<ClientSession>& session) {
        if (session->finished && session->finished->load()) {
            if (session->thread.joinable()) {
                session->thread.join();
            }
            return true;
        }
        return false;
    });
    m_sessions.erase(it, m_sessions.end());
}

void TcpServer::accept_loop() {
    while (!m_shutdown_requested) {
        reap_finished_connections();

        SOCKET raw_listen = m_listen_socket.get();
        if (raw_listen == INVALID_SOCKET) {
            break;
        }

        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(raw_listen, &readfds);

        timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 200000; // 200 ms timeout

        int sel_res = select(0, &readfds, nullptr, nullptr, &timeout);
        if (sel_res < 0) {
            int err = WSAGetLastError();
            if (err == WSAEINTR || m_shutdown_requested) {
                break;
            }
            Logger::instance().error("select() failed in accept loop. Error: " + std::to_string(err));
            break;
        }

        if (sel_res == 0) {
            // Timeout, check shutdown flag again
            continue;
        }

        if (FD_ISSET(raw_listen, &readfds)) {
            sockaddr_in client_addr;
            int client_addr_len = sizeof(client_addr);
            SOCKET client_sock = accept(raw_listen, reinterpret_cast<sockaddr*>(&client_addr), &client_addr_len);
            if (client_sock == INVALID_SOCKET) {
                if (m_shutdown_requested) break;
                Logger::instance().error("accept() failed. Error: " + std::to_string(WSAGetLastError()));
                continue;
            }

            char ip_buf[INET_ADDRSTRLEN] = {0};
            inet_ntop(AF_INET, &(client_addr.sin_addr), ip_buf, INET_ADDRSTRLEN);
            uint16_t client_port = ntohs(client_addr.sin_port);

            std::shared_ptr<Socket> sock_ptr = std::make_shared<Socket>(client_sock);

            // Check connection limit
            size_t active_count = 0;
            {
                std::lock_guard<std::mutex> lock(m_sessions_mutex);
                active_count = m_sessions.size();
            }

            if (active_count >= MAX_CONCURRENT_CONNECTIONS) {
                Logger::instance().warning("Connection limit reached (" + std::to_string(MAX_CONCURRENT_CONNECTIONS) + "). Rejecting connection from " + std::string(ip_buf) + ":" + std::to_string(client_port));
                Frame err_frame = FrameCodec::create_error_frame(0, ErrorCode::FRAME_TOO_LARGE, "Server reached max concurrent connection limit");
                std::vector<uint8_t> encoded_err = FrameCodec::encode(err_frame);
                sock_ptr->send_all(encoded_err.data(), encoded_err.size());
                sock_ptr->shutdown(SD_BOTH);
                sock_ptr->close();
                continue;
            }

            auto connection = std::make_shared<Connection>(sock_ptr, std::string(ip_buf), client_port);
            auto session = std::make_shared<ClientSession>();
            session->socket = sock_ptr;
            session->connection = connection;
            session->finished = std::make_shared<std::atomic<bool>>(false);

            session->thread = std::thread([this, session]() {
                session->connection->run(this->m_message_handler, this->m_shutdown_requested);
                session->finished->store(true);
            });

            {
                std::lock_guard<std::mutex> lock(m_sessions_mutex);
                m_sessions.push_back(session);
            }
        }
    }
}

} // namespace streamforge
