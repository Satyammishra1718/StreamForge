#include "streamforge/TcpServer.hpp"
#include "streamforge/Logger.hpp"
#include <algorithm>
#include <chrono>
#include <vector>

namespace streamforge {

TcpServer::TcpServer(ServerConfig config, MessageHandler handler)
    : m_config(std::move(config)),
      m_message_handler(std::move(handler)) {
    // Worker pool: N workers execute request handling and push completions.
    // Workers NEVER touch sockets (Single-Owner Rule).
    m_thread_pool = std::make_unique<ThreadPool>(
        m_config.workers, m_message_handler, m_comp_queue, m_wakeup
    );
}

TcpServer::TcpServer(std::string host, uint16_t port, MessageHandler handler)
    : TcpServer(ServerConfig{std::move(host), port, std::max(2u, std::thread::hardware_concurrency()), 1024, 30, 8 * 1024 * 1024, 2 * 1024 * 1024}, std::move(handler)) {}

TcpServer::~TcpServer() {
    request_stop();
    wait_until_stopped();
}

size_t TcpServer::active_connections() const {
    return m_connections.size();
}

void TcpServer::start() {
    m_listen_socket = Socket::create_listener(m_config.host, m_config.port);
    m_listen_socket.set_non_blocking(true);

    m_running = true;
    m_shutdown_requested = false;

    Logger::instance().info("StreamForge server started on " + m_config.host + ":" +
                           std::to_string(m_config.port) + " (workers: " +
                           std::to_string(m_config.workers) + ", max-conn: " +
                           std::to_string(m_config.max_connections) + ")");
    event_loop();
}

void TcpServer::request_stop() {
    m_shutdown_requested = true;
    m_wakeup.notify();
}

void TcpServer::close_connection(uint64_t conn_id) {
    auto it = m_connections.find(conn_id);
    if (it != m_connections.end()) {
        it->second->socket.shutdown(SD_BOTH);
        it->second->socket.close();
        m_connections.erase(it);
    }
}

void TcpServer::queue_immediate_error_and_close(ConnectionState* conn, const Frame& err_frame) {
    std::vector<uint8_t> encoded = FrameCodec::encode(err_frame);
    int sent = ::send(conn->socket.get(), reinterpret_cast<const char*>(encoded.data()), static_cast<int>(encoded.size()), 0);
    if (sent > 0 && static_cast<size_t>(sent) == encoded.size()) {
        close_connection(conn->conn_id);
    } else {
        conn->output_buffer = std::move(encoded);
        conn->output_offset = (sent > 0) ? static_cast<size_t>(sent) : 0;
        conn->close_after_send = true;
    }
}

void TcpServer::accept_new_connections() {
    while (true) {
        sockaddr_in client_addr;
        int addr_len = sizeof(client_addr);
        SOCKET s = ::accept(m_listen_socket.get(), reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
        if (s == INVALID_SOCKET) {
            int err = WSAGetLastError();
            if (err == WSAEWOULDBLOCK) break;
            break;
        }

        char ip_str[INET_ADDRSTRLEN] = {0};
        inet_ntop(AF_INET, &(client_addr.sin_addr), ip_str, INET_ADDRSTRLEN);
        uint16_t port = ntohs(client_addr.sin_port);

        // Enforce --max-connections limit
        if (m_connections.size() >= m_config.max_connections) {
            Logger::instance().warning("Connection limit reached (" + std::to_string(m_config.max_connections) +
                                      "). Rejecting connection from " + std::string(ip_str) + ":" + std::to_string(port));
            Frame err_frame = FrameCodec::create_error_frame(0, ErrorCode::FRAME_TOO_LARGE, "Server reached max concurrent connection limit");
            std::vector<uint8_t> encoded_err = FrameCodec::encode(err_frame);
            ::send(s, reinterpret_cast<const char*>(encoded_err.data()), static_cast<int>(encoded_err.size()), 0);
            ::shutdown(s, SD_BOTH);
            closesocket(s);
            continue;
        }

        // Set non-blocking mode on accepted socket
        u_long mode = 1;
        ioctlsocket(s, FIONBIO, &mode);

        uint64_t conn_id = m_next_conn_id++;
        auto conn_state = std::make_unique<ConnectionState>();
        conn_state->conn_id = conn_id;
        conn_state->socket = Socket(s);
        conn_state->peer_ip = ip_str;
        conn_state->peer_port = port;
        conn_state->last_read_time = std::chrono::steady_clock::now();
        conn_state->has_active_read_stall = false;

        Logger::instance().info("Client connected: " + conn_state->peer_address_string());
        m_connections[conn_id] = std::move(conn_state);
    }
}

void TcpServer::process_completions() {
    std::vector<Completion> comps = m_comp_queue.pop_all();
    for (auto& comp : comps) {
        // CONNECTION LIFETIME SAFETY:
        // Look up connection by unique generation ID. If connection was closed or reset
        // while worker was processing the request, dropping the completion here safely
        // avoids any dangling pointer dereferences.
        auto it = m_connections.find(comp.conn_id);
        if (it == m_connections.end()) {
            continue;
        }

        ConnectionState* conn = it->second.get();
        conn->in_flight = false;

        // SLOW-CONSUMER PROTECTION:
        // Verify output buffer does not exceed max_output_buffer_bytes.
        size_t unsent = conn->output_buffer.size() - conn->output_offset;
        if (unsent + comp.response_bytes.size() > m_config.max_output_buffer_bytes) {
            Logger::instance().warning("Slow consumer detected for " + conn->peer_address_string() +
                                      ". Output buffer exceeded limit (" +
                                      std::to_string(unsent + comp.response_bytes.size()) + " > " +
                                      std::to_string(m_config.max_output_buffer_bytes) + "). Disconnecting.");
            close_connection(conn->conn_id);
            continue;
        }

        // SINGLE-OWNER RULE:
        // Only the I/O thread writes to sockets. Workers produce raw bytes into the completion
        // queue; here the I/O thread attempts an opportunistic non-blocking send or buffers remainder.
        if (unsent == 0) {
            int sent = ::send(conn->socket.get(), reinterpret_cast<const char*>(comp.response_bytes.data()), static_cast<int>(comp.response_bytes.size()), 0);
            if (sent > 0) {
                if (static_cast<size_t>(sent) < comp.response_bytes.size()) {
                    conn->output_buffer.assign(comp.response_bytes.begin() + sent, comp.response_bytes.end());
                    conn->output_offset = 0;
                }
            } else if (sent < 0) {
                int err = WSAGetLastError();
                if (err == WSAEWOULDBLOCK) {
                    conn->output_buffer = std::move(comp.response_bytes);
                    conn->output_offset = 0;
                } else {
                    close_connection(conn->conn_id);
                    continue;
                }
            }
        } else {
            if (conn->output_offset > 0) {
                conn->output_buffer.erase(conn->output_buffer.begin(), conn->output_buffer.begin() + conn->output_offset);
                conn->output_offset = 0;
            }
            conn->output_buffer.insert(conn->output_buffer.end(), comp.response_bytes.begin(), comp.response_bytes.end());
        }

        // ORDERING & BACKPRESSURE:
        // At most one request in flight per connection. Now that the previous request has completed,
        // dispatch the next buffered pipelined request (if any) from this connection.
        if (!conn->pending_requests.empty()) {
            Frame next_req = std::move(conn->pending_requests.front());
            conn->pending_requests.pop();
            conn->in_flight = true;
            m_thread_pool->enqueue({ conn->conn_id, std::move(next_req) });
        }
    }
}

void TcpServer::check_read_stalls(std::chrono::steady_clock::time_point now) {
    // SLOW-LORIS DEFENSE:
    // If a client sends partial frame bytes and does not complete the frame within
    // read_stall_timeout_sec, close the connection to prevent connection exhaustion.
    std::vector<uint64_t> to_close;
    for (const auto& [conn_id, conn] : m_connections) {
        if (conn->has_active_read_stall) {
            auto elapsed_sec = std::chrono::duration_cast<std::chrono::seconds>(now - conn->last_read_time).count();
            if (elapsed_sec >= m_config.read_stall_timeout_sec) {
                Logger::instance().warning("Read stall timeout (" + std::to_string(elapsed_sec) + "s >= " +
                                          std::to_string(m_config.read_stall_timeout_sec) + "s) for client " +
                                          conn->peer_address_string() + ". Closing connection (slow-loris defense).");
                to_close.push_back(conn_id);
            }
        }
    }
    for (uint64_t conn_id : to_close) {
        close_connection(conn_id);
    }
}

void TcpServer::event_loop() {
    // SINGLE-OWNER RULE:
    // Only this single I/O thread runs WSAPoll and touches sockets (accept, recv, send, close).
    // This design avoids socket lock contention, prevents data races on socket descriptors,
    // and guarantees that no worker thread performs blocking or concurrent socket I/O.
    while (!m_shutdown_requested.load()) {
        std::vector<WSAPOLLFD> poll_fds;
        std::vector<uint64_t> fd_to_conn_id;

        poll_fds.reserve(2 + m_connections.size());
        fd_to_conn_id.reserve(2 + m_connections.size());

        // 1. Listen socket for new client connections
        if (m_listen_socket.is_valid()) {
            WSAPOLLFD pfd{};
            pfd.fd = m_listen_socket.get();
            pfd.events = POLLIN;
            poll_fds.push_back(pfd);
            fd_to_conn_id.push_back(0);
        }

        // 2. Loopback wakeup socket for completions from worker threads
        {
            WSAPOLLFD pfd{};
            pfd.fd = m_wakeup.read_fd();
            pfd.events = POLLIN;
            poll_fds.push_back(pfd);
            fd_to_conn_id.push_back(0);
        }

        // 3. Client sockets
        for (const auto& [conn_id, conn] : m_connections) {
            WSAPOLLFD pfd{};
            pfd.fd = conn->socket.get();
            pfd.events = 0;

            // BACKPRESSURE: stop polling POLLIN if input buffer is full
            bool can_read = (conn->assembler.buffered_bytes() < m_config.max_input_buffer_bytes) &&
                            (conn->pending_requests.size() < 1000) &&
                            (!conn->close_after_send);
            if (can_read) {
                pfd.events |= POLLIN;
            }

            // Request POLLOUT only while output is pending
            if (conn->output_offset < conn->output_buffer.size()) {
                pfd.events |= POLLOUT;
            }

            poll_fds.push_back(pfd);
            fd_to_conn_id.push_back(conn_id);
        }

        int poll_res = WSAPoll(poll_fds.data(), static_cast<ULONG>(poll_fds.size()), 250);
        if (poll_res < 0) {
            int err = WSAGetLastError();
            if (err == WSAEINTR || m_shutdown_requested.load()) {
                break;
            }
            Logger::instance().error("WSAPoll error: " + std::to_string(err));
            break;
        }

        size_t idx = 0;

        // Process listen socket
        if (m_listen_socket.is_valid()) {
            if (poll_fds[idx].revents & POLLIN) {
                accept_new_connections();
            }
            idx++;
        }

        // Process wakeup channel
        if (poll_fds[idx].revents & POLLIN) {
            m_wakeup.drain();
            process_completions();
        }
        idx++;

        // Process client sockets
        std::vector<uint64_t> to_close;
        for (; idx < poll_fds.size(); ++idx) {
            uint64_t conn_id = fd_to_conn_id[idx];
            auto it = m_connections.find(conn_id);
            if (it == m_connections.end()) continue;
            ConnectionState* conn = it->second.get();
            short revents = poll_fds[idx].revents;

            if (revents & (POLLERR | POLLHUP | POLLNVAL)) {
                to_close.push_back(conn_id);
                continue;
            }

            // Output handling
            if (revents & POLLOUT) {
                size_t to_send = conn->output_buffer.size() - conn->output_offset;
                int sent = ::send(conn->socket.get(), reinterpret_cast<const char*>(conn->output_buffer.data() + conn->output_offset), static_cast<int>(to_send), 0);
                if (sent > 0) {
                    conn->output_offset += sent;
                    if (conn->output_offset == conn->output_buffer.size()) {
                        conn->output_buffer.clear();
                        conn->output_offset = 0;
                        if (conn->close_after_send) {
                            to_close.push_back(conn_id);
                            continue;
                        }
                    }
                } else if (sent < 0) {
                    int err = WSAGetLastError();
                    if (err != WSAEWOULDBLOCK) {
                        to_close.push_back(conn_id);
                        continue;
                    }
                }
            }

            // Input handling
            if (revents & POLLIN) {
                uint8_t buf[32768];
                bool disconnected = false;
                while (true) {
                    int n = ::recv(conn->socket.get(), reinterpret_cast<char*>(buf), sizeof(buf), 0);
                    if (n > 0) {
                        conn->last_read_time = std::chrono::steady_clock::now();
                        conn->assembler.push_bytes(buf, static_cast<size_t>(n));
                    } else if (n == 0) {
                        disconnected = true;
                        break;
                    } else {
                        int err = WSAGetLastError();
                        if (err == WSAEWOULDBLOCK) {
                            break;
                        }
                        disconnected = true;
                        break;
                    }
                }

                if (disconnected) {
                    to_close.push_back(conn_id);
                    continue;
                }

                // Extract frames using FrameAssembler state machine
                Frame frame;
                FrameExtractResult res;
                while ((res = conn->assembler.extract_next_frame(frame)) == FrameExtractResult::FrameReady) {
                    if (conn->in_flight) {
                        // ORDERING: Request pipelined on same connection buffers until previous response completes
                        conn->pending_requests.push(std::move(frame));
                    } else {
                        conn->in_flight = true;
                        m_thread_pool->enqueue({ conn->conn_id, std::move(frame) });
                    }
                }

                if (res == FrameExtractResult::ErrorLengthTooSmall) {
                    Logger::instance().warning("Malformed frame received (length < 5). Sending error 3 and closing.");
                    Frame err_frame = FrameCodec::create_error_frame(0, ErrorCode::MALFORMED_FRAME, "Malformed frame: length less than header size");
                    queue_immediate_error_and_close(conn, err_frame);
                } else if (res == FrameExtractResult::ErrorLengthTooLarge) {
                    Logger::instance().warning("Frame too large received (length > 1 MiB). Sending error 2 and closing.");
                    Frame err_frame = FrameCodec::create_error_frame(0, ErrorCode::FRAME_TOO_LARGE, "Frame size exceeds maximum limit of 1 MiB");
                    queue_immediate_error_and_close(conn, err_frame);
                }

                conn->has_active_read_stall = conn->assembler.has_partial_frame();
            }
        }

        for (uint64_t conn_id : to_close) {
            close_connection(conn_id);
        }

        // Drain any completions queued during socket processing
        process_completions();

        // Check for slow-loris read stalls
        check_read_stalls(std::chrono::steady_clock::now());
    }
}

void TcpServer::wait_until_stopped() {
    if (!m_running.load()) return;

    Logger::instance().info("Gracefully stopping StreamForge TcpServer...");
    m_listen_socket.close();

    // Wait up to 5 seconds for in-flight requests to complete
    auto start_time = std::chrono::steady_clock::now();
    while (true) {
        bool any_in_flight = false;
        for (const auto& [id, conn] : m_connections) {
            if (conn->in_flight || !conn->pending_requests.empty()) {
                any_in_flight = true;
                break;
            }
        }
        if (!any_in_flight) break;

        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start_time).count();
        if (elapsed >= 5) {
            Logger::instance().warning("Shutdown wait timeout reached (5s). Dropping remaining in-flight requests.");
            break;
        }

        m_wakeup.drain();
        process_completions();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    // Best-effort flush remaining output buffers
    for (auto& [id, conn] : m_connections) {
        if (conn->output_offset < conn->output_buffer.size()) {
            size_t to_send = conn->output_buffer.size() - conn->output_offset;
            ::send(conn->socket.get(), reinterpret_cast<const char*>(conn->output_buffer.data() + conn->output_offset), static_cast<int>(to_send), 0);
        }
        conn->socket.shutdown(SD_BOTH);
        conn->socket.close();
    }
    m_connections.clear();

    // Stop and join all worker threads
    if (m_thread_pool) {
        m_thread_pool->stop_and_join();
        m_thread_pool.reset();
    }

    m_running = false;
    Logger::instance().info("StreamForge server shutdown complete.");
}

} // namespace streamforge
