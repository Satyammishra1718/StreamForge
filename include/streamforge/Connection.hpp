#ifndef STREAMFORGE_CONNECTION_HPP
#define STREAMFORGE_CONNECTION_HPP

#include "streamforge/Socket.hpp"
#include "streamforge/MessageHandler.hpp"
#include <atomic>
#include <memory>
#include <string>

namespace streamforge {

class Connection {
public:
    Connection(std::shared_ptr<Socket> socket, std::string peer_ip, uint16_t peer_port);
    ~Connection() = default;

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    void run(MessageHandler& handler, const std::atomic<bool>& shutdown_requested);
    void stop();

    const std::string& peer_ip() const { return m_peer_ip; }
    uint16_t peer_port() const { return m_peer_port; }
    std::string peer_address_string() const { return m_peer_ip + ":" + std::to_string(m_peer_port); }

private:
    std::shared_ptr<Socket> m_socket;
    std::string m_peer_ip;
    uint16_t m_peer_port;
};

} // namespace streamforge

#endif // STREAMFORGE_CONNECTION_HPP
