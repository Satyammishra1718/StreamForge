#include "streamforge/WakeupChannel.hpp"
#include <stdexcept>
#include <cstring>

namespace streamforge {

WakeupChannel::WakeupChannel() {
    Socket listener = Socket::create_listener("127.0.0.1", 0);
    std::string ip;
    uint16_t port = 0;
    if (!listener.get_local_address(ip, port)) {
        throw std::runtime_error("Failed to get local address for wakeup listener socket");
    }

    SOCKET write_s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (write_s == INVALID_SOCKET) {
        throw std::runtime_error("Failed to create wakeup write socket: " + std::to_string(WSAGetLastError()));
    }

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (::connect(write_s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        closesocket(write_s);
        throw std::runtime_error("Failed to connect wakeup write socket: " + std::to_string(WSAGetLastError()));
    }

    sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);
    SOCKET read_s = ::accept(listener.get(), reinterpret_cast<sockaddr*>(&client_addr), &client_addr_len);
    if (read_s == INVALID_SOCKET) {
        closesocket(write_s);
        throw std::runtime_error("Failed to accept wakeup read socket: " + std::to_string(WSAGetLastError()));
    }

    listener.close();

    // Disable Nagle's algorithm for low-latency wakeup signaling
    int nodelay = 1;
    setsockopt(write_s, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&nodelay), sizeof(nodelay));

    // Make read socket non-blocking for drain operations in WSAPoll loop
    u_long non_blocking = 1;
    ioctlsocket(read_s, FIONBIO, &non_blocking);

    m_read_sock = Socket(read_s);
    m_write_sock = Socket(write_s);
}

void WakeupChannel::notify() {
    if (!m_wake_pending.exchange(true, std::memory_order_acq_rel)) {
        char b = 1;
        ::send(m_write_sock.get(), &b, 1, 0);
    }
}

void WakeupChannel::drain() {
    m_wake_pending.store(false, std::memory_order_release);
    char buf[256];
    while (true) {
        int r = ::recv(m_read_sock.get(), buf, sizeof(buf), 0);
        if (r <= 0) break;
    }
}

SOCKET WakeupChannel::read_fd() const {
    return m_read_sock.get();
}

} // namespace streamforge
