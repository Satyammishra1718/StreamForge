#include "streamforge/Socket.hpp"
#include <stdexcept>
#include <cstring>

namespace streamforge {

Socket::Socket() : m_sock(INVALID_SOCKET) {}

Socket::Socket(SOCKET s) : m_sock(s) {}

Socket::~Socket() {
    close();
}

Socket::Socket(Socket&& other) noexcept : m_sock(other.m_sock) {
    other.m_sock = INVALID_SOCKET;
}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        close();
        m_sock = other.m_sock;
        other.m_sock = INVALID_SOCKET;
    }
    return *this;
}

bool Socket::is_valid() const {
    return m_sock != INVALID_SOCKET;
}

SOCKET Socket::get() const {
    return m_sock;
}

SOCKET Socket::release() {
    SOCKET temp = m_sock;
    m_sock = INVALID_SOCKET;
    return temp;
}

void Socket::close() {
    if (m_sock != INVALID_SOCKET) {
        closesocket(m_sock);
        m_sock = INVALID_SOCKET;
    }
}

void Socket::shutdown(int how) {
    if (m_sock != INVALID_SOCKET) {
        ::shutdown(m_sock, how);
    }
}

RecvResult Socket::recv_exact(uint8_t* buffer, size_t bytes_to_read) {
    size_t total_read = 0;
    while (total_read < bytes_to_read) {
        int bytes_left = static_cast<int>(bytes_to_read - total_read);
        int res = ::recv(m_sock, reinterpret_cast<char*>(buffer + total_read), bytes_left, 0);
        if (res == 0) {
            return RecvResult::Disconnected;
        }
        if (res < 0) {
            int err = WSAGetLastError();
            if (err == WSAEINTR) {
                continue;
            }
            return RecvResult::Error;
        }
        total_read += static_cast<size_t>(res);
    }
    return RecvResult::Success;
}

SendResult Socket::send_all(const uint8_t* buffer, size_t bytes_to_send) {
    size_t total_sent = 0;
    while (total_sent < bytes_to_send) {
        int bytes_left = static_cast<int>(bytes_to_send - total_sent);
        int res = ::send(m_sock, reinterpret_cast<const char*>(buffer + total_sent), bytes_left, 0);
        if (res <= 0) {
            int err = WSAGetLastError();
            if (err == WSAEINTR) {
                continue;
            }
            if (res == 0) {
                return SendResult::Disconnected;
            }
            return SendResult::Error;
        }
        total_sent += static_cast<size_t>(res);
    }
    return SendResult::Success;
}

bool Socket::get_peer_address(std::string& out_ip, uint16_t& out_port) const {
    if (m_sock == INVALID_SOCKET) return false;
    sockaddr_in addr;
    int addr_len = sizeof(addr);
    if (getpeername(m_sock, reinterpret_cast<sockaddr*>(&addr), &addr_len) != 0) {
        return false;
    }
    char ip_str[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &(addr.sin_addr), ip_str, INET_ADDRSTRLEN);
    out_ip = ip_str;
    out_port = ntohs(addr.sin_port);
    return true;
}

Socket Socket::create_listener(const std::string& host, uint16_t port) {
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) {
        throw std::runtime_error("Failed to create TCP socket. Error: " + std::to_string(WSAGetLastError()));
    }

    int optval = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&optval), sizeof(optval));

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        closesocket(s);
        throw std::runtime_error("Invalid IP address: " + host);
    }

    if (bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        int err = WSAGetLastError();
        closesocket(s);
        throw std::runtime_error("Bind failed on " + host + ":" + std::to_string(port) + ". Error: " + std::to_string(err));
    }

    if (listen(s, SOMAXCONN) != 0) {
        int err = WSAGetLastError();
        closesocket(s);
        throw std::runtime_error("Listen failed. Error: " + std::to_string(err));
    }

    return Socket(s);
}

} // namespace streamforge
