#ifndef STREAMFORGE_SOCKET_HPP
#define STREAMFORGE_SOCKET_HPP

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

#include <cstdint>
#include <cstddef>
#include <string>

namespace streamforge {

enum class RecvResult {
    Success,
    Disconnected,
    Error
};

enum class SendResult {
    Success,
    Disconnected,
    Error
};

class Socket {
public:
    Socket();
    explicit Socket(SOCKET s);
    ~Socket();

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;

    bool is_valid() const;
    SOCKET get() const;
    SOCKET release();
    void close();
    void shutdown(int how = SD_BOTH);

    RecvResult recv_exact(uint8_t* buffer, size_t bytes_to_read);
    SendResult send_all(const uint8_t* buffer, size_t bytes_to_send);

    bool get_peer_address(std::string& out_ip, uint16_t& out_port) const;
    bool get_local_address(std::string& out_ip, uint16_t& out_port) const;
    bool set_non_blocking(bool non_blocking = true);

    static Socket create_listener(const std::string& host, uint16_t port);

private:
    SOCKET m_sock;
};

} // namespace streamforge

#endif // STREAMFORGE_SOCKET_HPP
