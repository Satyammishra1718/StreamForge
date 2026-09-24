#ifndef STREAMFORGE_WINSOCK_RUNTIME_HPP
#define STREAMFORGE_WINSOCK_RUNTIME_HPP

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

namespace streamforge {

class WinsockRuntime {
public:
    WinsockRuntime();
    ~WinsockRuntime();

    WinsockRuntime(const WinsockRuntime&) = delete;
    WinsockRuntime& operator=(const WinsockRuntime&) = delete;
    WinsockRuntime(WinsockRuntime&&) = delete;
    WinsockRuntime& operator=(WinsockRuntime&&) = delete;

    bool is_initialized() const { return m_initialized; }

private:
    bool m_initialized{false};
};

} // namespace streamforge

#endif // STREAMFORGE_WINSOCK_RUNTIME_HPP
