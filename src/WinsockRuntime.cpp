#include "streamforge/WinsockRuntime.hpp"
#include <stdexcept>
#include <string>

namespace streamforge {

WinsockRuntime::WinsockRuntime() {
    WSADATA wsaData;
    int result = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (result != 0) {
        throw std::runtime_error("WSAStartup failed with error code: " + std::to_string(result));
    }
    m_initialized = true;
}

WinsockRuntime::~WinsockRuntime() {
    if (m_initialized) {
        WSACleanup();
        m_initialized = false;
    }
}

} // namespace streamforge
