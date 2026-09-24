#ifndef STREAMFORGE_WAKEUP_CHANNEL_HPP
#define STREAMFORGE_WAKEUP_CHANNEL_HPP

#include "streamforge/Socket.hpp"
#include <atomic>

namespace streamforge {

// WakeupChannel provides an inter-thread notification mechanism on Windows
// using a loopback TCP socket pair. Windows anonymous pipes (CreatePipe) do
// not possess socket handles and cannot be polled with WSAPoll or select.
// A loopback socket pair allows worker threads to wake up the I/O thread blocked
// in WSAPoll cleanly and asynchronously.
class WakeupChannel {
public:
    WakeupChannel();
    ~WakeupChannel() = default;

    WakeupChannel(const WakeupChannel&) = delete;
    WakeupChannel& operator=(const WakeupChannel&) = delete;

    void notify();
    void drain();
    SOCKET read_fd() const;

private:
    Socket m_read_sock;
    Socket m_write_sock;
    std::atomic<bool> m_wake_pending{false};
};

} // namespace streamforge

#endif // STREAMFORGE_WAKEUP_CHANNEL_HPP
