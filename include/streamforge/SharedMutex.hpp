#ifndef STREAMFORGE_SHARED_MUTEX_HPP
#define STREAMFORGE_SHARED_MUTEX_HPP

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

namespace streamforge {

/**
 * SharedMutex wraps Windows native SRWLOCK (Slim Read/Write Lock).
 * 
 * Unlike std::shared_mutex in MinGW GCC (which allocates kernel Event and Semaphore
 * handles per instance), Win32 SRWLOCK is pointer-sized, user-mode only, has zero kernel handle
 * allocation/overhead, cannot leak handles, and is the fastest reader/writer lock on Windows.
 * 
 * It implements the C++ SharedLockable concept, allowing it to be used with
 * std::unique_lock, std::shared_lock, and std::lock_guard.
 */
class SharedMutex {
public:
    SharedMutex() : m_srw(SRWLOCK_INIT) {}
    ~SharedMutex() = default;

    SharedMutex(const SharedMutex&) = delete;
    SharedMutex& operator=(const SharedMutex&) = delete;

    void lock() {
        AcquireSRWLockExclusive(&m_srw);
    }

    void unlock() {
        ReleaseSRWLockExclusive(&m_srw);
    }

    bool try_lock() {
        return TryAcquireSRWLockExclusive(&m_srw) != FALSE;
    }

    void lock_shared() {
        AcquireSRWLockShared(&m_srw);
    }

    void unlock_shared() {
        ReleaseSRWLockShared(&m_srw);
    }

    bool try_lock_shared() {
        return TryAcquireSRWLockShared(&m_srw) != FALSE;
    }

private:
    SRWLOCK m_srw;
};

} // namespace streamforge

#endif // STREAMFORGE_SHARED_MUTEX_HPP
