#ifndef STREAMFORGE_CRASH_POINT_HPP
#define STREAMFORGE_CRASH_POINT_HPP

#ifdef STREAMFORGE_CRASH_TESTING

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <cstdlib>
#include <cstring>

namespace streamforge {

class CrashPoint {
public:
    static void maybe_die(const char* label) {
        if (!label) return;
        char buf[256];
        DWORD len = GetEnvironmentVariableA("STREAMFORGE_CRASH_AT", buf, sizeof(buf));
        if (len > 0 && std::strcmp(buf, label) == 0) {
            std::_Exit(1);
        }
        const char* target = std::getenv("STREAMFORGE_CRASH_AT");
        if (target && std::strcmp(target, label) == 0) {
            std::_Exit(1);
        }
    }
};

} // namespace streamforge

#else

namespace streamforge {

class CrashPoint {
public:
    static inline void maybe_die(const char*) {
        // Zero-overhead no-op when STREAMFORGE_CRASH_TESTING is OFF
    }
};

} // namespace streamforge

#endif // STREAMFORGE_CRASH_TESTING

#endif // STREAMFORGE_CRASH_POINT_HPP
