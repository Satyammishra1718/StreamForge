#ifndef STREAMFORGE_CRASH_POINT_HPP
#define STREAMFORGE_CRASH_POINT_HPP

#ifdef STREAMFORGE_CRASH_TESTING
#include <cstdlib>
#include <cstring>
#include <iostream>

namespace streamforge {

class CrashPoint {
public:
    static void maybe_die(const char* label) {
        if (!label) return;
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
