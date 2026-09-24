#include "streamforge/Logger.hpp"
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace streamforge {

Logger& Logger::instance() {
    static Logger logger;
    return logger;
}

std::string Logger::get_timestamp() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    std::time_t timer = system_clock::to_time_t(now);
    std::tm tm_info;
#if defined(_WIN32)
    localtime_s(&tm_info, &timer);
#else
    localtime_r(&timer, &tm_info);
#endif

    std::ostringstream ss;
    ss << std::put_time(&tm_info, "%Y-%m-%d %H:%M:%S")
       << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return ss.str();
}

void Logger::log(LogLevel level, const std::string& message) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (static_cast<int>(level) < static_cast<int>(m_level)) {
        return;
    }

    const char* level_str = "INFO";
    switch (level) {
        case LogLevel::DEBUG:   level_str = "DBG "; break;
        case LogLevel::INFO:    level_str = "INFO"; break;
        case LogLevel::WARNING: level_str = "WARN"; break;
        case LogLevel::ERR:     level_str = "ERR "; break;
    }

    std::cout << "[" << get_timestamp() << "] [" << level_str << "] " << message << std::endl;
}

} // namespace streamforge
