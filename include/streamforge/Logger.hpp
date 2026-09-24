#ifndef STREAMFORGE_LOGGER_HPP
#define STREAMFORGE_LOGGER_HPP

#include <string>
#include <mutex>
#include <iostream>

namespace streamforge {

enum class LogLevel {
    DEBUG = 0,
    INFO = 1,
    WARNING = 2,
    ERR = 3
};

class Logger {
public:
    static Logger& instance();

    void set_level(LogLevel level) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_level = level;
    }

    LogLevel get_level() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_level;
    }

    void log(LogLevel level, const std::string& message);
    void info(const std::string& message) { log(LogLevel::INFO, message); }
    void warning(const std::string& message) { log(LogLevel::WARNING, message); }
    void error(const std::string& message) { log(LogLevel::ERR, message); }
    void debug(const std::string& message) { log(LogLevel::DEBUG, message); }

private:
    Logger() = default;
    mutable std::mutex m_mutex;
    LogLevel m_level{LogLevel::INFO};
    std::string get_timestamp();
};

} // namespace streamforge

#endif // STREAMFORGE_LOGGER_HPP
