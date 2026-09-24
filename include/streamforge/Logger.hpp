#ifndef STREAMFORGE_LOGGER_HPP
#define STREAMFORGE_LOGGER_HPP

#include <string>
#include <mutex>
#include <iostream>

namespace streamforge {

enum class LogLevel {
    INFO,
    WARNING,
    ERR,
    DEBUG
};

class Logger {
public:
    static Logger& instance();

    void log(LogLevel level, const std::string& message);
    void info(const std::string& message) { log(LogLevel::INFO, message); }
    void warning(const std::string& message) { log(LogLevel::WARNING, message); }
    void error(const std::string& message) { log(LogLevel::ERR, message); }
    void debug(const std::string& message) { log(LogLevel::DEBUG, message); }

private:
    Logger() = default;
    std::mutex m_mutex;
    std::string get_timestamp();
};

} // namespace streamforge

#endif // STREAMFORGE_LOGGER_HPP
