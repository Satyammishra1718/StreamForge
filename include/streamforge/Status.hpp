#ifndef STREAMFORGE_STATUS_HPP
#define STREAMFORGE_STATUS_HPP

#include <string>
#include <utility>

namespace streamforge {

enum class StatusCode {
    OK,
    NotFound,
    InvalidArgument,
    OffsetOutOfRange,
    Corrupt,
    IoError,
    AlreadyExists,
    RecordTooLarge
};

class Status {
public:
    Status() : m_code(StatusCode::OK) {}
    Status(StatusCode code, std::string msg = "") : m_code(code), m_msg(std::move(msg)) {}

    bool ok() const { return m_code == StatusCode::OK; }
    StatusCode code() const { return m_code; }
    const std::string& message() const { return m_msg; }

    static Status OK() { return Status(StatusCode::OK); }
    static Status NotFound(std::string msg) { return Status(StatusCode::NotFound, std::move(msg)); }
    static Status InvalidArgument(std::string msg) { return Status(StatusCode::InvalidArgument, std::move(msg)); }
    static Status OffsetOutOfRange(std::string msg) { return Status(StatusCode::OffsetOutOfRange, std::move(msg)); }
    static Status Corrupt(std::string msg) { return Status(StatusCode::Corrupt, std::move(msg)); }
    static Status IoError(std::string msg) { return Status(StatusCode::IoError, std::move(msg)); }
    static Status AlreadyExists(std::string msg) { return Status(StatusCode::AlreadyExists, std::move(msg)); }
    static Status RecordTooLarge(std::string msg) { return Status(StatusCode::RecordTooLarge, std::move(msg)); }

private:
    StatusCode m_code;
    std::string m_msg;
};

template <typename T>
class Result {
public:
    Result(T val) : m_status(Status::OK()), m_val(std::move(val)) {}
    Result(Status status) : m_status(std::move(status)) {}

    bool ok() const { return m_status.ok(); }
    const Status& status() const { return m_status; }
    T& value() { return m_val; }
    const T& value() const { return m_val; }

private:
    Status m_status;
    T m_val{};
};

} // namespace streamforge

#endif // STREAMFORGE_STATUS_HPP
