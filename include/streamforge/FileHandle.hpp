#ifndef STREAMFORGE_FILE_HANDLE_HPP
#define STREAMFORGE_FILE_HANDLE_HPP

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <windows.h>

#include <cstdint>
#include <string>
#include <cstring>
#include <filesystem>

namespace streamforge {

class FileHandle {
public:
    FileHandle() : m_handle(INVALID_HANDLE_VALUE) {}
    explicit FileHandle(HANDLE h) : m_handle(h) {}
    ~FileHandle() { close(); }

    FileHandle(const FileHandle&) = delete;
    FileHandle& operator=(const FileHandle&) = delete;

    FileHandle(FileHandle&& other) noexcept : m_handle(other.m_handle) {
        other.m_handle = INVALID_HANDLE_VALUE;
    }

    FileHandle& operator=(FileHandle&& other) noexcept {
        if (this != &other) {
            close();
            m_handle = other.m_handle;
            other.m_handle = INVALID_HANDLE_VALUE;
        }
        return *this;
    }

    bool is_valid() const {
        return m_handle != INVALID_HANDLE_VALUE && m_handle != nullptr;
    }

    HANDLE get() const { return m_handle; }

    void close() {
        if (is_valid()) {
            CloseHandle(m_handle);
            m_handle = INVALID_HANDLE_VALUE;
        }
    }

    bool write(const void* buffer, DWORD size, DWORD* bytes_written = nullptr) {
        if (!is_valid()) return false;
        // On Windows, passing 0xFFFFFFFF in Offset and OffsetHigh instructs WriteFile
        // to atomically append to the end of the file, completely immune to file pointer
        // modifications from concurrent or positional read_at calls.
        OVERLAPPED ov;
        std::memset(&ov, 0, sizeof(ov));
        ov.Offset = 0xFFFFFFFF;
        ov.OffsetHigh = 0xFFFFFFFF;
        DWORD written = 0;
        BOOL res = WriteFile(m_handle, buffer, size, &written, &ov);
        if (bytes_written) *bytes_written = written;
        return (res != FALSE) && (written == size);
    }

    bool read_at(uint64_t file_offset, void* buffer, DWORD size, DWORD* bytes_read = nullptr) const {
        if (!is_valid()) return false;
        OVERLAPPED ov;
        std::memset(&ov, 0, sizeof(ov));
        ov.Offset = static_cast<DWORD>(file_offset & 0xFFFFFFFF);
        ov.OffsetHigh = static_cast<DWORD>((file_offset >> 32) & 0xFFFFFFFF);

        DWORD read_count = 0;
        BOOL res = ReadFile(m_handle, buffer, size, &read_count, &ov);
        if (bytes_read) *bytes_read = read_count;
        return (res != FALSE) && (read_count == size);
    }

    uint64_t get_size() const {
        if (!is_valid()) return 0;
        LARGE_INTEGER sz;
        if (GetFileSizeEx(m_handle, &sz)) {
            return static_cast<uint64_t>(sz.QuadPart);
        }
        return 0;
    }

    void flush() {
        if (is_valid()) {
            FlushFileBuffers(m_handle);
        }
    }

    bool truncate(uint64_t new_size) {
        if (!is_valid()) return false;
        LARGE_INTEGER li;
        li.QuadPart = static_cast<LONGLONG>(new_size);
        if (!SetFilePointerEx(m_handle, li, nullptr, FILE_BEGIN)) {
            return false;
        }
        return SetEndOfFile(m_handle) != FALSE;
    }

    static FileHandle open_read_write(const std::filesystem::path& path, bool create_if_missing = true) {
        DWORD creation = create_if_missing ? OPEN_ALWAYS : OPEN_EXISTING;
        HANDLE h = CreateFileW(
            path.wstring().c_str(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr,
            creation,
            FILE_ATTRIBUTE_NORMAL,
            nullptr
        );
        return FileHandle(h);
    }

    static FileHandle open_read_only(const std::filesystem::path& path) {
        HANDLE h = CreateFileW(
            path.wstring().c_str(),
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            nullptr
        );
        return FileHandle(h);
    }

private:
    HANDLE m_handle{INVALID_HANDLE_VALUE};
};

} // namespace streamforge

#endif // STREAMFORGE_FILE_HANDLE_HPP
