#ifndef STREAMFORGE_TASK_QUEUE_HPP
#define STREAMFORGE_TASK_QUEUE_HPP

#include "streamforge/FrameCodec.hpp"
#include <queue>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <cstdint>

namespace streamforge {

struct Task {
    uint64_t conn_id{0};
    Frame request;
};

struct Completion {
    uint64_t conn_id{0};
    std::vector<uint8_t> response_bytes;
    bool close_after_send{false};
};

// Thread-safe TaskQueue carrying requests to worker threads.
// Notice: because each connection has at most ONE request in flight,
// the task queue never holds more than max-connections items.
class TaskQueue {
public:
    void push(Task task) {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_queue.push(std::move(task));
        }
        m_cv.notify_one();
    }

    bool pop(Task& out_task) {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cv.wait(lock, [this]() { return m_stop || !m_queue.empty(); });
        if (m_stop && m_queue.empty()) {
            return false;
        }
        out_task = std::move(m_queue.front());
        m_queue.pop();
        return true;
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_stop = true;
        }
        m_cv.notify_all();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_queue.size();
    }

private:
    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
    std::queue<Task> m_queue;
    bool m_stop{false};
};

// Thread-safe CompletionQueue carrying finished responses back to the I/O thread.
class CompletionQueue {
public:
    void push(Completion comp) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_queue.push(std::move(comp));
    }

    std::vector<Completion> pop_all() {
        std::vector<Completion> result;
        std::lock_guard<std::mutex> lock(m_mutex);
        result.reserve(m_queue.size());
        while (!m_queue.empty()) {
            result.push_back(std::move(m_queue.front()));
            m_queue.pop();
        }
        return result;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_queue.size();
    }

private:
    mutable std::mutex m_mutex;
    std::queue<Completion> m_queue;
};

} // namespace streamforge

#endif // STREAMFORGE_TASK_QUEUE_HPP
