#include "streamforge/ThreadPool.hpp"
#include "streamforge/Logger.hpp"

namespace streamforge {

ThreadPool::ThreadPool(size_t num_workers, MessageHandler& handler, CompletionQueue& comp_queue, WakeupChannel& wakeup)
    : m_handler(handler), m_comp_queue(comp_queue), m_wakeup(wakeup) {
    m_workers.reserve(num_workers);
    for (size_t i = 0; i < num_workers; ++i) {
        m_workers.emplace_back(&ThreadPool::worker_loop, this);
    }
}

ThreadPool::~ThreadPool() {
    stop_and_join();
}

void ThreadPool::enqueue(Task task) {
    if (!m_stopped.load()) {
        m_task_queue.push(std::move(task));
    }
}

void ThreadPool::stop_and_join() {
    bool expected = false;
    if (m_stopped.compare_exchange_strong(expected, true)) {
        m_task_queue.stop();
        for (auto& w : m_workers) {
            if (w.joinable()) {
                w.join();
            }
        }
        m_workers.clear();
    }
}

void ThreadPool::worker_loop() {
    // Workers execute independent request processing using the existing MessageHandler.
    // SINGLE OWNER RULE: Workers NEVER touch network sockets. All socket interactions
    // are strictly performed by the single I/O thread.
    while (true) {
        Task task;
        if (!m_task_queue.pop(task)) {
            break; // TaskQueue was stopped and is empty
        }

        Frame resp = m_handler.handle_request(task.request);
        if (resp.type == MessageType::MSG_ERROR) {
            uint16_t code = 0;
            std::string msg;
            FrameCodec::parse_error_frame(resp, code, msg);
            Logger::instance().warning("Protocol error (code " + std::to_string(code) + "): " + msg);
        }

        std::vector<uint8_t> encoded_resp = FrameCodec::encode(resp);

        // Push completed response bytes into thread-safe completion queue
        m_comp_queue.push({ task.conn_id, std::move(encoded_resp), false });

        // Wake up the I/O thread via loopback socket pair
        m_wakeup.notify();
    }
}

} // namespace streamforge
