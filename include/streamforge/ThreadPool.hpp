#ifndef STREAMFORGE_THREAD_POOL_HPP
#define STREAMFORGE_THREAD_POOL_HPP

#include "streamforge/TaskQueue.hpp"
#include "streamforge/MessageHandler.hpp"
#include "streamforge/WakeupChannel.hpp"
#include <vector>
#include <thread>
#include <atomic>
#include <cstddef>

namespace streamforge {

class ThreadPool {
public:
    ThreadPool(size_t num_workers, MessageHandler& handler, CompletionQueue& comp_queue, WakeupChannel& wakeup);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    void enqueue(Task task);
    void stop_and_join();

    size_t worker_count() const { return m_workers.size(); }

private:
    void worker_loop();

    MessageHandler& m_handler;
    CompletionQueue& m_comp_queue;
    WakeupChannel& m_wakeup;
    TaskQueue m_task_queue;
    std::vector<std::thread> m_workers;
    std::atomic<bool> m_stopped{false};
};

} // namespace streamforge

#endif // STREAMFORGE_THREAD_POOL_HPP
