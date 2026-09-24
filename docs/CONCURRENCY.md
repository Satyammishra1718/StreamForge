# StreamForge Concurrency Model & Lock Hierarchy

This document details the multi-threading architecture, lock hierarchy, thread boundaries, and concurrency guarantees of StreamForge Milestone 4.

---

## 1. Threading Architecture & Single-Owner Rule

Prior to Milestone 4, StreamForge spawned one dedicated thread per client connection. In Milestone 4, this model is replaced by a scalable event-driven I/O loop backed by a fixed worker thread pool.

```
                           ┌────────────────────────┐
                           │   TCP Clients (N)      │
                           └───────────┬────────────┘
                                       │ Non-blocking Sockets
                                       ▼
                     ┌───────────────────────────────────┐
                     │          I/O Event Loop           │
                     │  - Runs WSAPoll                   │
                     │  - Single Owner of all Sockets    │
                     │  - Assembles Frames               │
                     │  - Enforces Backpressure & Limits │
                     └─────────┬───────────────▲─────────┘
                   Request Task│               │Response Completion
                     (1/conn)  │               │+ Wakeup Channel
                               ▼               │
                     ┌───────────────────┐     │
                     │ Task Queue (FIFO) │     │
                     └─────────┬─────────┘     │
                               │               │
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
┌──────────────┐       ┌──────────────┐       ┌──────────────┐
│Worker Thread1│       │Worker Thread2│       │Worker ThreadN│
│ Runs Message │       │ Runs Message │       │ Runs Message │
│   Handler    │       │   Handler    │       │   Handler    │
└───────┬──────┘       └───────┬──────┘       └───────┬──────┘
        └──────────────────────┼──────────────────────┘
                               ▼
                   ┌───────────────────────┐
                   │ Completion Queue      │
                   └───────────────────────┘
```

### The Single-Owner Rule for Network Sockets
- **Rule**: ONLY the single I/O thread ever calls `socket`, `bind`, `listen`, `accept`, `recv`, `send`, `shutdown`, or `closesocket`.
- **Rationale**:
  1. **Race Elimination**: Having multiple threads perform concurrent `send()` or `recv()` on the same socket descriptor leads to interleaved data corruption and race conditions under non-blocking I/O.
  2. **No Socket Locking**: Because only the I/O thread interacts with sockets, no mutexes or locks are needed on socket handles.
  3. **Zero Dangling Pointers**: If a connection drops, the I/O thread frees the connection immediately. Because worker threads only receive decoupled data frames and unique 64-bit connection IDs (generations), worker threads never touch or reference socket handles. If a response completion arrives for a closed connection, the I/O thread simply drops it safely without dereferencing any invalid memory.

---

## 2. Lock Catalog & Shared-State Review

Every lock in StreamForge is documented below with the resource it guards, its type, and which threads access it:

| Mutex / Lock | Type | Protected State | Accessed By |
|---|---|---|---|
| `TopicManager::m_mutex` | `std::shared_mutex` | `m_topics` hash map registry | Worker threads (via MessageHandler) |
| `Topic::m_mutex` | `std::mutex` | `m_partitions` vector and topic metadata | Worker threads (via MessageHandler) |
| `Partition::m_mutex` | `std::mutex` | Active segment, `m_segments`, index, offsets | Worker threads (via MessageHandler) |
| `TaskQueue::m_mutex` | `std::mutex` | Worker request task FIFO queue | I/O thread (push), Worker threads (pop) |
| `CompletionQueue::m_mutex` | `std::mutex` | Worker response completion FIFO queue | Worker threads (push), I/O thread (pop_all) |
| `Logger::m_mutex` | `std::mutex` | Standard output console writing | I/O thread, Worker threads, CLI |

### Detailed Safety Analysis: Why `Partition` Retains Exclusive Mutex
The milestone specification allows replacing `Partition::m_mutex` with `std::shared_mutex` (appends exclusive, reads shared) **only if** readers are provably safe against concurrent appends to the active segment.

During our concurrency review, concurrent shared reading alongside exclusive appending was determined **unsafe** for three specific reasons:
1. **`OffsetIndex` Vector Reallocation**:
   `OffsetIndex::maybe_append()` pushes new `IndexEntry` structs into an internal `std::vector<IndexEntry> m_entries`. `std::vector::push_back` can reallocate its buffer, which invalidates all iterators and raw memory pointers. Concurrently, a reader calling `Partition::read()` executes `OffsetIndex::lookup()`, which runs `std::lower_bound` across `m_entries`. Reading a vector while another thread reallocates it is an undefined behavior data race and causes fatal access violations.
2. **Segment Rolling and Vector Mutation**:
   When the active log segment reaches `segment_max_bytes`, `roll_segment_unlocked()` creates a new `LogSegment` and invokes `m_segments.push_back()`. Readers iterating over `m_segments` concurrently would experience a data race on `m_segments`.
3. **Log Size Updates**:
   During `LogSegment::append()`, `m_log_size` is updated without atomic memory ordering while `LogSegment::lookup_file_position` and bounds checks read `m_log_size`.

**Conclusion**: Retaining the exclusive `std::mutex` on each `Partition` guarantees 100% memory safety and eliminates all data races. Concurrency between different partitions and different topics remains completely parallel and unblocked, because each partition has its own independent mutex.

### `TopicManager` Shared Lock Concurrency
- `TopicManager::m_mutex` is upgraded to `std::shared_mutex`:
  - **Shared Locks (`std::shared_lock<std::shared_mutex>`)**: Used by `get_topic()` and `list_topics()`. Multiple worker threads can simultaneously look up topics or list topics without contending with each other.
  - **Exclusive Locks (`std::unique_lock<std::shared_mutex>`)**: Used by `create_topic()` and `open_and_recover_all()`. Mutations to the topic registry map are serialized and safely isolated.

### Lock-Free & Atomic Components
1. `RoundRobinPartitioner::m_counter` (`std::atomic<uint32_t>`):
   Provides wait-free, round-robin partition distribution across concurrent unkeyed record batches using `fetch_add(1, std::memory_order_relaxed)`.
2. `WakeupChannel::m_wake_pending` (`std::atomic<bool>`):
   Deduplicates signaling between worker threads and the I/O thread using `exchange(true, std::memory_order_acq_rel)`.
3. `TcpServer::m_shutdown_requested` and `m_running` (`std::atomic<bool>`):
   Thread-safe coordination of server lifecycle across the console control handler and the event loop.

---

## 3. Strict Lock Acquisition Hierarchy (Deadlock Prevention)

To mathematically prevent deadlocks (violating Coffman's circular wait condition), locks must strictly be acquired in descending rank order:

```
[Level 1: TopicManager::m_mutex] (shared or unique)
           │
           ▼
[Level 2: Topic::m_mutex] (plain mutex)
           │
           ▼
[Level 3: Partition::m_mutex] (plain mutex)
```

- A thread holding `Partition::m_mutex` may NEVER attempt to acquire `Topic::m_mutex` or `TopicManager::m_mutex`.
- A thread holding `Topic::m_mutex` may NEVER attempt to acquire `TopicManager::m_mutex`.
- Queue locks (`TaskQueue::m_mutex` and `CompletionQueue::m_mutex`) and `Logger::m_mutex` are leaf locks with microsecond durations; they are never held while acquiring any storage or topic locks.

---

## 4. Backpressure and Ordering Guarantees

1. **Strict Request Ordering (Max 1 In-Flight Request Per Connection)**:
   - For any single client connection, at most **one request** is dispatched to the worker thread pool at any time.
   - Pipelined requests received on the same connection remain buffered in the connection's `pending_requests` queue on the I/O thread until the prior request's completion response is written to the output buffer.
   - *Cost*: Pipelined requests on a single connection are serialized through the worker pool rather than executing concurrently across multiple CPU cores.
   - *Benefit*: Responses are guaranteed to return in exact FIFO request order with zero reordering logic, and the total tasks queued across the worker pool cannot exceed `--max-connections`.
2. **Input Backpressure**:
   - When a connection's input buffer reaches `max_input_buffer_bytes` (default 2 MiB) or its pending request queue exceeds 1,000 requests, the I/O loop clears `POLLIN` for that socket, forcing the client's TCP window to close until the server drains the buffer.
3. **Slow-Consumer Defense**:
   - If a client stops reading and its unsent output buffer exceeds `--max-output-buffer-bytes` (default 8 MiB), the server logs a warning and forcibly closes the connection to protect server memory.
4. **Slow-Loris Defense**:
   - If a client sends partial frame bytes and fails to complete the frame within `--read-stall-timeout-sec` (default 30s), the server terminates the connection. Idle connections (0 bytes buffered) are not penalized.
