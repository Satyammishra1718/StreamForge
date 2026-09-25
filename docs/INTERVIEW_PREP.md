# StreamForge Technical Interview Preparation Guide

This guide is designed to prepare you to defend and explain every architectural decision, line of code, and performance characteristic of StreamForge in a systems engineering or backend infrastructure interview.

---

## Table of Contents
1. [Core Interview Questions by Domain](#1-core-interview-questions-by-domain)
   - [Domain 1: Networking & Windows Operating System](#domain-1-networking--windows-operating-system)
   - [Domain 2: Storage Engine & File Systems](#domain-2-storage-engine--file-systems)
   - [Domain 3: Concurrency, Threading & Memory Model](#domain-3-concurrency-threading--memory-model)
   - [Domain 4: Coordination, Rebalancing & Protocol](#domain-4-coordination-rebalancing--protocol)
   - [Domain 5: Systems Architecture & Tradeoffs](#domain-5-systems-architecture--tradeoffs)
2. [Deep Dives: The 5 Benchmark Interview Prompts](#2-deep-dives-the-5-benchmark-interview-prompts)
   - [Prompt 1: Storage Format & Offset Index Lookup](#prompt-1-storage-format--offset-index-lookup)
   - [Prompt 2: WSAPoll vs. IOCP vs. Thread-per-Connection](#prompt-2-wsapoll-vs-iocp-vs-thread-per-connection)
   - [Prompt 3: Crash Recovery & Corrupted Tail Truncation](#prompt-3-crash-recovery--corrupted-tail-truncation)
   - [Prompt 4: Consumer Group Rebalance & Cooperative Sticky Assignor](#prompt-4-consumer-group-rebalance--cooperative-sticky-assignor)
   - [Prompt 5: End-to-End Backpressure & Socket Throttling](#prompt-5-end-to-end-backpressure--socket-throttling)

---

## 1. Core Interview Questions by Domain

### Domain 1: Networking & Windows Operating System

#### Q1: Why did StreamForge adopt an asynchronous Reactor pattern over Thread-per-Connection?
**Answer:**
In a thread-per-connection model, every connected client requires a dedicated OS thread. On Windows, each thread allocates a default 1 MB virtual memory stack (and minimum 4 KB committed memory), and the Windows OS thread scheduler must context-switch across hundreds of active threads. In benchmarks, thread-per-connection thread counts scale linearly ($O(N)$), causing CPU cache thrashing and memory exhaustion.
StreamForge implements an asynchronous Reactor pattern using a single I/O thread driving `WSAPoll()`. The single I/O thread multiplexes thousands of idle connections with zero thread allocation overhead, delegating only active computation and disk I/O to a fixed-size worker pool ($O(1)$ thread footprint).

#### Q2: How does `WSAPoll()` work on Windows, and what are its subtleties?
**Answer:**
`WSAPoll()` is the Winsock implementation of POSIX `poll()`. It takes an array of `WSAPOLLFD` structures containing socket descriptors and bitmasks of requested events (`POLLRDNORM` for readable, `POLLWRNORM` for writable). It blocks the calling thread until an event fires or a timeout elapses.
Crucially, on Windows, non-blocking connect failures return `POLLHUP` or `POLLERR` rather than writable flags, and `WSAPoll` before Windows 8.1 had historical bugs regarding `POLLHUP`. StreamForge works with clean Windows 10/11 Winsock runtimes and cleanly checks `revents & (POLLERR | POLLHUP)` to detect peer disconnections without spinning.

#### Q3: How do worker threads wake up the `WSAPoll` loop when outbound data is ready?
**Answer:**
The I/O thread blocks inside `WSAPoll()` when there is no incoming traffic. If a worker thread finishes processing a request and queues response bytes in a connection's outbound queue, the I/O thread must be woken immediately to register `POLLWRNORM` and flush the socket.
StreamForge solves this using an internal loopback TCP `WakeupChannel` (a connected socket pair on `127.0.0.1`). The worker thread writes a single byte (`0x01`) to the channel. `WSAPoll()` monitors the read end of the wakeup socket, returns immediately from poll, drains the byte, and registers write readiness for pending connections.

#### Q4: Why is TCP framing necessary, and how does `FrameAssembler` handle packet fragmentation?
**Answer:**
TCP is a byte-stream protocol without message boundaries. A single `send()` call on a client may arrive across multiple `recv()` calls due to MTU limits or TCP windowing; conversely, multiple messages may be coalesced into a single packet (Nagle's algorithm).
StreamForge uses a fixed 12-byte header (`[Length: 4B][Type: 2B][Reserved: 2B][RequestId: 4B]`). `FrameAssembler` maintains a per-connection state machine:
1. Buffers incoming bytes until 12 bytes are accumulated.
2. Decodes `Length` to determine the total expected frame size.
3. Pre-allocates the body buffer and accumulates subsequent socket chunks until `received == Length`.
4. Dispatches the completed `Frame` and resets state for the next header.

---

### Domain 2: Storage Engine & File Systems

#### Q5: What is the anatomy of a StreamForge `.log` file?
**Answer:**
A `.log` file is a contiguous append-only binary sequence of variable-length records. Each record layout is:
`[RecordLength (4B)][Magic (1B)][Attributes (1B)][Timestamp (8B)][Offset (8B)][KeyLength (4B)][KeyBytes][ValLength (4B)][ValBytes][CRC32 (4B)]`.
All integers are big-endian. The `RecordLength` allows sequential scanning to jump from record to record. The trailing IEEE 802.3 `CRC32` protects against bit-rot and partial-write tearings.

#### Q6: What is a sparse index, and why not index every record?
**Answer:**
A dense index maps every offset to a physical file offset. For millions of records, a dense index would consume hundreds of megabytes of memory.
StreamForge uses a **sparse index**: an index entry is appended only after `index_interval_bytes` (e.g. 4096 bytes) of log data have elapsed. Each entry is a fixed 16-byte struct: `[RelativeOffset: 8B][PhysicalPosition: 8B]`.
The sparse index is tiny (roughly $0.4\%$ the size of the `.log` file), easily fitting into RAM or the OS page cache. Reads perform an $O(\log N)$ binary search on the sparse index to find the nearest physical position $\le$ target offset, followed by a short bounded sequential scan of at most 4 KB.

#### Q7: How does memory mapping (`CreateFileMappingW` / `MapViewOfFile`) improve read performance?
**Answer:**
Instead of issuing `ReadFile` system calls that copy index bytes into user-space buffers, `MapViewOfFile` maps the `.index` file directly into the broker process's 64-bit virtual memory space.
The OS page cache manages reading physical pages on demand. In C++, the mapped pointer is cast directly to `const IndexEntry*`, allowing `std::lower_bound` to perform binary search at CPU memory speeds without kernel transitions.

#### Q8: Why does StreamForge pre-allocate segment files?
**Answer:**
On Windows NTFS, appending small chunks repeatedly causes file fragmentation across non-contiguous disk clusters, degrading sequential write throughput.
When a new segment is rolled, StreamForge calls `SetFilePointerEx` followed by `SetEndOfFile` to allocate the full segment size (e.g. 10 MB or 64 MB) up front. Appends write into already-allocated NTFS clusters. On segment sealing, the file pointer is clamped back to the actual valid byte size.

#### Q9: How does retention cleanup (`RetentionManager`) work without interrupting ongoing reads?
**Answer:**
`RetentionManager` periodically evaluates closed segments against time (`retention_ms`) and size (`retention_bytes`) thresholds. Active segments are never evaluated.
When a closed segment qualifies for deletion, the partition's `SharedMutex` is acquired exclusively, the segment is removed from the partition's active segment list, its memory map is unmapped, file handles are closed, and `DeleteFileW` removes the files. Because historical segments are read via shared locks, active reads are not disrupted.

---

### Domain 3: Concurrency, Threading & Memory Model

#### Q10: How does StreamForge achieve concurrent partition reads and writes?
**Answer:**
Every partition is protected by a Windows Slim Reader/Writer Lock (`SharedMutex` wrapping `SRWLOCK` or `std::shared_mutex`).
- **Produces (Appends):** Acquire an exclusive write lock (`std::unique_lock<SharedMutex>`). Only one thread appends to a given partition at any instant.
- **Fetches (Reads):** Acquire a shared read lock (`std::shared_lock<SharedMutex>`). Any number of consumer threads can read from the partition simultaneously without blocking one another.
- Different partitions have independent locks, so writes to Partition 0 never block writes to Partition 1.

#### Q11: Why is an `SRWLOCK` preferred on Windows over `CRITICAL_SECTION`?
**Answer:**
A `CRITICAL_SECTION` is exclusive-only (no shared reader mode) and carries heavier initialization overhead.
An `SRWLOCK` (Slim Reader/Writer Lock) is pointer-sized, requires no heap allocation, does not require explicit destruction, and natively supports shared (reader) and exclusive (writer) modes with minimal latency.

#### Q12: Why are locks never held during network socket operations?
**Answer:**
If a worker thread acquired a partition lock and then attempted to `send()` or `recv()` on a socket, a slow client or stalled TCP window would freeze the entire partition for all other clients.
StreamForge enforces strict lock decoupling:
1. Locks are acquired *only* for the in-memory/disk log operations.
2. The response buffer is serialized.
3. The lock is released.
4. The response buffer is enqueued to the connection's thread-safe outbound queue.

---

### Domain 4: Coordination, Rebalancing & Protocol

#### Q13: What are the states of the `ConsumerGroup` state machine?
**Answer:**
1. **`Empty`:** No active members connected.
2. **`PreparingRebalance`:** A member joined or left; the coordinator is waiting for existing members to send `JoinGroup` requests within `rebalance_timeout`.
3. **`CompletingRebalance`:** All expected members joined; the coordinator elects a leader and waits for `SyncGroup` requests containing partition assignments.
4. **`Stable`:** All members received their assignments and are actively processing and sending heartbeats.
5. **`Dead`:** The group has been destroyed.

#### Q14: How does the coordinator detect dead consumers?
**Answer:**
Every active member periodically sends a `HeartbeatRequest` within `heartbeat_interval_ms` (e.g. 3,000 ms).
The coordinator tracks `last_heartbeat_time` per member. A background check runs every second. If `now - last_heartbeat > session_timeout_ms` (e.g. 10,000 ms), the member is evicted and the group transitions to `PreparingRebalance`.

#### Q15: How are consumer offsets stored and recovered?
**Answer:**
Offsets are not kept in volatile memory alone. When a consumer commits an offset, the coordinator appends an offset record to the internal `__consumer_offsets` topic.
An in-memory hash table (`OffsetStore`) acts as a read cache. On broker startup, `OffsetStore` replays all records from `__consumer_offsets`, rebuilding the latest committed offset for every `(group_id, topic, partition)` tuple.

---

### Domain 5: Systems Architecture & Tradeoffs

#### Q16: Why not use Protobuf or gRPC instead of a custom binary protocol?
**Answer:**
Protobuf introduces external code generation, runtime library dependencies, and field tagging overhead. gRPC mandates HTTP/2 framing, header compression, and TLS overhead.
StreamForge's binary wire protocol is 100% self-contained, header-fixed, zero-dependency, and allows zero-copy parsing with explicit big-endian memory serialization.

#### Q17: Why is pull-based consumption superior to push-based streaming for a message broker?
**Answer:**
In a push model, the broker must track consumer processing capacity and window sizes. A slow consumer gets overwhelmed with packets, causing broker memory exhaustion or network drops.
In a pull model:
- The consumer requests data only when ready.
- Batch sizing (`max_bytes`) is chosen by the consumer.
- Consumers can easily pause, buffer, or rewind offsets to re-process historical records.

---

## 2. Deep Dives: The 5 Benchmark Interview Prompts

### Prompt 1: Storage Format & Offset Index Lookup
> **Question:** "Explain the storage format and how an index lookup translates an offset to a byte offset."

**Explanation:**
1. **Topic and Partition Directory Hierarchy:**
   Data is stored under `data_dir/<topic_name>/partition_<id>/`. Each partition contains paired files named after their base offset: `00000000000000000000.log` and `00000000000000000000.index`.
2. **Log File Record Layout:**
   Each record in `.log` has a 4-byte big-endian `record_length` prefix followed by:
   - `magic` (1B) + `attributes` (1B)
   - `timestamp` (8B) + `offset` (8B)
   - `key_length` (4B) + `key_data`
   - `value_length` (4B) + `value_data`
   - `crc32` (4B)
3. **Index File Structure:**
   Each `.index` file contains fixed 16-byte entries:
   `[relative_offset (8B)][physical_position (8B)]`.
   Where $relative\_offset = offset - base\_offset$, and $physical\_position$ is the exact byte offset from the start of the `.log` file.
4. **Step-by-Step Lookup Translation:**
   - **Step 1 (Segment Selection):** Given target offset $T$, the partition performs `std::upper_bound` across its list of segments using `base_offset`. The segment immediately preceding the upper bound is selected (the segment where $base\_offset \le T < next\_base\_offset$).
   - **Step 2 (Index Binary Search):** The selected segment computes target relative offset $R = T - base\_offset$. It queries the memory-mapped `.index` file using `std::lower_bound` to find the largest index entry whose $relative\_offset \le R$. This yields a candidate `physical_position` $P$.
   - **Step 3 (Sequential Forward Scan):** The segment opens the `.log` file and seeks directly to byte offset $P$. Because the index is sparse (written every 4 KB), byte offset $P$ corresponds to a record at or slightly before offset $T$.
   - **Step 4 (Exact Match & Verification):** The engine reads record headers sequentially from $P$ forward, verifying CRC32 and advancing past records until the record with $offset == T$ is reached.
   - **Complexity:** $O(\log S)$ to find the segment + $O(\log I)$ binary search in the mapped index + bounded $O(K)$ scan of at most 4 KB of log data.

---

### Prompt 2: WSAPoll vs. IOCP vs. Thread-per-Connection
> **Question:** "Explain the difference between WSAPoll and IOCP, why we chose WSAPoll, and when you would switch to IOCP."

**Explanation:**
1. **Architectural Comparison:**
   - **Thread-per-Connection:** Each socket is assigned a blocking OS thread. Does not scale past several hundred connections due to thread stack memory allocations (1 MB per thread virtual space) and OS context-switching overhead.
   - **WSAPoll (Reactor Model):** Synchronous I/O multiplexing. A single thread asks the OS kernel: *"Which of these $N$ sockets are currently ready to read or write?"* The thread then performs non-blocking `recv()` and `send()` operations.
   - **IOCP (Input/Output Completion Ports - Proactor Model):** True asynchronous I/O. User code initiates an overlapped operation (`WSARecv` / `WSASend`) and immediately continues. The Windows kernel performs the DMA/network transfer directly into user buffers. When complete, the kernel posts a completion packet to a kernel queue. A pool of worker threads calls `GetQueuedCompletionStatus()` to process completed transfers.
2. **Why StreamForge Chose WSAPoll:**
   - **Conceptual Clarity:** StreamForge is built as an educational, auditable systems project. The Reactor pattern with `WSAPoll` directly mirrors standard POSIX event loop architectures (`poll`/`epoll`), allowing every phase of frame assembly and backpressure to be understood and reasoned about without opaque kernel overlapped buffer state.
   - **High Performance:** `WSAPoll` easily sustained **87,000+ requests/sec** and handled **1,000 concurrent connections** with sub-6 ms p99 latency in our benchmarks on standard consumer hardware.
3. **When to Switch to IOCP:**
   - When scaling to **10,000+ to 100,000+ concurrent connections** (C10K / C100K problem).
   - In `WSAPoll`, polling cost is $O(N)$ with respect to monitored file descriptors. IOCP completion notifications are $O(1)$ relative to active events, completely decoupling idle connection counts from polling overhead.
   - When zero-copy kernel networking is mandatory: IOCP allows user-allocated buffers to be locked directly by kernel drivers, avoiding intermediate socket buffer copying.

---

### Prompt 3: Crash Recovery & Corrupted Tail Truncation
> **Question:** "Walk through what happens on broker crash recovery: step by step, what files are read, what is verified, how corruption is handled."

**Explanation:**
1. **Directory Discovery:**
   On startup, `TopicManager::load_all()` enumerates all directories in `data_dir`. Each subdirectory represents a topic, and numeric child directories represent partitions.
2. **Segment Pairing:**
   Inside each partition directory, the recovery scanner matches `.log` and `.index` files by their 20-digit zero-padded base offset filenames and sorts them in ascending order.
3. **Fast vs. Full Startup Scan:**
   - **Clean Shutdown Marker:** On a graceful shutdown, StreamForge writes a clean shutdown metadata file. If present and `startup_scan` is set to `quick`, historical sealed segments are assumed intact and only the active (last) segment is validated.
   - **Full Scan:** If an ungraceful crash occurred (or `--startup-scan full` is specified), all segments are scanned.
4. **Log Validation & Corrupted Tail Truncation:**
   - The scanner opens the `.log` file and reads records sequentially from byte 0.
   - For every record, it verifies:
     1. Are there at least 4 bytes remaining for `record_length`?
     2. Does the file have enough remaining bytes to hold `record_length`?
     3. Does the magic byte match (`magic == 1`)?
     4. Is the record offset $\ge$ expected next offset?
     5. Does the IEEE 802.3 CRC32 computed over payload match the stored `crc32`?
   - **Handling Partial Writes / Torn Pages:** If a power outage or crash interrupted a write mid-record, CRC validation fails or the file ends prematurely.
   - **Truncation:** The engine immediately stops scanning, records the last valid byte position $P_{valid}$, and calls Win32 `SetFilePointerEx` to $P_{valid}$ followed by `SetEndOfFile`. All corrupted or partially-written trailing bytes are discarded.
5. **Index Reconstruction:**
   - If the `.index` file was missing, corrupted, or mismatched in size, it is deleted.
   - The recovery engine creates a new `.index` file and scans the verified valid `.log` file, appending a 16-byte index entry every `index_interval_bytes`.
6. **Offset Synchronization:**
   The partition's `next_offset` atomic is initialized to $LastValidOffset + 1$.

---

### Prompt 4: Consumer Group Rebalance & Cooperative Sticky Assignor
> **Question:** "Explain how consumer group rebalancing works, the sticky assignor algorithm, and why the cooperative sticky assignor is better than a simple range or round-robin assignor."

**Explanation:**
1. **Rebalance Protocol Lifecycle:**
   - **Phase 1 (JoinGroup):** When a consumer starts, leaves, or times out, the group enters `PreparingRebalance`. All consumers send `JoinGroupRequest`. The coordinator selects the first member as the **leader** and sends the member list to the leader.
   - **Phase 2 (SyncGroup):** The leader computes partition assignments for the entire group using `StickyAssignor` and submits them in a `SyncGroupRequest`. Other members send empty `SyncGroupRequest` calls. The coordinator responds to each member with its assigned partitions. The group transitions to `Stable`.
2. **The Sticky Assignor Algorithm:**
   - **Goal 1 (Fair Balance):** Every consumer receives $\lfloor P / C \rfloor$ or $\lceil P / C \rceil$ partitions, where $P$ is partition count and $C$ is consumer count.
   - **Goal 2 (Maximum Stickiness):** Preserve existing partition assignments as much as possible.
   - **Algorithm Steps:**
     1. Retain all current partition assignments for members that are still active.
     2. Remove assignments for members that left.
     3. Identify over-subscribed consumers (members holding more than their fair share) and unassign the excess partitions.
     4. Collect all unassigned partitions and distribute them among under-subscribed consumers, prioritizing consumers that previously owned them.
3. **Why Sticky is Superior to Range or Round-Robin:**
   - **Range Assignor:** Divides partitions into contiguous ranges per topic. When a consumer leaves, partitions shift dramatically across all surviving consumers.
   - **Eager Revocation Problem:** In range/round-robin assignors, every rebalance causes consumers to drop all cached data, state-store caches, and in-flight buffers, leading to significant latency spikes.
   - **Cooperative Sticky Advantage:** Only partitions that must move are reassigned. A surviving consumer retains 80–90% of its partitions across a rebalance, preserving local consumer state caches and avoiding network churn.

---

### Prompt 5: End-to-End Backpressure & Socket Throttling
> **Question:** "Explain how the broker handles backpressure end-to-end: what triggers it, what changes in the event loop, what the client sees, and what recovers it."

**Explanation:**
1. **The Trigger (TaskQueue High Watermark):**
   - The I/O reactor accepts requests and enqueues them into the `TaskQueue`.
   - If producers send data faster than the storage engine and worker threads can flush to disk, tasks accumulate in the `TaskQueue`.
   - When `task_queue.size() >= HIGH_WATERMARK` (e.g. 500 tasks), backpressure is triggered.
2. **Event Loop Throttling:**
   - The I/O reactor detects the high-watermark state.
   - For all active client sockets, the reactor:
     1. Removes `POLLRDNORM` from their `WSAPOLLFD` interest mask. The reactor stops reading incoming data from client sockets.
     2. Sets `setsockopt(sock, SOL_SOCKET, SO_RCVBUF, 0)`. Setting the TCP receive window buffer to 0 forces the Windows TCP stack to advertise a **TCP Zero Window** to clients.
3. **What the Client Sees:**
   - The client's operating system receives the TCP Zero Window probe from the broker.
   - The client's local OS network socket buffer fills up.
   - The client's `send()` call blocks (or returns `EWOULDBLOCK` / `WSAEWOULDBLOCK` if non-blocking).
   - The producer is physically halted from transmitting more bytes without throwing an error or dropping data.
4. **The Recovery (TaskQueue Low Watermark):**
   - As worker threads process queued tasks and write them to disk, queue depth drains.
   - When `task_queue.size() <= LOW_WATERMARK` (e.g. 200 tasks), backpressure is deactivated.
   - The reactor restores normal receive buffers (`SO_RCVBUF = 65536`) and re-enables `POLLRDNORM` in the `WSAPoll` interest set.
   - The Windows TCP stack sends a **TCP Window Update** packet to clients, and producers automatically resume sending at full rate.
