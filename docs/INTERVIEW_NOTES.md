# StreamForge Interview & Technical Notes

---

## Milestone 1: Winsock TCP Server and Binary Protocol

### 1. Request Flow Walkthrough
Every client request processed by StreamForge follows a clear 6-step lifecycle:

1. **Client Sending Raw Bytes**: The client serializes a request into a binary frame (`[4-byte length][1-byte type][4-byte request_id][N-byte body]`) and transmits the bytes over a TCP socket connection.
2. **`Socket::recv_exact()`**: The server reads the frame from the socket in two stages:
   - First, it reads exactly 4 bytes to determine the `length` of the incoming frame payload.
   - Second, after validating that `length` is between 5 bytes (minimum header size) and 1 MiB (maximum frame limit), it loops `recv()` until all `length` bytes of payload are buffered in memory. This guarantees partial TCP reads do not corrupt frame boundaries.
3. **`FrameCodec` Decoding**: The raw payload bytes are parsed:
   - `type` (1 byte, e.g., `0x01` PING, `0x02` ECHO).
   - `request_id` (4 bytes, converted from network byte order / big-endian to host byte order using `ntohl`).
   - `body` (N = length - 5 bytes).
4. **`MessageHandler` Dispatch**: The server inspects `type` and dispatches the request:
   - `0x01` (PING) -> generates a `PONG` (`0x81`) response with the same `request_id`.
   - `0x02` (ECHO) -> generates an `ECHO_REPLY` (`0x82`) response carrying the exact body payload and matching `request_id`.
   - Unknown type -> generates an `ERROR` (`0xFF`) response with error code `1` (`UNKNOWN_TYPE`). The connection remains open for subsequent valid requests.
5. **`FrameCodec` Encoding**: The response `Frame` object is serialized into a contiguous byte buffer. Multi-byte integers (`length`, `request_id`, `error_code`) are converted back to network byte order using `htonl` / `htons`.
6. **`Socket::send_all()`**: The encoded buffer is transmitted back to the client using a loop around `send()` to ensure every byte is fully written to the socket buffer before returning.

---

### 2. Deep-Dive Interview Questions & Answers

#### Q1: Why length-prefixed framing instead of newline-delimited text?
- **Binary & Payload Agnostic**: Newline-delimited text formats (like HTTP/1.1 chunking or Redis RESP simple strings) require parsing every incoming byte for delimiter characters (`\n` or `\r\n`). If a message body contains raw binary data (e.g. serialized protobuf, compressed audio, or video streams) that includes `0x0A` (`\n`), delimiter parsing breaks or requires expensive escaping. Length-prefixing allows arbitrary binary payloads of any structure or encoding without escaping.
- **Performance & Memory Allocation**: With a length prefix, the server knows the exact total size of the frame up front by reading just 4 bytes. It can allocate a buffer of exact capacity once, avoiding dynamic reallocation during stream parsing.
- **Simple Parsing**: Skipping to frame boundaries is $O(1)$ array indexing instead of byte-by-byte string scanning.

#### Q2: Why can a single `recv()` return half a frame, or two frames at once?
- **TCP is a Byte Stream, Not a Packet Protocol**: TCP operates at the transport layer (Layer 4) and provides a continuous stream of bytes without message boundaries.
- **Nagle's Algorithm & TCP Segmentation**: IP packets can be fragmented, merged, or delayed in transport due to MTU limits, OS socket buffer sizes, or network congestion.
- **Scenario A (Partial Frame)**: A client sends a 100-byte frame, but `recv()` returns only 40 bytes because the remaining 60 bytes are still in flight or held in the kernel socket buffer.
- **Scenario B (Multiple Frames)**: A fast client sends three 20-byte frames in quick succession. The server's OS buffers them together, so a single `recv()` call returns all 60 bytes.
- **Solution**: Applications must implement framing logic (like `recv_exact()`) to accumulate bytes until a full frame is reconstituted.

#### Q3: Why network byte order (big-endian)?
- **Heterogeneous Systems**: Different CPU architectures store multi-byte integers differently in RAM. x86_64 / ARM64 are predominantly Little-Endian (least significant byte at lowest address), while network hardware and older systems (e.g., SPARC, IBM mainframes) were Big-Endian (most significant byte at lowest address).
- **Universal Standard**: IETF RFCs establish Big-Endian as standard "Network Byte Order". Converting integers using `htonl`/`ntohl` ensures that a Windows Little-Endian client and a Linux/ARM server interpret integer fields (`length`, `request_id`) identically regardless of native CPU endianness.

#### Q4: What does `WSAStartup` do and why is it needed?
- **Windows Socket API Initialization**: Unlike POSIX (Linux/macOS) where socket syscalls are part of the core kernel C library, Windows network sockets are implemented in a dynamically linked DLL (`ws2_32.dll`).
- **Function**: `WSAStartup(MAKEWORD(2,2), &wsaData)` initializes the Windows Sockets DLL for the calling process, verifies version compatibility (requesting Winsock version 2.2), and allocates internal OS networking data structures.
- **Requirement**: Calling any Winsock socket function (like `socket()`, `bind()`, `listen()`) before `WSAStartup` returns `WSANOTINITIALISED` (error code 10093).
- **RAII Practice**: Wrapping `WSAStartup` and `WSACleanup` in an RAII class ensures sockets are properly initialized at startup and kernel socket resources are released upon process exit.

#### Q5: What are the downsides of thread-per-connection, and why will we replace it later?
- **High Memory Footprint**: Each OS thread on Windows reserves a default 1 MiB stack. 1,000 active connections consume 1 GiB of RAM just for thread stacks.
- **High Context-Switch Overhead**: Operating system thread scheduling overhead grows non-linearly with thousands of concurrent threads. CPU time is spent context switching between threads rather than executing application logic.
- **Scalability Limit**: Thread-per-connection fails under C10K / C100K high-concurrency loads.
- **Evolution Plan**:
  - **Milestone 4**: We will introduce a **Thread Pool** with a task queue to decouple thread counts from connection counts.
  - **Future High-Performance Iteration**: We can leverage Windows **IOCP** (Input/Output Completion Ports) with asynchronous non-blocking I/O (`WSARecv`/`WSASend`) for event-driven $O(1)$ scalability under tens of thousands of concurrent connections.

---

## Milestone 2: Partitioned Append-Only Log Storage Engine

### 1. Storage Operations Walkthrough

#### A. Appending One Record
1. **Partition Selection**: If a key is provided, `KeyHashPartitioner` hashes the key using FNV-1a (`hash(key) % num_partitions`) to map the message to a specific partition ID. If no key is provided, `RoundRobinPartitioner` cycles through partitions.
2. **Locking & Segment Capacity Check**: The `Partition` locks its mutex. It retrieves the current active `LogSegment` (the last segment in the partition). If adding the new record would exceed `segment_max_bytes` (and the active segment is not empty), the active segment is sealed and a new `LogSegment` is created with `base_offset` equal to `next_offset`.
3. **Record Encoding**: `RecordCodec::encode()` serializes the record:
   - Sets 64-bit monotonically increasing `offset` and 64-bit Unix timestamp `timestamp_ms`.
   - Packs header fields in Big-Endian: `[u32 length][u32 crc32][u64 offset][i64 timestamp_ms][u32 key_len][bytes key][bytes value]`.
   - Computes table-driven IEEE 802.3 CRC32 over `[offset ... value]` payload and stores it in the header.
4. **Sparse Indexing**: If this is the segment's first record OR the byte distance since the last index entry is `>= index_interval_bytes` (4096 bytes), a new index entry `[u32 relative_offset][u32 file_position]` is appended to the in-memory `OffsetIndex` and written to the `.index` file.
5. **Disk Write & Sync**: The encoded record buffer is written to the `.log` file at the current end of file using `WriteFile()`. If `sync_on_append == true`, `FlushFileBuffers()` is invoked to force OS kernel dirty pages to physical storage before returning the assigned offset.

#### B. Reading Offset 1,234 from a Partition with 5 Segments
1. **Segment Binary Search**: The `Partition` binary searches its list of 5 `LogSegment`s by `base_offset` to find the segment where `base_offset <= 1234 < next_segment.base_offset`.
2. **Sparse Index Binary Search**: Inside that segment's in-memory `OffsetIndex`, it binary searches for the entry with the largest `relative_offset <= (1234 - base_offset)`. This yields an exact `.log` file byte position $P$.
3. **Positional Read & Forward Scan**: The server executes a positional read at file position $P$ using `ReadFile()` with an `OVERLAPPED` offset structure (without altering the Win32 file pointer).
4. **Sequential Record Verification**: The reader decodes records sequentially starting at $P$:
   - Parses each record header, verifies its length, and validates its CRC32 checksum.
   - Skips records with `offset < 1234`.
   - Collects records into the `ReadResult` buffer once `offset >= 1234` until `max_messages` or `max_bytes` limit is met (spanning into subsequent segments if needed).

---

### 2. Deep-Dive Storage Questions & Answers

#### Q1: Why an append-only log? Why is sequential writing fast?
- **Sequential vs. Random I/O**: Solid-State Drives (SSDs) and traditional Hard Disk Drives (HDDs) execute sequential writes orders of magnitude faster than random writes because sequential I/O avoids random block erase cycles on SSD flash cells and disk head positioning delays on HDDs.
- **No In-Place Modifications**: In an append-only log, new records are always appended at the end of the file. Existing data is immutable, eliminating lock contention for updates, fragmentation, and complex B-Tree page rebalancing.

#### Q2: Why split the log into segments?
- **Garbage Collection & Retention**: Old data can be purged or archived by simply deleting old segment files (`.log` and `.index` pairs) in $O(1)$ filesystem operations without rewriting active data files.
- **Index Efficiency & Memory Footprint**: Smaller individual segment files keep index entries compact (using 32-bit relative offsets instead of 64-bit absolute offsets).
- **Crash Recovery Scope**: On startup, only the active segment needs full recovery scanning and index rebuilding; older sealed segments can be trusted.

#### Q3: Why a sparse index instead of one entry per record?
- **Memory Footprint**: An index with one entry per record scales linearly with message count, consuming massive RAM. A sparse index (recording byte position once every 4 KB) reduces index memory overhead by 10x to 100x while keeping lookup fast.
- **Time Complexity**: Finding a record takes $O(\log N_{\text{index}} + k)$ where $N_{\text{index}}$ is the number of sparse index entries and $k$ is the small sequential scan distance within the 4 KB chunk ($k \le 4096$ bytes).

#### Q4: What is the CRC for, and what does it not protect against?
- **Purpose**: The 32-bit CRC32 checksum protects against silent data corruption, bit rot, torn writes, or partial disk flushes during power failure by verifying payload integrity on read.
- **What it does NOT protect against**:
  - Malicious tampering (CRC32 is not cryptographically secure; an adversary can recalculate valid CRCs for altered data; SHA-256 or HMAC would be required).
  - Whole-file deletion or filesystem metadata corruption.

#### Q5: What does `FlushFileBuffers` do, and what is the cost of `sync_on_append`?
- **`FlushFileBuffers(HANDLE)`**: Forces the Windows kernel page cache to push dirty file pages directly to physical storage media (flushing drive write caches).
- **Cost**: Calling `FlushFileBuffers` on every append (`sync_on_append == true`) guarantees zero data loss on sudden power loss, but severely limits throughput to the physical disk latency (e.g. ~100-500 IOPS on HDDs or ~10,000 IOPS on SSDs). Setting `sync_on_append == false` relies on OS background page flushing for 10x-100x higher throughput, trading off potential loss of un-flushed tail records during sudden hardware power failure.

#### Q6: Why does the same key always land in the same partition, and what ordering guarantee does that give? What guarantee is NOT given across partitions?
- **Key Partitioning**: `KeyHashPartitioner` computes `hash(key) % num_partitions`. Because the hash function is deterministic, any record with key $K$ lands in partition $P_k$ every time.
- **Partition-Level Ordering Guarantee**: Records within a single partition are strictly ordered by offset (0, 1, 2, ...). Therefore, all messages for key $K$ are processed in exact write sequence.
- **Cross-Partition Non-Guarantee**: StreamForge (like Apache Kafka) provides **NO global ordering guarantee across different partitions**. Messages written to Partition 0 and Partition 1 concurrently have independent offset sequences.

#### Q7: What can go wrong if the machine loses power mid-append, and how does our startup scan handle it?
- **Failure Mode**: A power outage mid-append leaves a "torn tail"—a partially written record or incomplete length header at the end of the active `.log` file.
- **Startup Recovery Scan**:
  - `TopicManager` scans the active segment from byte 0 to the end of file.
  - It validates the header length, payload bounds, and CRC32 checksum for every record.
  - When it encounters a torn/corrupted tail record, it truncates the `.log` file back to the end of the last fully valid record using `SetEndOfFile()`, logs a `WARNING` specifying how many partial bytes were truncated, and sets `next_offset` to continue cleanly from the last valid record. Valid records are never lost.

---

## Milestone 3: Produce and Fetch Messages Over TCP

### 1. Request Lifecycle Walkthroughs

#### A. Producing One Record End-to-End
1. **Client Framing**: The client constructs a `ProduceRequest` containing `{ topic = "orders", partition = -1, records = [{ key = "user_10", val = "pay" }] }`. The payload is serialized using `BodyWriter` into a binary frame prefixed with `[u32 length][u8 type=0x11][u32 request_id]` in Big-Endian.
2. **Network Reception**: The server's worker thread reads the 4-byte frame length via `Socket::recv_exact()`, verifies `length <= 1 MiB`, and receives the full payload.
3. **Body & Bounds Validation**: `BodyReader` parses the request. It validates that string lengths and byte payload lengths fit within the buffer boundaries before reading.
4. **Partition Routing & Atomic Append**:
   - `TopicManager` looks up topic `"orders"`.
   - Because `partition == -1`, `Topic::produce_batch()` inspects `records[0].key` (`"user_10"`). Since the key is non-empty, it invokes `KeyHashPartitioner::partition("user_10", 3)` which deterministically returns target partition (e.g., Partition 1).
   - `Partition::append_batch()` acquires `Partition::m_mutex`, assigns contiguous monotonically increasing offset $O$, serializes records, appends them to the active `.log` segment (rolling segments if `segment_max_bytes` is exceeded), and flushes dirty pages to disk if `sync_on_append == true`.
5. **Response Transmission**: The server returns a `PRODUCE_OK` (`0x91`) response carrying `{ partition = 1, base_offset = O, count = 1 }`.

#### B. Fetching Messages Starting from Offset 500 End-to-End
1. **Client Request**: Client sends `FETCH` (`0x12`) carrying `{ topic = "orders", partition = 1, start_offset = 500, max_bytes = 1048576, max_messages = 50 }`.
2. **Bounds & Offset Range Checks**: Server validates topic existence and partition bounds, then checks `start_offset` against Partition 1's `earliest_offset` and `high_watermark` (`next_offset`):
   - If `start_offset == high_watermark`: returns `FETCH_OK` (`0x92`) with `record_count = 0` (caught up).
   - If `start_offset > high_watermark` or `< earliest`: returns `OFFSET_OUT_OF_RANGE` (`0x08`).
3. **Sparse Index Lookup & Sequential Disk Read**:
   - `Partition::read()` locates the segment containing offset 500 via binary search on `base_offset`.
   - Inside that segment's sparse `OffsetIndex`, it binary searches for the nearest index entry `<= 500`, yielding byte position $P$.
   - Using `ReadFile()` with `OVERLAPPED` offsets (positional read without moving file pointers), it reads records starting at $P$, skips records with `offset < 500`, and accumulates matching records until `max_messages` or `max_bytes` is met.
4. **Response Transmission**: Returns `FETCH_OK` carrying `next_offset` (offset of last record + 1), `high_watermark`, `earliest_offset`, and the array of record payloads.

---

### 2. Deep-Dive Network & Protocol Questions & Answers

#### Q1: What does an acknowledged produce guarantee, and what does it not guarantee with `sync_on_append = false`?
- **With `sync_on_append = true`**: An acknowledged `PRODUCE_OK` response guarantees that the batch has been written to disk AND flushed to physical storage media via Win32 `FlushFileBuffers()`. Even on sudden hardware power failure, the records remain intact.
- **With `sync_on_append = false`**: An acknowledged `PRODUCE_OK` response guarantees that the batch was written to the OS kernel page cache. If the process crashes or is forcefully terminated (`Stop-Process -Force`), the OS will complete dirty page writes and data is saved. However, if sudden physical hardware power failure occurs before the OS flushes dirty pages to disk, un-flushed tail records may be lost.

#### Q2: Why is validating every length field in the body a security and stability requirement?
- **Buffer Overflow & OOB Memory Reads**: In binary TCP protocols, untrusted clients can send malicious length prefixes (e.g. declaring a string length of $2^{32}-1$ or claiming a payload size larger than the remaining frame).
- **Security Defense**: Reading length fields without validating `length <= remaining_bytes` leads to process memory corruption, heap out-of-bounds reads, denial-of-service crashes, or potential remote code execution. `BodyReader` enforces bounds checks before every read operation, returning `MALFORMED_BODY` (`0x0A`) without crashing or reading out of bounds.

#### Q3: Why does FETCH return `next_offset` and `high_watermark`, and how does a consumer use them?
- **`next_offset`**: Tells the consumer the exact start offset to request in its next `FETCH` call, simplifying client loop logic regardless of how many records were returned.
- **`high_watermark`**: Represents the partition's current `next_offset` (end of log). The consumer compares `next_offset` against `high_watermark` to calculate consumer lag (`high_watermark - next_offset`).

#### Q4: Why is fetching at `high_watermark` an OK empty result and not an error?
- **Normal Caught-Up State**: Reaching `start_offset == high_watermark` means the consumer has successfully processed all available messages in the partition. Returning `FETCH_OK` with 0 records is a valid "caught-up" status allowing polling consumers (`consume --follow`) to sleep briefly and retry without throwing exception errors.

#### Q5: What does batching in PRODUCE buy us, and what does it cost?
- **Benefits**:
  - **Amortized I/O & Network Overhead**: Reduces TCP frame overhead (1 header for $N$ records) and amortizes Win32 system call context switches and disk flush costs across all records in the batch.
  - **Atomicity**: All records in a batch are assigned contiguous offsets atomically on disk.
- **Trade-off / Cost**: Increased latency per message for producers waiting to assemble a full batch, and larger per-frame memory buffers.

#### Q6: Why does ordering hold within a partition but not across partitions?
- **Partition-Level Guarantee**: A partition is backed by a single append-only log protected by a partition mutex. Offsets are strictly sequential ($0, 1, 2, \dots$), ensuring total ordering for records within that partition.
- **Cross-Partition Non-Guarantee**: Different partitions run independently with separate offset sequences. Concurrent writes to Partition 0 and Partition 1 have no shared sequence order or global timestamp synchronization.

---

## Milestone 4: I/O Event Loop and Worker Thread Pool

### 1. Request Lifecycle Walkthrough (PRODUCE Request Under Event Loop Architecture)

In Milestone 4, StreamForge replaces the thread-per-connection model with an asynchronous single-threaded I/O event loop driven by `WSAPoll`, decoupled worker thread pool, and non-blocking sockets.

Here is the exact lifecycle of a `PRODUCE` request through every layer of the new architecture:

1. **Wire Arrival & Kernel Buffering**:
   - **Thread**: Client OS network stack.
   - **Action**: The client transmits raw TCP bytes for a `PRODUCE` request frame over the network. Windows TCP stack receives the packets and appends them to the socket's kernel receive buffer.

2. **`WSAPoll` Event Detection**:
   - **Thread**: Dedicated I/O Thread.
   - **Lock**: None.
   - **Action**: The I/O thread waits inside `WSAPoll(pollfds, count, timeout)`. When incoming bytes arrive in the client socket's kernel buffer, `WSAPoll` returns with the `POLLRDNORM` / `POLLIN` flag set for that socket.
   - **What if it would block?**: `WSAPoll` sleeps in the kernel with a timeout (e.g. 100ms or until loopback wakeup). It uses 0% CPU while idle and never blocks any worker or application thread.

3. **Non-blocking `recv()` & Frame Reassembly**:
   - **Thread**: Dedicated I/O Thread.
   - **Lock**: None (Single-Owner Rule: only the I/O thread reads or writes socket data and connection state).
   - **Action**: The I/O thread invokes non-blocking `recv()`. The returned bytes are fed into the connection's `FrameAssembler`. The assembler's state machine transitions from `WAITING_FOR_LENGTH` (parsing 4-byte header) to `READING_PAYLOAD` (accumulating body).
   - **What if it would block?**: Because the socket is non-blocking (`FIONBIO`), if only a partial frame has arrived or `recv()` returns `WSAEWOULDBLOCK`, the I/O thread does NOT block. It simply preserves partial frame state in `FrameAssembler`, keeps the socket in `pollfds`, and moves on to service other connections.

4. **Backpressure Check & Task Enqueue**:
   - **Thread**: Dedicated I/O Thread.
   - **Lock**: `TaskQueue::m_mutex` (held only for the brief microseconds of pushing the task into `std::queue` and notifying via `std::condition_variable`).
   - **Backpressure Mechanism**: Before enqueueing, the I/O thread checks if `task_queue.size() >= max_queue_depth` (e.g. 1024).
   - **What if it would block?**: If the task queue is saturated, the I/O thread does NOT block; it immediately removes `POLLIN` from this connection's `pollfd` entry. The server stops reading from the socket, causing the client's TCP window to fill up and backpressure to propagate across the network to the client producer. Once the worker pool drains tasks below the low-water mark, `POLLIN` is re-enabled. When space is available, the assembled frame is packaged into an `InboundTask`, `m_in_flight_tasks` is incremented, and it is pushed into `TaskQueue`.

5. **Worker Pickup & Request Decoding**:
   - **Thread**: Worker Thread (from a fixed pool of $N$ threads).
   - **Lock**: `TaskQueue::m_mutex` acquired while waiting on `std::condition_variable::wait()`. The lock is held only during the $O(1)$ queue pop and released immediately.
   - **Action**: A worker thread wakes up via `m_cv.notify_one()`, extracts the task, and parses the request using `FrameCodec` and `BodyReader`.

6. **Storage Partition Routing & Append**:
   - **Thread**: Worker Thread.
   - **Locks**:
     - *Topic Lookup*: Acquires `TopicManager::m_rw_lock` with `std::shared_lock` (reader lock) held only while looking up the topic pointer in the map. Multiple workers can read topic metadata concurrently without blocking each other.
     - *Partition Append*: Acquires `Partition::m_mutex` (`std::mutex`, exclusive lock) on the target partition.
   - **Action**: The worker serializes records, assigns contiguous monotonically increasing offsets, appends records to the active `.log` segment file, appends sparse index entries to `.index`, and calls `FlushFileBuffers()` if `sync_on_append == true`.
   - **What if it would block?**: Workers appending to different partitions (or different topics) proceed completely in parallel with zero lock contention! If two workers target the exact same partition, the second worker waits on `Partition::m_mutex` until the first completes its append, guaranteeing strict offset ordering ($0, 1, 2, \dots$) on disk.

7. **Completion Enqueue & Loopback Wakeup**:
   - **Thread**: Worker Thread.
   - **Lock**: `CompletionQueue::m_mutex` (held briefly for an $O(1)$ push).
   - **Action**: The worker serializes the `PRODUCE_OK` frame into a `CompletionTask` with the connection ID. It pushes it into `CompletionQueue`, releases the lock, and sends a 1-byte notification (`0x01`) into the `WakeupChannel` loopback socket write end.
   - **What if it would block?**: Loopback socket writes for 1 byte return instantly without blocking.

8. **Loopback Notification & Completion Draining**:
   - **Thread**: Dedicated I/O Thread.
   - **Lock**: `CompletionQueue::m_mutex` held briefly during `drain()` to swap the pending responses into a local thread vector.
   - **Action**: `WSAPoll` detects readability on the loopback wakeup socket (`POLLRDNORM`). The I/O thread drains the 1-byte ping, extracts all completed responses from `CompletionQueue`, and decrements `m_in_flight_tasks`.

9. **Non-blocking `send()` & Writable Polling**:
   - **Thread**: Dedicated I/O Thread.
   - **Lock**: None (Single-Owner Rule).
   - **Action**: The I/O thread locates the target `ConnectionState` by connection ID. If the connection's output buffer is empty, it attempts an immediate non-blocking `send()`.
   - **What if it would block?**: If the socket kernel send buffer cannot accept the complete frame, `send()` writes as many bytes as possible and returns without blocking. The unsent bytes are placed in `ConnectionState::out_buffer`, and `POLLWRNORM` / `POLLOUT` is added to `pollfds` for this connection. The I/O thread immediately continues servicing other sockets. When the kernel buffer frees space, `WSAPoll` signals `POLLOUT` and the I/O thread flushes the remaining bytes.

10. **Slow Consumer Defense & Delivery to Wire**:
    - **Thread**: Dedicated I/O Thread.
    - **Action**: If a slow client stalls reading and its buffered output exceeds `max_output_buffer_bytes` (e.g. 64 MiB), the server forcibly disconnects the socket to prevent unbounded broker memory exhaustion. Otherwise, once all bytes are flushed, `POLLOUT` is removed, and the client receives its acknowledged `PRODUCE_OK` response.

---

### 2. Deep-Dive Concurrency & Event Loop Questions & Answers

#### Q1: What is an event loop and why can it handle 10,000 connections with 1 thread while thread-per-connection cannot?
- **Thread-per-Connection Overhead**: Each OS thread on Windows reserves 1 MiB of stack space by default. 10,000 connections require ~10 GiB of RAM just for thread stacks. Furthermore, operating system schedulers incur massive CPU overhead context-switching among 10,000 threads. In practice, 99% of connections are idle at any given millisecond (waiting on client think-time or network packets).
- **Event Loop Scalability**: An event loop uses a single OS thread that registers all 10,000 socket descriptors with an OS multiplexer (`WSAPoll`, epoll, or IOCP). The operating system puts the event loop thread to sleep until one or more sockets actually have I/O ready. When awakened, the event loop processes only the sockets with active traffic. Memory consumption for 10,000 connections drops to just the socket descriptors and connection state buffers (~few MBs total), and CPU context switches are eliminated.

#### Q2: What is backpressure and what bad thing happens if you do not have it?
- **Definition**: Backpressure is a flow-control mechanism where an overloaded downstream component (worker threads or disk storage) signals upstream producers to slow down or halt transmission until the backlog clears.
- **Consequences Without Backpressure**: If producers send 50,000 requests/second while storage can only write 10,000 requests/second, unbounded task queues accumulate millions of requests in memory. This inevitably leads to process crash due to Out-Of-Memory (OOM), or severe latency spikes where requests sit queued for minutes before being dropped.
- **Our Implementation**: When `TaskQueue` exceeds `max_queue_depth`, the I/O thread removes `POLLIN` from client sockets. The TCP receive window shrinks to zero, and the client's own `send()` blocks at the transport layer, safely regulating ingestion rate to disk write speed.

#### Q3: Why do we keep partition mutexes exclusive instead of using `shared_mutex` on partitions?
- **Append is an Exclusive Mutation**: Appending to a partition modifies the active log segment, writes to `.log`, appends to `.index`, and increments `next_offset`. This requires an exclusive lock.
- **Overhead of `shared_mutex`**: `std::shared_mutex` incurs non-trivial cache-line bouncing and atomic read-counter increments on every shared lock acquisition. In high-frequency messaging, this overhead can exceed the cost of the actual critical section.
- **Partitioning as Concurrency Domains**: Concurrency in Kafka/StreamForge is achieved by partitioning across logs, not by concurrent writes to the same log. Partitions 0, 1, 2, and 3 run completely concurrently on separate worker threads without any lock contention. Within a single partition, operations are serialized to guarantee strict linear ordering, making a standard `std::mutex` both simpler and faster.

#### Q4: What is a "thundering herd" and does our thread pool suffer from it? Why or why not?
- **Definition**: A "thundering herd" occurs when multiple waiting threads are simultaneously awakened to compete for a single unit of work. One thread claims the work while all other threads waste CPU cycles context switching only to discover nothing is available and go back to sleep.
- **Our Thread Pool**: StreamForge does NOT suffer from a thundering herd. When a new task is pushed to `TaskQueue`, it calls `m_cv.notify_one()`, waking exactly ONE worker thread. The Windows kernel schedules that single thread to pop the task. `notify_all()` is reserved strictly for server shutdown.

#### Q5: What is the single-owner rule for sockets and what race condition does it prevent?
- **Rule**: Only the I/O thread is allowed to read from, write to, poll, or close client sockets. Worker threads never interact with socket handles directly.
- **Prevented Race Conditions**:
  - *Interleaved Frame Corruption*: If two worker threads attempted to call `send()` on the same socket concurrently, their byte streams would interleave, corrupting binary protocol frames.
  - *Socket Descriptor Reuse Race*: If a worker closed a disconnected socket while the I/O thread was calling `WSAPoll()` or `recv()`, the socket descriptor could be closed mid-call or immediately reassigned by the OS kernel to a newly accepted connection, resulting in the I/O thread reading or closing another client's active connection.

#### Q6: Why did we use a loopback socket pair for wakeups instead of Windows Event objects? What would change if we used IOCP later?
- **`WSAPoll` Limitation**: `WSAPoll` is designed to monitor Winsock `SOCKET` descriptors only; it cannot wait on arbitrary Win32 kernel handles such as Windows Event objects (`CreateEvent`). A loopback TCP socket pair (`127.0.0.1`) provides a valid `SOCKET` descriptor that can be placed directly inside the `pollfd` array, allowing any worker thread to instantly wake `WSAPoll` with a 1-byte write.
- **With IOCP Later**: Windows I/O Completion Ports (IOCP) natively integrate with asynchronous I/O (`WSARecv` / `WSASend`). Completion events are dispatched directly by the Windows kernel to worker threads via `GetQueuedCompletionStatus()`. Threads can post custom completion packets using `PostQueuedCompletionStatus()` without needing loopback sockets or wakeup pings.

#### Q7: What is a "slow loris" attack and how does our read-stall timeout defend against it?
- **Attack Mechanism**: An attacker opens hundreds of connections and sends requests at an agonizingly slow pace (e.g. 1 byte every 10 seconds). In naive servers, these incomplete requests consume connection slots and buffer memory indefinitely, eventually exhausting all available descriptors and blocking legitimate clients.
- **Defense**: StreamForge tracks `last_read_time` on every `ConnectionState`. The I/O thread periodically scans connections that have partial, uncompleted frames in their `FrameAssembler`. If `now - last_read_time > read_stall_timeout_sec` (default 30 seconds), the broker forcibly terminates the connection, reclaiming resources and thwarting Slow Loris starvation.

#### Q8: What happens to in-flight requests when the server gets a shutdown signal?
- **Graceful Shutdown Sequence**:
  1. The listening socket stops accepting new incoming connections.
  2. The `TaskQueue` is stopped so no new tasks are accepted.
  3. Worker threads finish executing all tasks already queued, deposit their results into `CompletionQueue`, and exit.
  4. The I/O thread drains all remaining completed responses from `CompletionQueue`, writes them out to clients, flushes socket buffers, and closes connections with `shutdown(s, SD_SEND)` / `closesocket(s)`.
  5. `TopicManager` cleanly flushes and closes all open `.log` and `.index` file handles, ensuring zero data corruption or uncommitted records.

#### Q9: Why is non-blocking send necessary even after `WSAPoll` says the socket is writable?
- **Partial Space in OS Buffer**: `WSAPoll` reporting `POLLOUT` only guarantees that the socket's kernel output buffer has *some* space (even as little as 1 byte).
- **Preventing I/O Stalls**: If the server tries to send a 512 KB `FETCH` response on a blocking socket when only 64 KB of kernel buffer space is available, the thread will block until the client acknowledges packets. Because the I/O thread services all client connections, blocking on a slow receiver would freeze every other connection on the broker.
- **Non-blocking Strategy**: A non-blocking `send()` writes whatever space is available immediately and returns. If bytes remain, StreamForge buffers them in `out_buffer` and registers `POLLOUT` with `WSAPoll`, allowing the I/O thread to continue servicing other clients while waiting for buffer space to free up.

---

## Milestone 5: Consumer Groups and Offset Commits

### 1. Request Lifecycle Walkthroughs

#### A. JoinGroup & Rebalance Flow
1. **Client Registration**:
   - A consumer client issues a `JOIN_GROUP` (`0x20`) frame containing `group_id`, `member_id` (empty string on first join to request generation of a UUID-like ID, or existing ID on rejoin), `protocol_type` (`"consumer"`), `session_timeout_ms`, and subscribed `topics`.
2. **Coordinator Lock & Member Upsert**:
   - Worker thread acquires `m_groups_mutex` (shared) and target `Group::m_mutex` (exclusive).
   - If `member_id` is empty, coordinator generates a unique ID `group_id-uuid`.
   - If the group is new, the first joining member becomes the leader.
   - If membership or subscriptions change, group transitions to `PREPARING_REBALANCE` and bumps `generation_id` ($G \leftarrow G + 1$).
3. **Partition Assignment Execution**:
   - Coordinator invokes the assigned strategy (`RangeAssignor` or `RoundRobinAssignor`) passing all active members and topic partition counts.
   - Partitions are deterministically mapped to members.
   - Group state transitions to `STABLE`.
4. **Response Transmission**:
   - Coordinator builds `JOIN_GROUP_OK` (`0xA0`) containing `generation_id`, assigned `member_id`, leader status flag, and the list of partitions assigned to this member.
   - Transmitted back to the client via `CompletionQueue` and the I/O loop.

#### B. Offset Commit & Fetch Flow
1. **Offset Commit**:
   - Consumer finishes processing a batch of records up to offset $K$ for partition $P$.
   - Sends `COMMIT_OFFSET` (`0x23`) with `group_id`, `member_id`, `generation_id`, `topic`, `partition`, and `offset = K + 1`.
   - Worker validates:
     - Group exists and matches `generation_id` (fencing zombie consumers).
     - Member exists and currently owns partition $P$.
   - Worker appends a binary commit record `[group_id, topic, partition, offset, timestamp]` to internal topic `__consumer_offsets` partition 0.
   - Worker updates in-memory cache in `OffsetStore`.
   - Returns `COMMIT_OFFSET_OK` (`0xA3`).
2. **Offset Fetch**:
   - Consumer on startup (or after rebalance) sends `FETCH_OFFSET` (`0x24`) with `group_id` and list of `(topic, partition)` pairs.
   - Coordinator checks `OffsetStore` in-memory cache.
   - Returns `FETCH_OFFSET_OK` (`0xA4`) with committed offsets. If no offset was ever committed for a partition, returns `-1` (indicating consumer should start from earliest/latest offset according to policy).

#### C. Member Heartbeat & Reaper Flow
1. **Heartbeat Transmission**:
   - Client runs a lightweight background thread sending `HEARTBEAT` (`0x21`) frames every `session_timeout_ms / 3` (e.g., every 1000ms for a 3000ms timeout).
   - Server worker validates `generation_id` and refreshes `member.last_heartbeat = steady_clock::now()`.
   - Returns `HEARTBEAT_OK` (`0xA1`). If group has rebalanced in the interim, returns `REBALANCE_IN_PROGRESS` (`0x0E`) to signal the client to rejoin.
2. **Reaper Background Scan**:
   - Dedicated `TcpServer` background reaper thread wakes every 1 second.
   - Calls `GroupCoordinator::expire_members(now)`.
   - Iterates groups under `m_groups_mutex` (shared) and locks each `Group::m_mutex`.
   - Any member whose `now - last_heartbeat > session_timeout` is transitioned to `DEAD` and removed from active membership.
   - If active members remain, coordinator immediately triggers partition reassignment and bumps `generation_id`.
   - If all members die, group transitions to `EMPTY`.

#### D. Group Lag Inspection Flow
1. **CLI Command**: Operator runs `streamforge-cli describe-group --group <group_id>`.
2. **Group Metadata Retrieval**:
   - Sends `DESCRIBE_GROUP` (`0x25`).
   - Server returns group state, protocol type, leader ID, generation ID, active member IDs, and their assigned partitions.
3. **Partition Watermark & Committed Offset Collation**:
   - CLI issues `FETCH_OFFSET` (`0x24`) to read committed offset $C$ for each partition.
   - CLI issues `FETCH` (`0x12`) or inspects topic partition metadata to read the current log end offset (High Watermark $H$).
4. **Lag Calculation**:
   - $\text{Lag} = H - C$ (if $C \ge 0$, else $\text{Lag} = H$).
   - Printed in tabular format showing Group, Topic, Partition, Committed Offset, High Watermark, Lag, and Assigned Member ID.

---

### 2. Code Walkthroughs

#### A. Assignor (`RangeAssignor` & `RoundRobinAssignor`)
- Located in `include/streamforge/Assignor.hpp` and `src/Assignor.cpp`.
- `RangeAssignor::assign()`:
  - For each topic, partitions $0 \dots P-1$ are sorted numerically.
  - Active member IDs are sorted lexicographically to guarantee deterministic identical assignments.
  - Divides partitions: `base = num_partitions / num_members`, `rem = num_partitions % num_members`.
  - The first `rem` members receive `base + 1` partitions; the remaining receive `base` partitions.
  - Guarantees contiguous partition ranges per consumer.
- `RoundRobinAssignor::assign()`:
  - Sorts all `(topic, partition)` pairs across all topics.
  - Alternates assigning each pair to members in round-robin sequence: `member_idx = i % num_members`.
  - Guarantees maximally even distribution regardless of topic partition counts.

#### B. GroupCoordinator
- Located in `include/streamforge/GroupCoordinator.hpp` and `src/GroupCoordinator.cpp`.
- Implements group state management: `EMPTY`, `PREPARING_REBALANCE`, `STABLE`, `DEAD`.
- Guards global registry with `std::shared_mutex m_groups_mutex`.
- Each `Group` maintains its own `std::mutex m_mutex`:
  - `join_group()`: Validates member, updates subscription, bumps generation on member join/leave, triggers partition assignor, returns new assignment.
  - `heartbeat()`: Validates member existence and matching `generation_id`. Updates `last_heartbeat`. Returns error code 14 (`REBALANCE_IN_PROGRESS`) if generation changed.
  - `leave_group()`: Removes member, bumps generation, immediately rebalances remaining members.
  - `expire_members()`: Evaluates `steady_clock::now() - last_heartbeat > session_timeout`. Evicts expired members, increments generation, triggers rebalance.

#### C. OffsetStore
- Located in `include/streamforge/OffsetStore.hpp` and `src/OffsetStore.cpp`.
- Two-tier storage architecture:
  1. **In-Memory Cache**: `std::unordered_map<std::string, int64_t>` mapping `"group:topic:partition"` to committed offset. Lookups are $O(1)$ under `m_mutex`.
  2. **Durable Partition Storage**: Internal topic `"__consumer_offsets"`, Partition 0.
     - Record format: Key = `group_id:topic:partition`, Value = `[8-byte offset][8-byte timestamp]`.
     - Appends are durable and written to disk through standard `Partition::append_batch()`.
- `OffsetStore::recover()`:
  - Invoked during broker startup. Reads all records from `__consumer_offsets` partition 0 from offset 0 to end of log.
  - Replays every valid commit record into the in-memory hash map, restoring the latest committed offset for every group and partition before accepting client traffic.

#### D. `consume-group` CLI Client Loop
- Located in `src/cli_main.cpp`.
- Encapsulates production-grade consumer logic in native C++17:
  1. Issues `JOIN_GROUP` with desired group ID and topic. Obtains assigned partition IDs and `generation_id`.
  2. Spawns an independent background `std::thread` executing `heartbeat_worker_fn`:
     - Sleeps on `std::condition_variable` in `session_timeout / 3` intervals.
     - Sends `HEARTBEAT` frame. If response returns `REBALANCE_IN_PROGRESS`, flags rebalance needed and wakes main thread.
  3. Main consumer loop:
     - Fetches committed offsets via `FETCH_OFFSET`.
     - Polls `FETCH` requests across assigned partitions.
     - Processes received records.
     - Commits offsets via `COMMIT_OFFSET` after batch processing.
     - If rebalance flag is set or commit fails with `REBALANCE_IN_PROGRESS` or `ILLEGAL_GENERATION`, consumer cancels current poll, rejoins group, receives new partition assignments, and resumes consumption.
  4. On process termination (`Ctrl+C`):
     - Stops heartbeat thread and joins it.
     - Sends `LEAVE_GROUP` frame so broker immediately rebalances remaining consumers without waiting for session timeout.

---

### 3. Deep-Dive Interview Questions & Answers

#### Q1: Why does `GroupCoordinator` use two levels of locking (`m_groups_mutex` + per-group `m_mutex`)?
- **Two-Level Locking Strategy**:
  - `m_groups_mutex` (`std::shared_mutex`) guards the hash map mapping `group_id -> std::shared_ptr<Group>`.
  - `Group::m_mutex` (`std::mutex`) guards the internal state of a single group (member list, generation ID, partition assignments).
- **Contention Elimination**:
  - In a broker managing hundreds of distinct consumer groups (e.g. `order-processor`, `analytics-pipeline`, `notification-service`), acquiring a global exclusive lock on every heartbeat or commit would serialize all groups across the broker, creating an extreme scalability bottleneck.
  - With two levels of locking, heartbeats, offset commits, and joins for different groups acquire `m_groups_mutex` in **shared (read) mode**, allowing hundreds of worker threads to access different groups simultaneously.
  - Only lookups/insertions of new groups take an exclusive lock on `m_groups_mutex`.
  - Mutex acquisition is strictly hierarchical: `m_groups_mutex` is acquired *before* `Group::m_mutex`, mathematically preventing deadlocks.

#### Q2: How does the RangeAssignor handle uneven partition counts?
- **The Allocation Math**:
  - Suppose topic `clicks` has 7 partitions ($0 \dots 6$) and consumer group `analytics` has 3 members ($C_0, C_1, C_2$).
  - `base = 7 / 3 = 2` partitions per member.
  - `rem = 7 % 3 = 1` extra partition.
  - The assignor distributes the extra remainder partitions one-by-one to the first `rem` members:
    - $C_0$ receives `base + 1 = 3` partitions: `[0, 1, 2]`.
    - $C_1$ receives `base = 2` partitions: `[3, 4]`.
    - $C_2$ receives `base = 2` partitions: `[5, 6]`.
- **Deterministic Invariant**:
  - Because member IDs are sorted lexicographically before assignment, all broker nodes and group members calculate the exact same deterministic partition ownership without race conditions.

#### Q3: What is a "zombie consumer" and how does StreamForge fence it?
- **What is a Zombie Consumer?**:
  - A consumer thread that experiences a temporary stop-the-world garbage collection pause, thread freeze, or network partition long enough for its session timeout to expire.
  - The coordinator detects the timeout, marks the member `DEAD`, bumps `generation_id` from $G$ to $G+1$, and reassigns its partitions to another active consumer.
  - If the frozen consumer abruptly unfreezes, it is unaware that it has been evicted and attempts to commit offsets for records it processed before freezing.
- **Generation-Based Fencing**:
  - Every member is issued the current `generation_id` upon joining.
  - Every `COMMIT_OFFSET` request includes `generation_id`.
  - The coordinator validates `request.generation_id == group.generation_id`.
  - When the zombie consumer sends a commit with generation $G$, the coordinator rejects it with error code 15 (`ILLEGAL_GENERATION`). The zombie is fenced from corrupting committed offsets.

#### Q4: Why is `__consumer_offsets` stored as a regular topic rather than a separate database?
- **Unified Architectural Simplicity**:
  - By modeling committed offsets as messages in an internal topic `"__consumer_offsets"`, StreamForge reuses the exact same robust storage subsystem: append-only segments, sparse indexing, crash recovery, and CRC32 verification.
  - No auxiliary database engine (SQLite, RocksDB) or external dependencies are required.
- **Unified Replication & Durability**:
  - In distributed setups, offset commits automatically inherit cluster replication, log compaction, disk flush policies (`sync_on_append`), and crash-consistency guarantees identical to user topics.

#### Q5: What happens when a consumer crashes mid-batch without committing?
- **At-Least-Once Delivery**:
  - Consumer reads records at offsets 100–149.
  - Consumer processes records 100–120 and then crashes (e.g. power loss or process kill) before issuing `COMMIT_OFFSET(150)`.
  - The coordinator background reaper detects the heartbeat timeout, evicts the crashed consumer, bumps generation, and reassigns the partition to a surviving consumer.
  - The new consumer queries `FETCH_OFFSET`, which returns offset 100 (the last successfully committed offset).
  - The new consumer resumes reading from offset 100.
  - *Result*: Records 100–120 are processed again. Duplicate processing is possible, but **zero data loss** occurs (At-Least-Once semantics).

#### Q6: Why does `GroupCoordinator::expire_members()` run on a background reaper thread instead of the I/O event loop?
- **Single-Threaded I/O Event Loop Invariant**:
  - The `WSAPoll` I/O thread is the single owner of network sockets, responsible for accepting connections, non-blocking reads, and draining write buffers across thousands of clients.
  - If member expiration ran on the I/O thread, iterating hundreds of consumer groups, checking timestamps, executing rebalances, and bumping generations would introduce variable latency spikes into the event loop, causing network packet stalls.
- **Reaper Thread Independence**:
  - The reaper thread runs as a dedicated background thread waking on a condition variable every 1 second.
  - It acquires coordinator locks without interfering with socket I/O.
  - On shutdown, `m_reaper_cv.notify_all()` ensures instant thread joining without delay.

#### Q7: How does StreamForge prevent reserved topics (`__consumer_offsets`) from being corrupted by clients?
- **System Topic Reservation**:
  - Any topic name beginning with double underscores (`"__"`) is reserved for internal broker subsystems.
- **Protocol Enforcement**:
  - `MessageHandler::handle_produce()` and `handle_create_topic()` inspect incoming topic names:
    ```cpp
    if (topic_name.rfind("__", 0) == 0) {
        return make_error_frame(frame.request_id, 6 /* TOPIC_NOT_FOUND or PERMISSION_DENIED */,
                                "Direct access to internal topic is forbidden");
    }
    ```
  - External TCP clients sending `PRODUCE` or `CREATE_TOPIC` requests targeting `"__consumer_offsets"` receive an immediate error response.
  - Only internal server components (`OffsetStore`) have direct handle access to the internal partition.

#### Q8: Explain the single-phase JoinGroup protocol in StreamForge vs Kafka's two-phase JoinGroup/SyncGroup protocol.
- **Kafka's Two-Phase Protocol**:
  - Phase 1 (`JoinGroup`): Clients register subscriptions. Kafka broker elects one client as the *Group Leader* and returns member metadata to the leader.
  - Phase 2 (`SyncGroup`): The leader runs the assignor algorithm locally on the client and sends assignments back to the broker in `SyncGroup`. The broker distributes assignments to followers.
  - *Trade-off*: Highly flexible (custom client partitioners without broker recompilation), but requires two sequential network roundtrips for every rebalance, increasing rebalance latency.
- **StreamForge's Single-Phase Protocol**:
  - Single Phase (`JOIN_GROUP`): Clients send subscribed topics and timeout.
  - The broker coordinator maintains assignor strategies (`range`, `round-robin`) directly on the server.
  - When rebalance is triggered, the coordinator computes partition assignments immediately on the broker and returns assignments directly in the `JOIN_GROUP_OK` frame in a **single network roundtrip**.
  - *Benefit*: Dramatically faster rebalances, simpler client implementations, and zero rebalance state stalls.

#### Q9: How does `OffsetStore::recover()` detect torn or corrupt offset records on startup?
- **Log Recovery Integration**:
  - When the broker boots, `TopicManager::open_and_recover_all()` iterates all topics, including `"__consumer_offsets"`.
  - For each segment in Partition 0, `LogSegment::recover_and_rebuild_index()` scans all records from byte 0 to the physical end of file.
  - For every record:
    1. Validates that the record header length does not exceed file size.
    2. Recalculates IEEE 802.3 CRC32 checksum across `[offset ... payload]`.
    3. If a CRC mismatch or partial write is detected (torn tail from sudden power outage), the file is truncated using Win32 `SetEndOfFile()` to the last valid record boundary.
  - `OffsetStore::recover()` then sequentially reads valid records from offset 0, parsing keys `group:topic:partition` and values `[offset][timestamp]`, and populates the clean in-memory cache.

#### Q10: Why must `COMMIT_OFFSET` validate the partition assignment in addition to the generation?
- **Preventing Stale Partition Overwrites**:
  - Checking `generation_id` verifies that the group has not rebalanced globally since the member checked in.
  - However, in dynamic consumer groups where members subscribe to multiple topics or partition assignments change, a consumer might have uncommitted buffers for partition $P$ from a prior epoch.
  - Verifying `member.assigned_partitions.count(partition) > 0` ensures that a consumer can **never** commit an offset for a partition it does not actively own in the current generation, preventing partition offset corruption across peers.

#### Q11: How does `consume-group` handle broker restart transparently?
- **Automatic Reconnection & Rebalance Handshake**:
  - If the broker restarts, the client socket disconnects (`recv()` returns 0 or `SOCKET_ERROR`).
  - `consume-group` CLI detects the broken connection and initiates a reconnect backoff loop (retrying every 1 second).
  - Once the broker socket accepts the new connection, the client:
    1. Sends a new `JOIN_GROUP` frame with its member ID.
    2. Receives a new valid `generation_id` and assigned partitions.
    3. Re-spawns the background heartbeat thread.
    4. Fetches the latest committed offsets via `FETCH_OFFSET` (restored by broker's `OffsetStore::recover()`).
    5. Seamlessly resumes consumption without skipping or losing messages.

#### Q12: Explain how thread count and handles remain stable across hundreds of dynamic join/leave cycles.
- **Resource Management & RAII**:
  - **Thread Allocation**:
    - The broker utilizes a fixed-size worker thread pool (e.g. 4 threads) and 1 background reaper thread.
    - Joining or leaving consumer groups **never** creates or destroys OS threads on the server. All coordinator tasks execute on existing worker pool threads.
    - Client CLI processes spawn exactly one heartbeat thread per consumer, which is cleanly joined using RAII synchronization on shutdown.
  - **Handle Management**:
    - TCP connections are recycled through the single-owner event loop. Sockets are closed using Win32 `closesocket()` upon disconnect.
    - Storage file handles (`.log`, `.index`) belong to persistent partition objects and are not reopened or duplicated during group operations.
    - Memory for departed group members is erased from `m_members` hash maps, preventing memory leaks across thousands of rebalances.

