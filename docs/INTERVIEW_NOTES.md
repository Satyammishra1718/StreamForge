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
