# StreamForge High-Level Architecture (HLD)

StreamForge is a high-performance, crash-resilient, durable message broker built in C++17 that runs natively on Microsoft Windows. Inspired by Apache Kafka's architecture, StreamForge implements partitioned append-only commit logs, memory-mapped sparse indices, binary wire protocol serialization, an asynchronous WSAPoll reactor network model with bounded thread pooling, and consumer group coordination with cooperative partition rebalancing.

---

## 1. System Overview

StreamForge is designed around three foundational systems engineering principles:
1. **Zero External Dependencies on Windows:** Implemented entirely using modern standard C++17, native Win32 APIs, and Winsock2. No POSIX shims, Docker, or external libraries.
2. **Mechanical Sympathy with Windows Storage & OS:** Employs Win32 file handles (`CreateFileW`), sparse index memory mapping (`CreateFileMappingW` / `MapViewOfFile`), file pre-allocation (`SetFilePointerEx` + `SetEndOfFile`) to eliminate NTFS fragmentation, and strict flush controls (`FlushFileBuffers`).
3. **Structured Concurrency & Backpressure:** Decouples high-throughput network I/O from compute and disk writes using an asynchronous reactor pattern paired with a bounded worker thread pool, dynamic socket throttling (`SO_RCVBUF`), and reader-writer locking.

---

## 2. Four-Layer Architecture

StreamForge is organized into four strictly decoupled layers:

```mermaid
flowchart TD
    subgraph Layer1["1. Network Layer (Winsock2 & Concurrency)"]
        Client[TCP Clients / Producers / Consumers] <-->|Winsock TCP Sockets| EL[EventLoop / I/O Thread]
        EL <-->|Polls I/O Events via WSAPoll| Conns[Connection Table]
        EL -->|Dispatches Frames| TQ[Bounded TaskQueue]
        TQ -->|Dynamic Backpressure Watermarks| EL
        TQ -->|Work Items| TP[Worker Thread Pool (4..16 Workers)]
        TP -->|Queues Outgoing Responses| Conns
        Conns -->|Wakes Up I/O Thread via Loopback| EL
    end

    subgraph Layer2["2. Protocol Layer (Binary Wire Framing)"]
        TP <--> FA[FrameAssembler / FrameCodec]
        FA <--> PR[Protocol Messages: Produce, Fetch, JoinGroup, OffsetCommit, etc.]
        PR <--> CRC[CRC32 Integrity Validation]
    end

    subgraph Layer3["3. Storage Engine Layer (Durable Commit Log)"]
        TP <--> TM[TopicManager]
        TM <--> TOPIC[Topic: 1..N Partitions]
        TOPIC <--> PART[Partition: Read-Write Lock]
        PART <--> ACT[Active LogSegment]
        PART <--> CLS[Sealed Historical Segments]
        ACT --> LOG[Append-Only .log File]
        ACT --> IDX[Sparse 16-byte .index File (Memory-Mapped)]
        PART <--> RM[RetentionManager: Time & Size Expiry GC]
    end

    subgraph Layer4["4. Coordination & Consumer Group Layer"]
        TP <--> GC[GroupCoordinator]
        GC <--> CG[ConsumerGroup State Machine: Join/Sync/Heartbeat/Leave]
        CG <--> SA[StickyAssignor: Balanced Partition Distribution]
        GC <--> OS[OffsetStore: __consumer_offsets Compacted Topic]
    end
```

---

### Layer 1: Network Layer (Winsock2 & Concurrency)
- **Single I/O Reactor (`EventLoop` / `TcpServer`):** A dedicated thread drives multiplexed socket I/O using native Win32 `WSAPoll()`. It manages non-blocking client connections, handles new accepts, reads raw bytes into incremental frame assemblers, and writes queued outbound response buffers.
- **Inter-Thread Notification (`WakeupChannel`):** A loopback TCP socket pair allows worker threads to wake the `WSAPoll` blocking call immediately whenever outbound responses are enqueued, avoiding busy spinning or poll timeouts.
- **Bounded Worker Pool (`ThreadPool` & `TaskQueue`):** Offloads request decoding, disk access, partition locking, and group management to a pool of fixed worker threads.
- **Dynamic Backpressure Control:** The `TaskQueue` enforces high-watermark and low-watermark thresholds. When active jobs exceed the high watermark, the reactor suspends polling `POLLRDNORM` on client sockets and reduces TCP receive buffers (`SO_RCVBUF = 0`), signaling TCP zero-window backpressure to clients. Once queue depth drops below the low watermark, normal polling and buffer sizing resume.

---

### Layer 2: Protocol Layer (Binary Wire Framing)
- **Fixed 12-byte Frame Header:** Every message transferred over TCP begins with a 12-byte header:
  - `length` (uint32_t, big-endian): Total frame byte length including header.
  - `type` (uint16_t, big-endian): Protocol request/response opcode (`PRODUCE`, `FETCH`, `JOIN_GROUP`, etc.).
  - `request_id` (uint32_t, big-endian): Correlation identifier returned in responses.
  - `reserved` (uint16_t, big-endian): Reserved alignment flags.
- **Body Serialization (`BodyReader` & `BodyWriter`):** Deterministic big-endian encoding for strings, byte arrays, partition lists, and metadata structures.
- **Integrity (`CRC32`):** Every payload appended to disk and sent across the network is protected by an IEEE 802.3 CRC32 checksum, validated before storage and on consumption.

---

### Layer 3: Storage Engine Layer (Durable Commit Log)
- **Hierarchical Layout:** Topics are partitioned; each partition is an independent directory containing an append-only sequence of immutable historical segments and a single mutable active segment.
- **Segment Files:**
  - `.log` file: Raw contiguous records (`offset`, `timestamp`, `key`, `value`, `crc32`).
  - `.index` file: Fixed-size 16-byte entries mapping `relative_offset` (uint64_t) to `physical_file_position` (uint64_t).
- **Sparse Indexing:** An index entry is written only once every `index_interval_bytes` (default 4 KB) of log writes, keeping indices small enough to remain entirely in memory or OS cache.
- **Fast Lookup via Memory-Mapped Binary Search:** On read, indices are mapped using Win32 `CreateFileMappingW` and `MapViewOfFile`. Partition offsets are translated to log byte offsets in $O(\log N)$ time via `std::lower_bound` on the 16-byte stride view, followed by a bounded sequential scan through the `.log` file.
- **NTFS Optimization:** File pre-allocation via Win32 `SetFilePointerEx` + `SetEndOfFile` prevents file fragmentation under heavy concurrent append workloads.
- **Retention & Cleanup (`RetentionManager`):** Background garbage collection purges closed segments that exceed retention time (`retention_ms`) or size limits (`retention_bytes`). Active segments are never purged.

---

### Layer 4: Coordination & Consumer Group Layer
- **`GroupCoordinator`:** Manages consumer groups across their 5-state lifecycle:
  `Empty` $\rightarrow$ `PreparingRebalance` $\rightarrow$ `CompletingRebalance` $\rightarrow$ `Stable` $\rightarrow$ `Dead`.
- **Cooperative Sticky Partition Assignor (`StickyAssignor`):** Allocates topic partitions evenly across all active group members while minimizing partition churn between successive rebalances.
- **Internal Offsets Storage (`__consumer_offsets`):** Committed offsets are durably persisted to an internal compacted topic. An in-memory cache provides instantaneous lookup during fetch restarts.

---

## 3. End-to-End Produce Data Flow

The following sequence details how a produce request moves from a client socket through the server to durable disk storage and back:

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant EL as EventLoop (I/O Thread)
    participant TQ as TaskQueue
    participant TP as Worker Thread
    participant TM as TopicManager / Partition
    participant SEG as LogSegment (.log / .index)
    participant OS as Win32 OS Cache / Disk

    Client->>EL: Send TCP PRODUCE Frame [Header (12B) + Topic + Partition + Key + Value]
    EL->>EL: WSAPoll detects POLLRDNORM; FrameAssembler completes frame
    EL->>TQ: Enqueue Produce Task (monitors high watermark)
    TQ->>TP: Worker thread picks up task
    TP->>TP: Validate CRC32 and parse ProduceRequest body
    TP->>TM: Partition::append(key, value) [Acquires exclusive write lock]
    TM->>SEG: Active LogSegment::append()
    SEG->>OS: WriteFile (.log raw bytes)
    alt Sparse Index Interval Exceeded
        SEG->>OS: WriteFile (.index 16-byte entry)
    end
    opt Fsync Configured
        SEG->>OS: FlushFileBuffers (.log handle)
    end
    SEG-->>TM: Returns assigned 64-bit Offset
    TM-->>TP: Release write lock; return ProduceOk(offset)
    TP->>EL: Queue ProduceResponse frame in Connection outbound buffer
    TP->>EL: WakeupChannel.notify() (wakes WSAPoll)
    EL->>Client: WSAPoll detects POLLWRNORM; send TCP PRODUCE_OK Frame
```

---

## 4. End-to-End Fetch Data Flow

The following sequence details how a consumer fetches records using offset-directed binary search:

```mermaid
sequenceDiagram
    autonumber
    actor Consumer
    participant EL as EventLoop (I/O Thread)
    participant TQ as TaskQueue
    participant TP as Worker Thread
    participant TM as TopicManager / Partition
    participant SEG as LogSegment (.log / .index)
    participant OS as Win32 OS Cache / Disk

    Consumer->>EL: Send TCP FETCH Frame [Header (12B) + Topic + Partition + FetchOffset + MaxBytes]
    EL->>EL: WSAPoll detects POLLRDNORM; FrameAssembler completes frame
    EL->>TQ: Enqueue Fetch Task
    TQ->>TP: Worker thread picks up task
    TP->>TM: Partition::read(start_offset, max_bytes) [Acquires shared read lock]
    TM->>TM: Locate target LogSegment by base_offset using std::upper_bound
    TM->>SEG: LogSegment::read(offset, max_bytes)
    SEG->>SEG: Binary search mapped .index for largest relative_offset <= target_offset
    SEG->>OS: ReadFile (.log starting at indexed physical file position)
    OS-->>SEG: Returns contiguous log bytes
    SEG->>SEG: Scan forward record headers to exact offset; verify CRC32
    SEG-->>TM: Return std::vector<Record>
    TM-->>TP: Release shared read lock; return FetchOk(records, high_watermark)
    TP->>EL: Queue FetchResponse frame in Connection outbound buffer
    TP->>EL: WakeupChannel.notify()
    EL->>Consumer: WSAPoll detects POLLWRNORM; send TCP FETCH_OK Frame
```

---

## 5. Key Architectural Decisions & Tradeoffs

| Architecture Choice | Selected Approach | Alternative Evaluated | Rationale & Tradeoff |
|---|---|---|---|
| **Network Architecture** | Single Reactor (`WSAPoll`) + Thread Pool | Thread-per-Connection | Thread-per-connection allocates a 1 MB stack per client, degrading at hundreds of connections due to Windows thread context switching. Reactor holds thousands of idle sockets at flat memory and constant CPU. |
| **Network Multiplexing** | `WSAPoll` | Windows I/O Completion Ports (`IOCP`) | While IOCP offers slightly higher theoretical throughput on Windows for thousands of active sockets, `WSAPoll` matches the POSIX Reactor pattern closely, keeping the code clear and fully explainable for educational and interview purposes while delivering ~87,000 req/sec. |
| **Storage Engine** | Partitioned Append-Only Log Files + Sparse Mapped Index | Embedded DB (SQLite or RocksDB) | Embedded key-value stores incur LSM-tree compaction I/O write amplification and B-Tree rebalancing overhead. Append-only sequential disk writes maximize NVMe/SSD sequential write throughput and enable direct zero-copy byte streaming. |
| **Index Design** | Fixed-Stride (16B) Sparse Index with Memory Mapping | Dense Hash Index / B-Tree Index | Storing every record offset in memory wastes RAM. Storing an entry every 4 KB limits index size to 0.4% of log size, allowing binary search to run directly against memory-mapped virtual address space (`MapViewOfFile`). |
| **Consumer Assignment** | Cooperative Sticky Assignor | Static Range Assignor | Range assignment causes severe partition reassignment cascades when a consumer joins or leaves. Sticky assignment preserves existing ownership, migrating only the minimal required partitions. |
| **Consumption Model** | Pull-Based (Client Polling) | Push-Based Broker Delivery | Pull consumers self-pace consumption to prevent broker-driven consumer buffer overflow, handle batching naturally, and allow arbitrary offset rewinds for replayability. |

---

## 6. Delivery Guarantees & Fault Tolerance

1. **At-Least-Once Delivery:**
   - Offset advancement is controlled strictly by consumers via explicit `OFFSET_COMMIT` requests.
   - If a consumer crashes before committing its latest position, the next consumer assigned that partition reads from the last durably committed offset in `__consumer_offsets`.
2. **Crash Resilience & Data Integrity:**
   - Every on-disk record includes an IEEE 802.3 CRC32 checksum computed over the record headers, key, and payload.
   - On broker startup, if an un-graceful crash occurred, the recovery scanner detects incomplete or corrupted tail records, truncates the `.log` file at the last valid record boundary, and regenerates missing or corrupted `.index` files from the remaining clean data.
3. **Single-Broker Scope:**
   - StreamForge delivers partition durability on a single node. Distributed consensus algorithms (Raft/KRaft/Paxos) and multi-broker cluster replication are excluded to focus deeply on single-node systems programming, OS storage mechanics, and concurrency.
