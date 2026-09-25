# StreamForge Test Coverage & Verification Matrix

This document provides a comprehensive audit of all major system guarantees and architectural invariants across Milestones 1 through 6, mapping each claim to the exact unit test, integration test, or stress scenario verifying it.

---

## 1. System Guarantee to Test Mapping Matrix

| Milestone & Category | System Guarantee / Architectural Invariant | Verification Test File | Test Case / Routine Name | Status |
|---|---|---|---|---|
| **M1: Network & Protocol** | Winsock2 runtime RAII lifecycle initialization and cleanup | `test.ps1` | `Ping / Pong TCP roundtrip` | Verified |
| **M1: Network & Protocol** | Fixed 8-byte frame header encoding/decoding (`length:4`, `type:1`, `request_id:4`) | `tests/net_tests.cpp` | `oversized_and_small_length_errors` | Verified |
| **M1: Network & Protocol** | Frame fragmentation: reassembly of 1-byte incremental TCP chunks | `tests/net_tests.cpp` | `byte_at_a_time_feeding` | Verified |
| **M1: Network & Protocol** | Frame aggregation: multiple frames concatenated in single TCP packet | `tests/net_tests.cpp` | `several_frames_in_one_chunk` | Verified |
| **M1: Network & Protocol** | Arbitrary TCP chunking: 1,000 frames fragmented into random sized packets | `tests/net_tests.cpp` | `random_1000_frames_reassemble_exactly` | Verified |
| **M1: Network & Protocol** | Frame size enforcement: frames $\le$ 8 bytes or $> 16\text{ MB}$ rejected with error | `tests/net_tests.cpp` | `oversized_and_small_length_errors` | Verified |
| **M1: Network & Protocol** | Max payload handling: frames up to 16 MiB payload decoded without buffer overflow | `tests/net_tests.cpp` | `max_size_bodies` | Verified |
| **M1: Network & Protocol** | PING / PONG protocol exchange over TCP socket | `test.ps1` | `Ping / Pong TCP roundtrip` | Verified |
| **M1: Network & Protocol** | Console control signal handling (Ctrl+C / Break) triggers clean server shutdown | `src/server_main.cpp` | `console_ctrl_handler` | Verified |
| **M2: Storage Engine** | Binary record format: CRC, offset, timestamp, key, and value serialization fidelity | `tests/storage_tests.cpp` | `append_read_roundtrip` | Verified |
| **M2: Storage Engine** | Zero-byte keys, zero-byte values, and empty record encoding/decoding without corruption | `tests/protocol_tests.cpp` | `zero_byte_key_and_value_records` | **Gap Resolved** |
| **M2: Storage Engine** | Empty partition initial boundaries: read at offset 0 returns empty OK; read $> 0$ returns error | `tests/storage_tests.cpp` | `empty_partition_read_and_boundaries` | **Gap Resolved** |
| **M2: Storage Engine** | Boundary semantics: read at offset 0, middle, last offset; read past high-watermark returns empty | `tests/storage_tests.cpp` | `read_boundary_semantics` | Verified |
| **M2: Storage Engine** | Segment rolling: active segment rolls when `segment_max_bytes` is exceeded | `tests/storage_tests.cpp` | `segment_rolling` | Verified |
| **M2: Storage Engine** | Sparse index: 12-byte entries (`relative_offset`, `file_position`) interval mapping | `tests/storage_tests.cpp` | `sparse_index_correctness` | Verified |
| **M2: Storage Engine** | Index reconstruction: missing or deleted `.index` regenerated from `.log` on boot | `tests/storage_tests.cpp` | `missing_index_rebuilt_from_log_on_recovery` | **Gap Resolved** |
| **M2: Storage Engine** | Partition persistence: partition close and reopen preserves high-watermark and records | `tests/storage_tests.cpp` | `reopen_persistence` | Verified |
| **M2: Storage Engine** | Torn tail recovery: uncommitted or partial record at end of segment truncated via `SetEndOfFile` | `tests/storage_tests.cpp` | `torn_tail_recovery` | Verified |
| **M2: Storage Engine** | Record corruption detection: single-byte corruption caught via IEEE 802.3 CRC32 | `tests/storage_tests.cpp` | `record_corruption_detection` | Verified |
| **M2: Storage Engine** | Topic name validation: rejects directory traversal (`..`), slashes, leading/trailing dots | `tests/storage_tests.cpp` | `topic_name_validation` | Verified |
| **M2: Storage Engine** | Deterministic partitioning: Murmur2 key hash maps identical key to identical partition | `tests/storage_tests.cpp` | `partitioners` | Verified |
| **M2: Storage Engine** | Round-robin partitioning: null/empty keys distributed evenly across partitions | `tests/storage_tests.cpp` | `partitioners` | Verified |
| **M2: Storage Engine** | Partition concurrency: 8 threads x 1,000 appends (8,000 records) while reader thread scans | `tests/storage_tests.cpp` | `concurrency_append_read` | Verified |
| **M2: Storage Engine** | Storage handle leak safety: 1,000 partition open/close cycles with zero handle growth | `tests/storage_tests.cpp` | `partition_handle_leak_1000_cycles` | Verified |
| **M3: Broker Network** | Binary protocol primitives: u8, u16, u32, u64, string, bytes body encoding/decoding | `tests/protocol_tests.cpp` | `body_reader_writer_primitives` | Verified |
| **M3: Broker Network** | Protocol message structs roundtrip: all 10 request/response pairs serialize identically | `tests/protocol_tests.cpp` | `message_structs_roundtrip` | Verified |
| **M3: Broker Network** | Malformed body defense: truncated frames, invalid lengths, and garbage rejected with error | `tests/protocol_tests.cpp` | `malformed_body_validation_no_crash` | Verified |
| **M3: Broker Network** | Max record size boundary: records $\le 1\text{ MiB}$ succeed; records $> 1\text{ MiB}$ rejected | `tests/protocol_tests.cpp` | `max_size_boundary_limits` | Verified |
| **M3: Broker Network** | Topic administration: create, list, and describe topic metadata over TCP | `test_broker.ps1` | `Topic Operations (create, list, describe)` | Verified |
| **M3: Broker Network** | Produce and fetch single records over TCP socket with offset verification | `test_broker.ps1` | `Produce and Fetch Single Record` | Verified |
| **M3: Broker Network** | Produce and fetch batch of records over TCP socket with atomic append | `test_broker.ps1` | `Produce and Fetch Batch` | Verified |
| **M3: Broker Network** | Partition routing over TCP: keyed records routed to partition; null keys round-robined | `test_broker.ps1` | `Partition Routing (Key vs Non-Key)` | Verified |
| **M3: Broker Network** | Wire error reporting: TopicNotFound (1), PartitionNotFound (2), Exists (5), BadName (6) | `test_broker.ps1` | `Error Reporting Tests` | Verified |
| **M4: Concurrency & I/O** | Thread count stability: server thread count remains flat between 0 and 200 connections | `test_concurrency.ps1` | `TEST 1: Thread Count Stability` | Verified |
| **M4: Concurrency & I/O** | Non-blocking multiplexing: active requests succeed while 200 idle connections held | `test_concurrency.ps1` | `TEST 2: Active Requests while 200 Held` | Verified |
| **M4: Concurrency & I/O** | Read stall timeout: slow-loris partial frame sender disconnected after timeout | `test_concurrency.ps1` | `TEST 3: Read-Stall Timeout (Slow-Loris)` | Verified |
| **M4: Concurrency & I/O** | Slow reader backpressure: client overflowing output buffer disconnected by server | `test_concurrency.ps1` | `TEST 4: Slow Reader & Bounded Working Set` | Verified |
| **M4: Concurrency & I/O** | Large response partial-write path: ~2 MiB data fetched cleanly over 1 MiB clamp | `test_concurrency.ps1` | `TEST 5: Large Responses (Partial-Write Path)` | Verified |
| **M4: Concurrency & I/O** | Strict FIFO request ordering: 500 pipelined requests handled in exact sequential order | `test_concurrency.ps1` | `TEST 6: Strict FIFO Request Ordering` | Verified |
| **M4: Concurrency & I/O** | Heavy parallel load: 16 parallel producers x 500 records + 8 parallel consumers | `test_concurrency.ps1` | `TEST 7: Parallel Load` | Verified |
| **M4: Concurrency & I/O** | Network connection handle leak safety: 200 ping cycles with zero handle growth | `test_concurrency.ps1` | `TEST 8: Handle Leaks & Graceful Shutdown` | Verified |
| **M4: Concurrency & I/O** | Graceful shutdown: server drains and exits with code 0 while active client is connected | `test_concurrency.ps1` | `TEST 8: Handle Leaks & Graceful Shutdown` | Verified |
| **M4: Concurrency & I/O** | Durability across server restart: 8,000 produced records intact upon server reboot | `test_concurrency.ps1` | `TEST 9: Durability & Hard Kill Recovery` | Verified |
| **M5: Consumer Groups** | Assignor strategies: RangeAssignor and RoundRobinAssignor across uneven topologies | `tests/group_tests.cpp` | `assignor_strategies_comprehensive` | Verified |
| **M5: Consumer Groups** | Group lifecycle: join barrier, leader election, generation increment, clean leave | `tests/group_tests.cpp` | `join_leave_generation_and_rejoin` | Verified |
| **M5: Consumer Groups** | Heartbeat reaper & session timeout: unresponsive consumer evicted and rebalance triggered | `tests/group_tests.cpp` | `session_timeout_and_expiration` | Verified |
| **M5: Consumer Groups** | Generation fencing: commit with stale generation ID rejected with `ILLEGAL_GENERATION` | `tests/group_tests.cpp` | `commit_fencing_semantics` | Verified |
| **M5: Consumer Groups** | Partition ownership fencing: member cannot commit offsets for unassigned partitions | `tests/group_tests.cpp` | `validation_rules_and_limits` | Verified |
| **M5: Consumer Groups** | Offset persistence: `__consumer_offsets` partition log loaded and replayed on boot | `tests/group_tests.cpp` | `offset_store_persistence_and_replay` | Verified |
| **M5: Consumer Groups** | Coordinator concurrency: concurrent joins, leaves, commits maintain data invariants | `tests/group_tests.cpp` | `concurrency_stress_and_invariants` | Verified |
| **M5: Consumer Groups** | Multi-consumer rebalance over TCP: 2 consumers divide partitions evenly | `test_groups.ps1` | `Multi-Consumer Dynamic Rebalance` | Verified |
| **M5: Consumer Groups** | Consumer crash simulation: killed consumer detected by reaper; partitions redistributed | `test_groups.ps1` | `Consumer Crash Simulation` | Verified |
| **M6: Retention & Recovery** | Retention by age: closed segments older than `retention_ms` deleted | `tests/storage_tests.cpp` | `retention_by_age` | Verified |
| **M6: Retention & Recovery** | Retention by size: partition exceeding `retention_bytes` prunes oldest closed segments | `tests/storage_tests.cpp` | `retention_by_size` | Verified |
| **M6: Retention & Recovery** | Active segment invariant: active segment is NEVER deleted even if limits exceeded | `tests/storage_tests.cpp` | `retention_active_segment_never_deleted` | Verified |
| **M6: Retention & Recovery** | Pruned offset fetch error: fetching below earliest offset returns Error 8 | `tests/storage_tests.cpp` | `fetch_below_earliest_offset_error_8` | Verified |
| **M6: Retention & Recovery** | Immutable `.sealed` sidecar: valid sidecar validates segment and prevents full scan | `tests/storage_tests.cpp` | `sealed_sidecar_and_crc_verification` | Verified |
| **M6: Retention & Recovery** | Missing `.sealed` sidecar triggers recovery and recreates metadata sidecar | `tests/storage_tests.cpp` | `missing_sealed_sidecar_rebuilt_on_recovery` | Verified |
| **M6: Retention & Recovery** | Quick vs Full scan: corrupted byte in middle of sealed segment caught by full scan | `tests/storage_tests.cpp` | `quick_vs_full_scan_middle_corruption` | Verified |
| **M6: Retention & Recovery** | Crash injection at `mid_write_record`: partial record truncated; log restored | `test_crash_recovery.ps1` | `Crash injection: mid_write_record` | Verified |
| **M6: Retention & Recovery** | Crash injection at `between_log_and_index`: index rebuilt from log payload | `test_crash_recovery.ps1` | `Crash injection: between_log_and_index` | Verified |
| **M6: Retention & Recovery** | Crash injection at `between_seal_and_sealed`: missing sidecar regenerated on boot | `test_crash_recovery.ps1` | `Crash injection: between_seal_and_sealed` | Verified |
| **M6: Retention & Recovery** | Crash injection at `mid_write_offset_commit`: rolls back to previous valid offset | `test_crash_recovery.ps1` | `Crash injection: mid_write_offset_commit` | Verified |
| **M6: Retention & Recovery** | Hard kill server process (`Stop-Process -Force`): 100 acknowledged records intact | `test_crash_recovery.ps1` | `Real Server Hard-Kill & Restart Durability` | Verified |
| **M6: Retention & Recovery** | End-to-end broker retention: background reaper prunes segments; `earliest_offset > 0` | `test_retention.ps1` | `Retention pruned old segments` | Verified |
| **M6: Retention & Recovery** | Consumer group auto-reset: group resets offset from pruned position to earliest | `test_retention.ps1` | `Consumer group handles retention prune` | Verified |
| **M6: Retention & Recovery** | Win32 `FILE_SHARE_DELETE` safety: reader holds open handle while file unlinked | `test_retention.ps1` | `Windows delete-while-open sharing flags` | Verified |
| **M6: Retention & Recovery** | Offline storage GC: `streamforge_storage gc TOPIC [--dry-run]` preview and execution | `test_retention.ps1` | `Storage CLI gc --dry-run & execution` | Verified |

---

## 2. Identified Test Gaps & Resolutions

During the Milestone 7 audit, three subtle edge cases were identified as having no direct automated verification:

1. **Gap 1: Empty Partition Initial Boundary Semantics**
   - *Risk*: A freshly created partition with 0 records could exhibit unexpected behavior if read before any produces occur.
   - *Resolution*: Implemented `empty_partition_read_and_boundaries` in `tests/storage_tests.cpp`. Verifies read at offset 0 returns 0 records and `StatusCode::OK`, read at offset 1 returns `StatusCode::OffsetOutOfRange`, and subsequent produce transitions `next_offset` cleanly from 0 to 1.

2. **Gap 2: Missing or Corrupted `.index` Reconstruction from `.log`**
   - *Risk*: If an index file is lost or deleted due to unexpected filesystem corruption, the broker must be able to restore the index table entirely from the raw record payload without data loss.
   - *Resolution*: Implemented `missing_index_rebuilt_from_log_on_recovery` in `tests/storage_tests.cpp`. Appends records across multiple index interval steps, unlinks `.index`, calls `open_and_recover()`, confirms `.index` is regenerated, and tests random access seeks.

3. **Gap 3: Zero-Byte Key and Zero-Byte Value Records**
   - *Risk*: In Kafka workloads, tombstone messages (null/empty values) or keyless messages (null/empty keys) are ubiquitous. Record encoding and CRC calculation must handle 0-byte lengths without buffer underflows or assertion failures.
   - *Resolution*: Implemented `zero_byte_key_and_value_records` in `tests/protocol_tests.cpp`. Verifies encode, decode, and CRC roundtrips for: empty key + value, key + empty value, and both empty.
