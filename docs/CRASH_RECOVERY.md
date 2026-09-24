# StreamForge Crash Recovery & Durability Architecture

## 1. Crash Model and Durability Guarantees

In distributed storage systems, server crashes, power outages, and kernel panics can happen at any arbitrary instruction. StreamForge ensures the following invariants:
1. **Append Durability**: When synchronous flush is enabled (`--sync-on-append true`), any acknowledged write is guaranteed to be flushed to disk via `FlushFileBuffers` on Win32.
2. **Zero Inconsistent State**: In the event of a sudden kill, torn writes (partially written record bodies or truncated headers) are detected on startup and cleanly rolled back to the last valid boundary.
3. **Index Regenerability**: Indexes (`.index`) are pure secondary structures. If missing or corrupted, they are reconstructed from the authoritative log segment (`.log`).
4. **Consumer Offset Safety**: Consumer offset commits use an atomic rewrite/rename sequence or append-only log with CRC checks, guaranteeing no corrupted offsets on reload.

---

## 2. Segment Anatomy and the `.sealed` Sidecar

A closed log segment is composed of three files:
1. `00000000000000000000.log`: The binary append-only payload log containing serialized records with individual CRC32 checksums.
2. `00000000000000000000.index`: The sparse offset-to-file-position index table.
3. `00000000000000000000.sealed`: The sealed metadata sidecar certifying immutable segment closure.

### `.sealed` Sidecar Binary Format (44 bytes)
```
+------------------------------------+------------------+
| Field                              | Size             |
+------------------------------------+------------------+
| Magic ("SFSEAL1\0")                | 8 bytes          |
| Record Count (uint64)              | 8 bytes          |
| Byte Size (uint64)                 | 8 bytes          |
| Last Offset (uint64)               | 8 bytes          |
| Last Timestamp (int64)             | 8 bytes          |
| Header CRC32 (uint32)              | 4 bytes          |
+------------------------------------+------------------+
```
When a segment rolls, the broker writes this sidecar and flushes it with `FlushFileBuffers`. During startup recovery:
- If a valid `.sealed` file is present and matches the segment file size and CRC, the segment is known to be immutable and verified.
- If missing or corrupted, StreamForge triggers a full recovery scan across the segment to rebuild both the index and the sealed sidecar.

---

## 3. Dual-Mode Startup Verification: Quick vs. Full Scan

StreamForge implements two startup recovery strategies configured via `--startup-scan [quick|full]`:

### A. Quick Startup Scan (`verify_quick()`)
- **Use Case**: Default production startup for large datasets (gigabytes/terabytes of logs).
- **Behavior**:
  - Checks if `.sealed` sidecar is present and validates its CRC against the segment size.
  - Verifies index size alignment (multiple of 12 bytes: 4-byte offset delta + 8-byte position).
  - Performs a spot-check on the last record: uses the index to find the offset position of `last_offset`, reads the record header, verifies its length and CRC32.
  - Runtime: $O(1)$ per segment, enabling near-instant broker restart.

### B. Full Startup Scan (`verify_and_rebuild_full()`)
- **Use Case**: Corrupted segment repair, missing sidecars, or explicit `--startup-scan full` flag.
- **Behavior**:
  - Sequentially scans every record from position 0 to EOF.
  - Decodes and calculates CRC32 for every record.
  - If a torn record or corruption is encountered:
    - Truncates the segment at the last valid position using `SetFilePointerEx` + `SetEndOfFile`.
    - Discards invalid uncommitted bytes.
  - Regenerates the `.index` file from scratch with sparse interval entries.
  - Rewrites the `.sealed` marker sidecar.
  - Logs the number of records recovered, bytes scanned, and corruptions truncated.

---

## 4. Crash Injection Points (`CrashPoint`)

To formally test and verify recovery logic against arbitrary failures, StreamForge includes compile-time crash injection hooks enabled via `-DSTREAMFORGE_CRASH_TESTING=ON`:

| Crash Point | Injection Location | Recovery Action Tested |
|---|---|---|
| `mid_write_record` | Directly after writing partial record bytes, before writing record CRC or flushing | Torn tail detection; segment truncated to previous valid boundary. |
| `between_log_and_index` | After log write succeeds, before index write completes | Rebuilding missing index entries from the log payload. |
| `between_seal_and_sealed` | Active segment rolled, but process crashes before `.sealed` sidecar is written | Startup scan detects missing sidecar, fully verifies records, and recreates `.sealed`. |
| `mid_write_offset_commit` | While writing consumer group offset commit state | Corrupted offset entry detection; rolls back to last valid committed offset. |

---

## 5. Win32 Handle Exhaustion & `SharedMutex`

During extensive crash recovery testing with thousands of segment opens and closes, MinGW GCC's runtime implementation of `std::shared_mutex` caused handle exhaustion:
- **MinGW GCC Bug**: On Windows, `std::shared_mutex` allocates an Event and Semaphore Win32 handle per instance and fails to close them on destruction, leaking 2 handles per instance (2,000 handles in 1,000 partition open/close cycles).
- **StreamForge Solution (`SharedMutex`)**: StreamForge implements `streamforge::SharedMutex` wrapping the native Win32 Slim Reader/Writer (`SRWLOCK`) API:
  ```cpp
  class SharedMutex {
      SRWLOCK m_lock = SRWLOCK_INIT;
  public:
      void lock() { AcquireSRWLockExclusive(&m_lock); }
      void unlock() { ReleaseSRWLockExclusive(&m_lock); }
      void lock_shared() { AcquireSRWLockShared(&m_lock); }
      void unlock_shared() { ReleaseSRWLockShared(&m_lock); }
  };
  ```
- **Benefits**:
  - Exactly 8 bytes in user-space.
  - Zero Windows kernel handle allocations.
  - Zero handle leaks across millions of partition lifecycle cycles.
  - Implements the standard C++ `SharedLockable` concept for `std::unique_lock` and `std::shared_lock`.
