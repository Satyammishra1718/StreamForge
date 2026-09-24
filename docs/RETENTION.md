# StreamForge Log Retention & Garbage Collection

## 1. Overview and Design Principles

StreamForge implements a durable, multi-segment append-only storage engine inspired by Apache Kafka. As producers continuously append records, the storage engine partitions logs into finite immutable historical segments and one active segment. Log retention manages the lifecycle of these segments, reclaiming disk space based on age and size constraints while preserving broker throughput and consumer consistency.

### Core Retention Principles
1. **Segment-Level Granularity**: Data is never deleted record-by-record. Instead, retention operates strictly at the granularity of complete log segments (`.log`, `.index`, `.sealed`).
2. **Active Segment Invariant**: The currently active (head) segment is **never** deleted by retention under any circumstance, even if it exceeds size or age limits. This ensures that producers always have an active log segment available for immediate appends.
3. **Oldest-First Truncation**: Segments are evaluated in ascending order of their base offsets (`00000000000000000000.log`, `00000000000000000010.log`, etc.). The oldest historical segments are pruned first.
4. **Thread-Safe Concurrent Access**: Reads and writes proceed uninterrupted during retention cleanup using native reader-writer locks (`SRWLOCK`) and Windows handle sharing modes (`FILE_SHARE_DELETE`).

---

## 2. Retention Policies

StreamForge supports two primary retention policies, configured per-topic or via broker-wide defaults:

### A. Time-Based Retention (`retention_ms`)
- **Evaluation**: A closed segment is eligible for deletion if:
  $$\text{now}() - \text{segment.last\_timestamp\_ms}() > \text{retention\_ms}$$
- **Fallback**: If segment record timestamps are unavailable, the file system last modification time (`std::filesystem::last_write_time`) is utilized.
- **Default**: 7 days (`604,800,000 ms`).

### B. Size-Based Retention (`retention_bytes`)
- **Evaluation**: The partition calculates total disk usage across all closed segments and the active segment. If total bytes exceed `retention_bytes`, the oldest closed segments are deleted one by one until the partition size fits within the retention budget (or until only the active segment remains).
- **Default**: Unlimited (0 / disabled).

---

## 3. Background Retention Manager (`RetentionManager`)

The retention cleanup is orchestrated by a dedicated background thread managed by `streamforge::RetentionManager`:
- **Execution Interval**: Configurable via `--retention-check-interval-ms` (default 30,000 ms; tuned to 400-500 ms in integration tests).
- **Graceful Termination**: Utilizes `std::condition_variable` waiting against an atomic stop flag (`m_running`), allowing instantaneous shutdown without blocking thread join.
- **Pass Execution**: For every topic and partition:
  1. Identifies closed segments violating retention age or size.
  2. Acquires exclusive partition lock.
  3. Closes segment file handles.
  4. Deletes `.log`, `.index`, and `.sealed` files from disk.
  5. Updates partition `earliest_offset`.

---

## 4. Windows File Deletion Semantics & `FILE_SHARE_DELETE`

Under Windows Win32 APIs, standard file deletion (`DeleteFileW` or `std::filesystem::remove`) fails with error code `ERROR_SHARING_VIOLATION` (32) if any handle is open without delete permissions.

### StreamForge Windows Sharing Model
To allow lock-free concurrent reads without blocking garbage collection:
1. Every file in StreamForge (`LogSegment`, `OffsetIndex`, `OffsetStore`) is opened with:
   ```cpp
   DWORD share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
   HANDLE h = CreateFileW(path.c_str(), access_mode, share_mode, nullptr,
                          creation_disposition, FILE_ATTRIBUTE_NORMAL, nullptr);
   ```
2. **Delete-While-Open Safety**:
   - When `RetentionManager` unlinks a segment via `DeleteFileW`, Windows marks the directory entry as pending deletion.
   - Any in-flight reader threads holding open handles to that segment continue reading their buffered records safely.
   - Once all readers finish and close their handles, the Windows NTFS kernel driver reclaims the disk clusters automatically.

---

## 5. Consumer Group Retention Handling

When retention prunes historical segments, consumer groups holding offsets within the pruned range will attempt to fetch data that no longer exists on disk.

### Consumer Recovery Sequence
1. **Error Detection**: The broker detects that `fetch_offset < partition.earliest_offset` and responds with `ErrorCode::OFFSET_OUT_OF_RANGE` (code 8).
2. **Describe & Auto-Reset**: In `streamforge_cli consume-group`:
   - The CLI intercepts error code 8.
   - Sends a `DESCRIBE_TOPIC` frame to query the current partition earliest and high-watermark offsets.
   - Logs a warning:
     ```text
     [WARN] Resetting group <group_id> partition <P> offset from <old_offset> to <earliest_offset> due to retention
     ```
   - Automatically resets consumer partition position and uncommitted offset to `earliest_offset`.
   - Continues consuming incoming records without termination or consumer group stalls.

---

## 6. Offline Storage Management CLI (`streamforge_storage gc`)

Administrators can manually inspect and prune log segments while the broker is offline using the `streamforge_storage` utility:

```powershell
# Perform a dry-run GC to preview eligible segments without deleting them
.\build\streamforge_storage.exe --data-dir .\data gc my_topic --dry-run

# Execute immediate segment reclamation
.\build\streamforge_storage.exe --data-dir .\data gc my_topic
```
