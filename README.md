# StreamForge

**A Kafka-Inspired, High-Performance Durable Message Broker — built from scratch in C++17, running natively on Windows.**

[![C++17](https://img.shields.io/badge/Language-C%2B%2B17-blue.svg)](https://en.wikipedia.org/wiki/C%2B%2B17)
[![Platform](https://img.shields.io/badge/Platform-Windows%2010%20%2F%2011-0078D6.svg)](https://microsoft.com/windows)
[![Network](https://img.shields.io/badge/Network-Winsock2%20WSAPoll-brightgreen.svg)]()
[![Storage](https://img.shields.io/badge/Storage-Append--Only%20Log%20%2B%20Sparse%20Index-orange.svg)]()
[![Zero Dependencies](https://img.shields.io/badge/Dependencies-Zero%20External-success.svg)]()
[![Tests](https://img.shields.io/badge/Tests-7%2F7%20Suites%20Passing-brightgreen.svg)]()

---

> Built this to deeply understand how Apache Kafka works under the hood — partitioned logs, binary protocols, async I/O, consumer groups, crash recovery — everything from scratch, no external libraries.

---

## What is StreamForge?

StreamForge is a fully functional message broker inspired by Apache Kafka. It is written entirely in **modern C++17** using only native Windows APIs (Winsock2, Win32) — no POSIX shims, no WSL, no Docker, no third-party libraries.

It supports:
- Creating **topics** with multiple **partitions**
- **Producing** messages from multiple concurrent clients
- **Consuming** via offset-based **fetch** or a **consumer group** (with rebalancing)
- Durable on-disk **append-only log storage** with sparse memory-mapped indices
- **Crash recovery** — detects torn writes on restart, truncates corruption, rebuilds indices
- **Retention GC** — background cleanup of old log segments by time or size
- **Backpressure** — TCP zero-window throttling when the broker is overloaded

---

## Real Benchmark Numbers

> Measured on: Intel i5-13420H, 12 cores, 15.6 GB RAM, NVMe SSD — Windows 11

| Scenario | Result |
|---|---|
| Peak produce throughput | **77,306 records/sec** (7.37 MB/s) at 4 connections |
| Peak fetch throughput | **65,044 records/sec** (6.20 MB/s) large batch |
| 1,000 concurrent connections | **17 threads flat**, p99 latency **3.86 ms** |
| Crash recovery (100K records) | **735 ms** quick scan, **1,580 ms** full CRC scan |
| Thread-per-conn (old) vs WSAPoll (new) | 203 threads vs **17 threads** at 200 connections |

---

## Architecture

```
Producers / Consumers
        │
        │  TCP (Winsock2)
        ▼
┌─────────────────────────────────────────────┐
│          Single I/O Reactor Thread           │
│          WSAPoll Multiplexer                 │
│          WakeupChannel (loopback TCP)        │
│          Bounded TaskQueue + Backpressure    │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────┐
│         Worker Thread Pool (4–16)           │
│         Binary Frame Codec + CRC32          │
│         Message Handler / Router            │
└──────┬──────────────────────┬───────────────┘
       │                      │
       ▼                      ▼
┌─────────────┐    ┌──────────────────────────┐
│ TopicManager│    │    GroupCoordinator       │
│ Topic       │    │    Consumer Group FSM     │
│ Partition   │    │    StickyAssignor         │
│ LogSegment  │    │    OffsetStore            │
│ OffsetIndex │    │    __consumer_offsets     │
│ RetentionGC │    └──────────────────────────┘
└─────────────┘
       │
  .log + .index files (on disk)
```

Full architecture diagrams with Mermaid flowcharts and sequence diagrams are in [`docs/HLD.md`](docs/HLD.md).

---

## Quickstart

### Prerequisites
- Windows 10 or 11 (64-bit)
- MinGW GCC 11+ via [MSYS2 UCRT64](https://www.msys2.org/) **or** Visual Studio 2019+
- CMake 3.16+

### Build

```powershell
.\build.ps1
```

Builds everything with **zero warnings** (`-Wall -Wextra -Wpedantic`).

### Run the Broker

```powershell
.\build\streamforge_server.exe --port 9092 --data-dir .\data --workers 4
```

### Use the CLI (in a second terminal)

```powershell
# Create a topic with 4 partitions
.\build\streamforge_cli.exe --port 9092 create-topic orders 4

# Produce a message
.\build\streamforge_cli.exe --port 9092 produce orders "Hello StreamForge" --key k1

# Read it back
.\build\streamforge_cli.exe --port 9092 fetch orders 0 0

# Join a consumer group
.\build\streamforge_cli.exe --port 9092 group-join my-group orders
```

### Run All Tests

```powershell
powershell -ExecutionPolicy Bypass -File .\verify_all.ps1
```

**7/7 suites pass, 0 warnings, 0 leaks.**

---

## Project Structure

```
StreamForge/
├── include/streamforge/    # All header files
├── src/                    # All implementation files
├── tests/                  # Unit + integration test suites
├── scripts/                # Benchmark runner, PDF generator
├── docs/                   # HLD, LLD, Benchmarks, Interview Prep
├── build.ps1               # One-command build script
├── verify_all.ps1          # Master test verification
└── CMakeLists.txt
```

---

## Documentation

| Document | What's Inside |
|---|---|
| [`docs/HLD.md`](docs/HLD.md) | 4-layer architecture, produce/fetch data flows, design tradeoffs |
| [`docs/LLD.md`](docs/LLD.md) | C++ class diagrams, on-disk binary formats, full lock inventory |
| [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) | All 7 benchmark scenarios with real numbers |
| [`docs/TEST_COVERAGE.md`](docs/TEST_COVERAGE.md) | 53-point requirement traceability matrix |
| [`docs/INTERVIEW_PREP.md`](docs/INTERVIEW_PREP.md) | 25+ systems interview Q&As |

---

## Key Design Decisions

| Decision | What I chose | Why not the alternative |
|---|---|---|
| Networking | `WSAPoll` reactor + thread pool | Thread-per-connection wastes 1 MB stack per client, collapses at scale |
| Storage | Append-only `.log` + sparse `.index` | No LSM compaction overhead like RocksDB, pure sequential writes |
| Index lookup | Memory-mapped binary search | Zero-copy, O(log N), index stays < 0.4% of log size |
| Consumer assignment | Cooperative Sticky Assignor | Range/round-robin cause full partition reshuffles on every rebalance |
| Backpressure | `SO_RCVBUF = 0` (TCP zero-window) | Drops no data, client pauses naturally at the transport layer |

---

## Milestones Built

| # | What was built |
|---|---|
| M1 | Winsock2 TCP server + binary wire protocol + PING/ECHO |
| M2 | Partitioned append-only log engine + sparse memory-mapped index |
| M3 | PRODUCE / FETCH over TCP + CLI client |
| M4 | WSAPoll I/O reactor + bounded worker thread pool + backpressure |
| M5 | Consumer groups, partition assignment, heartbeats, offset commits |
| M6 | Retention GC + crash recovery + CRC32 tail truncation |
| M7 | Test coverage audit + benchmark suite + HLD/LLD documentation |

---

## License

MIT
