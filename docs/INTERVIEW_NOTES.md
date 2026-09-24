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
