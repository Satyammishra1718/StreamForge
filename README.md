# StreamForge

StreamForge is a high-performance, Kafka-inspired durable message broker written in modern C++ (C++17) running natively on Windows using Winsock2.

## Milestone 1: Skeleton, Winsock TCP Server, and Binary Protocol

### Features
- Native Windows 10/11 socket programming with Winsock2.
- RAII-managed network resources, sockets, and Winsock runtime initialization (`WSAStartup`/`WSACleanup`).
- Length-prefixed binary wire protocol using network byte order (big-endian).
- Multi-threaded TCP server with clean connection isolation.
- Automatic thread reaping and graceful shutdown support (`SetConsoleCtrlHandler`).
- Native PowerShell build, run, and smoke test scripts.

### Build and Run

#### Prerequisites
- Windows 10/11
- MinGW g++ compiler (e.g. MSYS2 UCRT64) or Visual Studio MSVC
- CMake 3.16+

#### Build
Run the build script from PowerShell:
```powershell
.\build.ps1
```

#### Run Server
To start the server on default port 9092 (listening on 127.0.0.1):
```powershell
.\run_server.ps1
```
Or specify a custom port:
```powershell
.\run_server.ps1 7000
```

#### Test
To run the automated smoke test suite:
```powershell
.\test.ps1
```
