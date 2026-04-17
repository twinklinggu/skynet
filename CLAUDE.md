# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Skynet is a lightweight multi-user Lua framework supporting the actor model for building high-concurrency services, primarily used in online game development.

- **Core written in C** (15k LOC): Handles message queuing, thread scheduling, socket I/O, timers, and memory management
- **Application logic written in Lua** (21k LOC): Each actor (service) runs in an isolated Lua VM state
- **Multi-threaded design**: Uses a thread pool for parallel processing of messages
- **Supports clustering**: Built-in multi-node communication via harbor/cluster
- **Current version**: 1.8.0 (2025)
- **License**: MIT

## Build Commands

### Prerequisites
- GCC or Clang with C99 support
- GNU make
- autoconf (for jemalloc on Linux)

### Basic Build
```bash
# Update git submodules (3rd party dependencies)
git submodule update --init

# Update 3rd party dependencies (if needed)
make update3rd

# Build for your platform (PLATFORM = linux|macosx|freebsd|mingw)
make linux

# Or set PLAT environment variable
export PLAT=linux
make
```

### Build with HTTPS/TLS support
```bash
make linux TLS_MODULE=ltls
```

### Clean
```bash
# Clean build outputs only
make clean

# Clean everything including 3rd-party builds
make cleanall
```

## Test Commands

```bash
# Run the example configuration (terminal 1)
./skynet examples/config

# Run example client (terminal 2)
./3rd/lua/lua examples/client.lua

# Run individual test file
./skynet test/test_xxx.lua
# Or with the standalone Lua interpreter
./3rd/lua/lua test/test_xxx.lua
```

## Project Structure

```
skynet/
├── skynet-src/          # Core C source files
│   ├── skynet_main.c     # Entry point
│   ├── skynet_start.c    # Thread initialization
│   ├── skynet_server.c   # Core server implementation
│   ├── skynet_mq.[ch]    # Message queue
│   ├── skynet_timer.[ch] # Timer management
│   ├── skynet_socket.c   # Socket handling
│   ├── socket_server.c   # Socket server implementation
│   └── ...
├── lualib-src/          # Lua C extensions
│   ├── lua-skynet.c      # Core skynet bindings
│   ├── lua-*.c           # Various modules (bson, crypt, mongo, etc.)
│   └── sproto/           # Sproto serialization
├── lualib/              # Lua libraries
│   ├── skynet/           # Core Lua API modules
│   │   ├── cluster.lua    # Cluster management
│   │   ├── coroutine.lua  # Coroutine utilities
│   │   ├── db/            # Database drivers (mongo, mysql, redis)
│   │   ├── socketchannel.lua # Socket abstraction
│   │   └── ...
│   ├── http/             # HTTP client/server + websocket
│   ├── snax/             # SNAX RPC framework
│   ├── skynet.lua        # Main Lua API entry point
│   └── timer.lua         # Timer utilities
├── service-src/         # C service sources
├── service/             # Lua built-in services
│   ├── bootstrap.lua      # Bootstrap
│   ├── launcher.lua       # Service launcher
│   └── ...
├── 3rd/                 # Third-party dependencies (git submodules)
│   ├── lua/              # Lua 5.4.7 (patched)
│   ├── jemalloc/          # Memory allocator
│   ├── lpeg/             # PEG library
│   └── ...
├── examples/            # Example configurations
├── test/                # Test files
├── Makefile
└── platform.mk          # Platform-specific settings
```

## Architecture

### Thread Model
1. **Monitor thread**: Checks for blocked services to prevent deadlocks
2. **Timer thread**: Updates timers every 2.5ms
3. **Socket thread**: Handles I/O polling via epoll/kqueue
4. **Worker threads**: Dispatch messages to services (configurable quantity)

### Key Concepts
- **Service**: Isolated actor (Lua VM state) that processes messages
- **Message passing**: Communication between services via message passing (skynet.send/skynet.call)
- **Coroutines**: Async/await-like synchronization without callbacks
- **Actor model**: Each service runs independently, shares nothing by default

### Main Lua API
- `skynet.send()` - Send fire-and-forget message to a service
- `skynet.call()` - Call a service and wait for response
- `skynet.newservice()` - Spawn a new service
- `skynet.fork()` - Create a new coroutine
- `skynet.sleep()` - Sleep current coroutine
- `skynet.wait()` / `skynet.wakeup()` - Coroutine synchronization

## Configuration

Configuration is done via Lua-based config file. Example:
```lua
thread = 8           -- Number of worker threads
harbor = 1           -- Harbor ID for clustering
address = "127.0.0.1:2526"
master = "127.0.0.1:2013"
standalone = "0.0.0.0:2013"
start = "main"       -- Main entry point service
bootstrap = "snlua bootstrap"
cpath = root.."cservice/?.so"
```

## Coding Notes

- **C**: C99 with 4-space indentation, traditional K&R style
- **Lua**: snake_case naming convention, all APIs under `skynet.` namespace
- **Lua version**: Lua 5.4.7 (patched, included in 3rd/lua)
- **jemalloc**: Enabled by default on Linux/FreeBSD, disabled on macOS
- Full documentation at: https://github.com/cloudwu/skynet/wiki
