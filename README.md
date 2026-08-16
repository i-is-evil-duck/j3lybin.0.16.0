# J3lyBin

A temporary file sharing service built with Zig 0.16.

## Features

- **5GB max file size** per upload
- **10GB daily quota** per IP address
- **Chunked uploads** (512KB chunks) for resumability and memory efficiency
- **Configurable TTL**: 5 min, 15 min, 6h, 48h (default), 2 weeks, 1 month, 3 months
- **Automatic cleanup** of expired files
- **Dark mode** frontend with drag & drop support

## Quick Start (Docker)

```bash
# Build and run
docker-compose up --build

# Or with docker directly
docker build -t j3lybin .
docker run -p 8080:8080 -v $(pwd)/data:/app/data j3lybin
```

Then open http://localhost:8080 in your browser.

## Build from Source

Requires Zig 0.16.0:

```bash
# Download Zig 0.16.0 from https://ziglang.org/download/
# Extract and add to PATH

zig build -Doptimize=ReleaseFast
./zig-out/bin/j3lybin 8080
```

## API

### Upload a file (chunked)

```http
POST /chunk
X-Upload-Id: <random-id>
X-Chunk-Index: <0-based-index>
X-Total-Chunks: <total>
X-Filename: <filename>
X-Ttl: <5m|15m|6h|48h|2w|1M|3M>
X-Total-Size: <bytes>
Content-Type: application/octet-stream

<binary-chunk-data>
```

### Download a file

```http
GET /s/<upload-id>
```

## Architecture

- Raw HTTP server using Zig 0.16's `std.Io.net` API
- Thread-per-connection model with mutex-protected shared state
- Files stored in `./data/` with metadata in `./data/meta/`
- Background cleanup thread removes expired files every 60 seconds
- IP quotas tracked in-memory (reset every 24 hours)

## File Structure

```
j3lybin/
├── build.zig           # Zig build configuration
├── build.zig.zon       # Package manifest
├── Dockerfile          # Multi-stage Docker build
├── docker-compose.yml  # Docker Compose config
├── src/
│   ├── main.zig        # Server implementation
│   ├── index.html      # Frontend UI
│   └── a.png           # Favicon
└── data/               # Runtime: uploaded files + metadata
    └── meta/
```
