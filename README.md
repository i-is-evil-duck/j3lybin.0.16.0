# J3lyBin  <br />  <img alt="Stargazers" src="https://img.shields.io/github/stars/i-is-evil-duck/j3lybin.0.16.0?style=for-the-badge&logo=starship&color=C9CBFF&logoColor=D9E0EE&labelColor=302D41">


## J3lyBin
A temporary file sharing service built with Zig 0.16.

## Features

- **25GB max file size** per upload
- **25GB daily quota** per IP address
- **Chunked uploads** (8MB chunks, 6 parallel streams) for speed and resumability
- **HTTP keep-alive** connections so chunk streams reuse their TCP/TLS handshake
- **Direct-to-final-file writes** — chunks land at their byte offset in the final file, no temp chunk files or reassembly pass
- **Configurable TTL**: 5 min, 15 min, 6h, 48h (default), 2 weeks, 1 month, 3 months
- **Automatic cleanup** of expired files
- **Dark mode** frontend with drag & drop support

## Quick Start (Docker)

```bash
# Build and run (uploaded files persist in the `j3lybin-data` volume)
docker-compose up --build

# Or with docker directly
docker build -t j3lybin .
docker volume create j3lybin-data
docker run -p 8080:8080 -v j3lybin-data:/app/data j3lybin
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
- Keep-alive connections (60s idle timeout) so parallel chunk streams avoid repeated handshakes
- Chunks written directly into `<id>.bin` at their byte offset (sparse file); the file is complete once all chunks arrive — no chunk files, no reassembly step
- Files stored in `/app/data/` (docker volume `j3lybin-data`) with metadata in `/app/data/meta/`
- Background cleanup thread removes expired files every 60 seconds
- IP quotas tracked in-memory (reset every 24 hours)

## File Structure

```
j3lybin/
├── build.zig           # Zig build configuration
├── build.zig.zon       # Package manifest
├── Dockerfile          # Multi-stage Docker build
├── docker-compose.yml  # Docker Compose config
└── src/
    ├── main.zig        # Server implementation
    ├── index.html      # Frontend UI
    └── a.png           # Favicon
```

Uploaded files and metadata live in the named Docker volume `j3lybin-data`
(mounted at `/app/data/`), not in the repo.

## Downloads

Download the pre-built executables from the [releases](https://github.com/i-is-evil-duck/j3lybin.0.16.0/releases) page.

| Platform | File |
|----------|------|
| Linux | `j3lybin` |
| Docker | `docker-compose up --build` |

## Views

<img src="http://moe.j3ly.com/@j3ly-bin-v2?name=j3ly-bin-v2&theme=rule34&padding=7&offset=0&align=top&scale=1&pixelated=1&darkmode=0&id=75814d8a" />
