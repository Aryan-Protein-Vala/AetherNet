# Aether — High-Performance File Transfer

> A global data logistics layer that optimizes how data moves.

Aether is a CLI-first, hyper-optimized file transfer tool built in Go. It splits files into SHA-256-verified chunks and transfers them through a relay server via parallel HTTP streams.

## Architecture

```
Sender                    Relay                      Recipient
aether send file.bin  →   POST /upload (chunks)  ←   aether fetch <fileID>
                          POST /manifest (JSON)      GET /manifest
                          stores in .aether_received  GET /chunk (parallel)
                                                     reassembles locally
```

## Quick Start

```bash
# Build
go build -o bin/aether ./cmd/aether

# Start relay (Terminal 1)
./bin/aether relay --port 8080

# Send a file (Terminal 2)
./bin/aether send model.bin --to http://localhost:8080
# → outputs: aether fetch <fileID>

# Download on another machine (Terminal 3)
./bin/aether fetch <fileID> --from http://localhost:8080
```

## CLI Commands

| Command | Description |
|---|---|
| `aether send <file> --to <relay>` | Chunk + upload to relay |
| `aether fetch <id> --from <relay>` | Download from relay + reassemble |
| `aether relay --port <port>` | Start relay server |
| `aether version` | Print version |

## Features

- **Pipelined** — Chunking and uploading happen concurrently
- **SHA-256** — Hardware-accelerated integrity verification (zero deps)
- **Parallel I/O** — 5 concurrent goroutines with connection pooling
- **Retry** — Exponential backoff on chunk failure (3 attempts)
- **Auto-reassembly** — Client reconstructs original file from chunks
- **Relay architecture** — Server is a dumb, fast chunk store

## Benchmarks (150 MB, localhost)

| Operation | Speed |
|---|---|
| Send (pipelined) | 570 MB/s |
| Fetch (parallel download) | 306 MB/s |
| SHA-256 vs BLAKE3 | SHA-256 wins on Apple Silicon (hardware accel) |

## Project Structure

```
cmd/aether/main.go          CLI (send, fetch, relay, version)
pkg/chunker/chunker.go      Pipelined file splitting engine
pkg/chunker/models.go       Chunk, FileManifest, TransferSession
pkg/network/network.go      Upload worker pool with retry
pkg/network/download.go     Download worker pool with retry
pkg/receiver/server.go      Relay server (upload, manifest, chunk endpoints)
pkg/receiver/reassemble.go  File reconstruction from chunks
```

## License

MIT
