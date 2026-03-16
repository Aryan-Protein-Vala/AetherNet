# Aether — High-Performance File Transfer

> A global data logistics layer that optimizes how data moves.

Aether is a CLI-first, hyper-optimized file transfer tool built in Go. It splits files into BLAKE3-verified chunks and transfers them over parallel HTTP streams with pipelined concurrency.

## Features

- **Pipelined Architecture** — Chunking and uploading happen concurrently (no sequential bottleneck)
- **BLAKE3-256 Hashing** — 5-14x faster than SHA-256, verified on both sender and receiver
- **Parallel Upload** — 5 concurrent goroutines with connection pooling
- **Auto-Reassembly** — Receiver automatically reconstructs the original file
- **Retry with Backoff** — Failed chunks retry up to 3x with exponential backoff (100-400ms)
- **Integrity Verified** — SHA1 of reassembled file matches original byte-for-byte

## Quick Start

```bash
# Build
go build -o bin/aether ./cmd/aether

# Start receiver (Terminal 1)
./bin/aether receive --port 8080

# Send a file (Terminal 2)
./bin/aether send model.bin --to http://localhost:8080

# The receiver auto-reassembles the file
```

## CLI Commands

```
aether send <file> --to <url>     Chunk + upload via pipelined parallel streams
aether receive --port <port>      Start HTTP chunk receiver server
aether version                    Print version
```

## Project Structure

```
.
├── cmd/aether/main.go             # Cobra CLI (send, receive, version)
├── pkg/
│   ├── chunker/
│   │   ├── chunker.go             # File splitting engine (pipelined + batch)
│   │   └── models.go              # Chunk, FileManifest, TransferSession
│   ├── network/
│   │   └── network.go             # HTTP worker pool with retry + backoff
│   ├── receiver/
│   │   ├── server.go              # HTTP receiver with BLAKE3 verification
│   │   └── reassemble.go          # File reconstruction from chunks
│   └── crypto/
│       └── crypto.go              # (future) encryption primitives
├── internal/config/config.go      # Engine defaults
├── go.mod
└── aether_master_plan.md
```

## Benchmarks

| Test | Result |
|---|---|
| 150 MB transfer (localhost) | 75/75 chunks verified |
| Throughput (upload phase) | ~1,350 MB/s |
| Integrity | SHA1 match confirmed |

## License

MIT
