# ⚡ Aether — High-Performance File Transfer

> A global data logistics layer that optimizes how data moves.

Aether is a CLI-first, hyper-optimized file transfer tool built in Go. It splits files into hash-verified chunks and transfers them over parallel streams for maximum throughput.

## 🚀 Phase 1 — TurboTransfer

- **Chunking Engine** — Split files into 1–4 MB chunks with BLAKE3 integrity hashes
- **Parallel Streams** — Multiple concurrent TCP/QUIC transfers
- **Resumable** — Manifests track per-chunk state for crash-proof resumption
- **Memory-Efficient** — Zero-alloc structs, stack-friendly fixed-size fields

## 📁 Project Structure

```
.
├── cmd/
│   └── aether/          # CLI entrypoint
│       └── main.go
├── pkg/
│   ├── chunker/         # File chunking engine + core models
│   │   ├── chunker.go
│   │   └── models.go    # FileManifest, Chunk, TransferSession
│   ├── network/         # Parallel transfer engine (TCP/QUIC)
│   │   └── network.go
│   └── crypto/          # BLAKE3 hashing & integrity verification
│       └── crypto.go
├── internal/
│   └── config/          # Internal defaults & configuration
│       └── config.go
├── go.mod
└── aether_master_plan.md
```

## 🛠️ Build & Run

```bash
go build -o bin/aether ./cmd/aether
./bin/aether version
./bin/aether send model.bin --to singapore
```

## 📄 License

MIT
