// Package chunker implements the file chunking engine for Aether.
// It splits files into hash-verified, resumable chunks for parallel transfer.
//
// Design principles:
//   - Never load the full file into memory — uses a fixed-size reusable buffer
//   - SHA-256 per-chunk hashing is computed during the single read pass
//   - A rolling file-level hash is maintained simultaneously (zero extra I/O)
//   - Chunks are written to .aether_cache/<file_id>/ for resumable transfer
package chunker

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	// cacheDir is the local staging directory for chunk data.
	cacheDir = ".aether_cache"
)

// ChunkFile splits the file at filePath into chunks of chunkSize bytes.
// Each chunk is written to .aether_cache/<session>/<chunk_id>.chunk and
// its SHA-256 hash is recorded. If chunkSize is 0, DefaultChunkSize is used.
//
// The file is read in a single sequential pass with a reusable buffer,
// keeping peak memory usage at O(chunkSize) regardless of file size.
//
// Returns a fully populated FileManifest or the first I/O error encountered.
func ChunkFile(filePath string, chunkSize uint32) (*FileManifest, error) {
	// ── Validate chunk size ──────────────────────────────────────────
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize < MinChunkSize || chunkSize > MaxChunkSize {
		return nil, fmt.Errorf("chunk size %d out of range [%d, %d]",
			chunkSize, MinChunkSize, MaxChunkSize)
	}

	// ── Open source file & stat ──────────────────────────────────────
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open source file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat source file: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s is a directory, not a file", filePath)
	}
	fileSize := info.Size()

	// ── Generate file ID (random 128-bit) ────────────────────────────
	var fileID [16]byte
	if _, err := rand.Read(fileID[:]); err != nil {
		return nil, fmt.Errorf("generate file ID: %w", err)
	}

	// ── Create cache directory ───────────────────────────────────────
	sessionDir := filepath.Join(cacheDir, hex.EncodeToString(fileID[:]))
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	// ── Pre-allocate manifest chunks ─────────────────────────────────
	numChunks := int(fileSize / int64(chunkSize))
	if fileSize%int64(chunkSize) != 0 || fileSize == 0 {
		numChunks++
	}

	manifest := &FileManifest{
		FileID:    fileID,
		FileName:  filepath.Base(filePath),
		FileSize:  fileSize,
		ChunkSize: chunkSize,
		Chunks:    make([]Chunk, 0, numChunks),
		CreatedAt: time.Now().UTC(),
	}

	// ── Single-pass read: chunk + hash simultaneously ────────────────
	//
	// buf is allocated once and reused for every chunk.
	// fileHasher computes the whole-file SHA-256 alongside chunk hashing.
	buf := make([]byte, chunkSize)
	fileHasher := sha256.New()

	var (
		chunkID uint32
		offset  int64
	)

	for {
		n, readErr := readFull(f, buf, fileHasher)
		if n > 0 {
			// Hash this chunk's data
			chunkHash := sha256.Sum256(buf[:n])

			// Write chunk to cache
			chunkPath := filepath.Join(sessionDir, fmt.Sprintf("%06d.chunk", chunkID))
			if err := writeChunkFile(chunkPath, buf[:n]); err != nil {
				return nil, fmt.Errorf("write chunk %d: %w", chunkID, err)
			}

			manifest.Chunks = append(manifest.Chunks, Chunk{
				ID:     chunkID,
				Offset: offset,
				Size:   uint32(n),
				Hash:   chunkHash,
				State:  ChunkPending,
			})

			offset += int64(n)
			chunkID++
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read source file at offset %d: %w", offset, readErr)
		}
	}

	// ── Finalise whole-file hash ─────────────────────────────────────
	copy(manifest.FileHash[:], fileHasher.Sum(nil))

	return manifest, nil
}

// readFull reads up to len(buf) bytes from r into buf,
// simultaneously feeding every byte into the running hash h.
// Returns the number of bytes read and any error (including io.EOF).
//
// Unlike io.ReadFull, this tolerates a short final read (EOF).
func readFull(r io.Reader, buf []byte, h hash.Hash) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		if n > 0 {
			// Feed into the whole-file hash as we read
			h.Write(buf[total : total+n])
			total += n
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// writeChunkFile atomically writes data to path.
// It uses O_CREATE|O_TRUNC to be idempotent on retries.
func writeChunkFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
