// Package chunker implements the file chunking engine for Aether.
//
// Two modes of operation:
//
//  1. ChunkFile — batch mode: chunks entire file, returns completed manifest.
//  2. ChunkFilePipelined — streaming mode: emits chunks on a channel as they
//     are created, allowing uploads to begin while the file is still being split.
//     This eliminates the sequential chunk-then-upload bottleneck.
//
// Hashing uses BLAKE3-256, which is 5-14x faster than SHA-256 on modern CPUs.
package chunker

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/zeebo/blake3"
)

const (
	// cacheDir is the local staging directory for chunk data.
	cacheDir = ".aether_cache"
)

// ChunkResult is emitted on the pipeline channel for each chunk produced.
type ChunkResult struct {
	Chunk     Chunk
	ChunkPath string
	Err       error
}

// PipelineInfo holds the metadata needed to start uploading
// before the manifest is fully built.
type PipelineInfo struct {
	FileID      [16]byte
	FileIDHex   string
	FileName    string
	FileSize    int64
	ChunkSize   uint32
	TotalChunks int
	CacheDir    string
	ChunkCh     <-chan ChunkResult // receive-only channel of produced chunks
}

// ChunkFilePipelined starts chunking in a background goroutine and
// returns immediately with pipeline metadata. Chunks are emitted on
// ChunkCh as they are written to disk. The channel is closed when
// all chunks have been produced (or on first error).
//
// The caller can start uploading from ChunkCh while chunking continues.
// After ChunkCh is drained, call FinishManifest() to get the final manifest.
func ChunkFilePipelined(filePath string, chunkSize uint32) (*PipelineInfo, error) {
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize < MinChunkSize || chunkSize > MaxChunkSize {
		return nil, fmt.Errorf("chunk size %d out of range [%d, %d]",
			chunkSize, MinChunkSize, MaxChunkSize)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open source file: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat source file: %w", err)
	}
	if info.IsDir() {
		f.Close()
		return nil, fmt.Errorf("%s is a directory, not a file", filePath)
	}
	fileSize := info.Size()

	var fileID [16]byte
	if _, err := rand.Read(fileID[:]); err != nil {
		f.Close()
		return nil, fmt.Errorf("generate file ID: %w", err)
	}

	sessionDir := filepath.Join(cacheDir, hex.EncodeToString(fileID[:]))
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		f.Close()
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	numChunks := int(fileSize / int64(chunkSize))
	if fileSize%int64(chunkSize) != 0 || fileSize == 0 {
		numChunks++
	}

	chunkCh := make(chan ChunkResult, 4) // small buffer for pipeline smoothness

	pipeline := &PipelineInfo{
		FileID:      fileID,
		FileIDHex:   hex.EncodeToString(fileID[:]),
		FileName:    filepath.Base(filePath),
		FileSize:    fileSize,
		ChunkSize:   chunkSize,
		TotalChunks: numChunks,
		CacheDir:    cacheDir,
		ChunkCh:     chunkCh,
	}

	// Background goroutine: read file → produce chunks
	go func() {
		defer f.Close()
		defer close(chunkCh)

		buf := make([]byte, chunkSize)
		fileHasher := blake3.New()

		var chunkID uint32
		var offset int64

		for {
			n, readErr := readFull(f, buf, fileHasher)
			if n > 0 {
				chunkHash := blake3.Sum256(buf[:n])

				chunkPath := filepath.Join(sessionDir, fmt.Sprintf("%06d.chunk", chunkID))
				if err := writeChunkFile(chunkPath, buf[:n]); err != nil {
					chunkCh <- ChunkResult{Err: fmt.Errorf("write chunk %d: %w", chunkID, err)}
					return
				}

				chunkCh <- ChunkResult{
					Chunk: Chunk{
						ID:     chunkID,
						Offset: offset,
						Size:   uint32(n),
						Hash:   chunkHash,
						State:  ChunkPending,
					},
					ChunkPath: chunkPath,
				}

				offset += int64(n)
				chunkID++
			}

			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				chunkCh <- ChunkResult{Err: fmt.Errorf("read at offset %d: %w", offset, readErr)}
				return
			}
		}
	}()

	return pipeline, nil
}

// ChunkFile is the original batch API. It chunks the entire file and
// returns a completed FileManifest. Kept for backward compatibility
// and simpler use cases.
func ChunkFile(filePath string, chunkSize uint32) (*FileManifest, error) {
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize < MinChunkSize || chunkSize > MaxChunkSize {
		return nil, fmt.Errorf("chunk size %d out of range [%d, %d]",
			chunkSize, MinChunkSize, MaxChunkSize)
	}

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

	var fileID [16]byte
	if _, err := rand.Read(fileID[:]); err != nil {
		return nil, fmt.Errorf("generate file ID: %w", err)
	}

	sessionDir := filepath.Join(cacheDir, hex.EncodeToString(fileID[:]))
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

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

	buf := make([]byte, chunkSize)
	fileHasher := blake3.New()

	var chunkID uint32
	var offset int64

	for {
		n, readErr := readFull(f, buf, fileHasher)
		if n > 0 {
			chunkHash := blake3.Sum256(buf[:n])

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
			return nil, fmt.Errorf("read at offset %d: %w", offset, readErr)
		}
	}

	var fileHash [32]byte
	bsum := blake3.Sum256(fileHasher.Sum(nil))
	copy(fileHash[:], bsum[:])
	copy(manifest.FileHash[:], fileHash[:])

	return manifest, nil
}

// readFull reads up to len(buf) bytes from r, feeding them into hash h.
func readFull(r io.Reader, buf []byte, h io.Writer) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		if n > 0 {
			h.Write(buf[total : total+n])
			total += n
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// writeChunkFile writes data to path (idempotent).
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
