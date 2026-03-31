// Package chunker — chunker_fast.go implements the mmap + parallel chunking engine.
//
// Architecture:
//   1. mmap the source file into memory (zero-copy, no read syscalls)
//   2. Split the mapped region into N segments (1 per CPU core)
//   3. Each goroutine hashes + writes its segment's chunks in parallel
//   4. Results fan-in to a single pipeline channel
//
// Falls back to standard Read()-based chunking for files < 4MB.
package chunker

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"

	crand "crypto/rand"
)

const (
	mmapThreshold = 4 << 20 // Only use mmap for files >= 4MB
)

// ChunkFileFast uses mmap + parallel goroutines for maximum throughput.
// Falls back to ChunkFilePipelined for small files.
func ChunkFileFast(filePath string, chunkSize uint32) (*PipelineInfo, error) {
	if chunkSize == 0 {
		chunkSize = AutoChunkSize(filePath)
	}
	if chunkSize < MinChunkSize || chunkSize > MaxChunkSize {
		return nil, fmt.Errorf("chunk size %d out of range [%d, %d]",
			chunkSize, MinChunkSize, MaxChunkSize)
	}

	info, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s is a directory", filePath)
	}

	fileSize := info.Size()

	// Fallback for small files (mmap overhead not worth it)
	if fileSize < mmapThreshold {
		return ChunkFilePipelined(filePath, chunkSize)
	}

	// ── mmap the file ────────────────────────────────────────────────
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(fileSize),
		syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mmap: %w", err)
	}

	// ── Generate file ID ─────────────────────────────────────────────
	var fileID [16]byte
	if _, err := crand.Read(fileID[:]); err != nil {
		syscall.Munmap(data)
		f.Close()
		return nil, fmt.Errorf("generate file ID: %w", err)
	}
	fileIDHex := hex.EncodeToString(fileID[:])

	numChunks := int(fileSize / int64(chunkSize))
	if fileSize%int64(chunkSize) != 0 || fileSize == 0 {
		numChunks++
	}

	chunkCh := make(chan ChunkResult, numChunks)

	pipeline := &PipelineInfo{
		FileID:      fileID,
		FileIDHex:   fileIDHex,
		FileName:    filepath.Base(filePath),
		FileSize:    fileSize,
		ChunkSize:   chunkSize,
		TotalChunks: numChunks,
		CacheDir:    cacheDir,
		ChunkCh:     chunkCh,
	}

	// ── Parallel chunking ────────────────────────────────────────────
	// Each goroutine processes a contiguous range of chunks.
	numWorkers := runtime.NumCPU()
	if numWorkers > numChunks {
		numWorkers = numChunks
	}

	go func() {
		defer f.Close()
		defer syscall.Munmap(data)
		defer close(chunkCh)

		var wg sync.WaitGroup
		chunksPerWorker := numChunks / numWorkers
		remainder := numChunks % numWorkers

		startChunk := 0
		for w := 0; w < numWorkers; w++ {
			count := chunksPerWorker
			if w < remainder {
				count++
			}
			endChunk := startChunk + count

			wg.Add(1)
			go func(start, end int) {
				defer wg.Done()
				processSegment(data, fileSize, chunkSize, start, end, chunkCh)
			}(startChunk, endChunk)

			startChunk = endChunk
		}

		wg.Wait()
	}()

	return pipeline, nil
}

func processSegment(data []byte, fileSize int64, chunkSize uint32,
	startChunk, endChunk int, out chan<- ChunkResult) {

	for i := startChunk; i < endChunk; i++ {
		offset := int64(i) * int64(chunkSize)
		end := offset + int64(chunkSize)
		if end > fileSize {
			end = fileSize
		}

		slice := data[offset:end]
		
		// Grab RAM from the pool, NO DISK WRITES!
		bufPtr := ChunkPool.Get().(*[]byte)
		buf := (*bufPtr)[:int(end-offset)]
		copy(buf, slice)
		
		chunkHash := sha256.Sum256(buf)

		out <- ChunkResult{
			Chunk: Chunk{
				ID:     uint32(i),
				Offset: offset,
				Size:   uint32(end - offset),
				Hash:   chunkHash,
				State:  ChunkPending,
			},
			Data:      buf,
			BufferPtr: bufPtr,
		}
	}
}

// AutoChunkSize picks optimal chunk size based on file size.
func AutoChunkSize(filePath string) uint32 {
	info, err := os.Stat(filePath)
	if err != nil {
		return DefaultChunkSize
	}
	size := info.Size()
	switch {
	case size < 10<<20: // <10MB
		return 512 << 10 // 512KB
	case size < 100<<20: // <100MB
		return 1 << 20 // 1MB
	case size < 1<<30: // <1GB
		return 2 << 20 // 2MB
	default:
		return 4 << 20 // 4MB
	}
}

// AutoWorkers returns optimal worker count.
func AutoWorkers() int {
	n := runtime.NumCPU()
	if n > 16 {
		return 16
	}
	if n < 2 {
		return 2
	}
	return n
}
