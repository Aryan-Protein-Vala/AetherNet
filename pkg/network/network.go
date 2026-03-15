// Package network implements Aether's parallel transfer engine.
//
// Architecture:
//   - A WorkerPool fans out chunk uploads across N goroutines
//   - Jobs are dispatched via an unbuffered channel (back-pressure by design)
//   - Results flow back through a buffered results channel
//   - A progress bar renders real-time CLI feedback
//
// In Phase 1, the "upload" is a mock copy to /tmp/aether_mock_server.
// The interface is designed so swapping in TCP/QUIC is a one-file change.
package network

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"

	"github.com/Aryan-Protein-Vala/AetherNet/pkg/chunker"
)

const (
	// DefaultWorkers is the number of concurrent upload goroutines.
	DefaultWorkers = 5

	// MockServerDir simulates a remote destination.
	MockServerDir = "/tmp/aether_mock_server"
)

// ──────────────────────────────────────────────────────────────────────
// Job / Result types
// ──────────────────────────────────────────────────────────────────────

// UploadJob represents a single chunk upload task pushed to workers.
type UploadJob struct {
	Chunk     chunker.Chunk // chunk metadata
	ChunkPath string        // path to the cached chunk file on disk
}

// UploadResult is sent back by a worker after processing a job.
type UploadResult struct {
	ChunkID   uint32
	Success   bool
	BytesSent uint32
	Err       error
	Duration  time.Duration
}

// ──────────────────────────────────────────────────────────────────────
// TransferStats — final summary returned to the caller
// ──────────────────────────────────────────────────────────────────────

// TransferStats holds aggregate metrics after all chunks have been processed.
type TransferStats struct {
	TotalChunks   int
	SuccessCount  int
	FailCount     int
	TotalBytes    int64
	TotalDuration time.Duration
}

// ──────────────────────────────────────────────────────────────────────
// WorkerPool
// ──────────────────────────────────────────────────────────────────────

// Upload fans out chunk uploads across `numWorkers` goroutines.
// It reads cached chunk files from cacheDir, copies them to the mock
// destination, verifies SHA-256 integrity on the receiving side,
// and returns aggregate transfer stats.
//
// The progress bar updates in real time as each chunk completes.
func Upload(manifest *chunker.FileManifest, cacheDir string, numWorkers int) (*TransferStats, error) {
	if numWorkers <= 0 {
		numWorkers = DefaultWorkers
	}

	// ── Ensure destination exists ────────────────────────────────────
	destDir := filepath.Join(MockServerDir, hex.EncodeToString(manifest.FileID[:]))
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return nil, fmt.Errorf("create mock server dir: %w", err)
	}

	totalChunks := len(manifest.Chunks)

	// ── Channels ─────────────────────────────────────────────────────
	jobs := make(chan UploadJob)              // unbuffered → back-pressure
	results := make(chan UploadResult, totalChunks) // buffered → non-blocking sends

	// ── Progress bar ─────────────────────────────────────────────────
	bar := progressbar.NewOptions(totalChunks,
		progressbar.OptionSetDescription("  ⚡ Uploading"),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "█",
			SaucerHead:    "▓",
			SaucerPadding: "░",
			BarStart:      "│",
			BarEnd:        "│",
		}),
		progressbar.OptionShowCount(),
		progressbar.OptionShowBytes(false),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
		progressbar.OptionOnCompletion(func() {
			fmt.Println()
		}),
	)

	// ── Spawn workers ────────────────────────────────────────────────
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				result := processUpload(job, destDir)
				results <- result
			}
		}()
	}

	// ── Dispatch jobs ────────────────────────────────────────────────
	go func() {
		for _, c := range manifest.Chunks {
			chunkPath := filepath.Join(cacheDir,
				hex.EncodeToString(manifest.FileID[:]),
				fmt.Sprintf("%06d.chunk", c.ID),
			)
			jobs <- UploadJob{Chunk: c, ChunkPath: chunkPath}
		}
		close(jobs) // signal workers to exit
	}()

	// ── Close results channel once all workers finish ─────────────────
	go func() {
		wg.Wait()
		close(results)
	}()

	// ── Collect results ──────────────────────────────────────────────
	stats := &TransferStats{TotalChunks: totalChunks}

	for res := range results {
		if res.Success {
			stats.SuccessCount++
			stats.TotalBytes += int64(res.BytesSent)
		} else {
			stats.FailCount++
		}
		stats.TotalDuration += res.Duration
		_ = bar.Add(1)
	}

	return stats, nil
}

// ──────────────────────────────────────────────────────────────────────
// Worker logic
// ──────────────────────────────────────────────────────────────────────

// processUpload copies a cached chunk file to the mock destination
// and verifies SHA-256 integrity on the "server" side.
func processUpload(job UploadJob, destDir string) UploadResult {
	start := time.Now()

	result := UploadResult{ChunkID: job.Chunk.ID}

	// Open source chunk
	src, err := os.Open(job.ChunkPath)
	if err != nil {
		result.Err = fmt.Errorf("open chunk %d: %w", job.Chunk.ID, err)
		result.Duration = time.Since(start)
		return result
	}
	defer src.Close()

	// Create destination file
	destPath := filepath.Join(destDir, fmt.Sprintf("%06d.chunk", job.Chunk.ID))
	dst, err := os.Create(destPath)
	if err != nil {
		result.Err = fmt.Errorf("create dest chunk %d: %w", job.Chunk.ID, err)
		result.Duration = time.Since(start)
		return result
	}

	// Copy and hash simultaneously — single pass on the reader
	hasher := sha256.New()
	writer := io.MultiWriter(dst, hasher)

	n, err := io.Copy(writer, src)
	dst.Close() // close before integrity check

	if err != nil {
		result.Err = fmt.Errorf("copy chunk %d: %w", job.Chunk.ID, err)
		result.Duration = time.Since(start)
		return result
	}

	// ── Verify integrity ─────────────────────────────────────────────
	var receivedHash [32]byte
	copy(receivedHash[:], hasher.Sum(nil))

	if receivedHash != job.Chunk.Hash {
		result.Err = fmt.Errorf("chunk %d integrity mismatch: expected %x, got %x",
			job.Chunk.ID, job.Chunk.Hash[:8], receivedHash[:8])
		result.Duration = time.Since(start)
		return result
	}

	result.Success = true
	result.BytesSent = uint32(n)
	result.Duration = time.Since(start)
	return result
}
