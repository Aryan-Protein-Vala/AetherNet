// Package network implements Aether's parallel transfer engine.
//
// Architecture:
//   - A WorkerPool fans out chunk uploads across N goroutines
//   - Jobs are dispatched via an unbuffered channel (back-pressure)
//   - Results flow back through a buffered results channel
//   - A progress bar renders real-time CLI feedback
//
// Each worker streams a cached chunk file directly into an HTTP POST
// request body to the target receiver (no intermediate buffer).
// Chunk metadata is passed via X-Aether-* headers.
package network

import (
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"

	"github.com/Aryan-Protein-Vala/AetherNet/pkg/chunker"
)

const (
	// DefaultWorkers is the number of concurrent upload goroutines.
	DefaultWorkers = 5
)

// ──────────────────────────────────────────────────────────────────────
// Shared HTTP client — connection pooling + sensible timeouts
// ──────────────────────────────────────────────────────────────────────

var httpClient = &http.Client{
	Timeout: 60 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		DisableCompression:  true, // chunks are binary, compression wastes CPU
		WriteBufferSize:     256 << 10, // 256 KB write buffer
		ReadBufferSize:      64 << 10,  // 64 KB read buffer
	},
}

// ──────────────────────────────────────────────────────────────────────
// Job / Result types
// ──────────────────────────────────────────────────────────────────────

// UploadJob represents a single chunk upload task pushed to workers.
type UploadJob struct {
	Chunk     chunker.Chunk
	ChunkPath string
	FileID    string // hex-encoded file ID
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
// TransferStats
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
// Upload — the main fan-out orchestrator
// ──────────────────────────────────────────────────────────────────────

// Upload fans out chunk uploads across `numWorkers` goroutines,
// each streaming cached chunk files via HTTP POST to targetURL/upload.
func Upload(manifest *chunker.FileManifest, cacheDir string, targetURL string, numWorkers int) (*TransferStats, error) {
	if numWorkers <= 0 {
		numWorkers = DefaultWorkers
	}

	// Normalise target URL
	targetURL = strings.TrimRight(targetURL, "/")
	uploadURL := targetURL + "/upload"

	fileIDHex := hex.EncodeToString(manifest.FileID[:])
	totalChunks := len(manifest.Chunks)

	// ── Channels ─────────────────────────────────────────────────────
	jobs := make(chan UploadJob)
	results := make(chan UploadResult, totalChunks)

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
				result := processUpload(job, uploadURL)
				results <- result
			}
		}()
	}

	// ── Dispatch jobs ────────────────────────────────────────────────
	go func() {
		for _, c := range manifest.Chunks {
			chunkPath := filepath.Join(cacheDir,
				fileIDHex,
				fmt.Sprintf("%06d.chunk", c.ID),
			)
			jobs <- UploadJob{
				Chunk:     c,
				ChunkPath: chunkPath,
				FileID:    fileIDHex,
			}
		}
		close(jobs)
	}()

	// ── Close results after all workers finish ───────────────────────
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
// Worker logic — real HTTP POST
// ──────────────────────────────────────────────────────────────────────

// processUpload streams a cached chunk file directly into an HTTP POST
// request body. Chunk metadata is passed via custom headers:
//   - X-Aether-File-ID:    hex file ID
//   - X-Aether-Chunk-ID:   chunk index
//   - X-Aether-Chunk-Hash: hex SHA-256
func processUpload(job UploadJob, uploadURL string) UploadResult {
	start := time.Now()
	result := UploadResult{ChunkID: job.Chunk.ID}

	// Open cached chunk file
	f, err := os.Open(job.ChunkPath)
	if err != nil {
		result.Err = fmt.Errorf("open chunk %d: %w", job.Chunk.ID, err)
		result.Duration = time.Since(start)
		return result
	}
	defer f.Close()

	// Get file size for Content-Length (enables efficient transfer)
	info, err := f.Stat()
	if err != nil {
		result.Err = fmt.Errorf("stat chunk %d: %w", job.Chunk.ID, err)
		result.Duration = time.Since(start)
		return result
	}

	// Build HTTP request — stream file directly as request body
	req, err := http.NewRequest(http.MethodPost, uploadURL, f)
	if err != nil {
		result.Err = fmt.Errorf("create request for chunk %d: %w", job.Chunk.ID, err)
		result.Duration = time.Since(start)
		return result
	}

	req.ContentLength = info.Size()
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Aether-File-ID", job.FileID)
	req.Header.Set("X-Aether-Chunk-ID", strconv.Itoa(int(job.Chunk.ID)))
	req.Header.Set("X-Aether-Chunk-Hash", hex.EncodeToString(job.Chunk.Hash[:]))

	// Execute request
	resp, err := httpClient.Do(req)
	if err != nil {
		result.Err = fmt.Errorf("upload chunk %d: %w", job.Chunk.ID, err)
		result.Duration = time.Since(start)
		return result
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // drain body to allow connection reuse

	if resp.StatusCode != http.StatusOK {
		result.Err = fmt.Errorf("chunk %d rejected: HTTP %d", job.Chunk.ID, resp.StatusCode)
		result.Duration = time.Since(start)
		return result
	}

	result.Success = true
	result.BytesSent = uint32(info.Size())
	result.Duration = time.Since(start)
	return result
}
