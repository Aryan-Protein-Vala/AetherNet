// Package network implements Aether's parallel transfer engine.
//
// Architecture:
//   - Upload accepts a pipelined chunk channel (chunks arrive as they're split)
//   - Workers start uploading immediately — no waiting for chunking to finish
//   - Jobs dispatched via unbuffered channel (back-pressure)
//   - Results flow through buffered results channel
//   - Progress bar renders real-time CLI feedback
//
// Each worker streams a cached chunk file directly into an HTTP POST
// request body. Chunk metadata passed via X-Aether-* headers.
package network

import (
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
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

// Shared HTTP client with connection pooling and sensible timeouts.
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
		DisableCompression: true,
		WriteBufferSize:    256 << 10,
		ReadBufferSize:     64 << 10,
	},
}

// UploadJob represents a single chunk upload task.
type UploadJob struct {
	Chunk     chunker.Chunk
	ChunkPath string
	FileID    string
}

// UploadResult is sent back by a worker after processing a job.
type UploadResult struct {
	ChunkID   uint32
	Success   bool
	BytesSent uint32
	Err       error
	Duration  time.Duration
}

// TransferStats holds aggregate metrics after all chunks have been processed.
type TransferStats struct {
	TotalChunks   int
	SuccessCount  int
	FailCount     int
	TotalBytes    int64
	TotalDuration time.Duration
}

// ──────────────────────────────────────────────────────────────────────
// UploadPipelined — reads from the chunker channel and uploads in parallel
// ──────────────────────────────────────────────────────────────────────

// UploadPipelined consumes chunks from a PipelineInfo channel and
// uploads them in parallel via HTTP POST. Uploads begin as soon as
// the first chunk is produced — no waiting for the full file to be split.
func UploadPipelined(pipe *chunker.PipelineInfo, targetURL string, numWorkers int) (*TransferStats, error) {
	if numWorkers <= 0 {
		numWorkers = DefaultWorkers
	}

	targetURL = strings.TrimRight(targetURL, "/")
	uploadURL := targetURL + "/upload"
	totalChunks := pipe.TotalChunks

	// ── Channels ─────────────────────────────────────────────────────
	jobs := make(chan UploadJob)
	results := make(chan UploadResult, totalChunks)

	// ── Progress bar ─────────────────────────────────────────────────
	bar := progressbar.NewOptions(totalChunks,
		progressbar.OptionSetDescription("  ⚡ Transferring"),
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
				results <- processUpload(job, uploadURL)
			}
		}()
	}

	// ── Feed from pipeline channel → job channel ─────────────────────
	// This goroutine bridges the chunker's output to the worker pool.
	var pipeErr error
	go func() {
		for cr := range pipe.ChunkCh {
			if cr.Err != nil {
				pipeErr = cr.Err
				break
			}
			jobs <- UploadJob{
				Chunk:     cr.Chunk,
				ChunkPath: cr.ChunkPath,
				FileID:    pipe.FileIDHex,
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

	if pipeErr != nil {
		return stats, fmt.Errorf("chunking pipeline error: %w", pipeErr)
	}

	return stats, nil
}

// Upload is the legacy batch API. It takes a completed manifest and uploads.
func Upload(manifest *chunker.FileManifest, cacheDir string, targetURL string, numWorkers int) (*TransferStats, error) {
	if numWorkers <= 0 {
		numWorkers = DefaultWorkers
	}

	targetURL = strings.TrimRight(targetURL, "/")
	uploadURL := targetURL + "/upload"

	fileIDHex := hex.EncodeToString(manifest.FileID[:])
	totalChunks := len(manifest.Chunks)

	jobs := make(chan UploadJob)
	results := make(chan UploadResult, totalChunks)

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

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				results <- processUpload(job, uploadURL)
			}
		}()
	}

	go func() {
		for _, c := range manifest.Chunks {
			chunkPath := fmt.Sprintf("%s/%s/%06d.chunk", cacheDir, fileIDHex, c.ID)
			jobs <- UploadJob{Chunk: c, ChunkPath: chunkPath, FileID: fileIDHex}
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

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
// Worker logic — with retry + exponential backoff
// ──────────────────────────────────────────────────────────────────────

const (
	maxRetries     = 3
	baseBackoff    = 100 * time.Millisecond
)

// processUpload attempts to upload a chunk with retry and exponential backoff.
// Each retry re-opens the file to reset the io.Reader position.
func processUpload(job UploadJob, uploadURL string) UploadResult {
	start := time.Now()
	result := UploadResult{ChunkID: job.Chunk.ID}

	var lastErr error
	backoff := baseBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2 // exponential: 100ms -> 200ms -> 400ms
		}

		lastErr = doUpload(job, uploadURL, &result)
		if lastErr == nil {
			result.Success = true
			result.Duration = time.Since(start)
			return result
		}
	}

	result.Err = fmt.Errorf("chunk %d failed after %d retries: %w",
		job.Chunk.ID, maxRetries, lastErr)
	result.Duration = time.Since(start)
	return result
}

// doUpload performs a single upload attempt. Returns nil on success.
func doUpload(job UploadJob, uploadURL string, result *UploadResult) error {
	f, err := os.Open(job.ChunkPath)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, uploadURL, f)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}

	req.ContentLength = info.Size()
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Aether-File-ID", job.FileID)
	req.Header.Set("X-Aether-Chunk-ID", strconv.Itoa(int(job.Chunk.ID)))
	req.Header.Set("X-Aether-Chunk-Hash", hex.EncodeToString(job.Chunk.Hash[:]))

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rejected: HTTP %d", resp.StatusCode)
	}

	result.BytesSent = uint32(info.Size())
	return nil
}
