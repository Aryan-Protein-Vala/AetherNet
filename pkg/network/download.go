package network

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
)

// RelayManifest is the JSON structure returned by GET /manifest.
type RelayManifest struct {
	FileName    string `json:"file_name"`
	FileSize    int64  `json:"file_size"`
	TotalChunks int    `json:"total_chunks"`
	ChunkSize   uint32 `json:"chunk_size"`
}

// DownloadResult is sent back by a download worker.
type DownloadResult struct {
	ChunkID   int
	Success   bool
	BytesRecv int64
	Err       error
	Duration  time.Duration
}

// DownloadStats holds aggregate download metrics.
type DownloadStats struct {
	TotalChunks  int
	SuccessCount int
	FailCount    int
	TotalBytes   int64
}

// DownloadPipelined fetches a file from a relay server.
// 1. GET /manifest?id=<fileID> to learn about the file
// 2. Spawn worker pool to concurrently GET /chunk?id=<fileID>&chunk=<n>
// 3. Verify SHA-256 hash of each chunk against the manifest hash header
// 4. Store verified chunks in .aether_cache/<fileID>/
//
// Returns the manifest and download stats.
func DownloadPipelined(fileID string, relayURL string, numWorkers int) (*RelayManifest, *DownloadStats, error) {
	if numWorkers <= 0 {
		numWorkers = DefaultWorkers
	}

	relayURL = trimSlash(relayURL)

	// ── Fetch manifest ───────────────────────────────────────────────
	manifestURL := fmt.Sprintf("%s/manifest?id=%s", relayURL, fileID)
	resp, err := httpClient.Get(manifestURL)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch manifest: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, nil, fmt.Errorf("manifest error (HTTP %d): %s", resp.StatusCode, body)
	}

	var manifest RelayManifest
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		return nil, nil, fmt.Errorf("decode manifest: %w", err)
	}

	// ── Prepare cache directory ──────────────────────────────────────
	cacheDir := filepath.Join(".aether_cache", fileID)
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create cache dir: %w", err)
	}

	totalChunks := manifest.TotalChunks

	// ── Channels ─────────────────────────────────────────────────────
	jobs := make(chan int) // chunk IDs
	results := make(chan DownloadResult, totalChunks)

	// ── Progress bar ─────────────────────────────────────────────────
	bar := progressbar.NewOptions(totalChunks,
		progressbar.OptionSetDescription("  ⚡ Downloading"),
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

	// ── Spawn download workers ───────────────────────────────────────
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunkID := range jobs {
				results <- downloadChunk(fileID, chunkID, relayURL, cacheDir)
			}
		}()
	}

	// ── Dispatch chunk download jobs ─────────────────────────────────
	go func() {
		for i := 0; i < totalChunks; i++ {
			jobs <- i
		}
		close(jobs)
	}()

	// ── Close results when workers finish ────────────────────────────
	go func() {
		wg.Wait()
		close(results)
	}()

	// ── Collect results ──────────────────────────────────────────────
	stats := &DownloadStats{TotalChunks: totalChunks}
	for res := range results {
		if res.Success {
			stats.SuccessCount++
			stats.TotalBytes += res.BytesRecv
		} else {
			stats.FailCount++
		}
		_ = bar.Add(1)
	}

	return &manifest, stats, nil
}

// downloadChunk downloads a single chunk with retry + exponential backoff.
func downloadChunk(fileID string, chunkID int, relayURL string, cacheDir string) DownloadResult {
	start := time.Now()
	result := DownloadResult{ChunkID: chunkID}

	var lastErr error
	backoff := baseBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
		}

		lastErr = doDownload(fileID, chunkID, relayURL, cacheDir, &result)
		if lastErr == nil {
			result.Success = true
			result.Duration = time.Since(start)
			return result
		}
	}

	result.Err = fmt.Errorf("chunk %d failed after %d retries: %w",
		chunkID, maxRetries, lastErr)
	result.Duration = time.Since(start)
	return result
}

// doDownload performs a single chunk download attempt.
func doDownload(fileID string, chunkID int, relayURL string, cacheDir string, result *DownloadResult) error {
	chunkURL := fmt.Sprintf("%s/chunk?id=%s&chunk=%d", relayURL, fileID, chunkID)

	resp, err := httpClient.Get(chunkURL)
	if err != nil {
		return fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	// Stream to disk while computing SHA-256
	chunkPath := filepath.Join(cacheDir, fmt.Sprintf("%06d.chunk", chunkID))
	f, err := os.Create(chunkPath)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer f.Close()

	hasher := sha256.New()
	w := io.MultiWriter(f, hasher)

	n, err := io.Copy(w, resp.Body)
	if err != nil {
		os.Remove(chunkPath)
		return fmt.Errorf("write: %w", err)
	}

	// Verify hash from response header if present
	expectedHash := resp.Header.Get("X-Aether-Chunk-Hash")
	if expectedHash != "" {
		computedHex := hex.EncodeToString(hasher.Sum(nil))
		if computedHex != expectedHash {
			os.Remove(chunkPath)
			return fmt.Errorf("hash mismatch")
		}
	}

	result.BytesRecv = n
	return nil
}

func trimSlash(url string) string {
	for len(url) > 0 && url[len(url)-1] == '/' {
		url = url[:len(url)-1]
	}
	return url
}
