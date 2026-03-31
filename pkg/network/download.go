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

	aecrypto "github.com/Aryan-Protein-Vala/AetherNet/pkg/crypto"
)

// RelayManifest is the JSON structure returned by GET /manifest.
type RelayManifest struct {
	FileID      string `json:"file_id"`
	FileName    string `json:"file_name"`
	FileSize    int64  `json:"file_size"`
	TotalChunks int    `json:"total_chunks"`
	ChunkSize   uint32 `json:"chunk_size"`
	Compressed  bool   `json:"compressed"`
	Encrypted   bool   `json:"encrypted"`
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
// 1. GET /manifest to learn about the file
// 2. Spawn worker pool to concurrently GET /chunk
// 3. Verify SHA-256 integrity of each downloaded chunk
// 4. If opts specify decrypt/decompress, reverse the transforms
// 5. Store final chunks in .aether_cache/<fileID>/
func DownloadPipelined(fileID string, relayURL string, numWorkers int, opts *TransferOptions) (*RelayManifest, *DownloadStats, error) {
	if numWorkers <= 0 {
		numWorkers = DefaultWorkers
	}
	if opts == nil {
		opts = &TransferOptions{}
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

	// Auto-configure transform options from manifest flags
	// Only decompress/decrypt if the sender actually used those transforms
	opts.Compress = manifest.Compressed
	opts.Encrypt = manifest.Encrypted && opts.Encrypt // need key from user

	// ── Prepare cache directory ──────────────────────────────────────
	cacheDir := filepath.Join(".aether_cache", fileID)
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create cache dir: %w", err)
	}

	totalChunks := manifest.TotalChunks

	// ── Channels ─────────────────────────────────────────────────────
	jobs := make(chan int)
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
				results <- downloadChunk(fileID, chunkID, relayURL, cacheDir, opts)
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
func downloadChunk(fileID string, chunkID int, relayURL string, cacheDir string, opts *TransferOptions) DownloadResult {
	start := time.Now()
	result := DownloadResult{ChunkID: chunkID}

	var lastErr error
	backoff := baseBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
		}

		lastErr = doDownload(fileID, chunkID, relayURL, cacheDir, &result, opts)
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
// Pipeline: download → verify hash → decrypt → decompress → write to disk
func doDownload(fileID string, chunkID int, relayURL string, cacheDir string, result *DownloadResult, opts *TransferOptions) error {
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

	// Read entire chunk into memory (needed for decrypt/decompress)
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	// Verify SHA-256 of the on-wire data (before any transforms)
	expectedHash := resp.Header.Get("X-Aether-Chunk-Hash")
	if expectedHash != "" {
		computedHash := sha256.Sum256(data)
		computedHex := hex.EncodeToString(computedHash[:])
		if computedHex != expectedHash {
			return fmt.Errorf("hash mismatch (on-wire)")
		}
	}

	// Reverse transforms: decrypt first, then decompress
	// (upload does: compress → encrypt, so download does: decrypt → decompress)
	if opts.Encrypt {
		decrypted, err := aecrypto.Decrypt(data, opts.EncryptKey)
		if err != nil {
			return fmt.Errorf("decrypt: %w", err)
		}
		data = decrypted
	}

	if opts.Compress {
		decompressed, err := aecrypto.DecompressLZ4(data)
		if err != nil {
			return fmt.Errorf("decompress: %w", err)
		}
		data = decompressed
	}

	// Write final plaintext chunk to disk
	chunkPath := filepath.Join(cacheDir, fmt.Sprintf("%06d.chunk", chunkID))
	if err := os.WriteFile(chunkPath, data, 0o644); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	result.BytesRecv = int64(len(data))
	return nil
}

func trimSlash(url string) string {
	for len(url) > 0 && url[len(url)-1] == '/' {
		url = url[:len(url)-1]
	}
	return url
}
