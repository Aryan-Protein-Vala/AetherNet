// Package receiver implements Aether's HTTP chunk receiver.
//
// It exposes a single POST /upload endpoint that accepts streamed chunk
// data, verifies SHA-256 integrity on-the-fly, and persists verified
// chunks to .aether_cache/<file_id>/.
//
// Designed for high concurrency: the Go HTTP server handles each
// request in its own goroutine, and I/O is fully streaming (no
// buffering the entire chunk body in memory).
package receiver

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
)

const (
	// receivedDir is the local directory for received chunks.
	receivedDir = ".aether_received"
)

// color presets for server logs
var (
	cyan  = color.New(color.FgCyan, color.Bold).SprintFunc()
	green = color.New(color.FgGreen, color.Bold).SprintFunc()
	red   = color.New(color.FgRed, color.Bold).SprintFunc()
	dim   = color.New(color.Faint).SprintFunc()
	bold  = color.New(color.Bold).SprintFunc()
)

// Server is the Aether chunk receiver.
type Server struct {
	Port int
	mux  *http.ServeMux
}

// New creates a new receiver Server on the given port.
func New(port int) *Server {
	s := &Server{Port: port}
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/upload", s.handleUpload)
	s.mux.HandleFunc("/health", s.handleHealth)
	return s
}

// Start begins listening. This blocks until the server is shut down.
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.Port)

	printBanner(s.Port)

	server := &http.Server{
		Addr:              addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16, // 64 KB
	}

	return server.ListenAndServe()
}

// ──────────────────────────────────────────────────────────────────────
// Handlers
// ──────────────────────────────────────────────────────────────────────

// handleHealth is a simple liveness probe.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"ok","version":"0.1.0-alpha"}`)
}

// handleUpload receives a single chunk via streamed POST body.
//
// Required headers:
//   - X-Aether-File-ID:    hex-encoded 16-byte file ID
//   - X-Aether-Chunk-ID:   integer chunk index
//   - X-Aether-Chunk-Hash: hex-encoded SHA-256 (64 chars)
//
// The body is streamed directly to disk while computing SHA-256.
// If the computed hash matches X-Aether-Chunk-Hash → 200 OK.
// Otherwise → 400 Bad Request.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// ── Parse headers ────────────────────────────────────────────────
	fileID := r.Header.Get("X-Aether-File-ID")
	chunkIDStr := r.Header.Get("X-Aether-Chunk-ID")
	expectedHash := r.Header.Get("X-Aether-Chunk-Hash")

	if fileID == "" || chunkIDStr == "" || expectedHash == "" {
		http.Error(w, `{"error":"missing required X-Aether-* headers"}`, http.StatusBadRequest)
		return
	}

	chunkID, err := strconv.Atoi(chunkIDStr)
	if err != nil {
		http.Error(w, `{"error":"invalid X-Aether-Chunk-ID"}`, http.StatusBadRequest)
		return
	}

	// ── Create destination directory ─────────────────────────────────
	destDir := filepath.Join(receivedDir, fileID)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"mkdir: %s"}`, err), http.StatusInternalServerError)
		return
	}

	// ── Stream body → disk + SHA-256 ─────────────────────────────────
	destPath := filepath.Join(destDir, fmt.Sprintf("%06d.chunk", chunkID))

	computedHash, bytesWritten, err := streamToDisk(destPath, r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"write: %s"}`, err), http.StatusInternalServerError)
		return
	}

	// ── Verify integrity ─────────────────────────────────────────────
	computedHex := hex.EncodeToString(computedHash[:])

	if !strings.EqualFold(computedHex, expectedHash) {
		// Delete the corrupted chunk
		os.Remove(destPath)

		logChunk(chunkID, fileID, false, bytesWritten)
		http.Error(w, fmt.Sprintf(
			`{"error":"hash mismatch","expected":"%s","got":"%s"}`,
			expectedHash, computedHex,
		), http.StatusBadRequest)
		return
	}

	logChunk(chunkID, fileID, true, bytesWritten)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"ok","chunk_id":%d,"bytes":%d}`, chunkID, bytesWritten)
}

// ──────────────────────────────────────────────────────────────────────
// I/O helpers
// ──────────────────────────────────────────────────────────────────────

// streamToDisk writes from reader to path while computing SHA-256.
// Returns the hash, bytes written, and any error.
func streamToDisk(path string, body io.Reader) ([32]byte, int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return [32]byte{}, 0, err
	}
	defer f.Close()

	var h hash.Hash = sha256.New()
	w := io.MultiWriter(f, h)

	n, err := io.Copy(w, body)
	if err != nil {
		return [32]byte{}, n, err
	}

	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, n, nil
}

// ──────────────────────────────────────────────────────────────────────
// Server log output
// ──────────────────────────────────────────────────────────────────────

func printBanner(port int) {
	fmt.Printf("\n  %s  %s %s\n", cyan("▼"), bold("Aether Receiver"), dim("v0.1.0-alpha"))
	fmt.Printf("  %s\n\n", dim("Listening for incoming chunks"))
	fmt.Printf("  %s  %s\n", dim("Endpoint"), fmt.Sprintf("http://localhost:%d/upload", port))
	fmt.Printf("  %s     %s\n", dim("Health"), fmt.Sprintf("http://localhost:%d/health", port))
	fmt.Printf("  %s    %s\n\n", dim("Storage"), receivedDir)
	fmt.Printf("  %s\n\n", dim("────────────────────────────────────────"))
}

func logChunk(chunkID int, fileID string, ok bool, bytes int64) {
	status := green("✓")
	if !ok {
		status = red("✗")
	}
	short := fileID
	if len(short) > 12 {
		short = short[:12] + "…"
	}
	fmt.Printf("  %s chunk %s  file %s  %s\n",
		status,
		bold(fmt.Sprintf("%04d", chunkID)),
		dim(short),
		dim(formatBytes(bytes)),
	)
}

func formatBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
	)
	switch {
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
