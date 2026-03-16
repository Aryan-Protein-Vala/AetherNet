// Package receiver implements Aether's HTTP chunk receiver.
//
// It exposes a POST /upload endpoint that accepts streamed chunk data,
// verifies SHA-256 integrity on-the-fly, and persists verified chunks
// to .aether_received/<file_id>/.
//
// Additional endpoints:
//   - POST /manifest  — stores file metadata for reassembly
//   - POST /reassemble — reconstructs original file from chunks
//   - GET  /health    — liveness probe
//
// Designed for high concurrency: the Go HTTP server handles each
// request in its own goroutine. I/O is fully streaming.
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
	receivedDir = ".aether_received"
)

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
	s.mux.HandleFunc("/manifest", s.handleManifest)
	s.mux.HandleFunc("/reassemble", s.handleReassemble)
	s.mux.HandleFunc("/health", s.handleHealth)
	return s
}

// Start begins listening. Blocks until shut down.
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.Port)
	printBanner(s.Port)

	server := &http.Server{
		Addr:              addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}

	return server.ListenAndServe()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"ok","version":"0.2.0-alpha"}`)
}

// handleUpload receives a single chunk via streamed POST body.
// Verifies SHA-256 integrity using X-Aether-Chunk-Hash header.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

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

	destDir := filepath.Join(receivedDir, fileID)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"mkdir: %s"}`, err), http.StatusInternalServerError)
		return
	}

	destPath := filepath.Join(destDir, fmt.Sprintf("%06d.chunk", chunkID))

	computedHash, bytesWritten, err := streamToDisk(destPath, r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"write: %s"}`, err), http.StatusInternalServerError)
		return
	}

	computedHex := hex.EncodeToString(computedHash[:])

	if !strings.EqualFold(computedHex, expectedHash) {
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

// handleManifest stores file manifest metadata for later reassembly.
func (s *Server) handleManifest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	fileID := r.Header.Get("X-Aether-File-ID")
	if fileID == "" {
		http.Error(w, `{"error":"missing X-Aether-File-ID"}`, http.StatusBadRequest)
		return
	}

	destDir := filepath.Join(receivedDir, fileID)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"mkdir: %s"}`, err), http.StatusInternalServerError)
		return
	}

	manifestPath := filepath.Join(destDir, "manifest.json")
	f, err := os.Create(manifestPath)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"create manifest: %s"}`, err), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	n, err := io.Copy(f, r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"write manifest: %s"}`, err), http.StatusInternalServerError)
		return
	}

	short := fileID
	if len(short) > 12 {
		short = short[:12] + "…"
	}
	fmt.Printf("  %s manifest saved for %s (%d bytes)\n", green("✓"), dim(short), n)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"ok","bytes":%d}`, n)
}

// handleReassemble reconstructs the original file from received chunks.
func (s *Server) handleReassemble(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	fileID := r.Header.Get("X-Aether-File-ID")
	outputDir := r.Header.Get("X-Aether-Output-Dir")
	if fileID == "" {
		http.Error(w, `{"error":"missing X-Aether-File-ID"}`, http.StatusBadRequest)
		return
	}
	if outputDir == "" {
		outputDir = "."
	}

	outPath, err := Reassemble(fileID, outputDir)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"reassemble: %s"}`, err), http.StatusInternalServerError)
		return
	}

	fmt.Printf("  %s file reassembled: %s\n", green("✓"), bold(outPath))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"ok","path":"%s"}`, outPath)
}

// streamToDisk writes from reader to path while computing SHA-256.
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

func printBanner(port int) {
	fmt.Printf("\n  %s  %s %s\n", cyan("▼"), bold("Aether Receiver"), dim("v0.2.0-alpha"))
	fmt.Printf("  %s\n\n", dim("Listening for incoming chunks"))
	fmt.Printf("  %s  %s\n", dim("Endpoint"), fmt.Sprintf("http://localhost:%d/upload", port))
	fmt.Printf("  %s     %s\n", dim("Health"), fmt.Sprintf("http://localhost:%d/health", port))
	fmt.Printf("  %s    %s\n", dim("Hash"), "SHA-256 (hardware-accelerated)")
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
		status, bold(fmt.Sprintf("%04d", chunkID)),
		dim(short), dim(formatBytes(bytes)),
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
