// Package receiver implements Aether's relay server.
//
// The relay is a dumb, fast chunk storage node. It accepts uploaded
// chunks via POST /upload, stores manifests, and serves both back
// via GET endpoints so a recipient can download and reassemble locally.
//
// Endpoints:
//   - POST /upload     — accept a streamed chunk (SHA-256 verified)
//   - POST /manifest   — store file manifest JSON
//   - GET  /manifest   — retrieve manifest by file ID
//   - GET  /chunk      — stream a stored chunk back to the requester
//   - GET  /health     — liveness probe
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
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
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

// Server is the Aether relay.
type Server struct {
	Port int
	mux  *http.ServeMux
	hub  *SignalingHub
}

// New creates a new relay Server.
func New(port int) *Server {
	s := &Server{
		Port: port,
		hub:  NewSignalingHub(),
	}
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/upload", s.handleUpload)
	s.mux.HandleFunc("/manifest", s.handleManifest)
	s.mux.HandleFunc("/chunk", s.handleGetChunk)
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/download-cli", s.handleDownloadCLI)
	s.mux.HandleFunc("/ws", s.hub.HandleWS)
	return s
}

// Start begins listening. Blocks until shut down.
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.Port)
	printBanner(s.Port)

	h2s := &http2.Server{
		MaxConcurrentStreams: 500,
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           h2c.NewHandler(s.mux, h2s),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      120 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}

	return server.ListenAndServe()
}

// ──────────────────────────────────────────────────────────────────────
// GET /download-cli — serve the aether binary
// ──────────────────────────────────────────────────────────────────────

func (s *Server) handleDownloadCLI(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "bin/aether")
}

// ──────────────────────────────────────────────────────────────────────
// POST /health
// ──────────────────────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","version":"0.3.0-alpha","role":"relay"}`)
}

// ──────────────────────────────────────────────────────────────────────
// POST /upload — accept a chunk (unchanged from before)
// ──────────────────────────────────────────────────────────────────────

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
	fmt.Fprintf(w, `{"status":"ok","chunk_id":%d,"bytes":%d}`, chunkID, bytesWritten)
}

// ──────────────────────────────────────────────────────────────────────
// POST /manifest — store manifest, GET /manifest?id= — retrieve it
// ──────────────────────────────────────────────────────────────────────

func (s *Server) handleManifest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.storeManifest(w, r)
	case http.MethodGet:
		s.getManifest(w, r)
	default:
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

func (s *Server) storeManifest(w http.ResponseWriter, r *http.Request) {
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
		http.Error(w, fmt.Sprintf(`{"error":"create: %s"}`, err), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	n, err := io.Copy(f, r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"write: %s"}`, err), http.StatusInternalServerError)
		return
	}

	short := fileID
	if len(short) > 12 {
		short = short[:12] + "…"
	}
	fmt.Printf("  %s manifest stored  file %s  %d bytes\n", green("✓"), dim(short), n)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","bytes":%d}`, n)
}

func (s *Server) getManifest(w http.ResponseWriter, r *http.Request) {
	fileID := r.URL.Query().Get("id")
	if fileID == "" {
		http.Error(w, `{"error":"missing ?id= parameter"}`, http.StatusBadRequest)
		return
	}

	manifestPath := filepath.Join(receivedDir, fileID, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, `{"error":"manifest not found"}`, http.StatusNotFound)
		} else {
			http.Error(w, fmt.Sprintf(`{"error":"read: %s"}`, err), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// ──────────────────────────────────────────────────────────────────────
// GET /chunk?id=<fileID>&chunk=<chunkID> — stream a chunk back
// ──────────────────────────────────────────────────────────────────────

func (s *Server) handleGetChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	fileID := r.URL.Query().Get("id")
	chunkIDStr := r.URL.Query().Get("chunk")

	if fileID == "" || chunkIDStr == "" {
		http.Error(w, `{"error":"missing ?id= or ?chunk= parameter"}`, http.StatusBadRequest)
		return
	}

	chunkID, err := strconv.Atoi(chunkIDStr)
	if err != nil {
		http.Error(w, `{"error":"invalid chunk parameter"}`, http.StatusBadRequest)
		return
	}

	chunkPath := filepath.Join(receivedDir, fileID, fmt.Sprintf("%06d.chunk", chunkID))
	f, err := os.Open(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, `{"error":"chunk not found"}`, http.StatusNotFound)
		} else {
			http.Error(w, fmt.Sprintf(`{"error":"open: %s"}`, err), http.StatusInternalServerError)
		}
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"stat: %s"}`, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.Header().Set("X-Aether-Chunk-ID", chunkIDStr)
	io.Copy(w, f)
}

// ──────────────────────────────────────────────────────────────────────
// I/O helpers
// ──────────────────────────────────────────────────────────────────────

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
// Logging
// ──────────────────────────────────────────────────────────────────────

func printBanner(port int) {
	fmt.Printf("\n  %s  %s %s\n", cyan("▼"), bold("Aether Relay"), dim("v0.3.0-alpha"))
	fmt.Printf("  %s\n\n", dim("Public chunk storage relay"))
	fmt.Printf("  %s   %s\n", dim("Upload"), fmt.Sprintf("POST http://localhost:%d/upload", port))
	fmt.Printf("  %s %s\n", dim("Manifest"), fmt.Sprintf("GET  http://localhost:%d/manifest?id=<fileID>", port))
	fmt.Printf("  %s    %s\n", dim("Chunk"), fmt.Sprintf("GET  http://localhost:%d/chunk?id=<fileID>&chunk=<n>", port))
	fmt.Printf("  %s   %s\n", dim("Health"), fmt.Sprintf("GET  http://localhost:%d/health", port))
	fmt.Printf("  %s     %s\n", dim("Hash"), "SHA-256 (hardware-accelerated)")
	fmt.Printf("  %s  %s\n\n", dim("Storage"), receivedDir)
	fmt.Printf("  %s\n\n", dim("────────────────────────────────────────"))
}

func logChunk(chunkID int, fileID string, ok bool, bytes int64) {
	status := green("▲")
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
