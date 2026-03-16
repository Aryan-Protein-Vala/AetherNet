// Package chunker implements the file chunking engine for Aether.
// It splits files into hash-verified, resumable chunks for parallel transfer.
package chunker

import (
	"time"
)

// ChunkSize defines the target chunk size range.
const (
	MinChunkSize = 1 << 20  // 1 MB
	MaxChunkSize = 4 << 20  // 4 MB
	DefaultChunkSize = 2 << 20 // 2 MB
)

// ChunkState represents the transfer state of a single chunk.
type ChunkState uint8

const (
	ChunkPending    ChunkState = iota // Not yet transferred
	ChunkInFlight                     // Currently being sent
	ChunkCompleted                    // Successfully transferred & verified
	ChunkFailed                       // Transfer or verification failed
)

// Chunk represents a single piece of a file.
// Kept as a value-friendly struct (no pointers to internal fields)
// so slices of Chunk avoid per-element heap allocations.
type Chunk struct {
	// ID is a zero-based sequential index within the parent file.
	ID uint32 `json:"id"`

	// Offset is the byte offset from the start of the original file.
	Offset int64 `json:"offset"`

	// Size is the chunk payload size in bytes (≤ MaxChunkSize).
	Size uint32 `json:"size"`

	// Hash is the SHA-256 digest of the chunk data (32 bytes, stack-allocated).
	Hash [32]byte `json:"hash"`

	// State tracks whether this chunk has been sent / verified.
	State ChunkState `json:"state"`
}

// FileManifest is the metadata envelope for a chunked file.
// It is serialised once at the start of a transfer and used by
// both sender and receiver for integrity validation and resume logic.
type FileManifest struct {
	// FileID is a globally unique identifier (UUIDv7 recommended for time-ordering).
	FileID [16]byte `json:"file_id"`

	// FileName is the original base name of the file.
	FileName string `json:"file_name"`

	// FileSize is the total size of the original file in bytes.
	FileSize int64 `json:"file_size"`

	// FileHash is the SHA-256 digest of the entire file.
	FileHash [32]byte `json:"file_hash"`

	// ChunkSize is the chunk size used for this manifest (bytes).
	ChunkSize uint32 `json:"chunk_size"`

	// Chunks is the ordered list of all chunks.
	// Pre-allocated to exact capacity to avoid re-slicing.
	Chunks []Chunk `json:"chunks"`

	// CreatedAt records when the manifest was generated (UTC).
	CreatedAt time.Time `json:"created_at"`
}

// TotalChunks returns the number of chunks in the manifest.
func (m *FileManifest) TotalChunks() int {
	return len(m.Chunks)
}

// CompletedChunks returns how many chunks have been verified.
func (m *FileManifest) CompletedChunks() int {
	n := 0
	for i := range m.Chunks {
		if m.Chunks[i].State == ChunkCompleted {
			n++
		}
	}
	return n
}

// IsComplete reports whether every chunk has been verified.
func (m *FileManifest) IsComplete() bool {
	return m.CompletedChunks() == m.TotalChunks()
}

// SessionState represents the high-level state of a transfer.
type SessionState uint8

const (
	SessionInitialising SessionState = iota
	SessionActive
	SessionPaused
	SessionCompleted
	SessionFailed
)

// TransferSession tracks an in-progress (or resumed) file transfer.
// A session references exactly one FileManifest and maintains
// transfer-level counters that are cheap to update atomically.
type TransferSession struct {
	// SessionID is a globally unique identifier for this transfer attempt.
	SessionID [16]byte `json:"session_id"`

	// Manifest is the file metadata and chunk list for this transfer.
	Manifest *FileManifest `json:"manifest"`

	// State is the overall transfer status.
	State SessionState `json:"state"`

	// BytesTransferred is a running total of verified bytes.
	BytesTransferred int64 `json:"bytes_transferred"`

	// StartedAt is when the transfer session began.
	StartedAt time.Time `json:"started_at"`

	// LastActivityAt is updated on every chunk completion for timeout detection.
	LastActivityAt time.Time `json:"last_activity_at"`

	// MaxParallelStreams caps the number of concurrent transfer goroutines.
	MaxParallelStreams uint16 `json:"max_parallel_streams"`

	// RetryLimit is the maximum number of retries per chunk before marking it failed.
	RetryLimit uint8 `json:"retry_limit"`
}

// Progress returns the transfer completion ratio in [0.0, 1.0].
func (s *TransferSession) Progress() float64 {
	if s.Manifest == nil || s.Manifest.FileSize == 0 {
		return 0
	}
	return float64(s.BytesTransferred) / float64(s.Manifest.FileSize)
}

// ElapsedTime returns time since the session started.
func (s *TransferSession) ElapsedTime() time.Duration {
	return time.Since(s.StartedAt)
}

// Throughput returns the average transfer speed in bytes/sec.
func (s *TransferSession) Throughput() float64 {
	elapsed := s.ElapsedTime().Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(s.BytesTransferred) / elapsed
}
