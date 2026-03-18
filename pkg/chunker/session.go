// Package chunker — session.go provides session persistence
// for resumable transfers. It serialises manifest + chunk states
// to disk so an interrupted transfer can be resumed.
package chunker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// SaveSession persists the current manifest state to disk.
// Called after chunking and periodically during upload.
func SaveSession(manifest *FileManifest) error {
	sessionDir := filepath.Join(cacheDir,
		fmt.Sprintf("%x", manifest.FileID))
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		return fmt.Errorf("create session dir: %w", err)
	}

	path := filepath.Join(sessionDir, "session.json")

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal session: %w", err)
	}

	return os.WriteFile(path, data, 0o644)
}

// LoadSession restores a manifest from a previous session.
// Returns nil if no session exists for the given file ID.
func LoadSession(fileIDHex string) (*FileManifest, error) {
	path := filepath.Join(cacheDir, fileIDHex, "session.json")

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read session: %w", err)
	}

	var manifest FileManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("unmarshal session: %w", err)
	}

	return &manifest, nil
}

// PendingChunks returns chunks that haven't been verified yet.
func PendingChunks(manifest *FileManifest) []Chunk {
	var pending []Chunk
	for _, c := range manifest.Chunks {
		if c.State != ChunkCompleted {
			pending = append(pending, c)
		}
	}
	return pending
}

// MarkCompleted flags a chunk as verified in the manifest.
func MarkCompleted(manifest *FileManifest, chunkID uint32) {
	for i := range manifest.Chunks {
		if manifest.Chunks[i].ID == chunkID {
			manifest.Chunks[i].State = ChunkCompleted
			return
		}
	}
}
