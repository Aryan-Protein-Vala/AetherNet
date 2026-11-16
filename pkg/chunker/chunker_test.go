package chunker

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
)

func TestChunkFilePipelined(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "sample.bin")

	// 2.5 MB payload so MinChunkSize (1 MB) splits into 3 chunks
	payload := bytes.Repeat([]byte("0123456789abcdef"), 160*1024)
	if err := os.WriteFile(srcPath, payload, 0o600); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	pipe, err := ChunkFilePipelined(srcPath, MinChunkSize)
	if err != nil {
		t.Fatalf("ChunkFilePipelined failed: %v", err)
	}

	if pipe.TotalChunks != 3 {
		t.Fatalf("expected 3 chunks, got %d", pipe.TotalChunks)
	}

	var reassembled []byte
	var count int
	for res := range pipe.ChunkCh {
		if res.Err != nil {
			t.Fatalf("chunk %d error: %v", count, res.Err)
		}
		wantHash := sha256.Sum256(res.Data)
		if res.Chunk.Hash != wantHash {
			t.Fatalf("chunk %d hash mismatch", res.Chunk.ID)
		}
		reassembled = append(reassembled, res.Data...)
		if res.BufferPtr != nil {
			ChunkPool.Put(res.BufferPtr)
		}
		count++
	}

	if count != pipe.TotalChunks {
		t.Fatalf("expected %d chunks on channel, got %d", pipe.TotalChunks, count)
	}
	if !bytes.Equal(reassembled, payload) {
		t.Fatal("reassembled bytes do not match original payload")
	}
}
