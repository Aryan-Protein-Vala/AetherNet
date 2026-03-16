package receiver

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Reassemble reads all chunks from a received file directory and
// concatenates them in order to produce the original file.
// It returns the path to the reassembled file.
func Reassemble(fileID string, outputDir string) (string, error) {
	srcDir := filepath.Join(receivedDir, fileID)

	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return "", fmt.Errorf("read chunk dir: %w", err)
	}

	// Collect and sort chunk files by ID
	type chunkFile struct {
		id   int
		path string
	}
	var chunks []chunkFile

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".chunk") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".chunk")
		id, err := strconv.Atoi(name)
		if err != nil {
			continue
		}
		chunks = append(chunks, chunkFile{id: id, path: filepath.Join(srcDir, e.Name())})
	}

	if len(chunks) == 0 {
		return "", fmt.Errorf("no chunks found for file %s", fileID)
	}

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].id < chunks[j].id
	})

	// Read manifest if available to get original filename
	outputName := fmt.Sprintf("aether_received_%s", fileID[:12])
	manifestPath := filepath.Join(srcDir, "manifest.json")
	if data, err := os.ReadFile(manifestPath); err == nil {
		var m struct {
			FileName string `json:"file_name"`
		}
		if json.Unmarshal(data, &m) == nil && m.FileName != "" {
			outputName = m.FileName
		}
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return "", fmt.Errorf("create output dir: %w", err)
	}

	outPath := filepath.Join(outputDir, outputName)
	out, err := os.Create(outPath)
	if err != nil {
		return "", fmt.Errorf("create output file: %w", err)
	}
	defer out.Close()

	var totalBytes int64
	for _, c := range chunks {
		f, err := os.Open(c.path)
		if err != nil {
			return "", fmt.Errorf("open chunk %d: %w", c.id, err)
		}
		n, err := io.Copy(out, f)
		f.Close()
		if err != nil {
			return "", fmt.Errorf("copy chunk %d: %w", c.id, err)
		}
		totalBytes += n
	}

	return outPath, nil
}

// ListReceivedFiles returns a list of file IDs that have chunks in
// the received directory.
func ListReceivedFiles() ([]string, error) {
	entries, err := os.ReadDir(receivedDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var fileIDs []string
	for _, e := range entries {
		if e.IsDir() {
			fileIDs = append(fileIDs, e.Name())
		}
	}
	return fileIDs, nil
}
