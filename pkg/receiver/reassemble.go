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

// Reassemble reads all chunks from .aether_received/<fileID>/ and
// concatenates them in order to produce the original file.
func Reassemble(fileID string, outputDir string) (string, error) {
	return reassembleFrom(filepath.Join(receivedDir, fileID), outputDir)
}

// ReassembleFromCache reads chunks from .aether_cache/<fileID>/
// and reassembles them. Used by the client after downloading chunks.
func ReassembleFromCache(fileID string, outputDir string) (string, error) {
	return reassembleFrom(filepath.Join(".aether_cache", fileID), outputDir)
}

func reassembleFrom(srcDir string, outputDir string) (string, error) {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return "", fmt.Errorf("read chunk dir: %w", err)
	}

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
		return "", fmt.Errorf("no chunks found in %s", srcDir)
	}

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].id < chunks[j].id
	})

	// Try to read manifest for original filename
	outputName := fmt.Sprintf("aether_download_%s", filepath.Base(srcDir)[:12])
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

	for _, c := range chunks {
		f, err := os.Open(c.path)
		if err != nil {
			return "", fmt.Errorf("open chunk %d: %w", c.id, err)
		}
		_, err = io.Copy(out, f)
		f.Close()
		if err != nil {
			return "", fmt.Errorf("copy chunk %d: %w", c.id, err)
		}
	}

	return outPath, nil
}

// ListReceivedFiles returns file IDs in the received directory.
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
