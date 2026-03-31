package network

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/schollz/progressbar/v3"

	"github.com/Aryan-Protein-Vala/AetherNet/pkg/chunker"
	aecrypto "github.com/Aryan-Protein-Vala/AetherNet/pkg/crypto"
)

const (
	DirectQUICPort = 4242
)

// ──────────────────────────────────────────────────────────────────────
// UploadDirectQUIC — send chunks directly to a peer (no relay)
// ──────────────────────────────────────────────────────────────────────

func UploadDirectQUIC(pipe *chunker.PipelineInfo, targetIP string, numWorkers int, opts *TransferOptions) (*TransferStats, error) {
	if opts == nil {
		opts = &TransferOptions{}
	}
	if numWorkers <= 0 {
		numWorkers = DefaultWorkers
	}

	targetAddr := fmt.Sprintf("%s:%d", targetIP, DirectQUICPort)

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"aether-p2p"},
	}

	log.Printf("[P2P] Dialing peer at %s ...", targetAddr)

	conn, err := quic.DialAddr(context.Background(), targetAddr, tlsConf, &quic.Config{
		MaxIdleTimeout:                120 * time.Second,
		Allow0RTT:                     true,
		DisablePathMTUDiscovery:       true,
		MaxIncomingStreams:             1000,
		InitialStreamReceiveWindow:    8 * 1024 * 1024, // 8MB
		InitialConnectionReceiveWindow: 32 * 1024 * 1024, // 32MB
	})
	if err != nil {
		return nil, fmt.Errorf("p2p quic dial %s: %w", targetAddr, err)
	}
	defer conn.CloseWithError(0, "done")

	log.Printf("[P2P] Connected to peer!")

	// Send manifest on stream 0
	manifestStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		return nil, fmt.Errorf("open manifest stream: %w", err)
	}

	manifestJSON, _ := json.Marshal(map[string]interface{}{
		"type":         "manifest",
		"file_id":      pipe.FileIDHex,
		"file_name":    pipe.FileName,
		"file_size":    pipe.FileSize,
		"total_chunks": pipe.TotalChunks,
		"chunk_size":   pipe.ChunkSize,
		"compressed":   opts.Compress,
		"encrypted":    opts.Encrypt,
	})
	manifestStream.Write([]byte{0}) // Opcode 0 = MANIFEST
	writeFrame(manifestStream, manifestJSON)
	manifestStream.Close()

	// Small delay to ensure receiver's accept loop is ready
	time.Sleep(100 * time.Millisecond)

	log.Printf("[P2P] Manifest sent (%d chunks)", pipe.TotalChunks)

	totalChunks := pipe.TotalChunks
	jobs := make(chan chunker.ChunkResult)
	results := make(chan UploadResult, totalChunks)

	bar := progressbar.NewOptions(totalChunks,
		progressbar.OptionSetDescription("  ⚡ P2P Direct Upload"),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer: "█", SaucerHead: "▓", SaucerPadding: "░",
			BarStart: "│", BarEnd: "│",
		}),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
		progressbar.OptionOnCompletion(func() { fmt.Println() }),
	)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for cr := range jobs {
				res := p2pUploadChunk(conn, cr, pipe.FileIDHex, opts)
				results <- res
			}
		}(w)
	}

	var pipeErr error
	go func() {
		for cr := range pipe.ChunkCh {
			if cr.Err != nil {
				pipeErr = cr.Err
				break
			}
			jobs <- cr
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	stats := &TransferStats{TotalChunks: totalChunks}
	for res := range results {
		if res.Success {
			stats.SuccessCount++
			stats.TotalBytes += int64(res.BytesSent)
		} else {
			stats.FailCount++
		}
		_ = bar.Add(1)
	}

	if pipeErr != nil {
		return stats, fmt.Errorf("pipeline: %w", pipeErr)
	}
	return stats, nil
}

// p2pUploadChunk sends a single chunk over a dedicated QUIC stream.
// The receiver reads the header+data, verifies the hash, and sends a 1-byte ACK.
func p2pUploadChunk(conn *quic.Conn, cr chunker.ChunkResult, fileID string, opts *TransferOptions) UploadResult {
	start := time.Now()
	result := UploadResult{ChunkID: cr.Chunk.ID}

	defer func() {
		if cr.BufferPtr != nil {
			chunker.ChunkPool.Put(cr.BufferPtr)
		}
	}()

	var data []byte
	if cr.Data != nil {
		data = cr.Data
	} else if cr.ChunkPath != "" {
		var err error
		data, err = os.ReadFile(cr.ChunkPath)
		if err != nil {
			result.Err = err
			return result
		}
	}

	if opts.Compress {
		if transformed, ok := aecrypto.CompressLZ4(data); ok {
			data = transformed
		}
	}
	if opts.Encrypt {
		enc, err := aecrypto.Encrypt(data, opts.EncryptKey)
		if err != nil {
			result.Err = err
			return result
		}
		data = enc
	}

	hash := sha256.Sum256(data)

	// Build the chunk frame: [type=1][fidLen][fid][chunkID_u32][hash_32][data]
	frame := &bytes.Buffer{}
	frame.WriteByte(1)
	frame.WriteByte(byte(len(fileID)))
	frame.WriteString(fileID)
	binary.Write(frame, binary.BigEndian, cr.Chunk.ID)
	frame.Write(hash[:])
	frame.Write(data)

	// Open a new bidirectional stream for this chunk
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		result.Err = fmt.Errorf("open stream: %w", err)
		return result
	}

	// Write entire frame at once
	if _, err := stream.Write(frame.Bytes()); err != nil {
		result.Err = fmt.Errorf("write chunk: %w", err)
		return result
	}

	// Close write direction — signals to receiver that data is complete
	stream.Close()

	// Wait for 1-byte ACK from receiver
	ack := make([]byte, 1)
	if _, err := io.ReadFull(stream, ack); err != nil {
		result.Err = fmt.Errorf("wait ack: %w", err)
		return result
	}
	if ack[0] != 1 {
		result.Err = fmt.Errorf("chunk %d rejected (hash mismatch)", cr.Chunk.ID)
		return result
	}

	result.Success = true
	result.BytesSent = uint32(len(data))
	result.Duration = time.Since(start)
	return result
}

// ──────────────────────────────────────────────────────────────────────
// ListenDirectQUIC — receive chunks directly from a peer (no relay)
// ──────────────────────────────────────────────────────────────────────

func ListenDirectQUIC(expectedFileID string, outputDir string, opts *TransferOptions) (*RelayManifest, *DownloadStats, error) {
	if opts == nil {
		opts = &TransferOptions{}
	}

	listenAddr := fmt.Sprintf("0.0.0.0:%d", DirectQUICPort)

	tlsConf := InsecureTLSConfig()
	tlsConf.NextProtos = []string{"aether-p2p"}

	listener, err := quic.ListenAddr(listenAddr, tlsConf, &quic.Config{
		MaxIdleTimeout:                120 * time.Second,
		Allow0RTT:                     true,
		DisablePathMTUDiscovery:       true,
		MaxIncomingStreams:             1000,
		InitialStreamReceiveWindow:    8 * 1024 * 1024, // 8MB
		InitialConnectionReceiveWindow: 32 * 1024 * 1024, // 32MB
	})
	if err != nil {
		return nil, nil, fmt.Errorf("p2p listen %s: %w", listenAddr, err)
	}
	defer listener.Close()

	log.Printf("[P2P] Listening for direct QUIC on %s ...", listenAddr)

	// Accept exactly one connection (the sender)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := listener.Accept(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("p2p accept: %w", err)
	}
	defer conn.CloseWithError(0, "done")

	log.Printf("[P2P] Peer connected from %s", conn.RemoteAddr())

	// Read manifest from stream 0
	stream0, err := conn.AcceptStream(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf("accept manifest stream: %w", err)
	}

	allData, err := io.ReadAll(stream0)
	if err != nil {
		return nil, nil, fmt.Errorf("read manifest stream: %w", err)
	}

	// Parse: [opcode=0] [4-byte len] [json]
	if len(allData) < 5 || allData[0] != 0 {
		return nil, nil, fmt.Errorf("invalid manifest frame (len=%d)", len(allData))
	}
	frameLen := binary.BigEndian.Uint32(allData[1:5])
	manifestData := allData[5 : 5+frameLen]

	var manifest RelayManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, nil, fmt.Errorf("decode manifest: %w", err)
	}

	if expectedFileID != "" && manifest.FileID != expectedFileID {
		log.Printf("[P2P] File ID mismatch (expected %s, got %s) — accepting anyway", expectedFileID, manifest.FileID)
	}

	opts.Compress = manifest.Compressed
	opts.Encrypt = manifest.Encrypted && opts.Encrypt

	cacheDir := filepath.Join(".aether_cache", manifest.FileID)
	os.MkdirAll(cacheDir, 0o755)

	totalChunks := manifest.TotalChunks
	log.Printf("[P2P] Receiving %d chunks for file %s", totalChunks, manifest.FileName)

	bar := progressbar.NewOptions(totalChunks,
		progressbar.OptionSetDescription("  ⚡ P2P Direct Download"),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer: "█", SaucerHead: "▓", SaucerPadding: "░",
			BarStart: "│", BarEnd: "│",
		}),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionClearOnFinish(),
		progressbar.OptionOnCompletion(func() { fmt.Println() }),
	)

	stats := &DownloadStats{TotalChunks: totalChunks}

	var wg sync.WaitGroup
	var statsMu sync.Mutex

	// Accept chunk streams in parallel!
	for i := 0; i < totalChunks; i++ {
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			log.Printf("[P2P-RX] accept stream %d err: %v", i, err)
			statsMu.Lock()
			stats.FailCount++
			statsMu.Unlock()
			continue
		}

		wg.Add(1)
		go func(s *quic.Stream) {
			defer wg.Done()
			res := p2pReceiveChunk(s, cacheDir, opts)
			
			statsMu.Lock()
			if res.Success {
				stats.SuccessCount++
				stats.TotalBytes += res.BytesRecv
			} else {
				stats.FailCount++
				log.Printf("[P2P-RX] chunk %d failed: %v", res.ChunkID, res.Err)
			}
			_ = bar.Add(1)
			statsMu.Unlock()
		}(stream)
	}

	wg.Wait() // Wait for all parallel chunks to finish

	return &manifest, stats, nil
}

// p2pReceiveChunk reads a single chunk from a QUIC stream (P2P direct).
func p2pReceiveChunk(stream *quic.Stream, cacheDir string, opts *TransferOptions) DownloadResult {
	start := time.Now()
	result := DownloadResult{}

	// Grab memory from pool to avoid Garbage Collection panics
	bufPtr := chunker.ChunkPool.Get().(*[]byte)
	defer chunker.ChunkPool.Put(bufPtr)
	
	// Read directly into the pre-allocated pool buffer
	buf := *bufPtr
	n, err := io.ReadFull(stream, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		result.Err = fmt.Errorf("read stream: %w", err)
		return result
	}
	allData := buf[:n]

	if len(allData) < 39 { 
		result.Err = fmt.Errorf("frame too short (%d bytes)", len(allData))
		return result
	}

	pos := 0
	_ = allData[pos] // opcode = 1
	pos++
	fidLen := int(allData[pos])
	pos++
	_ = string(allData[pos : pos+fidLen]) // fileID
	pos += fidLen

	chunkID := binary.BigEndian.Uint32(allData[pos : pos+4])
	pos += 4
	expectedHash := allData[pos : pos+32]
	pos += 32
	data := allData[pos:]

	result.ChunkID = int(chunkID)

	// Verify hash
	computedHash := sha256.Sum256(data)
	if !bytes.Equal(computedHash[:], expectedHash) {
		result.Err = fmt.Errorf("hash mismatch on chunk %d", chunkID)
		(*stream).Write([]byte{0}) // NACK
		return result
	}

	// Send ACK
	(*stream).Write([]byte{1})

	// Reverse transforms
	if opts.Encrypt {
		dec, err := aecrypto.Decrypt(data, opts.EncryptKey)
		if err != nil {
			result.Err = err
			return result
		}
		data = dec
	}
	if opts.Compress {
		dec, err := aecrypto.DecompressLZ4(data)
		if err != nil {
			result.Err = err
			return result
		}
		data = dec
	}

	// Write to cache
	chunkPath := filepath.Join(cacheDir, fmt.Sprintf("%06d.chunk", chunkID))
	if err := os.WriteFile(chunkPath, data, 0o644); err != nil {
		result.Err = err
		return result
	}

	result.Success = true
	result.BytesRecv = int64(len(data))
	result.Duration = time.Since(start)
	return result
}
