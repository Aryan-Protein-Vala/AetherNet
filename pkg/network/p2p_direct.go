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
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
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

	var streams []*quic.Stream
	for w := 0; w < numWorkers; w++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		stream, err := conn.OpenStreamSync(ctx)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("open worker stream: %w", err)
		}
		streams = append(streams, stream)
	}

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int, stream *quic.Stream) {
			defer wg.Done()
			defer (*stream).Close()
			for cr := range jobs {
				res := p2pUploadChunk(stream, cr, pipe.FileIDHex, opts)
				results <- res
			}
		}(w, streams[w])
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
func p2pUploadChunk(stream *quic.Stream, cr chunker.ChunkResult, fileID string, opts *TransferOptions) UploadResult {
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

	frameBytes := frame.Bytes()
	lengthPrefix := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthPrefix, uint32(len(frameBytes)))

	// Write length prefix then frame
	if _, err := (*stream).Write(lengthPrefix); err != nil {
		result.Err = fmt.Errorf("write length prefix: %w", err)
		return result
	}
	if _, err := (*stream).Write(frameBytes); err != nil {
		result.Err = fmt.Errorf("write chunk: %w", err)
		return result
	}

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

	finalPath := filepath.Join(outputDir, manifest.FileName)
	f, err := os.OpenFile(finalPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()

	if err := f.Truncate(manifest.FileSize); err != nil {
		return nil, nil, fmt.Errorf("truncate file: %w", err)
	}

	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(manifest.FileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, fmt.Errorf("mmap file: %w", err)
	}
	defer syscall.Munmap(mmapData)

	totalChunks := manifest.TotalChunks
	log.Printf("[P2P] Receiving %d chunks for file %s (mmap)", totalChunks, manifest.FileName)

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

	var chunkWg sync.WaitGroup
	var statsMu sync.Mutex

	chunkWg.Add(totalChunks)

	go func() {
		for {
			stream, err := conn.AcceptStream(context.Background())
			if err != nil {
				break
			}

			go func(s *quic.Stream) {
				for {
					res := p2pReceiveChunk(s, mmapData, manifest.ChunkSize, opts)
					if res.EOF {
						return
					}

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
					chunkWg.Done()
				}
			}(stream)
		}
	}()

	chunkWg.Wait() // Wait for all parallel chunks to finish
	return &manifest, stats, nil
}

// p2pReceiveChunk reads a single chunk from a multiplexed QUIC stream.
func p2pReceiveChunk(stream *quic.Stream, mmapData []byte, chunkSize uint32, opts *TransferOptions) DownloadResult {
	start := time.Now()
	result := DownloadResult{}

	// Read frame length
	lenBuf := make([]byte, 4)
	n, err := io.ReadFull(stream, lenBuf)
	if err != nil {
		if err == io.EOF && n == 0 {
			result.EOF = true
			return result
		}
		result.Err = fmt.Errorf("read frame len: %w", err)
		return result
	}
	frameLen := binary.BigEndian.Uint32(lenBuf)

	// 2. Read small header prefix (Opcode + fidLen)
	prefix := make([]byte, 2)
	if _, err := io.ReadFull(stream, prefix); err != nil {
		result.Err = fmt.Errorf("read prefix: %w", err)
		return result
	}
	fidLen := int(prefix[1])

	// 3. Read the rest of the header (FID + ChunkID + Hash)
	headerRest := make([]byte, fidLen+36)
	if _, err := io.ReadFull(stream, headerRest); err != nil {
		result.Err = fmt.Errorf("read header context: %w", err)
		return result
	}

	pos := fidLen // Skip FID
	chunkID := binary.BigEndian.Uint32(headerRest[pos : pos+4])
	pos += 4
	expectedHash := headerRest[pos : pos+32]

	result.ChunkID = int(chunkID)

	payloadLen := frameLen - uint32(2+fidLen+36)
	offset := int64(chunkID) * int64(chunkSize)

	// 4. Determine Zero-Copy capability
	needsTransform := opts.Encrypt || opts.Compress
	var data []byte

	if !needsTransform {
		// 🔥 PURE ZERO-COPY PATH: Read directly from socket into mapped kernel file
		data = mmapData[offset : offset+int64(payloadLen)]
		if _, err := io.ReadFull(stream, data); err != nil {
			result.Err = fmt.Errorf("read payload direct to mmap: %w", err)
			return result
		}

		// Verify Hash in-place
		computedHash := sha256.Sum256(data)
		if !bytes.Equal(computedHash[:], expectedHash) {
			result.Err = fmt.Errorf("hash mismatch on chunk %d", chunkID)
			return result
		}
	} else {
		// TRANSFORM PATH: Fallback to ChunkPool to prevent overwriting ciphertext during decryption
		bufPtr := chunker.ChunkPool.Get().(*[]byte)
		defer chunker.ChunkPool.Put(bufPtr)

		buf := (*bufPtr)[:payloadLen]
		if _, err := io.ReadFull(stream, buf); err != nil {
			result.Err = fmt.Errorf("read payload into pool: %w", err)
			return result
		}
		data = buf

		computedHash := sha256.Sum256(data)
		if !bytes.Equal(computedHash[:], expectedHash) {
			result.Err = fmt.Errorf("hash mismatch on chunk %d", chunkID)
			return result
		}

		if opts.Encrypt {
			if dec, err := aecrypto.Decrypt(data, opts.EncryptKey); err == nil {
				data = dec
			} else {
				result.Err = err
				return result
			}
		}
		if opts.Compress {
			if dec, err := aecrypto.DecompressLZ4(data); err == nil {
				data = dec
			} else {
				result.Err = err
				return result
			}
		}

		// Write directly to mmap memory
		copy(mmapData[offset:offset+int64(len(data))], data)
	}

	// Send ACK (important: sender is waiting for it)
	(*stream).Write([]byte{1})

	result.Success = true
	result.BytesRecv = int64(len(data))
	result.Duration = time.Since(start)
	return result
}

const DirectTCPPort = 4243

// ──────────────────────────────────────────────────────────────────────
// UploadDirectTCP — send chunks over Raw TCP
// ──────────────────────────────────────────────────────────────────────

func UploadDirectTCP(pipe *chunker.PipelineInfo, targetIP string, numWorkers int, opts *TransferOptions) (*TransferStats, error) {
	if opts == nil {
		opts = &TransferOptions{}
	}
	if numWorkers <= 0 {
		numWorkers = DefaultWorkers
	}

	targetAddr := fmt.Sprintf("%s:%d", targetIP, DirectTCPPort)
	log.Printf("[P2P-TCP] Dialing peer at %s ...", targetAddr)

	var conns []net.Conn
	for w := 0; w < numWorkers; w++ {
		c, err := net.DialTimeout("tcp", targetAddr, 5*time.Second)
		if err != nil {
			return nil, fmt.Errorf("tcp dial %s: %w", targetAddr, err)
		}
		conns = append(conns, c)
	}
	log.Printf("[P2P-TCP] Connected %d sockets!", numWorkers)

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
	
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(manifestJSON)+1))
	conns[0].Write(lenBuf)
	conns[0].Write([]byte{0})
	conns[0].Write(manifestJSON)

	time.Sleep(100 * time.Millisecond)

	log.Printf("[P2P-TCP] Manifest sent (%d chunks)", pipe.TotalChunks)

	totalChunks := pipe.TotalChunks
	jobs := make(chan chunker.ChunkResult)
	results := make(chan UploadResult, totalChunks)

	bar := progressbar.NewOptions(totalChunks,
		progressbar.OptionSetDescription("  ⚡ P2P TCP Upload"),
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
		go func(workerID int, c net.Conn) {
			defer wg.Done()
			defer c.Close()
			for cr := range jobs {
				res := p2pUploadChunkTCP(c, cr, pipe.FileIDHex, opts)
				results <- res
			}
		}(w, conns[w])
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

func p2pUploadChunkTCP(conn net.Conn, cr chunker.ChunkResult, fileID string, opts *TransferOptions) UploadResult {
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

	frame := &bytes.Buffer{}
	frame.WriteByte(1)
	frame.WriteByte(byte(len(fileID)))
	frame.WriteString(fileID)
	binary.Write(frame, binary.BigEndian, cr.Chunk.ID)
	frame.Write(hash[:])
	frame.Write(data)

	frameBytes := frame.Bytes()
	lengthPrefix := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthPrefix, uint32(len(frameBytes)))

	if _, err := conn.Write(lengthPrefix); err != nil {
		result.Err = fmt.Errorf("write length prefix: %w", err)
		return result
	}
	if _, err := conn.Write(frameBytes); err != nil {
		result.Err = fmt.Errorf("write chunk: %w", err)
		return result
	}

	ack := make([]byte, 1)
	if _, err := io.ReadFull(conn, ack); err != nil {
		result.Err = fmt.Errorf("wait ack: %w", err)
		return result
	}

	result.Success = true
	result.BytesSent = uint32(len(data))
	result.Duration = time.Since(start)
	return result
}

// ──────────────────────────────────────────────────────────────────────
// ListenDirectTCP — receive chunks directly over TCP
// ──────────────────────────────────────────────────────────────────────

func ListenDirectTCP(expectedFileID string, outputDir string, opts *TransferOptions) (*RelayManifest, *DownloadStats, error) {
	if opts == nil {
		opts = &TransferOptions{}
	}

	listenAddr := fmt.Sprintf("0.0.0.0:%d", DirectTCPPort)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("tcp listen %s: %w", listenAddr, err)
	}
	defer listener.Close()

	log.Printf("[P2P-TCP] Listening on %s ...", listenAddr)

	conn0, err := listener.Accept()
	if err != nil {
		return nil, nil, fmt.Errorf("accept conn0: %w", err)
	}

	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn0, lenBuf); err != nil {
		return nil, nil, fmt.Errorf("read manifest len: %w", err)
	}
	frameLen := binary.BigEndian.Uint32(lenBuf)

	allData := make([]byte, frameLen)
	if _, err := io.ReadFull(conn0, allData); err != nil {
		return nil, nil, fmt.Errorf("read manifest data: %w", err)
	}

	if allData[0] != 0 {
		return nil, nil, fmt.Errorf("invalid manifest opcode")
	}
	manifestData := allData[1:]

	var manifest RelayManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, nil, fmt.Errorf("decode manifest: %w", err)
	}

	if expectedFileID != "" && manifest.FileID != expectedFileID {
		log.Printf("[P2P-TCP] File ID mismatch: requested %s, got %s", expectedFileID, manifest.FileID)
	}

	opts.Compress = manifest.Compressed
	opts.Encrypt = manifest.Encrypted && opts.Encrypt

	finalPath := filepath.Join(outputDir, manifest.FileName)
	f, err := os.OpenFile(finalPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()

	if err := f.Truncate(manifest.FileSize); err != nil {
		return nil, nil, fmt.Errorf("truncate file: %w", err)
	}

	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(manifest.FileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, fmt.Errorf("mmap file: %w", err)
	}
	defer syscall.Munmap(mmapData)

	totalChunks := manifest.TotalChunks
	log.Printf("[P2P-TCP] Receiving %d chunks for file %s (mmap)", totalChunks, manifest.FileName)

	bar := progressbar.NewOptions(totalChunks,
		progressbar.OptionSetDescription("  ⚡ P2P TCP Download"),
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
	var chunkWg sync.WaitGroup
	var statsMu sync.Mutex

	chunkWg.Add(totalChunks)

	go handleTCPStream(conn0, mmapData, manifest.ChunkSize, opts, bar, stats, &statsMu, &chunkWg)

	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				break
			}
			go handleTCPStream(c, mmapData, manifest.ChunkSize, opts, bar, stats, &statsMu, &chunkWg)
		}
	}()

	chunkWg.Wait()
	return &manifest, stats, nil
}

func handleTCPStream(conn net.Conn, mmapData []byte, chunkSize uint32, opts *TransferOptions, bar *progressbar.ProgressBar, stats *DownloadStats, statsMu *sync.Mutex, chunkWg *sync.WaitGroup) {
	defer conn.Close()
	for {
		res := p2pReceiveChunkTCP(conn, mmapData, chunkSize, opts)
		if res.EOF { return }

		statsMu.Lock()
		if res.Success {
			stats.SuccessCount++
			stats.TotalBytes += res.BytesRecv
		} else {
			stats.FailCount++
			log.Printf("[P2P-TCP] chunk %d failed: %v", res.ChunkID, res.Err)
		}
		_ = bar.Add(1)
		statsMu.Unlock()
		chunkWg.Done()
	}
}

func p2pReceiveChunkTCP(conn net.Conn, mmapData []byte, chunkSize uint32, opts *TransferOptions) DownloadResult {
	start := time.Now()
	result := DownloadResult{}

	lenBuf := make([]byte, 4)
	n, err := io.ReadFull(conn, lenBuf)
	if err != nil {
		if err == io.EOF && n == 0 {
			result.EOF = true
			return result
		}
		result.Err = fmt.Errorf("read frame len: %w", err)
		return result
	}
	frameLen := binary.BigEndian.Uint32(lenBuf)

	// 2. Read small header prefix (Opcode + fidLen)
	prefix := make([]byte, 2)
	if _, err := io.ReadFull(conn, prefix); err != nil {
		result.Err = fmt.Errorf("read prefix: %w", err)
		return result
	}
	fidLen := int(prefix[1])

	// 3. Read the rest of the header (FID + ChunkID + Hash)
	headerRest := make([]byte, fidLen+36)
	if _, err := io.ReadFull(conn, headerRest); err != nil {
		result.Err = fmt.Errorf("read header context: %w", err)
		return result
	}

	pos := fidLen // Skip FID
	chunkID := binary.BigEndian.Uint32(headerRest[pos : pos+4])
	pos += 4
	expectedHash := headerRest[pos : pos+32]

	result.ChunkID = int(chunkID)

	payloadLen := frameLen - uint32(2+fidLen+36)
	offset := int64(chunkID) * int64(chunkSize)

	// 4. Determine Zero-Copy capability
	needsTransform := opts.Encrypt || opts.Compress
	var data []byte

	if !needsTransform {
		// 🔥 PURE ZERO-COPY PATH: Read directly from socket into mapped kernel file
		data = mmapData[offset : offset+int64(payloadLen)]
		if _, err := io.ReadFull(conn, data); err != nil {
			result.Err = fmt.Errorf("read payload direct to mmap: %w", err)
			return result
		}

		// Verify Hash in-place
		computedHash := sha256.Sum256(data)
		if !bytes.Equal(computedHash[:], expectedHash) {
			result.Err = fmt.Errorf("hash mismatch on chunk %d", chunkID)
			return result
		}
	} else {
		// TRANSFORM PATH: Fallback to ChunkPool to prevent overwriting ciphertext during decryption
		bufPtr := chunker.ChunkPool.Get().(*[]byte)
		defer chunker.ChunkPool.Put(bufPtr)

		buf := (*bufPtr)[:payloadLen]
		if _, err := io.ReadFull(conn, buf); err != nil {
			result.Err = fmt.Errorf("read payload into pool: %w", err)
			return result
		}
		data = buf

		computedHash := sha256.Sum256(data)
		if !bytes.Equal(computedHash[:], expectedHash) {
			result.Err = fmt.Errorf("hash mismatch on chunk %d", chunkID)
			return result
		}

		if opts.Encrypt {
			if dec, err := aecrypto.Decrypt(data, opts.EncryptKey); err == nil {
				data = dec
			} else {
				result.Err = err
				return result
			}
		}
		if opts.Compress {
			if dec, err := aecrypto.DecompressLZ4(data); err == nil {
				data = dec
			} else {
				result.Err = err
				return result
			}
		}

		// Write transformed data to mmap
		copy(mmapData[offset:offset+int64(len(data))], data)
	}

	conn.Write([]byte{1})

	result.Success = true
	result.BytesRecv = int64(len(data))
	result.Duration = time.Since(start)
	return result
}
