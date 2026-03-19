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
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/schollz/progressbar/v3"

	"github.com/Aryan-Protein-Vala/AetherNet/pkg/chunker"
	aecrypto "github.com/Aryan-Protein-Vala/AetherNet/pkg/crypto"
)

// UploadQUIC sends chunks over QUIC. Each chunk is a separate stream.
func UploadQUIC(pipe *chunker.PipelineInfo, relayAddr string, numWorkers int, opts *TransferOptions) (*TransferStats, error) {
	if opts == nil {
		opts = &TransferOptions{}
	}
	if numWorkers <= 0 {
		numWorkers = DefaultWorkers
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"aether-quic"},
	}

	conn, err := quic.DialAddr(context.Background(), relayAddr, tlsConf, &quic.Config{
		MaxIdleTimeout: 30 * time.Second,
		Allow0RTT:      true,
	})
	if err != nil {
		return nil, fmt.Errorf("quic dial: %w", err)
	}
	defer conn.CloseWithError(0, "done")

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
	})
	manifestStream.Write([]byte{0}) // Opcode 0 = STORE MANIFEST
	writeFrame(manifestStream, manifestJSON)
	manifestStream.Close()

	totalChunks := pipe.TotalChunks
	jobs := make(chan chunker.ChunkResult)
	results := make(chan UploadResult, totalChunks)

	bar := progressbar.NewOptions(totalChunks,
		progressbar.OptionSetDescription("  ⚡ QUIC Upload"),
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
		go func() {
			defer wg.Done()
			for cr := range jobs {
				results <- uploadChunkQUIC(conn, cr, pipe.FileIDHex, opts)
			}
		}()
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

func uploadChunkQUIC(conn *quic.Conn, cr chunker.ChunkResult, fileID string, opts *TransferOptions) UploadResult {
	start := time.Now()
	result := UploadResult{ChunkID: cr.Chunk.ID}

	data, err := os.ReadFile(cr.ChunkPath)
	if err != nil {
		result.Err = err
		return result
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

	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		result.Err = err
		return result
	}

	// Frame: [type=1] [fileID_len] [fileID] [chunkID_u32] [hash_32] [data]
	header := &bytes.Buffer{}
	header.WriteByte(1)
	header.WriteByte(byte(len(fileID)))
	header.WriteString(fileID)
	binary.Write(header, binary.BigEndian, cr.Chunk.ID)
	header.Write(hash[:])

	stream.Write(header.Bytes())
	stream.Write(data)
	stream.Close() // Close send direction

	// Wait for server ACK
	ack := make([]byte, 1)
	if _, err := io.ReadFull(stream, ack); err != nil {
		result.Err = fmt.Errorf("wait for ack: %w", err)
		return result
	}
	if ack[0] != 1 {
		result.Err = fmt.Errorf("server rejected chunk (hash mismatch)")
		return result
	}

	result.Success = true
	result.BytesSent = uint32(len(data))
	result.Duration = time.Since(start)
	return result
}

// DownloadQUIC fetches chunks from a QUIC relay.
func DownloadQUIC(fileID string, relayAddr string, numWorkers int, opts *TransferOptions) (*RelayManifest, *DownloadStats, error) {
	if opts == nil {
		opts = &TransferOptions{}
	}
	if numWorkers <= 0 {
		numWorkers = DefaultWorkers
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"aether-quic"},
	}

	conn, err := quic.DialAddr(context.Background(), relayAddr, tlsConf, &quic.Config{
		MaxIdleTimeout: 30 * time.Second,
		Allow0RTT:      true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("quic dial: %w", err)
	}
	defer conn.CloseWithError(0, "done")

	// Request manifest
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf("open manifest stream: %w", err)
	}

	reqJSON, _ := json.Marshal(map[string]interface{}{
		"type":    "get_manifest",
		"file_id": fileID,
	})
	stream.Write([]byte{3}) // Opcode 3 = GET MANIFEST
	writeFrame(stream, reqJSON)
	stream.Close()

	manifestData, err := io.ReadAll(stream)
	if err != nil {
		return nil, nil, fmt.Errorf("read manifest: %w", err)
	}

	var manifest RelayManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, nil, fmt.Errorf("decode manifest: %w", err)
	}

	opts.Compress = manifest.Compressed
	opts.Encrypt = manifest.Encrypted && opts.Encrypt

	cacheDir := filepath.Join(".aether_cache", fileID)
	os.MkdirAll(cacheDir, 0o755)

	totalChunks := manifest.TotalChunks
	jobs := make(chan int)
	results := make(chan DownloadResult, totalChunks)

	bar := progressbar.NewOptions(totalChunks,
		progressbar.OptionSetDescription("  ⚡ QUIC Download"),
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
		go func() {
			defer wg.Done()
			for chunkID := range jobs {
				results <- downloadChunkQUIC(conn, fileID, chunkID, cacheDir, opts)
			}
		}()
	}

	go func() {
		for i := 0; i < totalChunks; i++ {
			jobs <- i
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	stats := &DownloadStats{TotalChunks: totalChunks}
	for res := range results {
		if res.Success {
			stats.SuccessCount++
			stats.TotalBytes += res.BytesRecv
		} else {
			stats.FailCount++
		}
		_ = bar.Add(1)
	}

	return &manifest, stats, nil
}

func downloadChunkQUIC(conn *quic.Conn, fileID string, chunkID int, cacheDir string, opts *TransferOptions) DownloadResult {
	start := time.Now()
	result := DownloadResult{ChunkID: chunkID}

	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		result.Err = err
		return result
	}

	req := &bytes.Buffer{}
	req.WriteByte(2)
	req.WriteByte(byte(len(fileID)))
	req.WriteString(fileID)
	binary.Write(req, binary.BigEndian, uint32(chunkID))
	stream.Write(req.Bytes())
	stream.Close()

	data, err := io.ReadAll(stream)
	if err != nil {
		result.Err = err
		return result
	}

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

// Frame helpers
func writeFrame(w io.Writer, data []byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := w.Write(lenBuf); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// InsecureTLSConfig returns a self-signed TLS config for QUIC relay.
func InsecureTLSConfig() *tls.Config {
	cert, err := tls.X509KeyPair(localhostCert, localhostKey)
	if err != nil {
		panic("failed to load cert: " + err.Error())
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"aether-quic"},
	}
}

var localhostCert = []byte(`-----BEGIN CERTIFICATE-----
MIIBLjCB1aADAgECAgEBMAoGCCqGSM49BAMCMBQxEjAQBgNVBAMTCWxvY2FsaG9z
dDAeFw0yNjAzMTkxMDA3MzZaFw0zNjAzMTYxMDA3MzZaMBQxEjAQBgNVBAMTCWxv
Y2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABAPX8Tk2md7qKlXsC5JL
Y+tYVpuYTKJtnRTi/p48TCxEwqUA3jfWSqDvcgSb7P+hSNIFmePm2ESPwGfsVuBr
zz+jGDAWMBQGA1UdEQQNMAuCCWxvY2FsaG9zdDAKBggqhkjOPQQDAgNIADBFAiEA
0SWCM7I4UTmBlC7iuFtpPd/Cu8ExeCyvtJjr0otTFKwCIBvZNP08MRknS46t8jVJ
rz/IhpLAogKIVl5Yu/ll9HQu
-----END CERTIFICATE-----`)

var localhostKey = []byte(`-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIKKE9sXhD9p+PMCsXOhlVt71SDj60RTpAwGe1NinQWmuoAoGCCqGSM49
AwEHoUQDQgAEA9fxOTaZ3uoqVewLkktj61hWm5hMom2dFOL+njxMLETCpQDeN9ZK
oO9yBJvs/6FI0gWZ4+bYRI/AZ+xW4GvPPw==
-----END EC PRIVATE KEY-----`)
