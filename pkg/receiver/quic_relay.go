package receiver

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/quic-go/quic-go"
)

// StartQUICWithConfig starts a QUIC listener on the given port.
func (s *Server) StartQUICWithConfig(addr string, listener *quic.Listener) {
	fmt.Printf("  %s   QUIC listening on %s\n\n", dim("QUIC"), addr)

	for {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			fmt.Printf("  %s QUIC accept: %v\n", red("✗"), err)
			return
		}
		go s.handleQUICConn(conn)
	}
}

func (s *Server) handleQUICConn(conn *quic.Conn) {
	defer conn.CloseWithError(0, "done")

	for {
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			return
		}
		go s.handleQUICStream(conn, stream)
	}
}

func (s *Server) handleQUICStream(conn *quic.Conn, stream *quic.Stream) {
	defer stream.Close()

	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(stream, typeBuf); err != nil {
		return
	}

	switch typeBuf[0] {
	case 0:
		s.quicManifestStore(stream)
	case 1:
		s.quicChunkUpload(stream)
	case 2:
		s.quicChunkRequest(conn, stream)
	case 3:
		s.quicManifestRequest(conn, stream)
	}
}

func (s *Server) quicManifestStore(stream *quic.Stream) {
	data, err := io.ReadAll(stream)
	if err != nil {
		return
	}

	// Skip 4-byte frame length prefix
	if len(data) > 4 {
		length := binary.BigEndian.Uint32(data[:4])
		if int(length) <= len(data)-4 {
			data = data[4 : 4+length]
		}
	}

	var msg struct {
		FileID      string `json:"file_id"`
		FileName    string `json:"file_name"`
		FileSize    int64  `json:"file_size"`
		TotalChunks int    `json:"total_chunks"`
		ChunkSize   uint32 `json:"chunk_size"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}

	destDir := filepath.Join(receivedDir, msg.FileID)
	os.MkdirAll(destDir, 0o755)

	manifestJSON, _ := json.Marshal(map[string]interface{}{
		"file_name":    msg.FileName,
		"file_size":    msg.FileSize,
		"total_chunks": msg.TotalChunks,
		"chunk_size":   msg.ChunkSize,
	})
	os.WriteFile(filepath.Join(destDir, "manifest.json"), manifestJSON, 0o644)

	short := msg.FileID
	if len(short) > 12 {
		short = short[:12] + "…"
	}
	fmt.Printf("  %s manifest (QUIC)  file %s\n", green("▲"), dim(short))
}

func (s *Server) quicChunkUpload(stream *quic.Stream) {
	fileIDLenBuf := make([]byte, 1)
	if _, err := io.ReadFull(stream, fileIDLenBuf); err != nil {
		return
	}

	fileIDBuf := make([]byte, fileIDLenBuf[0])
	if _, err := io.ReadFull(stream, fileIDBuf); err != nil {
		return
	}
	fileID := string(fileIDBuf)

	var chunkID uint32
	if err := binary.Read(stream, binary.BigEndian, &chunkID); err != nil {
		return
	}

	hashBuf := make([]byte, 32)
	if _, err := io.ReadFull(stream, hashBuf); err != nil {
		return
	}

	data, err := io.ReadAll(stream)
	if err != nil {
		return
	}

	computedHash := sha256.Sum256(data)
	ok := hex.EncodeToString(computedHash[:]) == hex.EncodeToString(hashBuf)

	destDir := filepath.Join(receivedDir, fileID)
	os.MkdirAll(destDir, 0o755)
	chunkPath := filepath.Join(destDir, fmt.Sprintf("%06d.chunk", chunkID))
	os.WriteFile(chunkPath, data, 0o644)

	if ok {
		stream.Write([]byte{1}) // ACK
	} else {
		stream.Write([]byte{0}) // NACK
	}

	logChunk(int(chunkID), fileID, ok, int64(len(data)))
}

func (s *Server) quicChunkRequest(conn *quic.Conn, stream *quic.Stream) {
	fileIDLenBuf := make([]byte, 1)
	if _, err := io.ReadFull(stream, fileIDLenBuf); err != nil {
		return
	}

	fileIDBuf := make([]byte, fileIDLenBuf[0])
	if _, err := io.ReadFull(stream, fileIDBuf); err != nil {
		return
	}
	fileID := string(fileIDBuf)

	var chunkID uint32
	if err := binary.Read(stream, binary.BigEndian, &chunkID); err != nil {
		return
	}

	chunkPath := filepath.Join(receivedDir, fileID, fmt.Sprintf("%06d.chunk", chunkID))
	data, err := os.ReadFile(chunkPath)
	if err != nil {
		return
	}

	stream.Write(data)
}

func (s *Server) quicManifestRequest(conn *quic.Conn, stream *quic.Stream) {
	data, err := io.ReadAll(stream)
	if err != nil {
		return
	}

	if len(data) > 4 {
		length := binary.BigEndian.Uint32(data[:4])
		if int(length) <= len(data)-4 {
			data = data[4 : 4+length]
		}
	}

	var msg struct {
		FileID      string `json:"file_id"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}

	manifestPath := filepath.Join(receivedDir, msg.FileID, "manifest.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return
	}

	stream.Write(manifestData)
}
