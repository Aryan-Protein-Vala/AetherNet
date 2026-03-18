// Package crypto provides encryption and compression for Aether chunks.
//
// Compression: LZ4 (ultra-fast, used when beneficial)
// Encryption:  AES-256-GCM (authenticated encryption)
//
// The pipeline is: raw chunk → compress → encrypt → upload
// On download:     download → decrypt → decompress → write
package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

// ──────────────────────────────────────────────────────────────────────
// LZ4 Compression
// ──────────────────────────────────────────────────────────────────────

// CompressLZ4 compresses data using LZ4. Returns compressed data and
// whether compression was beneficial (saved at least 5%).
// If not beneficial, returns the original data with beneficial=false.
func CompressLZ4(data []byte) ([]byte, bool) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	w.Apply(lz4.CompressionLevelOption(lz4.Fast))

	_, err := w.Write(data)
	if err != nil {
		return data, false
	}
	if err := w.Close(); err != nil {
		return data, false
	}

	compressed := buf.Bytes()

	// Only use compression if it saves at least 5%
	threshold := len(data) - len(data)/20
	if len(compressed) >= threshold {
		return data, false
	}

	return compressed, true
}

// DecompressLZ4 decompresses LZ4-compressed data.
func DecompressLZ4(data []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, fmt.Errorf("lz4 decompress: %w", err)
	}

	return buf.Bytes(), nil
}

// ──────────────────────────────────────────────────────────────────────
// AES-256-GCM Encryption
// ──────────────────────────────────────────────────────────────────────

// DeriveKey derives a 32-byte AES-256 key from a passphrase using
// SHA-256. Simple but effective for CLI password-based encryption.
func DeriveKey(passphrase string) [32]byte {
	return sha256.Sum256([]byte(passphrase))
}

// Encrypt encrypts data using AES-256-GCM with the given key.
// Returns: nonce (12 bytes) + ciphertext + tag (16 bytes)
func Encrypt(data []byte, key [32]byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	// Seal appends ciphertext+tag to nonce
	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

// Decrypt decrypts AES-256-GCM encrypted data.
// Expects: nonce (12 bytes) + ciphertext + tag (16 bytes)
func Decrypt(data []byte, key [32]byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return plaintext, nil
}
