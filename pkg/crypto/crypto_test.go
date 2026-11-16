package crypto

import (
	"bytes"
	"testing"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	key := DeriveKey("super-secret-passphrase")
	plain := []byte("zero-copy quic payload verification block")

	ct, err := Encrypt(plain, key)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}
	if bytes.Equal(ct, plain) {
		t.Fatal("ciphertext must differ from plaintext")
	}

	got, err := Decrypt(ct, key)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("plaintext mismatch: got %q, want %q", got, plain)
	}
}

func TestDecryptRejectsTamperingAndWrongKey(t *testing.T) {
	key := DeriveKey("correct-key")
	wrongKey := DeriveKey("wrong-key")
	ct, err := Encrypt([]byte("authentic payload"), key)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if _, err := Decrypt(ct, wrongKey); err == nil {
		t.Fatal("expected error decrypting with wrong key")
	}

	tampered := append([]byte(nil), ct...)
	tampered[len(tampered)-1] ^= 0xFF
	if _, err := Decrypt(tampered, key); err == nil {
		t.Fatal("expected error decrypting tampered ciphertext")
	}
}

func TestCompressDecompressLZ4(t *testing.T) {
	compressible := bytes.Repeat([]byte("AetherNet-ZeroCopy-QUIC-"), 512)
	comp, beneficial := CompressLZ4(compressible)
	if !beneficial {
		t.Fatal("expected repetitive payload to compress beneficially")
	}
	if len(comp) >= len(compressible) {
		t.Fatalf("compressed size %d >= original %d", len(comp), len(compressible))
	}

	decomp, err := DecompressLZ4(comp)
	if err != nil {
		t.Fatalf("DecompressLZ4 failed: %v", err)
	}
	if !bytes.Equal(decomp, compressible) {
		t.Fatal("decompressed payload mismatch")
	}
}

func BenchmarkEncrypt1MB(b *testing.B) {
	key := DeriveKey("bench-key")
	payload := bytes.Repeat([]byte{0xAB}, 1<<20)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encrypt(payload, key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompressLZ4_1MB(b *testing.B) {
	payload := bytes.Repeat([]byte("AetherNet-Benchmark-Block-"), 40960)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressLZ4(payload)
	}
}
