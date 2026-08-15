package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sync"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"
)

var encryptedObjectMagic = []byte("ZS3ENC1\n")

const (
	encryptionEnvelopeVersion = 1
	encryptionAlgorithm       = "XChaCha20-Poly1305"
	encryptionKDF             = "argon2id"
	encryptionSalt            = "zfs-s3nd/storage-encryption/v1"
	encryptionTime            = uint32(3)
	encryptionMemoryKiB       = uint32(64 * 1024)
	encryptionParallelism     = uint8(4)
)

type EncryptedStore struct {
	base       Store
	passphrase string
	mu         sync.Mutex
	keyCache   map[string][]byte
}

type objectEnvelope struct {
	Version     int    `json:"version"`
	Algorithm   string `json:"algorithm"`
	KDF         string `json:"kdf"`
	Salt        string `json:"salt"`
	Time        uint32 `json:"time"`
	MemoryKiB   uint32 `json:"memory_kib"`
	Parallelism uint8  `json:"parallelism"`
	Nonce       string `json:"nonce"`
}

func NewEncryptedStore(base Store, passphrase string) (*EncryptedStore, error) {
	if base == nil {
		return nil, fmt.Errorf("base store is required")
	}
	if passphrase == "" {
		return nil, fmt.Errorf("storage encryption passphrase is required")
	}
	return &EncryptedStore{base: base, passphrase: passphrase, keyCache: make(map[string][]byte)}, nil
}

func (s *EncryptedStore) PutBytes(ctx context.Context, key string, data []byte) (ObjectInfo, error) {
	if isPlaintextMetadataKey(key) {
		return s.base.PutBytes(ctx, key, data)
	}

	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := rand.Read(nonce); err != nil {
		return ObjectInfo{}, fmt.Errorf("generate object nonce: %w", err)
	}
	header := objectEnvelope{
		Version:     encryptionEnvelopeVersion,
		Algorithm:   encryptionAlgorithm,
		KDF:         encryptionKDF,
		Salt:        encryptionSalt,
		Time:        encryptionTime,
		MemoryKiB:   encryptionMemoryKiB,
		Parallelism: encryptionParallelism,
		Nonce:       base64.StdEncoding.EncodeToString(nonce),
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("marshal encryption envelope: %w", err)
	}
	aead, err := s.aeadFor(header)
	if err != nil {
		return ObjectInfo{}, err
	}
	ciphertext := aead.Seal(nil, nonce, data, headerBytes)
	envelope := encodeEncryptedObject(headerBytes, ciphertext)
	if _, err := s.base.PutBytes(ctx, key, envelope); err != nil {
		return ObjectInfo{}, err
	}
	sha := sha256.Sum256(data)
	return ObjectInfo{Key: key, Size: int64(len(data)), SHA256: hex.EncodeToString(sha[:])}, nil
}

func (s *EncryptedStore) GetBytes(ctx context.Context, key string) ([]byte, error) {
	data, err := s.base.GetBytes(ctx, key)
	if err != nil {
		return nil, err
	}
	if isPlaintextMetadataKey(key) {
		return data, nil
	}
	plaintext, err := s.decryptObject(key, data)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}

func (s *EncryptedStore) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	data, err := s.GetBytes(ctx, key)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *EncryptedStore) Head(ctx context.Context, key string) (ObjectInfo, error) {
	data, err := s.GetBytes(ctx, key)
	if err != nil {
		return ObjectInfo{}, err
	}
	sha := sha256.Sum256(data)
	return ObjectInfo{Key: key, Size: int64(len(data)), SHA256: hex.EncodeToString(sha[:])}, nil
}

func (s *EncryptedStore) Delete(ctx context.Context, key string) error {
	return s.base.Delete(ctx, key)
}

func (s *EncryptedStore) decryptObject(key string, data []byte) ([]byte, error) {
	if !bytes.HasPrefix(data, encryptedObjectMagic) {
		return nil, fmt.Errorf("object %s is not encrypted", key)
	}
	headerBytes, ciphertext, err := decodeEncryptedObject(data)
	if err != nil {
		return nil, fmt.Errorf("decrypt object %s: %w", key, err)
	}
	var header objectEnvelope
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("decrypt object %s: parse envelope: %w", key, err)
	}
	nonce, err := base64.StdEncoding.DecodeString(header.Nonce)
	if err != nil {
		return nil, fmt.Errorf("decrypt object %s: parse nonce: %w", key, err)
	}
	aead, err := s.aeadFor(header)
	if err != nil {
		return nil, fmt.Errorf("decrypt object %s: %w", key, err)
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, headerBytes)
	if err != nil {
		return nil, fmt.Errorf("decrypt object %s: %w", key, err)
	}
	return plaintext, nil
}

func (s *EncryptedStore) aeadFor(header objectEnvelope) (cipherAEAD, error) {
	if header.Version != encryptionEnvelopeVersion {
		return nil, fmt.Errorf("unsupported encryption envelope version %d", header.Version)
	}
	if header.Algorithm != encryptionAlgorithm {
		return nil, fmt.Errorf("unsupported encryption algorithm %q", header.Algorithm)
	}
	if header.KDF != encryptionKDF {
		return nil, fmt.Errorf("unsupported encryption kdf %q", header.KDF)
	}
	if header.Time == 0 || header.MemoryKiB == 0 || header.Parallelism == 0 {
		return nil, fmt.Errorf("invalid encryption kdf parameters")
	}
	cacheKey := fmt.Sprintf("%s/%d/%d/%d", header.Salt, header.Time, header.MemoryKiB, header.Parallelism)
	s.mu.Lock()
	key := append([]byte(nil), s.keyCache[cacheKey]...)
	s.mu.Unlock()
	if key == nil {
		key = argon2.IDKey([]byte(s.passphrase), []byte(header.Salt), header.Time, header.MemoryKiB, header.Parallelism, chacha20poly1305.KeySize)
		s.mu.Lock()
		s.keyCache[cacheKey] = append([]byte(nil), key...)
		s.mu.Unlock()
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("create XChaCha20-Poly1305 cipher: %w", err)
	}
	return aead, nil
}

type cipherAEAD interface {
	NonceSize() int
	Overhead() int
	Seal(dst, nonce, plaintext, additionalData []byte) []byte
	Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error)
}

func encodeEncryptedObject(header, ciphertext []byte) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(header)))
	out := make([]byte, 0, len(encryptedObjectMagic)+len(length)+len(header)+len(ciphertext))
	out = append(out, encryptedObjectMagic...)
	out = append(out, length[:]...)
	out = append(out, header...)
	out = append(out, ciphertext...)
	return out
}

func decodeEncryptedObject(data []byte) ([]byte, []byte, error) {
	offset := len(encryptedObjectMagic)
	if len(data) < offset+4 {
		return nil, nil, fmt.Errorf("encrypted envelope is truncated")
	}
	headerLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	if headerLen <= 0 || len(data) < offset+headerLen {
		return nil, nil, fmt.Errorf("encrypted envelope header is truncated")
	}
	header := data[offset : offset+headerLen]
	ciphertext := data[offset+headerLen:]
	return header, ciphertext, nil
}

func isPlaintextMetadataKey(key string) bool {
	return path.Base(key) == "manifest.json"
}

func EncryptionMetadata() map[string]interface{} {
	return map[string]interface{}{
		"object_envelope_version": encryptionEnvelopeVersion,
		"algorithm":               encryptionAlgorithm,
		"kdf":                     encryptionKDF,
		"salt":                    encryptionSalt,
		"time":                    encryptionTime,
		"memory_kib":              encryptionMemoryKiB,
		"parallelism":             encryptionParallelism,
	}
}
