package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
)

const Version = 1

type Manifest struct {
	Version    int        `json:"version"`
	Format     string     `json:"format"`
	Encryption Encryption `json:"encryption"`
	Identity   Identity   `json:"identity"`
	Lineage    Lineage    `json:"lineage"`
	Stream     Stream     `json:"stream"`
	Chunks     []Chunk    `json:"chunks"`
}

type Encryption struct {
	ObjectEnvelopeVersion int    `json:"object_envelope_version"`
	Algorithm             string `json:"algorithm"`
	KDF                   string `json:"kdf"`
	Salt                  string `json:"salt"`
	Time                  uint32 `json:"time"`
	MemoryKiB             uint32 `json:"memory_kib"`
	Parallelism           uint8  `json:"parallelism"`
}

type Identity struct {
	Source   string `json:"source"`
	Pool     string `json:"pool"`
	Dataset  string `json:"dataset"`
	Snapshot string `json:"snapshot"`
}

type Lineage struct {
	BaseSnapshot string `json:"base_snapshot,omitempty"`
}

type Stream struct {
	Raw        bool   `json:"raw"`
	Compressed bool   `json:"compressed"`
	FromGUID   string `json:"from_guid,omitempty"`
	ToGUID     string `json:"to_guid,omitempty"`
	SizeBytes  int64  `json:"size_bytes"`
	SHA256     string `json:"sha256"`
	ChunkSize  int64  `json:"chunk_size_bytes"`
}

type Chunk struct {
	Index       int64  `json:"index"`
	ObjectKey   string `json:"object_key"`
	SizeBytes   int64  `json:"size_bytes"`
	OffsetStart int64  `json:"offset_start"`
	OffsetEnd   int64  `json:"offset_end"`
	SHA256      string `json:"sha256"`
}

func New(identity Identity, lineage Lineage, stream Stream, chunks []Chunk) Manifest {
	return Manifest{
		Version: Version,
		Format:  "zfs-s3nd.snapshot.v1",
		Encryption: Encryption{
			ObjectEnvelopeVersion: 1,
			Algorithm:             "XChaCha20-Poly1305",
			KDF:                   "argon2id",
			Salt:                  "zfs-s3nd/storage-encryption/v1",
			Time:                  3,
			MemoryKiB:             64 * 1024,
			Parallelism:           4,
		},
		Identity: identity,
		Lineage:  lineage,
		Stream:   stream,
		Chunks:   append([]Chunk(nil), chunks...),
	}
}

func (m Manifest) Validate() error {
	if m.Version != Version {
		return fmt.Errorf("unsupported manifest version %d", m.Version)
	}
	if m.Format != "zfs-s3nd.snapshot.v1" {
		return fmt.Errorf("unsupported manifest format %q", m.Format)
	}
	if m.Encryption.ObjectEnvelopeVersion == 0 || m.Encryption.Algorithm == "" || m.Encryption.KDF == "" || m.Encryption.Salt == "" {
		return errors.New("manifest encryption metadata is incomplete")
	}
	if m.Identity.Source == "" || m.Identity.Pool == "" || m.Identity.Dataset == "" || m.Identity.Snapshot == "" {
		return errors.New("manifest identity is incomplete")
	}
	if len(m.Chunks) == 0 {
		return errors.New("manifest has no chunks")
	}
	var total int64
	for i, chunk := range m.Chunks {
		if chunk.Index != int64(i) {
			return fmt.Errorf("chunk index mismatch at position %d: got %d", i, chunk.Index)
		}
		if chunk.ObjectKey == "" || chunk.SHA256 == "" || chunk.SizeBytes <= 0 {
			return fmt.Errorf("chunk %d is incomplete", i)
		}
		if chunk.OffsetEnd-chunk.OffsetStart != chunk.SizeBytes {
			return fmt.Errorf("chunk %d offset/size mismatch", i)
		}
		total += chunk.SizeBytes
	}
	if m.Stream.SizeBytes != total {
		return fmt.Errorf("stream size mismatch: got %d want chunk total %d", m.Stream.SizeBytes, total)
	}
	if m.Stream.SHA256 == "" {
		return errors.New("stream sha256 is required")
	}
	return nil
}

func (m Manifest) MarshalCanonical() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.MarshalIndent(m, "", "  ")
}

func Unmarshal(data []byte) (Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, err
	}
	if err := m.Validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}
