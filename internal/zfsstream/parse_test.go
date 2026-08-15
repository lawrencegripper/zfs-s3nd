package zfsstream

import (
	"encoding/binary"
	"testing"
)

func TestParseBegin(t *testing.T) {
	header := make([]byte, BeginRecordSize)
	binary.LittleEndian.PutUint32(header[0:4], drrBegin)
	binary.LittleEndian.PutUint64(header[8:16], dmuMagic)
	features := dmuBackupFeatureRaw | dmuBackupFeatureCompressed
	binary.LittleEndian.PutUint64(header[16:24], (features<<2)|1)
	binary.LittleEndian.PutUint64(header[40:48], 0xbbb)
	binary.LittleEndian.PutUint64(header[48:56], 0xaaa)
	copy(header[56:], []byte("tank/vms/mail-data@snap2"))

	begin, err := ParseBegin(header)
	if err != nil {
		t.Fatalf("ParseBegin: %v", err)
	}
	if begin.Pool != "tank" || begin.Dataset != "vms/mail-data" || begin.Snapshot != "snap2" {
		t.Fatalf("identity got %#v", begin)
	}
	if begin.FromGUID != "0xaaa" || begin.ToGUID != "0xbbb" {
		t.Fatalf("guids got from=%s to=%s", begin.FromGUID, begin.ToGUID)
	}
	if !begin.Raw || !begin.Compressed {
		t.Fatalf("flags got raw=%v compressed=%v", begin.Raw, begin.Compressed)
	}
}

func TestParseBeginRejectsPoolRootSnapshot(t *testing.T) {
	header := make([]byte, BeginRecordSize)
	binary.LittleEndian.PutUint32(header[0:4], drrBegin)
	binary.LittleEndian.PutUint64(header[8:16], dmuMagic)
	copy(header[56:], []byte("tank@snap1"))

	if _, err := ParseBegin(header); err == nil {
		t.Fatalf("ParseBegin unexpectedly accepted pool root snapshot")
	}
}
