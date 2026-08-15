package zfsstream

import (
	"encoding/binary"
	"fmt"
	"strings"
)

const (
	BeginRecordSize = 312
	drrBegin        = uint32(0)
	dmuMagic        = uint64(0x2f5bacbac)
	maxNameLen      = 256

	dmuBackupFeatureCompressed = uint64(1 << 22)
	dmuBackupFeatureRaw        = uint64(1 << 24)
)

// Begin contains the metadata from the first DRR_BEGIN record in a simple
// OpenZFS send stream. It intentionally supports the common single-dataset
// stream shape produced by `zfs send <snapshot>` and `zfs send -i ...`.
type Begin struct {
	Pool        string
	Dataset     string
	Snapshot    string
	ToName      string
	FromGUID    string
	ToGUID      string
	Raw         bool
	Compressed  bool
	FeatureBits uint64
}

// ParseBegin parses the fixed dmu_replay_record_t DRR_BEGIN header at the
// start of an OpenZFS send stream without consuming the rest of the stream.
func ParseBegin(header []byte) (Begin, error) {
	if len(header) < BeginRecordSize {
		return Begin{}, fmt.Errorf("zfs stream header too short: got %d want %d", len(header), BeginRecordSize)
	}

	order, err := byteOrder(header)
	if err != nil {
		return Begin{}, err
	}
	if recordType := order.Uint32(header[0:4]); recordType != drrBegin {
		return Begin{}, fmt.Errorf("zfs stream starts with record type %d, not DRR_BEGIN", recordType)
	}

	versionInfo := order.Uint64(header[16:24])
	features := (versionInfo >> 2) & ((uint64(1) << 62) - 1)
	toGUID := order.Uint64(header[40:48])
	fromGUID := order.Uint64(header[48:56])
	toName := cString(header[56 : 56+maxNameLen])
	pool, dataset, snapshot, err := splitToName(toName)
	if err != nil {
		return Begin{}, err
	}

	return Begin{
		Pool:        pool,
		Dataset:     dataset,
		Snapshot:    snapshot,
		ToName:      toName,
		FromGUID:    formatGUID(fromGUID),
		ToGUID:      formatGUID(toGUID),
		Raw:         features&dmuBackupFeatureRaw != 0,
		Compressed:  features&dmuBackupFeatureCompressed != 0,
		FeatureBits: features,
	}, nil
}

func byteOrder(header []byte) (binary.ByteOrder, error) {
	if binary.LittleEndian.Uint64(header[8:16]) == dmuMagic {
		return binary.LittleEndian, nil
	}
	if binary.BigEndian.Uint64(header[8:16]) == dmuMagic {
		return binary.BigEndian, nil
	}
	return nil, fmt.Errorf("invalid zfs stream magic 0x%x", binary.LittleEndian.Uint64(header[8:16]))
}

func cString(data []byte) string {
	if i := strings.IndexByte(string(data), 0); i >= 0 {
		return string(data[:i])
	}
	return string(data)
}

func splitToName(toName string) (pool, dataset, snapshot string, err error) {
	at := strings.LastIndex(toName, "@")
	if at <= 0 || at == len(toName)-1 {
		return "", "", "", fmt.Errorf("invalid zfs stream toname %q", toName)
	}
	fs := toName[:at]
	snapshot = toName[at+1:]
	slash := strings.Index(fs, "/")
	if slash <= 0 || slash == len(fs)-1 {
		return "", "", "", fmt.Errorf("zfs stream toname %q must include pool/dataset@snapshot", toName)
	}
	return fs[:slash], fs[slash+1:], snapshot, nil
}

func formatGUID(guid uint64) string {
	if guid == 0 {
		return "0"
	}
	return fmt.Sprintf("0x%x", guid)
}
