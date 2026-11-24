package memtable

import (
	"encoding/binary"
	"strings"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// cowValue stores metadata and payload in one owned immutable string. Tree
// copies duplicate only its string header; roots share the backing exactly as
// they shared the previous owned payload string. This is an in-memory encoding,
// independent of the persistent leaf/value-log formats.
type cowValue string

const cowValueMetadataBytes = 25 // ValuePtr16, EntryRevision8, flags1

var (
	cowEmptyInlineValue    = cowEncodeEmptyFlagValue(node.FlagInline)
	cowEmptyTombstoneValue = cowEncodeEmptyFlagValue(node.FlagTombstone)
)

func cowEncodeEmptyFlagValue(flags byte) cowValue {
	var metadata [cowValueMetadataBytes]byte
	metadata[24] = flags
	return cowValue(metadata[:])
}

func cowValueSize(payloadBytes int) (int, bool) {
	if payloadBytes < 0 || payloadBytes > int(^uint(0)>>1)-cowValueMetadataBytes {
		return 0, false
	}
	return cowValueMetadataBytes + payloadBytes, true
}

func cowIsEmptyConstantValue(e COWMutation) (cowValue, bool) {
	if len(e.Value) == 0 && e.Ptr == (page.ValuePtr{}) && e.Revision == 0 {
		switch e.Flags {
		case node.FlagInline:
			return cowEmptyInlineValue, true
		case node.FlagTombstone:
			return cowEmptyTombstoneValue, true
		}
	}
	return "", false
}

// cowEncodeValue is called only after Prepare admits the rounded encoded size
// and cowValueSize checks int overflow. Grow creates one backing for both the
// metadata and payload; String transfers its immutable alias without a copy.
func cowEncodeValue(e COWMutation) cowValue {
	if v, ok := cowIsEmptyConstantValue(e); ok {
		return v
	}
	var metadata [cowValueMetadataBytes]byte
	binary.LittleEndian.PutUint64(metadata[0:8], e.Ptr.Offset)
	binary.LittleEndian.PutUint32(metadata[8:12], e.Ptr.Length)
	binary.LittleEndian.PutUint32(metadata[12:16], e.Ptr.FileID)
	binary.LittleEndian.PutUint64(metadata[16:24], uint64(e.Revision))
	metadata[24] = e.Flags
	var value strings.Builder
	value.Grow(cowValueMetadataBytes + len(e.Value))
	value.Write(metadata[:])
	value.Write(e.Value)
	return cowValue(value.String())
}

// cowEncodeValueInto encodes e into a pre-allocated batch arena when non-empty,
// returning the encoded cowValue and updated arena offset.
func cowEncodeValueInto(arena []byte, off int, e COWMutation) (cowValue, int) {
	if v, ok := cowIsEmptyConstantValue(e); ok {
		return v, off
	}
	n := cowValueMetadataBytes + len(e.Value)
	dst := arena[off : off+n]
	binary.LittleEndian.PutUint64(dst[0:8], e.Ptr.Offset)
	binary.LittleEndian.PutUint32(dst[8:12], e.Ptr.Length)
	binary.LittleEndian.PutUint32(dst[12:16], e.Ptr.FileID)
	binary.LittleEndian.PutUint64(dst[16:24], uint64(e.Revision))
	dst[24] = e.Flags
	copy(dst[cowValueMetadataBytes:], e.Value)
	return cowValue(bytesToStringNoCopy(dst)), off + n
}

// record borrows the immutable encoding through its owning root/view. Payload
// slicing and fixed-width metadata decoding allocate no temporary byte slices.
// Only present, writer-encoded values may be decoded; the missing value is "".
func (v cowValue) record(key string) COWRecord {
	return COWRecord{
		Key: key, Value: string(v[cowValueMetadataBytes:]),
		Ptr: page.ValuePtr{
			Offset: cowValueUint64(v[0:8]),
			Length: cowValueUint32(v[8:12]),
			FileID: cowValueUint32(v[12:16]),
		},
		Revision: page.EntryRevision(cowValueUint64(v[16:24])),
		Flags:    v[24],
	}
}

func cowValueUint32(v cowValue) uint32 {
	_ = v[3]
	return uint32(v[0]) | uint32(v[1])<<8 | uint32(v[2])<<16 | uint32(v[3])<<24
}

func cowValueUint64(v cowValue) uint64 {
	_ = v[7]
	return uint64(v[0]) | uint64(v[1])<<8 | uint64(v[2])<<16 | uint64(v[3])<<24 |
		uint64(v[4])<<32 | uint64(v[5])<<40 | uint64(v[6])<<48 | uint64(v[7])<<56
}
