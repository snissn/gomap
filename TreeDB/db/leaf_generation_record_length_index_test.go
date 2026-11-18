package db

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// legacyLeafLengthIndexFixture encodes the historical TLGI V1 layout from the
// actual records in a small test segment. Production no longer writes it.
func legacyLeafLengthIndexFixture(t *testing.T, dir string, rawFileID uint32) []byte {
	t.Helper()
	path := leafGenerationFallbackPath(dir, rawFileID)
	segment, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	index := make([]byte, 16)
	copy(index, "TLGI")
	binary.LittleEndian.PutUint32(index[4:8], 1)
	binary.LittleEndian.PutUint32(index[8:12], rawFileID)
	for start := 0; start < len(segment); {
		if len(segment)-start < valuelog.HeaderSize || segment[start+4] != valuelog.Version {
			t.Fatalf("invalid test leaf header in %q at %d", path, start)
		}
		bodyLength := uint64(binary.LittleEndian.Uint32(segment[start+16 : start+20]))
		if bodyLength > uint64(len(segment)-start-valuelog.HeaderSize) {
			t.Fatalf("incomplete test leaf record in %q at %d", path, start)
		}
		recordLength := valuelog.HeaderSize + int(bodyLength)
		index = binary.LittleEndian.AppendUint32(index, uint32(start+4))
		index = binary.LittleEndian.AppendUint32(index, uint32(recordLength-4))
		start += recordLength
	}
	if len(index) == 16 {
		t.Fatalf("no records in test leaf segment %q", path)
	}
	binary.LittleEndian.PutUint32(index[12:16], uint32((len(index)-16)/8))
	return index
}
