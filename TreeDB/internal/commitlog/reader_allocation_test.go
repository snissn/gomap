package commitlog

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

func TestValidatedWALScanReusesSegmentHeader(t *testing.T) {
	// Read the same small on-disk stream frame repeatedly. The validated scan
	// owns its scratch space and only returns an LSN.
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: []byte("k"), Value: []byte("v")}})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := EncodeCommandFrameV2(CommandEnvelope{
		LSN: 7, DurabilityClass: CommandDurabilityRelaxed, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded := make([]byte, segmentHeaderSize+len(frame))
	binary.LittleEndian.PutUint32(encoded[:4], uint32(len(frame)))
	binary.LittleEndian.PutUint32(encoded[4:8], crc.Checksum(frame))
	copy(encoded[segmentHeaderSize:], frame)
	source := bytes.NewReader(encoded)
	r := &Reader{input: source, maxSegmentSize: normalizeMaxSegmentSize(0)}
	scan := func() {
		source.Reset(encoded)
		if lsn, err := r.ReadValidatedCommandFrameLSN(); err != nil || lsn != 7 {
			t.Fatalf("scan returned LSN %d, error %v", lsn, err)
		}
	}
	if got := testing.AllocsPerRun(100, scan); got != 0 {
		t.Fatalf("steady-state allocations per validated stream frame = %v, want 0", got)
	}
}
