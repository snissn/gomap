package valuelog

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

func TestReaderReadNextMeta_NoAllocsAfterWarm_DiscardWithChecksum(t *testing.T) {
	record := buildNonGroupedRecord(1234, make([]byte, discardScratchSize*4+1))

	br := bytes.NewReader(record)
	rr := bufio.NewReaderSize(br, len(record))
	r := &Reader{
		r:              rr,
		fileID:         1,
		verifies:       true,
		decodeValues:   false,
		discardScratch: make([]byte, discardScratchSize),
	}

	resetAllocs := testing.AllocsPerRun(100, func() {
		br.Reset(record)
		rr.Reset(br)
		r.pos = 0
		r.pending = nil
		r.pendingIndex = 0
	})

	readAllocs := testing.AllocsPerRun(100, func() {
		br.Reset(record)
		rr.Reset(br)
		r.pos = 0
		r.pending = nil
		r.pendingIndex = 0

		gotRID, _, err := r.ReadNextMeta()
		if err != nil {
			t.Fatalf("ReadNextMeta: %v", err)
		}
		if gotRID != 1234 {
			t.Fatalf("RID=%d, want %d", gotRID, 1234)
		}
	})
	if delta := readAllocs - resetAllocs; delta > 0 {
		t.Fatalf("ReadNextMeta adds allocs/run=%v (reset=%v, read=%v), want 0", delta, resetAllocs, readAllocs)
	}
}

func BenchmarkReaderReadNextMeta_DiscardWithChecksum(b *testing.B) {
	record := buildNonGroupedRecord(1234, make([]byte, discardScratchSize*4+1))

	br := bytes.NewReader(record)
	rr := bufio.NewReaderSize(br, len(record))
	r := &Reader{
		r:              rr,
		fileID:         1,
		verifies:       true,
		decodeValues:   false,
		discardScratch: make([]byte, discardScratchSize),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		br.Reset(record)
		rr.Reset(br)
		r.pos = 0
		r.pending = nil
		r.pendingIndex = 0

		_, _, err := r.ReadNextMeta()
		if err != nil {
			b.Fatalf("ReadNextMeta: %v", err)
		}
	}
}

func BenchmarkReaderReadNextMeta_GroupedDiscardWithChecksum(b *testing.B) {
	record := buildGroupedRecordK1(1234, make([]byte, discardScratchSize*4+1))

	br := bytes.NewReader(record)
	rr := bufio.NewReaderSize(br, len(record))
	r := &Reader{
		r:              rr,
		fileID:         1,
		verifies:       true,
		decodeValues:   false,
		pending:        make([]frameEntry, 0, 1),
		discardScratch: make([]byte, discardScratchSize),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		br.Reset(record)
		rr.Reset(br)
		r.pos = 0
		r.pending = r.pending[:0]
		r.pendingIndex = 0

		_, _, err := r.ReadNextMeta()
		if err != nil {
			b.Fatalf("ReadNextMeta: %v", err)
		}
	}
}

func buildNonGroupedRecord(rid uint64, payload []byte) []byte {
	var header [HeaderSize]byte
	header[4] = Version
	binary.LittleEndian.PutUint64(header[8:16], rid)
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(payload)))
	sum := crc.ChecksumParts(header[4:], payload)
	binary.LittleEndian.PutUint32(header[0:4], sum)

	record := make([]byte, 0, HeaderSize+len(payload))
	record = append(record, header[:]...)
	record = append(record, payload...)
	return record
}

func buildGroupedRecordK1(rid uint64, value []byte) []byte {
	// Frame: [FrameHeader][RID][offset0][offset1][value bytes]
	var frameHeader [FrameHeaderSize]byte
	frameHeader[0] = FrameVersion
	frameHeader[1] = 0
	frameHeader[2] = 1
	frameHeader[3] = 0
	binary.LittleEndian.PutUint64(frameHeader[4:12], 0)

	var ridBytes [8]byte
	binary.LittleEndian.PutUint64(ridBytes[:], rid)

	var offsets [8]byte
	binary.LittleEndian.PutUint32(offsets[0:4], 0)
	binary.LittleEndian.PutUint32(offsets[4:8], uint32(len(value)))

	frame := make([]byte, 0, FrameHeaderSize+len(ridBytes)+len(offsets)+len(value))
	frame = append(frame, frameHeader[:]...)
	frame = append(frame, ridBytes[:]...)
	frame = append(frame, offsets[:]...)
	frame = append(frame, value...)

	var header [HeaderSize]byte
	header[4] = Version
	header[5] = recordFlagGrouped
	binary.LittleEndian.PutUint64(header[8:16], 0)
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(frame)))
	sum := crc.ChecksumParts(header[4:], frame)
	binary.LittleEndian.PutUint32(header[0:4], sum)

	record := make([]byte, 0, HeaderSize+len(frame))
	record = append(record, header[:]...)
	record = append(record, frame...)
	return record
}
