package caching

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

const recordFlagGroupedTest byte = 1 << 0

func scanGroupedFrameCounts(t *testing.T, path string) (groupedFrames, groupedSubrecords, ungroupedRecords, maxGrouped int) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	var payloadBuf []byte
	for {
		var header [valuelog.HeaderSize]byte
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("read header: %v", err)
		}
		valueLen := int(binary.LittleEndian.Uint32(header[16:20]))
		if cap(payloadBuf) < valueLen {
			payloadBuf = make([]byte, valueLen)
		}
		payload := payloadBuf[:valueLen]
		if _, err := io.ReadFull(r, payload); err != nil {
			t.Fatalf("read payload: %v", err)
		}
		if header[5]&recordFlagGroupedTest == 0 {
			ungroupedRecords++
			continue
		}
		_, _, offsets, _, err := valuelog.DecodeFrame(payload)
		if err != nil {
			t.Fatalf("DecodeFrame: %v", err)
		}
		groupedFrames++
		subrecs := len(offsets) - 1
		groupedSubrecords += subrecs
		if subrecs > maxGrouped {
			maxGrouped = subrecs
		}
	}
	return groupedFrames, groupedSubrecords, ungroupedRecords, maxGrouped
}

func TestChooseValueLogBlockWriteK_LargePayloadUsesExpandedTarget(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockTargetBytes: 4096,
	}
	l := &lane{}

	const (
		records         = 32
		avgPayloadBytes = 42 << 10
	)
	rawPayloadBytes := records * avgPayloadBytes
	got := db.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecLZ4)
	wantTargetBytes := valuelog.NormalizeBlockTargetCompressedBytes(avgPayloadBytes * largePayloadBlockTargetMultiplier)
	want := valuelog.ChooseBlockGroupK(records, rawPayloadBytes, wantTargetBytes, largePayloadBlockBootstrapRatio)
	if got != want {
		t.Fatalf("expected large-payload K to use expanded target, got=%d want=%d", got, want)
	}
	if got <= 1 {
		t.Fatalf("expected large payload K > 1, got %d", got)
	}
}

func TestAppendValueLogForRecords_LargePayloadBatchGroupsFrames(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value.log")
	writer, err := valuelog.NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	db := &DB{
		closeCh:                  make(chan struct{}),
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockCodec:       valuelog.BlockCodecLZ4,
		valueLogBlockTargetBytes: 4096,
		lanes: []lane{
			{id: 0, vlog: writer},
		},
	}

	value := bytes.Repeat([]byte("repeatable-large-payload-"), 1792)
	if len(value) < (42 << 10) {
		value = append(value, bytes.Repeat([]byte("Z"), (42<<10)-len(value))...)
	}
	value = value[:42<<10]
	records := make([]valuelog.Record, 16)
	for i := range records {
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: append([]byte(nil), value...)}
	}

	ptrs, err := db.appendValueLog(&db.lanes[0], 0, nil, records, journalDurabilityFlush)
	if err != nil {
		t.Fatalf("appendValueLog: %v", err)
	}
	if len(ptrs) != len(records) {
		t.Fatalf("expected %d ptrs, got %d", len(records), len(ptrs))
	}

	groupedFrames, groupedSubrecords, ungroupedRecords, maxGrouped := scanGroupedFrameCounts(t, path)
	if groupedSubrecords != len(records) {
		t.Fatalf("expected grouped subrecords=%d, got %d", len(records), groupedSubrecords)
	}
	if ungroupedRecords != 0 {
		t.Fatalf("expected no ungrouped records, got %d", ungroupedRecords)
	}
	if groupedFrames >= len(records) {
		t.Fatalf("expected fewer grouped frames than records, frames=%d records=%d", groupedFrames, len(records))
	}
	if maxGrouped < 8 {
		t.Fatalf("expected max grouped frame >= 8 subrecords, got %d", maxGrouped)
	}
}

func TestChooseValueLogBlockWriteK_LargePayloadBootstrapKeepsSelectorIncompressibleSignal(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionAuto),
		valueLogAutoPolicy:       uint8(vlogAutoBalanced),
		valueLogBlockTargetBytes: 4096,
		valueLogThreshold:        1 << 20,
	}
	selector := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	selector.dwellBytes = 0
	selector.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 42<<10, 42<<10, 1000, false)
	l := &lane{vlogCompressionSelector: selector}

	got := db.chooseValueLogBlockWriteK(l, 16, 16*(42<<10), valuelog.BlockCodecSnappy)
	if got != 1 {
		t.Fatalf("expected incompressible selector signal to keep k=1, got %d", got)
	}
}
