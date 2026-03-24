package caching

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func readValueLogFrameHeaderAtPathPtr(t *testing.T, path string, ptr page.ValuePtr) valuelog.FrameHeader {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	start := int64(ptr.Offset - 4)
	var header [valuelog.HeaderSize + valuelog.FrameHeaderSize]byte
	if _, err := f.ReadAt(header[:], start); err != nil {
		t.Fatalf("ReadAt(%s,%d): %v", path, start, err)
	}
	if header[4] != valuelog.Version {
		t.Fatalf("unexpected record version %d", header[4])
	}
	if header[5]&1 == 0 {
		t.Fatalf("expected grouped record")
	}
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if valueLen < valuelog.FrameHeaderSize {
		t.Fatalf("grouped record too short: %d", valueLen)
	}
	return valuelog.FrameHeader{
		Version:  header[valuelog.HeaderSize+0],
		Flags:    header[valuelog.HeaderSize+1],
		K:        header[valuelog.HeaderSize+2],
		Reserved: header[valuelog.HeaderSize+3],
		DictID:   binary.LittleEndian.Uint64(header[valuelog.HeaderSize+4 : valuelog.HeaderSize+12]),
	}
}

func targetOrdinaryValue(seed byte, n int) []byte {
	v := make([]byte, n)
	copy(v, []byte("staking-cohort-value:"))
	for i := 24; i < len(v); i++ {
		v[i] = seed + byte(i%7)
	}
	return v
}

func TestAppendValueLogOne_AutoTargetCohortUsesGroupedConfiguredBlockCodec(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	db := &DB{
		closeCh:                 make(chan struct{}),
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
		lanes: []lane{
			{id: 0, vlog: writer, vlogPath: path, vlogSeq: 1},
		},
	}
	db.lanes[0].vlogCh = make(chan vlogWriteRequest, vlogWriteBuffer)
	defer func() {
		close(db.closeCh)
		db.wg.Wait()
		_ = writer.Close()
	}()

	const valueSize = 43629
	const writes = 8

	start := make(chan struct{})
	ptrs := make([]page.ValuePtr, writes)
	errs := make([]error, writes)
	var wg sync.WaitGroup
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ptrs[i], _, errs[i] = db.appendValueLogOne(&db.lanes[0], 0, nil, uint64(i+1), targetOrdinaryValue(byte('a'+i), valueSize), journalDurabilityNone)
		}(i)
	}
	close(start)

	deadline := time.Now().Add(250 * time.Millisecond)
	for len(db.lanes[0].vlogCh) < writes && time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(100 * time.Microsecond)
	}
	if got := len(db.lanes[0].vlogCh); got != writes {
		t.Fatalf("queued requests=%d want %d before starting worker", got, writes)
	}

	db.wg.Add(1)
	go db.vlogWriteLoop(&db.lanes[0])
	wg.Wait()
	if err := db.flushValueLog(0); err != nil {
		t.Fatalf("flushValueLog: %v", err)
	}

	for i, err := range errs {
		if err != nil {
			t.Fatalf("appendValueLogOne[%d]: %v", i, err)
		}
	}

	sawGroupedBatch := false
	for _, ptr := range ptrs {
		frame := readValueLogFrameHeaderAtPathPtr(t, path, ptr)
		if frame.K > 1 || page.ValuePtrSubIndex(ptr) > 0 {
			sawGroupedBatch = true
		}
		if frame.DictID != 0 {
			t.Fatalf("unexpected dict frame dictID=%d", frame.DictID)
		}
		if frame.Flags&valuelog.FrameFlagCompressed != 0 && valuelog.BlockCodec(frame.Reserved) == valuelog.BlockCodecLZ4 {
			t.Fatalf("target cohort frame used lz4 block codec")
		}
	}
	if !sawGroupedBatch {
		t.Fatalf("expected at least one grouped target-cohort frame")
	}
}
