package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestForceValueLogPointers_UsesPointersForSmallValues(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:            dir,
		FlushThreshold: 1,
		ValueLog: ValueLogOptions{
			ForcePointers:    true,
			PointerThreshold: 1 << 20, // Large enough that small values would be inline without ForcePointers.
		},
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	key := []byte("k1")
	val := []byte("v1")
	if err := db.Set(key, val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	backend := db.backend
	snap := backend.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot nil")
	}
	entry, err := snap.GetEntry(key)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("GetEntry: %v", err)
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		_ = snap.Close()
		t.Fatalf("expected value-log pointer for small value, got flags=%#x file_id=%#x", entry.Flags, entry.ValuePtr.FileID)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot close: %v", err)
	}

	got, err := backend.Get(key)
	if err != nil {
		t.Fatalf("backend Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("backend Get mismatch")
	}
}

func TestValuePlacement_PerDomainThreshold_Respected(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:            dir,
		FlushThreshold: 1,
		ValueLog: ValueLogOptions{
			PointerThreshold: 256,
			DomainInlineThresholds: []ValueLogDomainThreshold{
				{Prefix: []byte("hot/"), InlineThreshold: 16},
				{Prefix: []byte("cold/"), InlineThreshold: 1024},
			},
		},
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	hotKey := []byte("hot/key")
	coldKey := []byte("cold/key")
	defaultKey := []byte("other/key")
	hotVal := bytes.Repeat([]byte("h"), 64)
	coldVal := bytes.Repeat([]byte("c"), 64)
	defaultVal := bytes.Repeat([]byte("d"), 300)

	if err := db.Set(hotKey, hotVal); err != nil {
		t.Fatalf("Set hot: %v", err)
	}
	if err := db.Set(coldKey, coldVal); err != nil {
		t.Fatalf("Set cold: %v", err)
	}
	if err := db.Set(defaultKey, defaultVal); err != nil {
		t.Fatalf("Set default: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	backend := db.backend
	snap := backend.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot nil")
	}
	defer func() {
		if err := snap.Close(); err != nil {
			t.Fatalf("snapshot close: %v", err)
		}
	}()

	hotEntry, err := snap.GetEntry(hotKey)
	if err != nil {
		t.Fatalf("GetEntry hot: %v", err)
	}
	if hotEntry.Flags&node.FlagPointer == 0 {
		t.Fatalf("expected hot domain key to use pointer placement")
	}

	coldEntry, err := snap.GetEntry(coldKey)
	if err != nil {
		t.Fatalf("GetEntry cold: %v", err)
	}
	if coldEntry.Flags&node.FlagPointer != 0 {
		t.Fatalf("expected cold domain key to stay inline")
	}

	defaultEntry, err := snap.GetEntry(defaultKey)
	if err != nil {
		t.Fatalf("GetEntry default: %v", err)
	}
	if defaultEntry.Flags&node.FlagPointer == 0 {
		t.Fatalf("expected non-domain key to use default threshold fallback")
	}

	gotHot, err := db.Get(hotKey)
	if err != nil || !bytes.Equal(gotHot, hotVal) {
		t.Fatalf("Get hot mismatch: err=%v", err)
	}
	gotCold, err := db.Get(coldKey)
	if err != nil || !bytes.Equal(gotCold, coldVal) {
		t.Fatalf("Get cold mismatch: err=%v", err)
	}
	gotDefault, err := db.Get(defaultKey)
	if err != nil || !bytes.Equal(gotDefault, defaultVal) {
		t.Fatalf("Get default mismatch: err=%v", err)
	}
}

func TestForceValueLogPointers_DictTrainingPublishesDictionary(t *testing.T) {
	dir := t.TempDir()
	bgErrCh := make(chan error, 16)
	opts := Options{
		Dir:            dir,
		FlushThreshold: 1 << 20,
		ValueLog: ValueLogOptions{
			ForcePointers:    true,
			PointerThreshold: 1,
			Compression:      ValueLogCompressionDict,
			DictTrain: TrainConfig{
				TrainBytes:     64 << 10,
				DictBytes:      8 << 10,
				MinRecords:     8,
				MaxRecordBytes: 16 << 10,
				SampleStride:   1,
				DedupWindow:    16,
			},
			// Keep this test deterministic: publish the first accepted dictionary
			// profile without autotune dwell/gain gating.
			CompressionAutotune: valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
			// Disable adaptive pause defaults so trainer publication can be observed
			// reliably under race/slow CI hosts.
			DictAdaptiveRatio: -1,
		},
		NotifyError: func(err error) {
			select {
			case bgErrCh <- err:
			default:
			}
		},
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	const valueSize = 16 << 10
	base := bytes.Repeat([]byte("compressible-"), valueSize/len("compressible-")+1)[:valueSize]

	for i := 0; i < 128; i++ {
		key := []byte(fmt.Sprintf("k%06d", i))
		val := make([]byte, valueSize)
		copy(val, base)
		binary.LittleEndian.PutUint32(val[valueSize-4:], uint32(i))
		if err := db.Set(key, val); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-bgErrCh:
			t.Fatalf("background error: %v", err)
		default:
		}
		stats := db.Stats()
		if stats != nil && stats["treedb.cache.vlog_dict.last_applied_dict_id"] != "0" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	stats := db.Stats()
	t.Fatalf("expected dict to publish, got last_applied_dict_id=%q frames_attempted=%q",
		stats["treedb.cache.vlog_dict.last_applied_dict_id"],
		stats["treedb.cache.vlog_dict.frames_attempted"],
	)
}
