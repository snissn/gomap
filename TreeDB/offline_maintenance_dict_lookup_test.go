package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestVacuumIndexOffline_WithDictFrames_WiresDictLookup(t *testing.T) {
	dir := t.TempDir()
	bgErrCh := make(chan error, 16)

	opts := Options{
		Dir:            dir,
		FlushThreshold: 1 << 20,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
			Compression:      ValueLogCompressionAuto,
			AutoPolicy:       ValueLogAutoSize,
			CompressionAutotune: AutotuneOptions{
				Mode: AutotuneOff,
			},
			// Keep dict training/publish deterministic across hosts.
			DictAdaptiveRatio: -1,
		},
		NotifyError: func(err error) {
			select {
			case bgErrCh <- err:
			default:
			}
		},
	}
	EnableValueLogDictCompression(&opts)

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const valueSize = 16 << 10
	base := bytes.Repeat([]byte("compressible-"), valueSize/len("compressible-")+1)[:valueSize]

	writeBatch := func(prefix string, n int) {
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("%s%06d", prefix, i))
			val := make([]byte, valueSize)
			copy(val, base)
			binary.LittleEndian.PutUint32(val[valueSize-4:], uint32(i))
			if err := db.Set(key, val); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
	}

	// Phase 1: publish a dictionary.
	writeBatch("a", 128) // ~2MiB
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint(phase1): %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-bgErrCh:
			_ = db.Close()
			t.Fatalf("background error: %v", err)
		default:
		}
		stats := db.Stats()
		if stats != nil && stats["treedb.cache.vlog_dict.last_applied_dict_id"] != "0" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Phase 2: write enough bytes for auto mode to emit dict frames.
	writeBatch("b", 512) // ~8MiB
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint(phase2): %v", err)
	}
	stats := db.Stats()
	if stats == nil {
		_ = db.Close()
		t.Fatalf("Stats: nil")
	}
	dictFrames, _ := strconv.ParseUint(stats["treedb.cache.vlog_auto.frames.dict"], 10, 64)
	if dictFrames == 0 {
		_ = db.Close()
		t.Fatalf("expected at least one dict frame, got frames.dict=%q last_applied_dict_id=%q",
			stats["treedb.cache.vlog_auto.frames.dict"],
			stats["treedb.cache.vlog_dict.last_applied_dict_id"],
		)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := VacuumIndexOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("VacuumIndexOffline: %v", err)
	}
}
