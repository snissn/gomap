package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

func TestVlogGC_BackendOpenWithDictFrames_WiresDictLookup(t *testing.T) {
	dir := t.TempDir()
	bgErrCh := make(chan error, 16)
	opts := treedb.Options{
		Dir:            dir,
		FlushThreshold: 1 << 20,
		ValueLog: treedb.ValueLogOptions{
			ForcePointers:    true,
			PointerThreshold: 1,
			Compression:      treedb.ValueLogCompressionAuto,
			AutoPolicy:       treedb.ValueLogAutoSize,
			CompressionAutotune: treedb.AutotuneOptions{
				Mode: treedb.AutotuneOff,
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
	treedb.EnableValueLogDictCompression(&opts)

	db, err := treedb.Open(opts)
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

	// Phase 2: write enough bytes for auto mode to probe dict frames.
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

	// Direct backend open should succeed for finished WAL-off DBs even without an
	// explicit DictLookup, because there are no commit-log segments to replay.
	// The maintenance path still uses OpenBackend below so future reads that do
	// require side-store lookups are wired correctly.
	backendDir := filepath.Join(dir, "maindb")
	backendOpts := treedbdb.Options{Dir: backendDir, ReadOnly: false}
	if cfg, ok, err := treedbdb.LoadFormatConfig(backendDir); err == nil && ok {
		cfg.ApplyIndexFormatToOptions(&backendOpts)
	}
	if backend, err := treedbdb.Open(backendOpts); err != nil {
		t.Fatalf("backend Open: %v", err)
	} else if err := backend.Close(); err != nil {
		t.Fatalf("backend Close: %v", err)
	}

	backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: dir, ReadOnly: false})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })

	if _, err := backend.ValueLogGC(context.Background(), treedbdb.ValueLogGCOptions{DryRun: true}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
}
