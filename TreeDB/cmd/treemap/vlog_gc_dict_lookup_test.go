package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
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
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.FlushThreshold = 1 << 20
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.Compression = treedb.ValueLogCompressionAuto
	opts.ValueLog.AutoPolicy = treedb.ValueLogAutoSize
	opts.ValueLog.CompressionAutotune = treedb.AutotuneOptions{Mode: treedb.AutotuneOff}
	// Keep dict training/publish deterministic across hosts.
	opts.ValueLog.DictAdaptiveRatio = -1
	opts.NotifyError = func(err error) {
		select {
		case bgErrCh <- err:
		default:
		}
	}
	treedb.EnableValueLogDictCompression(&opts)

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})

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
	if testDeadline, ok := t.Deadline(); ok {
		testDeadline = testDeadline.Add(-2 * time.Second)
		if testDeadline.Before(deadline) {
			deadline = testDeadline
		}
	}
	published := false
	var lastStats map[string]string
	var lastDictID string
	for time.Now().Before(deadline) {
		select {
		case err := <-bgErrCh:
			_ = db.Close()
			t.Fatalf("background error: %v", err)
		default:
		}
		stats := db.Stats()
		if stats != nil {
			lastStats = stats
			rawDictID, ok := stats["treedb.cache.vlog_dict.last_applied_dict_id"]
			if ok {
				lastDictID = rawDictID
				dictID, parseErr := strconv.ParseUint(rawDictID, 10, 64)
				if parseErr != nil {
					_ = db.Close()
					t.Fatalf("invalid last_applied_dict_id stat %q: %v (all stats: %#v)", rawDictID, parseErr, stats)
				}
				if dictID > 0 {
					published = true
					break
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !published {
		_ = db.Close()
		if lastStats != nil {
			t.Fatalf("timed out waiting for dict publish: last_applied_dict_id=%q stats=%#v",
				lastDictID, lastStats)
		}
		t.Fatalf("timed out waiting for dict publish: stats=%#v", lastStats)
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
	rawDictFrames, ok := stats["treedb.cache.vlog_auto.frames.dict"]
	if !ok {
		_ = db.Close()
		t.Fatalf("missing treedb.cache.vlog_auto.frames.dict stat: %#v", stats)
	}
	dictFrames, err := strconv.ParseUint(rawDictFrames, 10, 64)
	if err != nil {
		_ = db.Close()
		t.Fatalf("invalid treedb.cache.vlog_auto.frames.dict stat %q: %v (all stats: %#v)", rawDictFrames, err, stats)
	}
	if dictFrames == 0 {
		_ = db.Close()
		t.Fatalf("expected at least one dict frame, got frames.dict=%q last_applied_dict_id=%q",
			rawDictFrames,
			stats["treedb.cache.vlog_dict.last_applied_dict_id"],
		)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db = nil

	// Direct backend open should succeed for finished WAL-off DBs even without an
	// explicit DictLookup, because there are no commit-log segments to replay.
	// The maintenance path still uses OpenBackend below so future reads that do
	// require side-store lookups are wired correctly.
	backendDir, err := resolveTreemapMainDir(dir)
	if err != nil {
		t.Fatalf("resolveTreemapMainDir: %v", err)
	}
	backendOpts := treedb.OptionsFor(treedb.ProfileNoWALFast, backendDir)
	backendOpts.ReadOnly = false
	cfg, ok, err := treedbdb.LoadFormatConfig(backendDir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if ok {
		cfg.ApplyIndexFormatToOptions(&backendOpts)
	}
	if backend, err := treedbdb.Open(backendOpts); err != nil {
		t.Fatalf("backend Open: %v", err)
	} else if err := backend.Close(); err != nil {
		t.Fatalf("backend Close: %v", err)
	}

	openBackendOpts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	openBackendOpts.ReadOnly = false
	backend, cleanup, err := treedb.OpenBackend(openBackendOpts)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = cleanup() })

	if _, err := backend.ValueLogGC(context.Background(), treedbdb.ValueLogGCOptions{DryRun: true}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
}

func TestResolveTreeDBRootDir_DictDBDirUsesParentRoot(t *testing.T) {
	root := t.TempDir()
	mainDir := filepath.Join(root, "maindb")
	dictDir := filepath.Join(root, "dictdb")
	if err := os.MkdirAll(mainDir, 0o755); err != nil {
		t.Fatalf("mkdir maindb: %v", err)
	}
	if err := os.MkdirAll(dictDir, 0o755); err != nil {
		t.Fatalf("mkdir dictdb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mainDir, "index.db"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write maindb/index.db: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dictDir, "index.db"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write dictdb/index.db: %v", err)
	}
	if got := resolveTreeDBRootDir(dictDir); got != root {
		t.Fatalf("resolveTreeDBRootDir(%q)=%q want %q", dictDir, got, root)
	}
}
