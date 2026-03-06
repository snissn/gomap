package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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
	published := false
	for time.Now().Before(deadline) {
		select {
		case err := <-bgErrCh:
			_ = db.Close()
			t.Fatalf("background error: %v", err)
		default:
		}
		stats := db.Stats()
		if stats != nil {
			rawDictID, ok := stats["treedb.cache.vlog_dict.last_applied_dict_id"]
			if ok {
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
		stats := db.Stats()
		_ = db.Close()
		t.Fatalf("timed out waiting for dict publish: last_applied_dict_id=%q stats=%#v",
			stats["treedb.cache.vlog_dict.last_applied_dict_id"], stats)
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

	// This mirrors treemap's previous behavior: backend open without DictLookup
	// fails when WAL/value-log segments contain dict-compressed frames.
	backendDir := resolveMainDBDir(dir)
	backendOpts := treedbdb.Options{Dir: backendDir, ReadOnly: false}
	applyPersistedFormatConfig(dir, &backendOpts)
	if backend, err := treedbdb.Open(backendOpts); err == nil {
		_ = backend.Close()
		t.Fatalf("expected backend Open to fail without DictLookup")
	} else if !errors.Is(err, valuelog.ErrMissingDict) && !strings.Contains(err.Error(), valuelog.ErrMissingDict.Error()) {
		t.Fatalf("expected ErrMissingDict, got %v", err)
	}

	backend, cleanup, err := openBackendForVlogGC(dir)
	if err != nil {
		t.Fatalf("openBackendForVlogGC: %v", err)
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
