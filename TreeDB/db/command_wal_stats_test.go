package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestCommandWALStatsProveModeAndFrames(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, CommandWALStatsScan: true, DisableBackgroundPrune: true, WALMaxSegmentBytes: 1024})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	dbClosed := false
	t.Cleanup(func() {
		if !dbClosed {
			_ = db.Close()
		}
	})
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.command_wal.enabled"]; got != "true" {
		t.Fatalf("command WAL enabled stat=%q, want true (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.required_feature"]; got != "true" {
		t.Fatalf("command WAL required feature stat=%q, want true (stats=%#v)", got, stats)
	}
	if got := stats["treedb.applied_command_lsn"]; got != "1" {
		t.Fatalf("applied command LSN stat=%q, want 1 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.frames"]; got != "1" {
		t.Fatalf("command WAL frame stat=%q, want 1 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.typed_segments"]; got != "1" {
		t.Fatalf("command WAL typed segment stat=%q, want 1 (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.stats_error"]; got != "" {
		t.Fatalf("unexpected command WAL stats error %q (stats=%#v)", got, stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	dbClosed = true
}

func TestCommandWALStatsDefaultDoesNotScanWAL(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	dbClosed := false
	t.Cleanup(func() {
		if !dbClosed {
			_ = db.Close()
		}
	})
	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	_ = b.Close()
	stats := db.Stats()
	if got := stats["treedb.command_wal.required_feature"]; got != "true" {
		t.Fatalf("required feature stat=%q, want true (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.stats_scan"]; got != "false" {
		t.Fatalf("stats_scan=%q, want false (stats=%#v)", got, stats)
	}
	if got := stats["treedb.command_wal.frames"]; got != "0" {
		t.Fatalf("frames=%q, want 0 without diagnostic scan (stats=%#v)", got, stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	dbClosed = true
}

func TestCommandWALStatsDisabledDoesNotScanWAL(t *testing.T) {
	dir := t.TempDir()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll WAL dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "commit-l0-000001.log"), []byte{0x01, 0x02, 0x03}, 0o600); err != nil {
		t.Fatalf("WriteFile bogus WAL: %v", err)
	}
	stats := make(map[string]string)
	writeCommandWALStats(stats, &DB{dir: dir})
	if got := stats["treedb.command_wal.stats_error"]; got != "" {
		t.Fatalf("disabled command WAL stats scanned WAL and errored: %q", got)
	}
	if got := stats["treedb.command_wal.frames"]; got != "0" {
		t.Fatalf("frames=%q, want 0 for disabled command WAL", got)
	}
	if got := stats["treedb.command_wal.stats_scan"]; got != "false" {
		t.Fatalf("stats_scan=%q, want false for disabled command WAL", got)
	}
}

func TestCommandWALRIDCacheBounded(t *testing.T) {
	db := &DB{}
	for i := 0; i < commandWALRIDCacheMaxEntries+17; i++ {
		ptr := page.ValuePtr{FileID: 1, Offset: uint64(i), Length: 1}
		db.storeCachedCommandWALValueLogRID(ptr, uint64(i+1))
	}
	if db.commandWALRIDCacheN > commandWALRIDCacheMaxEntries {
		t.Fatalf("command WAL RID cache size=%d, want <= %d", db.commandWALRIDCacheN, commandWALRIDCacheMaxEntries)
	}
	latest := page.ValuePtr{FileID: 1, Offset: uint64(commandWALRIDCacheMaxEntries + 16), Length: 1}
	if rid, ok := db.lookupCachedCommandWALValueLogRID(latest); !ok || rid != uint64(commandWALRIDCacheMaxEntries+17) {
		t.Fatalf("latest cached RID=(%d,%t), want (%d,true)", rid, ok, commandWALRIDCacheMaxEntries+17)
	}
}
