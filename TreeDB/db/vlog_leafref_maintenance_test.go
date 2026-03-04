package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestValueLogGC_WithLeafPagesInValueLog_KeepsReferencedLeafSegments(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		_ = db.Close()
		t.Fatalf("mkdir wal: %v", err)
	}

	leafLog := newRewriteWriter(walDir, 0, 0, 16<<10)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	defer func() {
		_ = leafLog.Close()
		_ = db.Close()
	}()

	// Inline values so that leaf pages are the only value-log references.
	value := bytes.Repeat([]byte("v"), 32)
	for i := 0; i < 512; i++ {
		key := []byte(fmt.Sprintf("k%06d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
	}

	referenced, err := db.referencedValueLogSegments(context.Background())
	if err != nil {
		t.Fatalf("referencedValueLogSegments: %v", err)
	}
	if len(referenced) == 0 {
		t.Fatalf("expected non-empty referenced value-log segments with leaf pages in vlog")
	}

	refPaths := make([]string, 0, len(referenced))
	for id := range referenced {
		refPaths = append(refPaths, db.valueLogManager.SegmentPath(id))
	}

	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	for _, path := range refPaths {
		if path == "" {
			t.Fatalf("expected non-empty referenced segment path")
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("referenced segment removed unexpectedly: %s err=%v", path, err)
		}
	}
}

func TestValueLogRewriteOffline_PreservesLeafPagesInValueLogFormatConfig(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		_ = db.Close()
		t.Fatalf("mkdir wal: %v", err)
	}

	leafLog := newRewriteWriter(walDir, 0, 0, 0)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	// Keep the tree as a single leaf so the root page ID itself is a leaf ref.
	value := bytes.Repeat([]byte("v"), 16)
	for i := 0; i < 32; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		if err := db.Set(key, value); err != nil {
			_ = leafLog.Close()
			_ = db.Close()
			t.Fatalf("Set(%q): %v", key, err)
		}
	}

	state := db.State()
	if state == nil {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("missing db state")
	}
	if _, ok := page.DecodeLeafRef(state.RootPageID); !ok {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("expected leaf-ref root page with leaf pages in vlog; root=%d", state.RootPageID)
	}

	if err := leafLog.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("leafLog close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}

	// Preserve leaf-page-in-vlog mode via format.json (Options should not need to
	// restate it here).
	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	state2 := reopen.State()
	if state2 == nil {
		t.Fatalf("missing state after rewrite reopen")
	}
	if _, ok := page.DecodeLeafRef(state2.RootPageID); !ok {
		t.Fatalf("expected leaf-ref root page after rewrite; root=%d", state2.RootPageID)
	}

	got, err := reopen.Get([]byte("k0000"))
	if err != nil {
		t.Fatalf("Get(k0000): %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value mismatch after rewrite")
	}
}

func TestValueLogRewriteOnline_WithLeafPagesInValueLog_ReopenPreservesData(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	leafLog := newRewriteWriter(filepath.Join(dir, "wal"), 0, 0, 64<<10)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)

	ptrs := appendPointersInNewSegment(t, dir, 42, 1, 100_000, 8, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i + 1)}, 1024)
	})
	b := db.NewBatch().(*Batch)
	for i := range ptrs {
		if err := b.SetPointer([]byte{byte('a' + i)}, ptrs[i]); err != nil {
			_ = leafLog.Close()
			_ = db.Close()
			t.Fatalf("SetPointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		BatchSize:     2,
		SyncEachBatch: true,
	}); err != nil {
		_ = leafLog.Close()
		_ = db.Close()
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("leafLog close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db close: %v", err)
	}

	reopen, err := Open(Options{
		Dir:                        dir,
		ReadOnly:                   true,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	for i := range ptrs {
		key := []byte{byte('a' + i)}
		got, err := reopen.Get(key)
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		want := bytes.Repeat([]byte{byte(i + 1)}, 1024)
		if !bytes.Equal(got, want) {
			t.Fatalf("value mismatch for %q", key)
		}
	}
}
