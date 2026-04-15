package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func waitForPathRemoval(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		_, err := os.Stat(path)
		if err == nil {
			if time.Now().After(deadline) {
				return fmt.Errorf("path still exists after %s: %s", timeout, path)
			}
			time.Sleep(25 * time.Millisecond)
			continue
		}
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
}

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

	walDir := filepath.Join(dir, "value_vlog")
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

	walDir := filepath.Join(dir, "value_vlog")
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
