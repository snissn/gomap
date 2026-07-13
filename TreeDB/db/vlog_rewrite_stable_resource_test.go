package db

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestValueLogRewriteOnlinePreservesStablePinnedSegmentUntilRelease(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	live := appendPointersInNewSegment(t, dir, 0, 1, 500_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("rewrite-live|"), 64)
	})[0]
	pinned := appendPointersInNewSegment(t, dir, 250, 1, 510_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("rewrite-pinned|"), 64)
	})[0]
	appendPointersInNewSegment(t, dir, 250, 2, 520_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("rewrite-unpinned|"), 64)
	})
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("live"), live); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	closeNoErr(t, b)

	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil || set.Files[pinned.FileID] == nil {
		t.Fatalf("stable-pinned segment %d is not registered", pinned.FileID)
	}
	identity, err := set.Files[pinned.FileID].StableFileIdentity()
	_ = db.valueLogManager.Release(set)
	if err != nil {
		t.Fatalf("StableFileIdentity: %v", err)
	}
	lease := db.pinStableValueLogResource(identity)
	defer lease.Release()

	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{BatchSize: 1}); err != nil {
		t.Fatalf("ValueLogRewriteOnline pinned: %v", err)
	}
	pinnedPath := filepath.Join(dir, "value_vlog", "value-l250-000001.log")
	unpinnedPath := filepath.Join(dir, "value_vlog", "value-l250-000002.log")
	if _, err := os.Stat(pinnedPath); err != nil {
		t.Fatalf("stable-pinned rewrite source was retired: %v", err)
	}
	if _, err := os.Stat(unpinnedPath); !os.IsNotExist(err) {
		t.Fatalf("unreferenced unpinned rewrite source stat=%v want not-exist", err)
	}

	lease.Release()
	if _, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{BatchSize: 1}); err != nil {
		t.Fatalf("ValueLogRewriteOnline released: %v", err)
	}
	if _, err := os.Stat(pinnedPath); !os.IsNotExist(err) {
		t.Fatalf("released rewrite source stat=%v want not-exist", err)
	}
}
