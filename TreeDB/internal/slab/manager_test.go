package slab

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"treedb/internal/page"
	"treedb/internal/pager"
	"treedb/internal/tree"
)

func TestLoadTruncatesTailAndDeletesGhosts(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "data-0000.slab"), []byte("seed"), 0o600); err != nil {
		t.Fatalf("write slab0: %v", err)
	}

	activePath := filepath.Join(dir, "data-0001.slab")
	if err := os.WriteFile(activePath, bytes.Repeat([]byte{0xaa}, 100), 0o600); err != nil {
		t.Fatalf("write active slab: %v", err)
	}

	ghostPath := filepath.Join(dir, "data-0002.slab")
	if err := os.WriteFile(ghostPath, []byte("ghost"), 0o600); err != nil {
		t.Fatalf("write ghost slab: %v", err)
	}

	mgr, set, err := Load(dir, 1, 50)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	if _, err := os.Stat(ghostPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ghost slab should be deleted, stat err=%v", err)
	}

	fi, err := os.Stat(activePath)
	if err != nil {
		t.Fatalf("stat active slab: %v", err)
	}
	if fi.Size() != 50 {
		t.Fatalf("active slab size: got %d want 50", fi.Size())
	}

	if _, ok := set.Get(0); !ok {
		t.Fatalf("expected slab 0 in set")
	}
	if _, ok := set.Get(1); !ok {
		t.Fatalf("expected slab 1 in set")
	}
	if _, ok := set.Get(2); ok {
		t.Fatalf("unexpected ghost slab in set")
	}
}

func openTestPager(t *testing.T) *pager.Pager {
	t.Helper()
	dir := t.TempDir()
	p, err := pager.Open(dir, int64(page.PageSize*4))
	if err != nil {
		t.Fatalf("pager.Open: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestStatsKeyValuePersistence(t *testing.T) {
	p := openTestPager(t)
	sys := tree.NewSystemTree(p, 0)

	stats := map[uint32]SlabStats{
		1: {DeadBytes: 10, TotalBytes: 100},
		2: {DeadBytes: 5, TotalBytes: 50},
		3: {DeadBytes: 0, TotalBytes: 1},
	}

	for id, st := range stats {
		key := StatsKey(id)
		if gotID, ok := ParseStatsKey(key); !ok || gotID != id {
			t.Fatalf("ParseStatsKey mismatch: got (%d,%v) want (%d,true)", gotID, ok, id)
		}
		val := EncodeStatsValue(st)
		entry := tree.LeafEntry{Flags: page.LeafFlagInline, InlineValue: val}
		if _, err := sys.SetRaw(key, entry); err != nil {
			t.Fatalf("SetRaw stats %d: %v", id, err)
		}
	}

	for id, want := range stats {
		key := StatsKey(id)
		gotEntry, err := sys.GetRaw(key)
		if err != nil {
			t.Fatalf("GetRaw stats %d: %v", id, err)
		}
		got, err := DecodeStatsValue(gotEntry.InlineValue)
		if err != nil {
			t.Fatalf("DecodeStatsValue stats %d: %v", id, err)
		}
		if got != want {
			t.Fatalf("stats %d mismatch: got %+v want %+v", id, got, want)
		}
	}
}

