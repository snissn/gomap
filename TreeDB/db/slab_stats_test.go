package db

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/tree"
)

func TestSlabStatsPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	key := []byte("A")
	val := bytes.Repeat([]byte("x"), 300) // > 256 => pointer => slab write

	if err := d.SetSync(key, val); err != nil {
		t.Fatalf("set: %v", err)
	}

	sysTree := tree.New(d.Pager(), d.slabManager, d.meta.SystemRootPageID)
	raw, err := sysTree.Get(slabStatsKey(0))
	if err != nil {
		t.Fatalf("system get: %v", err)
	}
	dead, total, err := decodeSlabStatsValue(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	wantTotal := uint64(2 + 4 + len(key) + len(val))
	if dead != 0 {
		t.Fatalf("expected dead=0, got %d", dead)
	}
	if total != wantTotal {
		t.Fatalf("expected total=%d, got %d", wantTotal, total)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()

	sysTree2 := tree.New(d2.Pager(), d2.slabManager, d2.meta.SystemRootPageID)
	raw2, err := sysTree2.Get(slabStatsKey(0))
	if err != nil {
		t.Fatalf("system get 2: %v", err)
	}
	dead2, total2, err := decodeSlabStatsValue(raw2)
	if err != nil {
		t.Fatalf("decode 2: %v", err)
	}

	if dead2 != dead {
		t.Fatalf("expected dead=%d, got %d", dead, dead2)
	}
	if total2 != total {
		t.Fatalf("expected total=%d, got %d", total, total2)
	}
}

func TestSlabStatsDeadBytesOnOverwrite(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	key := []byte("A")
	val := bytes.Repeat([]byte("x"), 300)
	val2 := bytes.Repeat([]byte("y"), 300)

	if err := d.SetSync(key, val); err != nil {
		t.Fatalf("set 1: %v", err)
	}
	if err := d.SetSync(key, val2); err != nil {
		t.Fatalf("set 2: %v", err)
	}

	sysTree := tree.New(d.Pager(), d.slabManager, d.meta.SystemRootPageID)
	raw, err := sysTree.Get(slabStatsKey(0))
	if err != nil {
		t.Fatalf("system get: %v", err)
	}
	dead, total, err := decodeSlabStatsValue(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	entryBytes := uint64(2 + 4 + len(key) + len(val))
	if dead != entryBytes {
		t.Fatalf("expected dead=%d, got %d", entryBytes, dead)
	}
	if total != 2*entryBytes {
		t.Fatalf("expected total=%d, got %d", 2*entryBytes, total)
	}
}
