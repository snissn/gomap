package db

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestVacuumIndexOnline_ShrinksAndPreservesData(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()
	chunkSize := int64(64 * 1024)

	d, err := Open(Options{
		Dir:               dir,
		ChunkSize:         chunkSize,
		KeepRecent:        1,
		PreferAppendAlloc: true, // intentionally bloat index.db under churn
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	value := bytes.Repeat([]byte("v"), 200) // inline-ish to force page pressure
	for round := 0; round < 6; round++ {
		b := d.NewBatch()
		for i := 0; i < 4000; i++ {
			k := []byte(fmt.Sprintf("k%06d", i))
			if err := b.Set(k, value); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}

	indexPath := filepath.Join(dir, indexFileName)
	beforeInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	afterInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if afterInfo.Size() >= beforeInfo.Size() {
		t.Fatalf("expected vacuum to shrink index.db: before=%d after=%d", beforeInfo.Size(), afterInfo.Size())
	}

	got, err := d.Get([]byte("k000010"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value mismatch")
	}
}

func TestVacuumIndexOnline_AllowsSnapshotAcrossSwap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()
	chunkSize := int64(64 * 1024)

	d, err := Open(Options{
		Dir:        dir,
		ChunkSize:  chunkSize,
		KeepRecent: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.SetSync([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("set v1: %v", err)
	}

	snap := d.AcquireSnapshot()

	if err := d.SetSync([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("set v2: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	// DB reads see the latest value.
	got, err := d.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("expected v2, got %q", got)
	}

	// Old snapshot remains valid and sees the older value.
	old, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("snap get: %v", err)
	}
	if string(old) != "v1" {
		t.Fatalf("expected v1 from snapshot, got %q", old)
	}

	d.idxMu.Lock()
	genCount := len(d.idxAll)
	d.idxMu.Unlock()
	if genCount < 2 {
		t.Fatalf("expected at least 2 index generations after vacuum, got %d", genCount)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("snap close: %v", err)
	}

	d.idxMu.Lock()
	genCountAfter := len(d.idxAll)
	d.idxMu.Unlock()
	if genCountAfter != 1 {
		t.Fatalf("expected old index generation to be released after snapshot close; gens=%d", genCountAfter)
	}
}

func TestVacuumIndexOnline_RepeatWhileSnapshotPinned(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()
	chunkSize := int64(64 * 1024)

	d, err := Open(Options{
		Dir:        dir,
		ChunkSize:  chunkSize,
		KeepRecent: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.SetSync([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("set v1: %v", err)
	}

	snap := d.AcquireSnapshot()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := d.SetSync([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("set v2: %v", err)
	}
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum 1: %v", err)
	}

	d.idxMu.Lock()
	genCount1 := len(d.idxAll)
	d.idxMu.Unlock()
	if genCount1 != 2 {
		t.Fatalf("expected 2 index generations after first vacuum, got %d", genCount1)
	}

	if err := d.SetSync([]byte("k"), []byte("v3")); err != nil {
		t.Fatalf("set v3: %v", err)
	}
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum 2: %v", err)
	}

	// DB reads see the latest value.
	got, err := d.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "v3" {
		t.Fatalf("expected v3, got %q", got)
	}

	// Old snapshot remains valid and sees the older value.
	old, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("snap get: %v", err)
	}
	if string(old) != "v1" {
		t.Fatalf("expected v1 from snapshot, got %q", old)
	}

	d.idxMu.Lock()
	genCount2 := len(d.idxAll)
	d.idxMu.Unlock()
	if genCount2 < 2 {
		t.Fatalf("expected at least 2 index generations after second vacuum, got %d", genCount2)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("snap close: %v", err)
	}

	d.idxMu.Lock()
	genCountAfter := len(d.idxAll)
	d.idxMu.Unlock()
	if genCountAfter != 1 {
		t.Fatalf("expected old index generations to be released after snapshot close; gens=%d", genCountAfter)
	}
}
