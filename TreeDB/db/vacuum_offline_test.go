package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestVacuumIndexOffline_PreservesDataAndShrinksFile(t *testing.T) {
	dir := t.TempDir()
	chunkSize := int64(64 * 1024)

	d, err := Open(Options{
		Dir:               dir,
		ChunkSize:         chunkSize,
		KeepRecent:        1,
		PreferAppendAlloc: true, // intentionally bloat index.db
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	value := bytes.Repeat([]byte("v"), 200) // inline-ish to force page pressure
	for round := 0; round < 8; round++ {
		b := d.NewBatch()
		for i := 0; i < 5000; i++ {
			// Rewrite the same keyset to create lots of retired pages (index bloat)
			// when PreferAppendAlloc is enabled.
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
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	indexPath := filepath.Join(dir, indexFileName)
	beforeInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}

	if err := VacuumIndexOffline(Options{Dir: dir, ChunkSize: chunkSize, KeepRecent: 1}); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	afterInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if afterInfo.Size() >= beforeInfo.Size() {
		t.Fatalf("expected vacuum to shrink index.db: before=%d after=%d", beforeInfo.Size(), afterInfo.Size())
	}

	verify, err := Open(Options{Dir: dir, ChunkSize: chunkSize, KeepRecent: 1})
	if err != nil {
		t.Fatalf("open after: %v", err)
	}
	defer func() { _ = verify.Close() }()

	got, err := verify.Get([]byte("k000010"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value mismatch")
	}
}

func TestVacuumIndexOffline_CrashPointsRecoverOnOpen(t *testing.T) {
	failpoints := []vacuumFailpoint{
		vacuumFailAfterNewSync,
		vacuumFailAfterReady,
		vacuumFailAfterRenameOld,
		vacuumFailAfterRenameNew,
	}

	for _, fp := range failpoints {
		fp := fp
		t.Run(string(fp), func(t *testing.T) {
			dir := t.TempDir()
			chunkSize := int64(64 * 1024)

			d, err := Open(Options{Dir: dir, ChunkSize: chunkSize, KeepRecent: 1})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := d.SetSync([]byte("k"), []byte("v")); err != nil {
				t.Fatalf("set: %v", err)
			}
			if err := d.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			err = vacuumIndexOffline(Options{Dir: dir, ChunkSize: chunkSize, KeepRecent: 1}, fp)
			if err == nil {
				t.Fatalf("expected failpoint error")
			}
			if !errors.Is(err, errVacuumFailpoint) {
				t.Fatalf("unexpected error: %v", err)
			}

			reopen, err := Open(Options{Dir: dir, ChunkSize: chunkSize, KeepRecent: 1})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = reopen.Close() }()

			val, err := reopen.Get([]byte("k"))
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if string(val) != "v" {
				t.Fatalf("bad value: %q", val)
			}
		})
	}
}
