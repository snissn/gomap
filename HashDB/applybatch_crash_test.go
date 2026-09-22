package hashdb

import (
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestApplyBatchSync_SurvivesCrash(t *testing.T) {
	dir := t.TempDir()

	runCrashHelper(t, "HASHDB_BATCH_HELPER", "1", dir)

	var db DB
	if err := db.Open(dir); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	got, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get k1: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("k1: got %q, want %q", string(got), "v1")
	}

	got, err = db.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("get k2: %v", err)
	}
	if got != nil {
		t.Fatalf("k2: got %q, want nil", string(got))
	}
}

func TestApplyBatch_UncommittedBatchIsIgnoredOnRecovery(t *testing.T) {
	dir := t.TempDir()

	runCrashHelper(t, "HASHDB_UNCOMMITTED_BATCH_HELPER", "1", dir)

	var db DB
	if err := db.Open(dir); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	got, err := db.Get([]byte("uncommitted"))
	if err != nil {
		t.Fatalf("get uncommitted: %v", err)
	}
	if got != nil {
		t.Fatalf("uncommitted key visible after recovery: got %q, want nil", string(got))
	}

	fi, err := os.Stat(filepath.Join(dir, "slab-0"))
	if err != nil {
		t.Fatalf("stat slab-0: %v", err)
	}
	if fi.Size() != int64(len("offset")) {
		t.Fatalf("expected slab-0 to be truncated to sentinel only, got size %d", fi.Size())
	}
}

func runCrashHelper(t *testing.T, envKey, envVal, dir string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperHashDBCrashWriter$", "-test.v")
	cmd.Env = append(os.Environ(),
		envKey+"="+envVal,
		"HASHDB_CRASH_DIR="+dir,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper failed: %v\n%s", err, string(out))
	}
}

func TestHelperHashDBCrashWriter(t *testing.T) {
	dir := os.Getenv("HASHDB_CRASH_DIR")
	if dir == "" {
		t.Skip("helper")
	}

	switch {
	case os.Getenv("HASHDB_BATCH_HELPER") == "1":
		var db DB
		if err := db.Open(dir); err != nil {
			t.Fatalf("open: %v", err)
		}

		ops := []BatchOp{
			PutOp([]byte("k1"), []byte("v1")),
			PutOp([]byte("k2"), []byte("v2")),
			DeleteOp([]byte("k2")),
		}
		if err := db.ApplyBatchSync(ops); err != nil {
			t.Fatalf("apply batch sync: %v", err)
		}

		os.Exit(0)

	case os.Getenv("HASHDB_UNCOMMITTED_BATCH_HELPER") == "1":
		var db DB
		if err := db.Open(dir); err != nil {
			t.Fatalf("open: %v", err)
		}

		slabPath := filepath.Join(dir, "slab-0")
		f, err := os.OpenFile(slabPath, os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatalf("open slab: %v", err)
		}
		defer f.Close()

		var header [16]byte
		var idScratch [8]byte

		batchID := uint64(123)
		binary.LittleEndian.PutUint64(idScratch[:], batchID)

		// BEGIN marker.
		binary.LittleEndian.PutUint64(header[:8], slabKeyLenControl)
		binary.LittleEndian.PutUint64(header[8:], packLength(9, FlagControl))
		if _, err := f.Write(header[:]); err != nil {
			t.Fatalf("write begin header: %v", err)
		}
		if _, err := f.Write([]byte{controlBatchBegin}); err != nil {
			t.Fatalf("write begin type: %v", err)
		}
		if _, err := f.Write(idScratch[:]); err != nil {
			t.Fatalf("write begin id: %v", err)
		}

		// One PUT record, but no COMMIT marker => should be ignored on recovery.
		key := []byte("uncommitted")
		val := []byte("value")

		binary.LittleEndian.PutUint64(header[:8], uint64(len(key)))
		binary.LittleEndian.PutUint64(header[8:], packLength(uint64(len(val)), 0))
		if _, err := f.Write(header[:]); err != nil {
			t.Fatalf("write put header: %v", err)
		}
		if _, err := f.Write(key); err != nil {
			t.Fatalf("write key: %v", err)
		}
		if _, err := f.Write(val); err != nil {
			t.Fatalf("write val: %v", err)
		}

		if err := f.Sync(); err != nil {
			t.Fatalf("sync slab: %v", err)
		}

		os.Exit(0)
	default:
		t.Skip("helper")
	}
}
