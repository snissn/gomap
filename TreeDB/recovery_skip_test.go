package treedb_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/wal"
)

func TestRecovery_SkipsOldSequences(t *testing.T) {
	dir := t.TempDir()

	// 1. Initialize a DB to establish a LastSeq.
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// We need to advance LastSeq. The easiest way is to perform writes.
	// But we want to simulate a specific state.
	// Write "base" -> "data" (Seq 1)
	if err := db.SetSync([]byte("base"), []byte("data")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	lastSeq := db.LastSeq()
	if lastSeq == 0 {
		t.Fatalf("Expected LastSeq > 0 after write")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 2. Manually craft a WAL segment that overlaps with the persisted LastSeq.
	// This simulates a crash where the WAL wasn't truncated/removed but the meta WAS updated.
	// Record 1: Seq = lastSeq (Should be skipped) -> Set "skipped" "val"
	// Record 2: Seq = lastSeq + 1 (Should be replayed) -> Set "replayed" "val"

	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	walPath := filepath.Join(walDir, "wal-000002.log") // Use a higher segment number to ensure it's picked up

	w, err := wal.NewWriter(walPath)
	if err != nil {
		t.Fatalf("wal.NewWriter: %v", err)
	}

	// Append skipped record
	if err := w.Append(lastSeq, wal.OpSet, []byte("skipped"), []byte("val")); err != nil {
		t.Fatalf("wal.Append skipped: %v", err)
	}
	// Append replayed record
	if err := w.Append(lastSeq+1, wal.OpSet, []byte("replayed"), []byte("val")); err != nil {
		t.Fatalf("wal.Append replayed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}

	// 3. Open DB again (Backend mode to force immediate replay check)
	db2, err := treedb.OpenBackend(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	defer db2.Close()

	// 4. Verify "skipped" is NOT present (or at least not overwritten if it existed, but here it's new)
	val, err := db2.Get([]byte("skipped"))
	if err != nil {
		t.Fatalf("Get skipped: %v", err)
	}
	if val != nil {
		t.Fatalf("Expected 'skipped' key to be ignored during replay, but got value: %q", val)
	}

	// 5. Verify "replayed" IS present
	val, err = db2.Get([]byte("replayed"))
	if err != nil {
		t.Fatalf("Get replayed: %v", err)
	}
	if !bytes.Equal(val, []byte("val")) {
		t.Fatalf("Expected 'replayed' key to be present, got: %q", val)
	}
}
