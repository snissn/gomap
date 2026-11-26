package commitlog

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestWriterCleanEmptySyncSkipsRedundantDefaultSyncAndHonorsOverride(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "wal-000001.log")
	path2 := filepath.Join(dir, "wal-000002.log")

	w, err := NewWriter(path1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()

	// First Sync on a 0-byte segment performs 1 real sync and marks cleanEmptySynced.
	if err := w.Sync(); err != nil {
		t.Fatalf("first empty Sync: %v", err)
	}
	if got := w.fileSyncCalls.Load(); got != 1 {
		t.Fatalf("first empty Sync calls=%d want 1", got)
	}
	// Second redundant Sync on the same 0-byte segment is skipped.
	if err := w.Sync(); err != nil {
		t.Fatalf("second empty Sync: %v", err)
	}
	if got := w.fileSyncCalls.Load(); got != 1 {
		t.Fatalf("second empty Sync calls=%d want 1", got)
	}

	// Rotating with syncCurrent=true into a fresh 0-byte segment sets cleanEmptySynced.
	if err := w.RotateTo(path2); err != nil {
		t.Fatalf("RotateTo: %v", err)
	}
	callsAfterRotate := w.fileSyncCalls.Load()
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync after RotateTo empty: %v", err)
	}
	if got := w.fileSyncCalls.Load(); got != callsAfterRotate {
		t.Fatalf("Sync after RotateTo empty calls=%d want %d", got, callsAfterRotate)
	}

	// Appending data clears cleanEmptySynced and executes a real sync.
	if err := w.Append(Record{Op: OpSetInline, Key: []byte("k1"), Value: []byte("v1")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync after Append: %v", err)
	}
	if got := w.fileSyncCalls.Load(); got != callsAfterRotate+1 {
		t.Fatalf("Sync after Append calls=%d want %d", got, callsAfterRotate+1)
	}

	// Custom syncFn overrides must ALWAYS run even when cleanEmptySynced is true.
	var customCalls atomic.Int32
	wantErr := errors.New("injected sync failure")
	w2, err := NewWriter(filepath.Join(dir, "wal-000003.log"))
	if err != nil {
		t.Fatalf("NewWriter w2: %v", err)
	}
	defer func() { _ = w2.Close() }()
	w2.cleanEmptySynced = true
	w2.syncFn = func(*os.File) error {
		customCalls.Add(1)
		return wantErr
	}
	if err := w2.Sync(); !errors.Is(err, wantErr) {
		t.Fatalf("custom syncFn Sync err=%v want %v", err, wantErr)
	}
	if got := customCalls.Load(); got != 1 {
		t.Fatalf("custom syncFn calls=%d want 1", got)
	}
}
