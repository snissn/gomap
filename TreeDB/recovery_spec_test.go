package treedb_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/wal"
)

func runCrashRecoveryWriter(t *testing.T, dir string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperTreeDBCrashRecoveryWriter$", "-test.v")
	cmd.Env = append(os.Environ(),
		"TREEDB_CRASH_HELPER=1",
		"TREEDB_CRASH_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("crash writer helper failed: %v\n%s", err, string(out))
	}
}

func TestHelperTreeDBCrashRecoveryWriter(t *testing.T) {
	if os.Getenv("TREEDB_CRASH_HELPER") != "1" {
		t.Skip("helper")
	}

	dir := os.Getenv("TREEDB_CRASH_DIR")
	if dir == "" {
		t.Fatalf("missing TREEDB_CRASH_DIR")
	}

	db, err := treedb.Open(treedb.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	_ = db.SetSync([]byte("keep"), []byte("val1"))
	_ = db.SetSync([]byte("delete"), []byte("val2"))
	_ = db.DeleteSync([]byte("delete"))

	// Simulate a crash by exiting without calling Close() (no defers run, but OS releases locks).
	os.Exit(0)
}

func TestCrashRecovery_WALReplayIsCoherentAcrossModes(t *testing.T) {
	dir := t.TempDir()
	runCrashRecoveryWriter(t, dir)

	backend, err := treedb.OpenBackend(treedb.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}

	val, err := backend.Get([]byte("keep"))
	if err != nil {
		_ = backend.Close()
		t.Fatalf("get keep: %v", err)
	}
	if string(val) != "val1" {
		_ = backend.Close()
		t.Fatalf("get keep: got %q, want %q", string(val), "val1")
	}

	val, err = backend.Get([]byte("delete"))
	if err != nil {
		_ = backend.Close()
		t.Fatalf("get delete: %v", err)
	}
	if val != nil {
		_ = backend.Close()
		t.Fatalf("expected deleted key to be absent, got %q", string(val))
	}

	if err := backend.Close(); err != nil {
		t.Fatalf("close backend: %v", err)
	}

	// WAL segments should be retired after successful recovery.
	walDir := filepath.Join(dir, "wal")
	if entries, err := os.ReadDir(walDir); err == nil {
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "wal-") && strings.HasSuffix(entry.Name(), ".log") {
				t.Fatalf("expected WAL to be clean after recovery; found %q", entry.Name())
			}
		}
	}

	cached, err := treedb.Open(treedb.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer cached.Close()

	val, err = cached.Get([]byte("keep"))
	if err != nil {
		t.Fatalf("get keep (cached): %v", err)
	}
	if string(val) != "val1" {
		t.Fatalf("get keep (cached): got %q, want %q", string(val), "val1")
	}
}

func TestRecovery_TruncatedWALRecord(t *testing.T) {
	dir := t.TempDir()

	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	walPath := filepath.Join(walDir, "wal-000001.log")
	writer, err := wal.NewWriter(walPath)
	if err != nil {
		t.Fatalf("wal.NewWriter: %v", err)
	}
	if err := writer.Append(wal.OpSet, []byte("k1"), []byte("v1")); err != nil {
		_ = writer.Close()
		t.Fatalf("wal.Append: %v", err)
	}
	if err := writer.Sync(); err != nil {
		_ = writer.Close()
		t.Fatalf("wal.Sync: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}

	// Append a partial record to simulate a torn write.
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatalf("open wal for append: %v", err)
	}
	_, _ = f.Write([]byte{0x01, 0x02, 0x03})
	_ = f.Close()

	backend, err := treedb.OpenBackend(treedb.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	val, err := backend.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "v1" {
		t.Fatalf("get: got %q, want %q", string(val), "v1")
	}

	if _, err := os.Stat(walPath); err == nil {
		t.Fatalf("expected wal file to be removed after recovery")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat wal file: %v", err)
	}
}
