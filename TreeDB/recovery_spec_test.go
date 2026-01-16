package treedb_test

import (
	"bytes"
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

func runCrashRecoveryDeleteRangeWriter(t *testing.T, dir string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperTreeDBCrashRecoveryDeleteRangeWriter$", "-test.v")
	cmd.Env = append(os.Environ(),
		"TREEDB_CRASH_HELPER=1",
		"TREEDB_CRASH_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("crash writer helper failed: %v\n%s", err, string(out))
	}
}

func runCrashRecoveryDurabilityWriter(t *testing.T, dir string, extraEnv ...string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperTreeDBCrashRecoveryDurabilityWriter$", "-test.v")
	cmd.Env = append(os.Environ(),
		"TREEDB_CRASH_HELPER=1",
		"TREEDB_CRASH_DIR="+dir,
	)
	cmd.Env = append(cmd.Env, extraEnv...)
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

func TestHelperTreeDBCrashRecoveryDurabilityWriter(t *testing.T) {
	if os.Getenv("TREEDB_CRASH_HELPER") != "1" {
		t.Skip("helper")
	}

	dir := os.Getenv("TREEDB_CRASH_DIR")
	if dir == "" {
		t.Fatalf("missing TREEDB_CRASH_DIR")
	}

	disableWAL := os.Getenv("TREEDB_CRASH_DISABLE_WAL") == "1"
	relaxedSync := os.Getenv("TREEDB_CRASH_RELAXED_SYNC") == "1"
	disableValueLog := os.Getenv("TREEDB_CRASH_DISABLE_VALUE_LOG") == "1"
	memtablePointers := os.Getenv("TREEDB_CRASH_MEMTABLE_POINTERS") == "1"
	largeValue := os.Getenv("TREEDB_CRASH_LARGE_VALUE") == "1"

	opts := treedb.Options{
		Dir:                      dir,
		ChunkSize:                64 * 1024,
		DisableWAL:               disableWAL,
		RelaxedSync:              relaxedSync,
		DisableValueLog:          disableValueLog,
		MemtableValueLogPointers: memtablePointers,
		AllowUnsafe:              disableWAL || relaxedSync,
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	val := []byte("small")
	if largeValue {
		val = bytes.Repeat([]byte("x"), 4096)
	}

	if err := db.SetSync([]byte("k"), val); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	os.Exit(0)
}

func TestHelperTreeDBCrashRecoveryDeleteRangeWriter(t *testing.T) {
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

	_ = db.SetSync([]byte("a"), []byte("1"))
	_ = db.SetSync([]byte("b"), []byte("2"))
	_ = db.SetSync([]byte("c"), []byte("3"))

	// DeleteRange itself is not a Sync operation. Add a subsequent Sync write so
	// the WAL (including the range delete tombstones) is persisted before we
	// simulate a crash.
	_ = db.DeleteRange([]byte("b"), []byte("d"))
	_ = db.SetSync([]byte("z"), []byte("9"))

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
			name := entry.Name()
			if strings.HasSuffix(name, ".log") &&
				(strings.HasPrefix(name, "wal-") || strings.HasPrefix(name, "vlog-")) {
				t.Fatalf("expected WAL to be clean after recovery; found %q", name)
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

func TestCrashRecovery_DeleteRangeReplaysCorrectKeys(t *testing.T) {
	dir := t.TempDir()
	runCrashRecoveryDeleteRangeWriter(t, dir)

	backend, err := treedb.OpenBackend(treedb.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	val, err := backend.Get([]byte("a"))
	if err != nil {
		t.Fatalf("get a: %v", err)
	}
	if string(val) != "1" {
		t.Fatalf("get a: got %q, want %q", string(val), "1")
	}

	val, err = backend.Get([]byte("b"))
	if err != nil {
		t.Fatalf("get b: %v", err)
	}
	if val != nil {
		t.Fatalf("expected deleted key b to be absent, got %q", string(val))
	}

	val, err = backend.Get([]byte("c"))
	if err != nil {
		t.Fatalf("get c: %v", err)
	}
	if val != nil {
		t.Fatalf("expected deleted key c to be absent, got %q", string(val))
	}

	val, err = backend.Get([]byte("z"))
	if err != nil {
		t.Fatalf("get z: %v", err)
	}
	if string(val) != "9" {
		t.Fatalf("get z: got %q, want %q", string(val), "9")
	}
}

func TestCrashRecovery_DurabilityTiers(t *testing.T) {
	type tier struct {
		name            string
		env             []string
		expectWALRetain bool
		expectLarge     bool
	}

	tiers := []tier{
		{
			name: "wal_disabled_strict_sync_forces_checkpoint",
			env: []string{
				"TREEDB_CRASH_DISABLE_WAL=1",
				"TREEDB_CRASH_RELAXED_SYNC=0",
			},
		},
		{
			name: "wal_enabled_relaxed_sync_flushes_wal",
			env: []string{
				"TREEDB_CRASH_DISABLE_WAL=0",
				"TREEDB_CRASH_RELAXED_SYNC=1",
			},
		},
		{
			name: "wal_enabled_strict_sync_fsyncs_wal",
			env: []string{
				"TREEDB_CRASH_DISABLE_WAL=0",
				"TREEDB_CRASH_RELAXED_SYNC=0",
			},
		},
		{
			name: "value_log_pointer_replays_and_retains_segment",
			env: []string{
				"TREEDB_CRASH_DISABLE_WAL=0",
				"TREEDB_CRASH_RELAXED_SYNC=1",
				"TREEDB_CRASH_DISABLE_VALUE_LOG=0",
				"TREEDB_CRASH_LARGE_VALUE=1",
			},
			expectWALRetain: false,
			expectLarge:     true,
		},
	}

	for _, tc := range tiers {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			runCrashRecoveryDurabilityWriter(t, dir, tc.env...)

			backend, err := treedb.OpenBackend(treedb.Options{Dir: dir, ChunkSize: 64 * 1024})
			if err != nil {
				t.Fatalf("open backend: %v", err)
			}

			val, err := backend.Get([]byte("k"))
			if err != nil {
				t.Fatalf("get k: %v", err)
			}
			if tc.expectLarge {
				if len(val) != 4096 {
					t.Fatalf("get k: got len %d, want %d", len(val), 4096)
				}
			} else if string(val) != "small" {
				t.Fatalf("get k: got %q, want %q", string(val), "small")
			}

			if err := backend.Close(); err != nil {
				t.Fatalf("close backend: %v", err)
			}

			entries, err := os.ReadDir(filepath.Join(dir, "wal"))
			if err != nil {
				if os.IsNotExist(err) {
					return
				}
				t.Fatalf("readdir wal: %v", err)
			}

			foundLog := false
			foundVlog := false
			for _, entry := range entries {
				name := entry.Name()
				if strings.HasSuffix(name, ".log") &&
					(strings.HasPrefix(name, "wal-") || strings.HasPrefix(name, "vlog-")) {
					foundLog = true
					if strings.HasPrefix(name, "vlog-") {
						foundVlog = true
					}
				}
			}

			if tc.expectWALRetain {
				if !foundVlog {
					t.Fatalf("expected a retained value-log segment after recovery, found none")
				}
			} else if foundLog {
				t.Fatalf("expected WAL to be clean after recovery; found log segments")
			}
		})
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
	if err := writer.Append(1, wal.OpSet, []byte("k1"), []byte("v1")); err != nil {
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
