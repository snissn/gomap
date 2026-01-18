package treedb_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
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
	largeValue := os.Getenv("TREEDB_CRASH_LARGE_VALUE") == "1"
	splitValueLog := os.Getenv("TREEDB_CRASH_SPLIT_VALUE_LOG") == "1"

	opts := treedb.Options{
		Dir:             dir,
		ChunkSize:       64 * 1024,
		DisableWAL:      disableWAL,
		RelaxedSync:     relaxedSync,
		DisableValueLog: disableValueLog,
		SplitValueLog:   splitValueLog,
		AllowUnsafe:     disableWAL || relaxedSync,
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
	// the commit log (including the range delete tombstones) is persisted before we
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

	// Log segments should be retired after successful recovery.
	walDir := filepath.Join(dir, "maindb", "wal")
	if entries, err := os.ReadDir(walDir); err == nil {
		for _, entry := range entries {
			name := entry.Name()
			if strings.HasSuffix(name, ".log") &&
				(strings.HasPrefix(name, "commit-") || strings.HasPrefix(name, "value-")) {
				t.Fatalf("expected logs to be clean after recovery; found %q", name)
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
		name        string
		env         []string
		expectLarge bool
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
			name: "value_log_pointer_replays",
			env: []string{
				"TREEDB_CRASH_DISABLE_WAL=0",
				"TREEDB_CRASH_RELAXED_SYNC=1",
				"TREEDB_CRASH_DISABLE_VALUE_LOG=0",
				"TREEDB_CRASH_LARGE_VALUE=1",
			},
			expectLarge: true,
		},
		{
			name: "split_value_log_write_sync_requires_commit_and_payload",
			env: []string{
				"TREEDB_CRASH_DISABLE_WAL=0",
				"TREEDB_CRASH_RELAXED_SYNC=0",
				"TREEDB_CRASH_SPLIT_VALUE_LOG=1",
				"TREEDB_CRASH_LARGE_VALUE=1",
			},
			expectLarge: true,
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

			entries, err := os.ReadDir(filepath.Join(dir, "maindb", "wal"))
			if err != nil {
				if os.IsNotExist(err) {
					return
				}
				t.Fatalf("readdir wal: %v", err)
			}

			foundLog := false
			for _, entry := range entries {
				name := entry.Name()
				if strings.HasSuffix(name, ".log") &&
					(strings.HasPrefix(name, "commit-") || strings.HasPrefix(name, "value-")) {
					foundLog = true
				}
			}
			if foundLog {
				t.Fatalf("expected logs to be clean after recovery; found log segments")
			}
		})
	}
}

func TestRecovery_RIDJoinReplaysValueLog(t *testing.T) {
	dir := t.TempDir()

	walDir := filepath.Join(dir, "maindb", "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	valuePath := filepath.Join(walDir, "value-l0-000001.log")
	vw, err := valuelog.NewWriter(valuePath, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("valuelog.NewWriter: %v", err)
	}
	if _, err := vw.Append(0, nil, 1, []byte("v1")); err != nil {
		_ = vw.Close()
		t.Fatalf("valuelog.Append: %v", err)
	}
	if err := vw.Sync(); err != nil {
		_ = vw.Close()
		t.Fatalf("valuelog.Sync: %v", err)
	}
	if err := vw.Close(); err != nil {
		t.Fatalf("valuelog.Close: %v", err)
	}

	commitPath := filepath.Join(walDir, "commit-l0-000001.log")
	cw, err := commitlog.NewWriter(commitPath)
	if err != nil {
		t.Fatalf("commitlog.NewWriter: %v", err)
	}
	rec := commitlog.Record{Op: commitlog.OpSetRID, Key: []byte("k1"), RID: 1, Seq: 1}
	if err := cw.AppendBatch([]commitlog.Record{rec}); err != nil {
		_ = cw.Close()
		t.Fatalf("commitlog.AppendBatch: %v", err)
	}
	if err := cw.Sync(); err != nil {
		_ = cw.Close()
		t.Fatalf("commitlog.Sync: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("commitlog.Close: %v", err)
	}

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

	if _, err := os.Stat(commitPath); err == nil {
		t.Fatalf("expected commitlog file to be removed after recovery")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat commitlog file: %v", err)
	}
	if _, err := os.Stat(valuePath); err == nil {
		t.Fatalf("expected valuelog file to be removed after recovery")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat valuelog file: %v", err)
	}
}

func TestRecovery_TruncatedCommitLogRecord(t *testing.T) {
	dir := t.TempDir()

	walDir := filepath.Join(dir, "maindb", "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	commitPath := filepath.Join(walDir, "commit-l0-000001.log")
	writer, err := commitlog.NewWriter(commitPath)
	if err != nil {
		t.Fatalf("commitlog.NewWriter: %v", err)
	}
	rec := commitlog.Record{Op: commitlog.OpSetInline, Key: []byte("k1"), Value: []byte("v1"), Seq: 1}
	if err := writer.AppendBatch([]commitlog.Record{rec}); err != nil {
		_ = writer.Close()
		t.Fatalf("commitlog.AppendBatch: %v", err)
	}
	if err := writer.Sync(); err != nil {
		_ = writer.Close()
		t.Fatalf("commitlog.Sync: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("commitlog.Close: %v", err)
	}

	// Append a partial record to simulate a torn write.
	f, err := os.OpenFile(commitPath, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatalf("open commitlog for append: %v", err)
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

	if _, err := os.Stat(commitPath); err == nil {
		t.Fatalf("expected commitlog file to be removed after recovery")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat commitlog file: %v", err)
	}
}

func TestRecovery_TruncatedValueLogRecord(t *testing.T) {
	dir := t.TempDir()

	walDir := filepath.Join(dir, "maindb", "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	valuePath := filepath.Join(walDir, "value-l0-000001.log")
	vw, err := valuelog.NewWriter(valuePath, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("valuelog.NewWriter: %v", err)
	}
	if _, err := vw.Append(0, nil, 1, []byte("v1")); err != nil {
		_ = vw.Close()
		t.Fatalf("valuelog.Append: %v", err)
	}
	if err := vw.Sync(); err != nil {
		_ = vw.Close()
		t.Fatalf("valuelog.Sync: %v", err)
	}
	if err := vw.Close(); err != nil {
		t.Fatalf("valuelog.Close: %v", err)
	}

	commitPath := filepath.Join(walDir, "commit-l0-000001.log")
	cw, err := commitlog.NewWriter(commitPath)
	if err != nil {
		t.Fatalf("commitlog.NewWriter: %v", err)
	}
	rec := commitlog.Record{Op: commitlog.OpSetRID, Key: []byte("k1"), RID: 1, Seq: 1}
	if err := cw.AppendBatch([]commitlog.Record{rec}); err != nil {
		_ = cw.Close()
		t.Fatalf("commitlog.AppendBatch: %v", err)
	}
	if err := cw.Sync(); err != nil {
		_ = cw.Close()
		t.Fatalf("commitlog.Sync: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("commitlog.Close: %v", err)
	}

	// Append a partial record to simulate a torn write.
	f, err := os.OpenFile(valuePath, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatalf("open valuelog for append: %v", err)
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

	if _, err := os.Stat(commitPath); err == nil {
		t.Fatalf("expected commitlog file to be removed after recovery")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat commitlog file: %v", err)
	}
	if _, err := os.Stat(valuePath); err == nil {
		t.Fatalf("expected valuelog file to be removed after recovery")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat valuelog file: %v", err)
	}
}
