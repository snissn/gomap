package treedb_test

import (
	"bytes"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/compress/zstd"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func legacyCachedRecoveryOptions(dir string) treedb.Options {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.ChunkSize = 64 * 1024
	opts.AllowLegacyCachedRedoJournalReplay = true
	return opts
}

func assertCommitLogCleared(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err == nil {
		if info.Size() > 0 {
			t.Fatalf("expected commitlog file to be removed or truncated after recovery")
		}
		return
	}
	if !os.IsNotExist(err) {
		t.Fatalf("stat commitlog file: %v", err)
	}
}

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

func runCrashRecoveryDeleteRangeNoTrailingSyncWriter(t *testing.T, dir string, extraEnv ...string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperTreeDBCrashRecoveryDeleteRangeNoTrailingSyncWriter$", "-test.v")
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

func runCommandWALDurableUncheckpointedWriter(t *testing.T, dir string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperTreeDBCommandWALDurableUncheckpointedWriter$", "-test.v")
	cmd.Env = append(os.Environ(),
		"TREEDB_CRASH_HELPER=1",
		"TREEDB_CRASH_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("crash writer helper failed: %v\n%s", err, string(out))
	}
}

func runCommandWALRelaxedRotatedPrefixWriter(t *testing.T, dir string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestHelperTreeDBCommandWALRelaxedRotatedPrefixWriter$", "-test.v")
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

	db, err := treedb.Open(treedb.Options{Dir: dir, ChunkSize: 64 * 1024, CommandWAL: true})
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
	largeValue := os.Getenv("TREEDB_CRASH_LARGE_VALUE") == "1"
	emptySync := os.Getenv("TREEDB_CRASH_EMPTY_SYNC") == "1"

	profile := treedb.ProfileCommandWALDurable
	if disableWAL {
		profile = treedb.ProfileNoWALFast
	} else if relaxedSync {
		profile = treedb.ProfileCommandWALRelaxed
	}

	opts := treedb.OptionsFor(profile, dir)
	opts.ChunkSize = 64 * 1024

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	val := []byte("small")
	if largeValue {
		val = bytes.Repeat([]byte("x"), 4096)
	}

	if emptySync {
		if err := db.Set([]byte("k"), val); err != nil {
			t.Fatalf("Set before empty WriteSync: %v", err)
		}
		empty := db.NewBatch()
		if err := empty.WriteSync(); err != nil {
			_ = empty.Close()
			t.Fatalf("empty WriteSync: %v", err)
		}
		if err := empty.Close(); err != nil {
			t.Fatalf("empty batch Close: %v", err)
		}
	} else if err := db.SetSync([]byte("k"), val); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	os.Exit(0)
}

func TestHelperTreeDBCommandWALDurableUncheckpointedWriter(t *testing.T) {
	if os.Getenv("TREEDB_CRASH_HELPER") != "1" {
		t.Skip("helper")
	}

	dir := os.Getenv("TREEDB_CRASH_DIR")
	if dir == "" {
		t.Fatalf("missing TREEDB_CRASH_DIR")
	}

	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.CommandWALStatsScan = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	opts.FlushThreshold = 1 << 30
	opts.MaxQueuedMemtables = -1
	opts.WriterFlushMaxMemtables = 0
	opts.WriterFlushMaxDuration = 0
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationOff
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.CommandWALSegmentTargetBytes = 1

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := db.Set([]byte("relaxed-pointer-a"), bytes.Repeat([]byte("a"), 4096)); err != nil {
		t.Fatalf("Set relaxed-pointer-a: %v", err)
	}
	if err := db.Set([]byte("relaxed-pointer-b"), bytes.Repeat([]byte("b"), 4096)); err != nil {
		t.Fatalf("Set relaxed-pointer-b: %v", err)
	}
	empty := db.NewBatch()
	if err := empty.WriteSync(); err != nil {
		_ = empty.Close()
		t.Fatalf("empty WriteSync durable-prefix barrier: %v", err)
	}
	if err := empty.Close(); err != nil {
		t.Fatalf("empty batch Close: %v", err)
	}

	if err := db.SetSync([]byte("point-sync"), []byte("keep-point")); err != nil {
		t.Fatalf("SetSync point-sync: %v", err)
	}
	if err := db.SetSync([]byte("deleted-sync"), []byte("remove-me")); err != nil {
		t.Fatalf("SetSync deleted-sync: %v", err)
	}
	if err := db.DeleteSync([]byte("deleted-sync")); err != nil {
		t.Fatalf("DeleteSync deleted-sync: %v", err)
	}

	b := db.NewBatch()
	if err := b.Set([]byte("batch-sync"), []byte("keep-batch")); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set batch-sync: %v", err)
	}
	if err := b.Set([]byte("range-delete"), []byte("remove-me")); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set range-delete: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}
	if err := db.DeleteRange([]byte("range-"), []byte("range-\xff")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	stats := db.Stats()
	if stats["treedb.cache.redo_log.mode"] != "external_command_wal" ||
		stats["treedb.cache.redo_log.enabled"] != "false" ||
		stats["treedb.cache.command_wal.external_durability"] != "true" {
		t.Fatalf("unexpected command-WAL cached durability stats: %#v", stats)
	}
	// Durable-root V1 seals the fresh bootstrap as commit 1. The synced command
	// WAL frames above must remain ahead of that unchanged checkpoint until
	// recovery replays them.
	if stats["treedb.commit_seq"] != "1" {
		t.Fatalf("commit_seq=%q, want bootstrap commit 1 before crash without checkpoint", stats["treedb.commit_seq"])
	}

	// Simulate a crash by exiting without Close(), so no close-time checkpoint can
	// publish the pending command-WAL LSNs.
	os.Exit(0)
}

func TestHelperTreeDBCommandWALRelaxedRotatedPrefixWriter(t *testing.T) {
	if os.Getenv("TREEDB_CRASH_HELPER") != "1" {
		t.Skip("helper")
	}

	dir := os.Getenv("TREEDB_CRASH_DIR")
	if dir == "" {
		t.Fatalf("missing TREEDB_CRASH_DIR")
	}

	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.CommandWALStatsScan = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	opts.FlushThreshold = 1 << 30
	opts.MaxQueuedMemtables = -1
	opts.WriterFlushMaxMemtables = 0
	opts.WriterFlushMaxDuration = 0
	opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold
	opts.ValueLog.Generational.HotSegmentTargetBytes = 1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.CommandWALSegmentTargetBytes = 5000

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for _, tt := range []struct {
		key   string
		value byte
	}{
		{key: "rotated-pointer-a", value: 'a'},
		{key: "rotated-pointer-b", value: 'b'},
	} {
		if err := db.Set([]byte(tt.key), bytes.Repeat([]byte{tt.value}, 4096)); err != nil {
			t.Fatalf("Set %s: %v", tt.key, err)
		}
	}

	valueSegments, err := filepath.Glob(filepath.Join(dir, "maindb", "value_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob value-log segments: %v", err)
	}
	if len(valueSegments) < 2 {
		t.Fatalf("value-log segments=%v, want an actual relaxed rotation", valueSegments)
	}
	walSegments, err := filepath.Glob(filepath.Join(dir, "maindb", "wal", "commit-l*.log"))
	if err != nil {
		t.Fatalf("glob command-WAL segments: %v", err)
	}
	if len(walSegments) < 2 {
		t.Fatalf("command-WAL segments=%v, want an actual relaxed rotation", walSegments)
	}
	walSizes := make([]int64, len(walSegments))
	for i, path := range walSegments {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat command-WAL segment %s: %v", path, err)
		}
		walSizes[i] = info.Size()
	}

	empty := db.NewBatch()
	if err := empty.WriteSync(); err != nil {
		_ = empty.Close()
		t.Fatalf("empty WriteSync durable-prefix barrier: %v", err)
	}
	if err := empty.Close(); err != nil {
		t.Fatalf("empty batch Close: %v", err)
	}
	barrierWALSegments, err := filepath.Glob(filepath.Join(dir, "maindb", "wal", "commit-l*.log"))
	if err != nil {
		t.Fatalf("glob command-WAL segments after barrier: %v", err)
	}
	if len(barrierWALSegments) != len(walSegments) {
		t.Fatalf("empty WriteSync rotated command WAL: segments before=%v sizes=%v after=%v", walSegments, walSizes, barrierWALSegments)
	}
	if got := db.Stats()["treedb.command_wal.durable_wal_lsn"]; got != "3" {
		t.Fatalf("durable_wal_lsn=%q, want 3 immediately after barrier WAL sync", got)
	}

	// Power loss immediately after the non-rotating explicit-sync barrier has
	// acknowledged both relaxed pointer frames and their rotated dependencies.
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

	db, err := treedb.Open(treedb.Options{Dir: dir, ChunkSize: 64 * 1024, CommandWAL: true})
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

func TestHelperTreeDBCrashRecoveryDeleteRangeNoTrailingSyncWriter(t *testing.T) {
	if os.Getenv("TREEDB_CRASH_HELPER") != "1" {
		t.Skip("helper")
	}

	dir := os.Getenv("TREEDB_CRASH_DIR")
	if dir == "" {
		t.Fatalf("missing TREEDB_CRASH_DIR")
	}

	disableWAL := os.Getenv("TREEDB_CRASH_DISABLE_WAL") == "1"
	relaxedSync := os.Getenv("TREEDB_CRASH_RELAXED_SYNC") == "1"

	durability := treedb.DurabilityDurable
	if disableWAL {
		durability = treedb.DurabilityWALOffRelaxed
	} else if relaxedSync {
		durability = treedb.DurabilityWALOnRelaxed
	}

	opts := treedb.Options{
		Dir:        dir,
		ChunkSize:  64 * 1024,
		Durability: durability,
		CommandWAL: !disableWAL,
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	_ = db.SetSync([]byte("a"), []byte("1"))
	_ = db.SetSync([]byte("b"), []byte("2"))
	_ = db.SetSync([]byte("c"), []byte("3"))
	_ = db.SetSync([]byte("z"), []byte("9"))

	// Simulate a crash immediately after DeleteRange without an additional Sync
	// operation. The complete flushed V2 frame survives this fixture and remains
	// replayable; only a defective relaxed suffix may be discarded.
	_ = db.DeleteRange([]byte("b"), []byte("d"))
	os.Exit(0)
}

func TestCrashRecovery_WALReplayIsCoherent(t *testing.T) {
	dir := t.TempDir()
	runCrashRecoveryWriter(t, dir)

	db, err := treedb.Open(treedb.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	val, err := db.Get([]byte("keep"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("get keep: %v", err)
	}
	if string(val) != "val1" {
		_ = db.Close()
		t.Fatalf("get keep: got %q, want %q", string(val), "val1")
	}

	val, err = db.Get([]byte("delete"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("get delete: %v", err)
	}
	if val != nil {
		_ = db.Close()
		t.Fatalf("expected deleted key to be absent, got %q", string(val))
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
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

	db, err := treedb.Open(treedb.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	val, err := db.Get([]byte("a"))
	if err != nil {
		t.Fatalf("get a: %v", err)
	}
	if string(val) != "1" {
		t.Fatalf("get a: got %q, want %q", string(val), "1")
	}

	val, err = db.Get([]byte("b"))
	if err != nil {
		t.Fatalf("get b: %v", err)
	}
	if val != nil {
		t.Fatalf("expected deleted key b to be absent, got %q", string(val))
	}

	val, err = db.Get([]byte("c"))
	if err != nil {
		t.Fatalf("get c: %v", err)
	}
	if val != nil {
		t.Fatalf("expected deleted key c to be absent, got %q", string(val))
	}

	val, err = db.Get([]byte("z"))
	if err != nil {
		t.Fatalf("get z: %v", err)
	}
	if string(val) != "9" {
		t.Fatalf("get z: got %q, want %q", string(val), "9")
	}
}

func TestCrashRecovery_DeleteRangeWithoutTrailingSync_ReplaysCorrectKeys(t *testing.T) {
	tiers := []struct {
		name string
		env  []string
	}{
		{
			name: "durable_default",
		},
		{
			name: "wal_on_relaxed",
			env: []string{
				"TREEDB_CRASH_DISABLE_WAL=0",
				"TREEDB_CRASH_RELAXED_SYNC=1",
			},
		},
	}

	for _, tc := range tiers {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			runCrashRecoveryDeleteRangeNoTrailingSyncWriter(t, dir, tc.env...)

			opts := treedb.Options{Dir: dir, ChunkSize: 64 * 1024}

			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			val, err := db.Get([]byte("a"))
			if err != nil {
				t.Fatalf("get a: %v", err)
			}
			if string(val) != "1" {
				t.Fatalf("get a: got %q, want %q", string(val), "1")
			}

			val, err = db.Get([]byte("b"))
			if err != nil {
				t.Fatalf("get b: %v", err)
			}
			if val != nil {
				t.Fatalf("expected deleted key b to be absent, got %q", string(val))
			}

			val, err = db.Get([]byte("c"))
			if err != nil {
				t.Fatalf("get c: %v", err)
			}
			if val != nil {
				t.Fatalf("expected deleted key c to be absent, got %q", string(val))
			}

			val, err = db.Get([]byte("z"))
			if err != nil {
				t.Fatalf("get z: %v", err)
			}
			if string(val) != "9" {
				t.Fatalf("get z: got %q, want %q", string(val), "9")
			}

		})
	}
}

func TestCrashRecovery_CommandWALDurableSyncedUncheckpointedFramesReplay(t *testing.T) {
	dir := t.TempDir()
	runCommandWALDurableUncheckpointedWriter(t, dir)

	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.CommandWALStatsScan = true
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	tests := []struct {
		key     string
		want    string
		wantNil bool
	}{
		{key: "point-sync", want: "keep-point"},
		{key: "batch-sync", want: "keep-batch"},
		{key: "deleted-sync", wantNil: true},
		{key: "relaxed-pointer-a", want: string(bytes.Repeat([]byte("a"), 4096))},
		{key: "relaxed-pointer-b", want: string(bytes.Repeat([]byte("b"), 4096))},
		// DeleteRange has no explicit-sync variant. Its complete flushed frame
		// remains replayable above the last durable V2 frontier.
		{key: "range-delete", wantNil: true},
	}
	for _, tt := range tests {
		got, err := db.Get([]byte(tt.key))
		if err != nil {
			t.Fatalf("Get(%s): %v", tt.key, err)
		}
		if tt.wantNil {
			if got != nil {
				t.Fatalf("Get(%s)=%q, want nil", tt.key, string(got))
			}
			continue
		}
		if string(got) != tt.want {
			t.Fatalf("Get(%s)=%q, want %q", tt.key, string(got), tt.want)
		}
	}

	stats := db.Stats()
	applied, err := strconv.ParseUint(stats["treedb.applied_command_lsn"], 10, 64)
	if err != nil {
		t.Fatalf("parse applied_command_lsn=%q: %v", stats["treedb.applied_command_lsn"], err)
	}
	if applied != 8 {
		t.Fatalf("applied_command_lsn=%d, want 8 after replaying the durable mutation prefix (stats=%#v)", applied, stats)
	}
	if stats["treedb.cache.redo_log.mode"] != "external_command_wal" ||
		stats["treedb.cache.redo_log.enabled"] != "false" {
		t.Fatalf("unexpected command-WAL cached durability stats after reopen: %#v", stats)
	}
}

func TestCrashRecovery_CommandWALRelaxedRotatedDependenciesSurviveEmptySyncBarrier(t *testing.T) {
	dir := t.TempDir()
	runCommandWALRelaxedRotatedPrefixWriter(t, dir)

	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.CommandWALStatsScan = true
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, tt := range []struct {
		key   string
		value byte
	}{
		{key: "rotated-pointer-a", value: 'a'},
		{key: "rotated-pointer-b", value: 'b'},
	} {
		got, err := db.Get([]byte(tt.key))
		if err != nil {
			t.Fatalf("Get(%s): %v", tt.key, err)
		}
		want := bytes.Repeat([]byte{tt.value}, 4096)
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%s) len=%d, want %d bytes of %q", tt.key, len(got), len(want), tt.value)
		}
	}

	stats := db.Stats()
	if got := stats["treedb.applied_command_lsn"]; got != "3" {
		t.Fatalf("applied_command_lsn=%q, want acknowledged durable prefix 3 (stats=%#v)", got, stats)
	}
}

func TestCrashRecovery_DurabilityTiers(t *testing.T) {
	type tier struct {
		name        string
		profile     treedb.Profile
		env         []string
		expectLarge bool
	}

	tiers := []tier{
		{
			name:    "wal_off_strict_sync_large_value",
			profile: treedb.ProfileNoWALFast,
			env: []string{
				"TREEDB_CRASH_DISABLE_WAL=1",
				"TREEDB_CRASH_RELAXED_SYNC=0",
				"TREEDB_CRASH_LARGE_VALUE=1",
			},
			expectLarge: true,
		},
		{
			name:    "wal_off_relaxed_write_empty_sync_large_value",
			profile: treedb.ProfileNoWALFast,
			env: []string{
				"TREEDB_CRASH_DISABLE_WAL=1",
				"TREEDB_CRASH_RELAXED_SYNC=0",
				"TREEDB_CRASH_LARGE_VALUE=1",
				"TREEDB_CRASH_EMPTY_SYNC=1",
			},
			expectLarge: true,
		},
		{
			name:    "wal_on_relaxed_sync_large_value",
			profile: treedb.ProfileCommandWALRelaxed,
			env: []string{
				"TREEDB_CRASH_DISABLE_WAL=0",
				"TREEDB_CRASH_RELAXED_SYNC=1",
				"TREEDB_CRASH_LARGE_VALUE=1",
			},
			expectLarge: true,
		},
		{
			name:    "wal_on_strict_sync_large_value",
			profile: treedb.ProfileCommandWALDurable,
			env: []string{
				"TREEDB_CRASH_DISABLE_WAL=0",
				"TREEDB_CRASH_RELAXED_SYNC=0",
				"TREEDB_CRASH_LARGE_VALUE=1",
			},
			expectLarge: true,
		},
	}

	for _, tc := range tiers {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			runCrashRecoveryDurabilityWriter(t, dir, tc.env...)

			opts := treedb.OptionsFor(tc.profile, dir)
			opts.ChunkSize = 64 * 1024
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatalf("open: %v", err)
			}

			val, err := db.Get([]byte("k"))
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

			commandWALRequired := db.Stats()["treedb.command_wal.required_feature"] == "true"
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			if commandWALRequired {
				// Command-WAL recovery may leave a fresh active typed segment; a
				// second reopen proves no replayable legacy redo debt remains.
				reopened, err := treedb.Open(opts)
				if err != nil {
					t.Fatalf("reopen after recovery: %v", err)
				}
				defer reopened.Close()

				val, err := reopened.Get([]byte("k"))
				if err != nil {
					t.Fatalf("get k after recovery reopen: %v", err)
				}
				if tc.expectLarge {
					if len(val) != 4096 {
						t.Fatalf("get k after recovery reopen: got len %d, want %d", len(val), 4096)
					}
				} else if string(val) != "small" {
					t.Fatalf("get k after recovery reopen: got %q, want %q", string(val), "small")
				}
				return
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
					strings.HasPrefix(name, "commit-") {
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
	valueLogDir := filepath.Join(dir, "maindb", "value_vlog")
	for _, path := range []string{walDir, valueLogDir} {
		if err := os.MkdirAll(path, 0755); err != nil {
			t.Fatalf("mkdir log dir %s: %v", path, err)
		}
	}

	valuePath := filepath.Join(valueLogDir, "value-l0-000001.log")
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

	backend, err := treedb.Open(legacyCachedRecoveryOptions(dir))
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

	assertCommitLogCleared(t, commitPath)
	if _, err := os.Stat(valuePath); err != nil {
		t.Fatalf("expected valuelog file to remain after recovery: %v", err)
	}
}

func TestRecovery_CommitFence_PublishesOnlyCommittedVLogRefs(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "maindb", "wal")
	valueLogDir := filepath.Join(dir, "maindb", "value_vlog")
	for _, path := range []string{walDir, valueLogDir} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("mkdir log dir %s: %v", path, err)
		}
	}

	valuePath := filepath.Join(valueLogDir, "value-l0-000001.log")
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
	if err := cw.AppendBatch([]commitlog.Record{
		{Op: commitlog.OpSetRID, Key: []byte("k1"), RID: 1, Seq: 1},
	}); err != nil {
		_ = cw.Close()
		t.Fatalf("commitlog.AppendBatch seq1: %v", err)
	}
	if err := cw.AppendBatch([]commitlog.Record{
		{Op: commitlog.OpSetRID, Key: []byte("k2"), RID: 2, Seq: 2},
	}); err != nil {
		_ = cw.Close()
		t.Fatalf("commitlog.AppendBatch seq2: %v", err)
	}
	if err := cw.Sync(); err != nil {
		_ = cw.Close()
		t.Fatalf("commitlog.Sync: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("commitlog.Close: %v", err)
	}

	db, err := treedb.Open(legacyCachedRecoveryOptions(dir))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	got, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get k1: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("get k1: got %q want %q", string(got), "v1")
	}
	got, err = db.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("get k2: %v", err)
	}
	if got != nil {
		t.Fatalf("expected k2 to be absent because seq2 fence is unsatisfied, got %q", string(got))
	}
}

func TestRecovery_PartialFlushFence_NoPhantomPointers(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "maindb", "wal")
	valueLogDir := filepath.Join(dir, "maindb", "value_vlog")
	for _, path := range []string{walDir, valueLogDir} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("mkdir log dir %s: %v", path, err)
		}
	}

	valuePath := filepath.Join(valueLogDir, "value-l0-000001.log")
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
	if err := cw.AppendBatch([]commitlog.Record{
		{Op: commitlog.OpSetRID, Key: []byte("k-good"), RID: 1, Seq: 1},
		{Op: commitlog.OpSetRID, Key: []byte("k-missing"), RID: 2, Seq: 1},
	}); err != nil {
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

	db, err := treedb.Open(legacyCachedRecoveryOptions(dir))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	got, err := db.Get([]byte("k-good"))
	if err != nil {
		t.Fatalf("get k-good: %v", err)
	}
	if got != nil {
		t.Fatalf("expected k-good to be absent when mixed RID batch fence is unsatisfied, got %q", string(got))
	}
	got, err = db.Get([]byte("k-missing"))
	if err != nil {
		t.Fatalf("get k-missing: %v", err)
	}
	if got != nil {
		t.Fatalf("expected k-missing to be absent, got %q", string(got))
	}
}

func TestRecovery_MultiLaneOrdering(t *testing.T) {
	dir := t.TempDir()

	walDir := filepath.Join(dir, "maindb", "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	commitPathLane0 := filepath.Join(walDir, "commit-l0-000001.log")
	cw0, err := commitlog.NewWriter(commitPathLane0)
	if err != nil {
		t.Fatalf("commitlog.NewWriter lane0: %v", err)
	}
	rec0 := commitlog.Record{Op: commitlog.OpSetInline, Key: []byte("k"), Value: []byte("v2"), Seq: 2}
	if err := cw0.AppendBatch([]commitlog.Record{rec0}); err != nil {
		_ = cw0.Close()
		t.Fatalf("commitlog.AppendBatch lane0: %v", err)
	}
	if err := cw0.Sync(); err != nil {
		_ = cw0.Close()
		t.Fatalf("commitlog.Sync lane0: %v", err)
	}
	if err := cw0.Close(); err != nil {
		t.Fatalf("commitlog.Close lane0: %v", err)
	}

	commitPathLane1 := filepath.Join(walDir, "commit-l1-000001.log")
	cw1, err := commitlog.NewWriter(commitPathLane1)
	if err != nil {
		t.Fatalf("commitlog.NewWriter lane1: %v", err)
	}
	rec1 := commitlog.Record{Op: commitlog.OpSetInline, Key: []byte("k"), Value: []byte("v1"), Seq: 1}
	if err := cw1.AppendBatch([]commitlog.Record{rec1}); err != nil {
		_ = cw1.Close()
		t.Fatalf("commitlog.AppendBatch lane1: %v", err)
	}
	if err := cw1.Sync(); err != nil {
		_ = cw1.Close()
		t.Fatalf("commitlog.Sync lane1: %v", err)
	}
	if err := cw1.Close(); err != nil {
		t.Fatalf("commitlog.Close lane1: %v", err)
	}

	backend, err := treedb.Open(legacyCachedRecoveryOptions(dir))
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	val, err := backend.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "v2" {
		t.Fatalf("expected replay order to honor Seq; got %q", string(val))
	}
}

func TestRecovery_PartialCommitBatchIgnored(t *testing.T) {
	dir := t.TempDir()

	walDir := filepath.Join(dir, "maindb", "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	commitPath := filepath.Join(walDir, "commit-l0-000001.log")
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], 32)
	if err := os.WriteFile(commitPath, header[:], 0600); err != nil {
		t.Fatalf("write commit header: %v", err)
	}

	backend, err := treedb.Open(legacyCachedRecoveryOptions(dir))
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	val, err := backend.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if val != nil {
		t.Fatalf("expected partial batch to be ignored, got %q", string(val))
	}

	assertCommitLogCleared(t, commitPath)
}

func TestRecovery_MissingDictFails(t *testing.T) {
	dir := t.TempDir()

	walDir := filepath.Join(dir, "maindb", "wal")
	valueLogDir := filepath.Join(dir, "maindb", "value_vlog")
	for _, path := range []string{walDir, valueLogDir} {
		if err := os.MkdirAll(path, 0755); err != nil {
			t.Fatalf("mkdir log dir %s: %v", path, err)
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, "dictdb"), 0755); err != nil {
		t.Fatalf("mkdir dictdb: %v", err)
	}

	dictID := uint64(1)
	records := []valuelog.Record{{RID: 1, Value: bytes.Repeat([]byte("value"), 1024)}}
	valuePath := filepath.Join(valueLogDir, "value-l0-000001.log")
	frame, _, err := valuelog.EncodeFrame(0, nil, records)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}
	frameHeader, _, _, framePayload, err := valuelog.DecodeFrame(frame)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderCRC(false))
	if err != nil {
		t.Fatalf("zstd.NewWriter: %v", err)
	}
	compressed := enc.EncodeAll(framePayload, nil)
	enc.Close()
	if len(compressed) >= len(framePayload) {
		t.Fatalf("expected payload to compress for missing-dict test (raw=%d compressed=%d)", len(framePayload), len(compressed))
	}

	payloadStart := len(frame) - len(framePayload)
	compressedFrame := make([]byte, payloadStart+len(compressed))
	copy(compressedFrame, frame[:payloadStart])
	copy(compressedFrame[payloadStart:], compressed)
	compressedFrame[1] = frameHeader.Flags | valuelog.FrameFlagCompressed
	binary.LittleEndian.PutUint64(compressedFrame[4:12], dictID)

	raw := make([]byte, valuelog.HeaderSize+len(compressedFrame))
	raw[4] = valuelog.Version
	raw[5] = 1 // recordFlagGrouped
	raw[6] = 0
	raw[7] = 0
	binary.LittleEndian.PutUint64(raw[8:16], 0)
	binary.LittleEndian.PutUint32(raw[16:20], uint32(len(compressedFrame)))
	copy(raw[valuelog.HeaderSize:], compressedFrame)
	sum := crc.ChecksumParts(raw[4:valuelog.HeaderSize], compressedFrame)
	binary.LittleEndian.PutUint32(raw[0:4], sum)
	if err := os.WriteFile(valuePath, raw, 0600); err != nil {
		t.Fatalf("write valuelog: %v", err)
	}

	payload := raw[valuelog.HeaderSize:]
	frameHeader, _, _, _, err = valuelog.DecodeFrame(payload)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if frameHeader.Flags&valuelog.FrameFlagCompressed == 0 {
		t.Fatalf("expected compressed frame for missing-dict test")
	}
	if frameHeader.DictID != dictID {
		t.Fatalf("expected dict ID %d, got %d", dictID, frameHeader.DictID)
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

	if _, err := treedb.Open(legacyCachedRecoveryOptions(dir)); err == nil {
		t.Fatalf("expected recovery error due to missing dict")
	}
	if _, err := os.Stat(commitPath); err != nil {
		t.Fatalf("expected commitlog to remain after failed recovery: %v", err)
	}
	if _, err := os.Stat(valuePath); err != nil {
		t.Fatalf("expected valuelog to remain after failed recovery: %v", err)
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

	backend, err := treedb.Open(legacyCachedRecoveryOptions(dir))
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

	assertCommitLogCleared(t, commitPath)
}

func TestRecovery_TruncatedValueLogRecord(t *testing.T) {
	dir := t.TempDir()

	walDir := filepath.Join(dir, "maindb", "wal")
	valueLogDir := filepath.Join(dir, "maindb", "value_vlog")
	for _, path := range []string{walDir, valueLogDir} {
		if err := os.MkdirAll(path, 0755); err != nil {
			t.Fatalf("mkdir log dir %s: %v", path, err)
		}
	}

	valuePath := filepath.Join(valueLogDir, "value-l0-000001.log")
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

	backend, err := treedb.Open(legacyCachedRecoveryOptions(dir))
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

	assertCommitLogCleared(t, commitPath)
	if _, err := os.Stat(valuePath); err != nil {
		t.Fatalf("expected valuelog file to remain after recovery: %v", err)
	}
}
