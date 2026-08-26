package treedb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestPublicCommandWALDirectRoutesEmitAppendAndFlushCuts(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileCommandWALDurable, dir)
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer db.Close()

	assertCuts := func(name string, write func() error) {
		t.Helper()
		var events []durabilitycut.Event
		restore := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Resource == durabilitycut.ResourceCommandWAL && event.Point != "" {
				events = append(events, event)
			}
			return nil
		})
		err := write()
		restore()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		want := []durabilitycut.Point{
			durabilitycut.BeforeDependencyAppend,
			durabilitycut.AfterDependencyAppend,
			durabilitycut.BeforeDependencyFileSync,
			durabilitycut.AfterDependencyFileSync,
		}
		if len(events) != len(want) {
			t.Fatalf("%s events=%+v, want points %v", name, events, want)
		}
		for i, point := range want {
			if events[i].Point != point {
				t.Fatalf("%s event[%d].Point=%q, want %q (events=%+v)", name, i, events[i].Point, point, events)
			}
		}
		if events[1].LSN == 0 {
			t.Fatalf("%s mutation LSN=%d, want non-zero", name, events[1].LSN)
		}
		if events[1].Path == "" {
			t.Fatalf("%s after-append omitted exact segment path", name)
		}
		if events[2].Path == "" || events[3].Path != events[2].Path {
			t.Fatalf("%s sync paths=(%q,%q), want paired non-empty paths",
				name, events[2].Path, events[3].Path)
		}
	}

	assertCuts("Set", func() error {
		return db.Set([]byte("point"), []byte("value"))
	})
	assertCuts("SetSync", func() error {
		return db.SetSync([]byte("point-sync"), []byte("value"))
	})
	assertCuts("batch Write", func() error {
		b := db.NewBatch()
		defer b.Close()
		if err := b.Set([]byte("batch"), []byte("value")); err != nil {
			return err
		}
		return b.Write()
	})
	assertCuts("batch WriteSync", func() error {
		b := db.NewBatch()
		defer b.Close()
		if err := b.Set([]byte("batch-sync"), []byte("value")); err != nil {
			return err
		}
		return b.WriteSync()
	})
}

func TestPublicCommandWALRawKVWritesUseTypedFrames(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	if got := db.Stats()["treedb.write_path.mode"]; got != "command_wal_cached" {
		_ = db.Close()
		t.Fatalf("write_path.mode=%q, want command_wal_cached", got)
	}
	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		_ = db.Close()
		t.Fatalf("Set: %v", err)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("k2"), []byte("v2")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Delete([]byte("k1")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Delete: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	assertPublicCommandWALFrames(t, db, 2)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{
		Dir:                 dir,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("reopen command WAL from format: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	if got := reopen.Stats()["treedb.write_path.mode"]; got != "command_wal_cached" {
		t.Fatalf("reopen write_path.mode=%q, want command_wal_cached", got)
	}
	got, err := reopen.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("Get(k2): %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("Get(k2)=%q, want v2", got)
	}
	hasK1, err := reopen.Has([]byte("k1"))
	if err != nil {
		t.Fatalf("Has(k1): %v", err)
	}
	if hasK1 {
		t.Fatal("k1 exists after command-WAL batch delete")
	}
	if got := reopen.backend.State().AppliedCommandLSN; got < 2 {
		t.Fatalf("AppliedCommandLSN=%d, want at least 2", got)
	}
}

func TestPublicCommandWALDurablePointWriteSyncUsesExternalCachedMode(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileCommandWALDurable, dir)
	opts.CommandWALStatsScan = true
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	opts.FlushThreshold = 1 << 30
	opts.MaxQueuedMemtables = -1
	opts.WriterFlushMaxMemtables = 0
	opts.WriterFlushMaxDuration = 0
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL durable: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if got := stats["treedb.cache.redo_log.mode"]; got != "external_command_wal" {
		t.Fatalf("redo_log.mode=%q, want external_command_wal", got)
	}
	if got := stats["treedb.cache.redo_log.enabled"]; got != "false" {
		t.Fatalf("redo_log.enabled=%q, want false", got)
	}
	if got := stats["treedb.cache.command_wal.external_durability"]; got != "true" {
		t.Fatalf("command_wal.external_durability=%q, want true", got)
	}

	commitSeqBefore, err := strconv.ParseUint(stats["treedb.commit_seq"], 10, 64)
	if err != nil {
		t.Fatalf("parse commit_seq before: %v", err)
	}

	if err := db.SetSync([]byte("durable-point-sync"), []byte("value")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	if err := db.DeleteSync([]byte("durable-point-sync")); err != nil {
		t.Fatalf("DeleteSync: %v", err)
	}

	commitSeqAfterSync, err := strconv.ParseUint(db.Stats()["treedb.commit_seq"], 10, 64)
	if err != nil {
		t.Fatalf("parse commit_seq after sync: %v", err)
	}
	if commitSeqAfterSync != commitSeqBefore {
		t.Fatalf("commit_seq after point sync writes=%d want %d before explicit checkpoint", commitSeqAfterSync, commitSeqBefore)
	}
	assertPublicCommandWALFrames(t, db, 2)
}

func TestPublicCommandWALRawKVMethodMatrix(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	if err := db.Set([]byte("set"), []byte("before-delete")); err != nil {
		_ = db.Close()
		t.Fatalf("Set: %v", err)
	}
	if err := db.SetSync([]byte("set-sync"), []byte("before-delete")); err != nil {
		_ = db.Close()
		t.Fatalf("SetSync: %v", err)
	}
	if err := db.Delete([]byte("set")); err != nil {
		_ = db.Close()
		t.Fatalf("Delete: %v", err)
	}
	if err := db.DeleteSync([]byte("set-sync")); err != nil {
		_ = db.Close()
		t.Fatalf("DeleteSync: %v", err)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("batch-write"), []byte("visible-after-reopen")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	syncBatch := db.NewBatchWithSize(128)
	if err := syncBatch.Set([]byte("batch-sync"), []byte("visible-after-sync-reopen")); err != nil {
		_ = syncBatch.Close()
		_ = db.Close()
		t.Fatalf("sync batch Set: %v", err)
	}
	if err := syncBatch.WriteSync(); err != nil {
		_ = syncBatch.Close()
		_ = db.Close()
		t.Fatalf("sync batch WriteSync: %v", err)
	}
	if err := syncBatch.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("sync batch Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{
		Dir:                 dir,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("reopen command WAL from format: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	if got := reopen.Stats()["treedb.write_path.mode"]; got != "command_wal_cached" {
		t.Fatalf("reopen write_path.mode=%q, want command_wal_cached", got)
	}
	for _, key := range []string{"set", "set-sync"} {
		has, err := reopen.Has([]byte(key))
		if err != nil {
			t.Fatalf("Has(%s): %v", key, err)
		}
		if has {
			t.Fatalf("%s exists after command-WAL delete", key)
		}
	}
	for key, want := range map[string]string{
		"batch-write": "visible-after-reopen",
		"batch-sync":  "visible-after-sync-reopen",
	} {
		got, err := reopen.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("Get(%s)=%q, want %q", key, got, want)
		}
	}
	if got := reopen.backend.State().AppliedCommandLSN; got < 6 {
		t.Fatalf("AppliedCommandLSN=%d, want at least 6", got)
	}
}

func TestPublicCommandWALRawKVEmptyPointKeyAndZeroLengthValues(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}

	if err := db.Set([]byte{}, []byte("value")); err != nil {
		_ = db.Close()
		t.Fatalf("Set empty key: %v", err)
	}
	if err := db.SetSync([]byte("zero-sync"), nil); err != nil {
		_ = db.Close()
		t.Fatalf("SetSync nil value: %v", err)
	}
	if err := db.Delete(nil); err != nil {
		_ = db.Close()
		t.Fatalf("Delete nil/empty key: %v", err)
	}
	if err := db.Set(nil, nil); err != nil {
		_ = db.Close()
		t.Fatalf("Set nil key/value: %v", err)
	}

	b := db.NewBatch()
	if err := b.Set([]byte{}, []byte("batch-empty")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set empty key: %v", err)
	}
	if err := b.Set([]byte("batch-zero"), nil); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set nil value: %v", err)
	}
	if err := b.Delete([]byte("zero-sync")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Delete zero-sync: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	assertPublicCommandWALFrames(t, db, 5)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true})
	if err != nil {
		t.Fatalf("reopen command WAL dir: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	requireRawKVValue(t, reopen, []byte{}, []byte("batch-empty"))
	requireRawKVValue(t, reopen, nil, []byte("batch-empty"))
	requireRawKVValue(t, reopen, []byte("batch-zero"), []byte{})
	has, err := reopen.Has([]byte("zero-sync"))
	if err != nil {
		t.Fatalf("Has(zero-sync): %v", err)
	}
	if has {
		t.Fatal("zero-sync exists after command-WAL batch delete")
	}
	if got := reopen.backend.State().AppliedCommandLSN; got < 5 {
		t.Fatalf("AppliedCommandLSN=%d, want at least 5", got)
	}
}

func TestPublicCommandWALRejectsUnsupportedCachedRawMutations(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Set([]byte("keep"), []byte("original")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	updateCalled := false
	if err := db.Update([]byte("keep"), func(old []byte) (UpdateResult, error) {
		updateCalled = true
		return SetUpdate([]byte("updated")), nil
	}); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("Update err=%v, want ErrCommandWALRejected", err)
	}
	if updateCalled {
		t.Fatal("Update callback ran after command-WAL rejection")
	}
	updateSyncCalled := false
	if err := db.UpdateSync([]byte("keep"), func(old []byte) (UpdateResult, error) {
		updateSyncCalled = true
		return DeleteUpdate(), nil
	}); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("UpdateSync err=%v, want ErrCommandWALRejected", err)
	}
	if updateSyncCalled {
		t.Fatal("UpdateSync callback ran after command-WAL rejection")
	}
	got, err := db.Get([]byte("keep"))
	if err != nil {
		t.Fatalf("Get(keep): %v", err)
	}
	if string(got) != "original" {
		t.Fatalf("Get(keep)=%q, want original", got)
	}
	stats := db.Stats()
	for _, route := range []string{"update", "update_sync"} {
		key := "treedb.raw.span_native.public.route." + route + ".fallback.reason.command_wal_barrier.count_total"
		raw := stats[key]
		count, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("parse %s=%q: %v", key, raw, err)
		}
		if count != 1 {
			t.Fatalf("%s=%d want 1", key, count)
		}
	}
}

func assertPublicCommandWALFrames(t *testing.T, db *DB, minFrames uint64) {
	t.Helper()
	stats := db.Stats()
	if stats["treedb.command_wal.required_feature"] != "true" {
		t.Fatalf("required_feature=%q, want true", stats["treedb.command_wal.required_feature"])
	}
	if stats["treedb.command_wal.stats_scan"] != "true" {
		t.Fatalf("stats_scan=%q, want true", stats["treedb.command_wal.stats_scan"])
	}
	frames := publicCommandWALFrameCount(t, db)
	if frames < minFrames {
		t.Fatalf("command_wal.frames=%d, want at least %d", frames, minFrames)
	}
	maxLSN, err := strconv.ParseUint(stats["treedb.command_wal.max_lsn"], 10, 64)
	if err != nil {
		t.Fatalf("parse command_wal.max_lsn=%q: %v", stats["treedb.command_wal.max_lsn"], err)
	}
	if maxLSN < minFrames {
		t.Fatalf("command_wal.max_lsn=%d, want at least %d", maxLSN, minFrames)
	}
}

func publicCommandWALFrameCount(t *testing.T, db *DB) uint64 {
	t.Helper()
	stats := db.Stats()
	frames, err := strconv.ParseUint(stats["treedb.command_wal.frames"], 10, 64)
	if err != nil {
		t.Fatalf("parse command_wal.frames=%q: %v", stats["treedb.command_wal.frames"], err)
	}
	return frames
}

func statMapUint64(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw := stats[key]
	got, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", key, raw, err)
	}
	return got
}

func publicStatUint64(t *testing.T, db *DB, key string) uint64 {
	t.Helper()
	return statMapUint64(t, db.Stats(), key)
}

func requirePublicStatDelta(t *testing.T, before, after map[string]string, key string, want uint64) {
	t.Helper()
	got := statMapUint64(t, after, key) - statMapUint64(t, before, key)
	if got != want {
		t.Fatalf("%s delta=%d, want %d (before=%#v after=%#v)", key, got, want, before, after)
	}
}

func requirePublicBatchWriteSyncPhasePartitions(t *testing.T, stats map[string]string) {
	t.Helper()
	prefix := "treedb.public.batch.write_sync.phase."
	wall := statMapUint64(t, stats, prefix+"wall.ns_total")
	topLevel := statMapUint64(t, stats, prefix+"checkpoint_gate.ns_total") +
		statMapUint64(t, stats, prefix+"preflight_materialization.ns_total") +
		statMapUint64(t, stats, prefix+"command_callback.ns_total") +
		statMapUint64(t, stats, prefix+"memtable_publication_reset.ns_total") +
		statMapUint64(t, stats, prefix+"residual.ns_total")
	if topLevel != wall {
		t.Fatalf("request phase sum=%d, want wall=%d (stats=%#v)", topLevel, wall, stats)
	}
	commandCallback := statMapUint64(t, stats, prefix+"command_callback.ns_total")
	commandPartition := statMapUint64(t, stats, prefix+"command_public_payload_entry_scan_preparation.ns_total") +
		statMapUint64(t, stats, prefix+"command_publish_lock_barrier_wait.ns_total") +
		statMapUint64(t, stats, prefix+"command_backend_intent_planning_serialization.ns_total") +
		statMapUint64(t, stats, prefix+"command_external_ref_ordering.ns_total") +
		statMapUint64(t, stats, prefix+"command_append.ns_total") +
		statMapUint64(t, stats, prefix+"command_flush.ns_total") +
		statMapUint64(t, stats, prefix+"command_group_commit_wait.ns_total") +
		statMapUint64(t, stats, prefix+"command_sync.ns_total") +
		statMapUint64(t, stats, prefix+"command_post_append_pending_lsn_bookkeeping.ns_total") +
		statMapUint64(t, stats, prefix+"command_empty_barrier.ns_total") +
		statMapUint64(t, stats, prefix+"command_other.ns_total")
	if commandPartition != commandCallback {
		t.Fatalf("command phase sum=%d, want callback=%d (stats=%#v)", commandPartition, commandCallback, stats)
	}
	if got := statMapUint64(t, stats, prefix+"top_level_partition_overruns_total"); got != 0 {
		t.Fatalf("top-level partition overruns=%d, want 0 (stats=%#v)", got, stats)
	}
	if got := statMapUint64(t, stats, prefix+"command_partition_overruns_total"); got != 0 {
		t.Fatalf("command partition overruns=%d, want 0 (stats=%#v)", got, stats)
	}
}

func TestPublicBatchWriteSyncPhaseStatsMakesNestedPreparationExclusive(t *testing.T) {
	var phases publicBatchWriteSyncPhaseStats
	phases.observe(time.Now().Add(-time.Millisecond), nil, publicBatchWriteSyncPhaseSample{
		// Backend planning begins before the public preparation callback, so
		// this 100ns interval contains the 30ns preparation interval.
		commandCallback:                           100 * time.Nanosecond,
		commandPublicPayloadEntryScanPreparation:  30 * time.Nanosecond,
		commandPublicPreparationObserved:          true,
		commandBackendIntentPlanningSerialization: 100 * time.Nanosecond,
		commandBackendIntentPlanningObserved:      true,
	})

	stats := make(map[string]string)
	publicBatchWriteSyncPhaseStatsInto(stats, true, &phases)
	prefix := "treedb.public.batch.write_sync.phase."
	if got := statMapUint64(t, stats, prefix+"command_backend_intent_planning_serialization.ns_total"); got != 70 {
		t.Fatalf("exclusive backend planning ns=%d, want 70 after removing nested preparation (stats=%#v)", got, stats)
	}
	requirePublicBatchWriteSyncPhasePartitions(t, stats)
}

func TestPublicBatchWriteSyncPhaseStatsSignalsMalformedNestedTiming(t *testing.T) {
	var phases publicBatchWriteSyncPhaseStats
	phases.observe(time.Now().Add(-time.Millisecond), nil, publicBatchWriteSyncPhaseSample{
		// This cannot arise from either measured command-WAL construction path:
		// backend timing starts before preparation and ends after it. Keep the
		// overrun visible instead of manufacturing a negative backend phase if
		// a future source violates that containment contract.
		commandCallback:                           100 * time.Nanosecond,
		commandPublicPayloadEntryScanPreparation:  101 * time.Nanosecond,
		commandPublicPreparationObserved:          true,
		commandBackendIntentPlanningSerialization: 100 * time.Nanosecond,
		commandBackendIntentPlanningObserved:      true,
	})

	stats := make(map[string]string)
	publicBatchWriteSyncPhaseStatsInto(stats, true, &phases)
	prefix := "treedb.public.batch.write_sync.phase."
	if got := statMapUint64(t, stats, prefix+"command_backend_intent_planning_serialization.ns_total"); got != 100 {
		t.Fatalf("malformed backend planning ns=%d, want unmodified 100 (stats=%#v)", got, stats)
	}
	if got := statMapUint64(t, stats, prefix+"command_partition_overruns_total"); got != 1 {
		t.Fatalf("malformed timing overruns=%d, want 1 (stats=%#v)", got, stats)
	}
}

func requirePublicCommandWALNoCheckpointSince(t *testing.T, db *DB, before map[string]string) {
	t.Helper()
	after := db.Stats()
	for _, key := range []string{
		"treedb.commit_seq",
		"treedb.public.checkpoint.calls_total",
		"treedb.cache.checkpoint.runs",
	} {
		if got, want := after[key], before[key]; got != want {
			t.Fatalf("%s after public write=%q, want unchanged %q (before=%#v after=%#v)", key, got, want, before, after)
		}
	}
}

func commandWALDurabilityProofOptions(dir string) Options {
	opts := OptionsFor(ProfileCommandWALDurable, dir)
	opts.CommandWALStatsScan = true
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	opts.FlushThreshold = 1 << 30
	opts.MaxQueuedMemtables = -1
	opts.WriterFlushMaxMemtables = 0
	opts.WriterFlushMaxDuration = 0
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	return opts
}

func TestPublicCommandWALRuntimeStatsExposeAppendAndSyncCounters(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
	opts.CommandWALStatsScan = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	before := db.Stats()
	for _, key := range []string{
		"treedb.command_wal.append.count_total",
		"treedb.command_wal.append.point.count_total",
		"treedb.command_wal.append.payload.count_total",
		"treedb.command_wal.flush.count_total",
		"treedb.command_wal.sync.count_total",
	} {
		if got := statMapUint64(t, before, key); got != 0 {
			t.Fatalf("%s before writes=%d, want 0 (stats=%#v)", key, got, before)
		}
	}

	if err := db.Set([]byte("point"), []byte("one")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("payload"), []byte("two")); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}

	stats := db.Stats()
	for key, want := range map[string]uint64{
		"treedb.command_wal.append.count_total":         2,
		"treedb.command_wal.append.point.count_total":   1,
		"treedb.command_wal.append.payload.count_total": 1,
		"treedb.command_wal.flush.count_total":          1,
		"treedb.command_wal.sync.count_total":           1,
	} {
		if got := statMapUint64(t, stats, key); got != want {
			t.Fatalf("%s=%d, want %d (stats=%#v)", key, got, want, stats)
		}
	}
	for _, key := range []string{
		"treedb.command_wal.append.ns_total",
		"treedb.command_wal.flush.ns_total",
		"treedb.command_wal.sync.ns_total",
	} {
		// Very short timers can round to zero on some CI clocks; the matching
		// count stats above prove the intended paths were observed.
		_ = statMapUint64(t, stats, key)
	}
	for _, key := range []string{
		"treedb.command_wal.append.point.ns_total",
		"treedb.command_wal.append.payload.ns_total",
	} {
		_ = statMapUint64(t, stats, key)
	}
}

func TestPublicCommandWALBatchWriteSyncPhaseStatsDisabledByDefault(t *testing.T) {
	db, err := Open(commandWALDurabilityProofOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	before := db.Stats()
	prefix := "treedb.public.batch.write_sync.phase."
	if got := before[prefix+"enabled"]; got != "false" {
		t.Fatalf("phase enabled=%q, want false", got)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("disabled"), []byte("value")); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}
	after := db.Stats()
	requirePublicStatDelta(t, before, after, "treedb.public.batch.write_sync.calls_total", 1)
	if got := statMapUint64(t, after, prefix+"calls_total"); got != 0 {
		t.Fatalf("disabled phase calls=%d, want 0 (stats=%#v)", got, after)
	}
	if got := statMapUint64(t, after, prefix+"wall.ns_total"); got != 0 {
		t.Fatalf("disabled phase wall=%d, want 0 (stats=%#v)", got, after)
	}
}

func TestPublicCommandWALBatchWriteSyncPhaseStatsAttributesSoloDurableSync(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	opts.PublicBatchWriteSyncPhaseStats = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := db.NewBatch()
	if err := b.Set([]byte("solo-sync"), []byte("value")); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}

	stats := db.Stats()
	prefix := "treedb.public.batch.write_sync.phase."
	if got := statMapUint64(t, stats, prefix+"command_sync.calls_total"); got != 1 {
		t.Fatalf("phase command sync calls=%d, want 1 for solo durable sync (stats=%#v)", got, stats)
	}
	if got := statMapUint64(t, stats, prefix+"command_sync.ns_total"); got == 0 {
		t.Fatalf("phase command sync ns=0, want measured solo durable sync (stats=%#v)", stats)
	}
	if got := statMapUint64(t, stats, prefix+"command_flush.calls_total"); got != 0 {
		t.Fatalf("phase command flush calls=%d, want 0 because solo durable flush includes sync (stats=%#v)", got, stats)
	}
	if got := statMapUint64(t, stats, prefix+"command_group_commit_wait.calls_total"); got != 0 {
		t.Fatalf("phase command group wait calls=%d, want 0 for solo durable bypass (stats=%#v)", got, stats)
	}
	requirePublicBatchWriteSyncPhasePartitions(t, stats)
}

func TestPublicCommandWALBatchWriteSyncPhaseStatsAreRequestScopedAndAdditive(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	opts.PublicBatchWriteSyncPhaseStats = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.commandWALGroupCommit.testBeforeSync = func(int) {}

	prefix := "treedb.public.batch.write_sync.phase."
	before := db.Stats()
	if got := before[prefix+"enabled"]; got != "true" {
		t.Fatalf("phase enabled=%q, want true", got)
	}
	if got := before[prefix+"top_level_partition"]; got != "checkpoint_gate+preflight_materialization+command_callback+memtable_publication_reset+residual" {
		t.Fatalf("top-level partition=%q", got)
	}
	if got := before[prefix+"command_callback_partition"]; got != "command_public_payload_entry_scan_preparation+command_publish_lock_barrier_wait+command_backend_intent_planning_serialization+command_external_ref_ordering+command_append+command_flush+command_group_commit_wait+command_sync+command_post_append_pending_lsn_bookkeeping+command_empty_barrier+command_other" {
		t.Fatalf("command partition=%q", got)
	}

	// A non-sync public batch and an explicit checkpoint both touch global
	// command-WAL counters, but must not enter this request-scoped WriteSync lane.
	writeBatch := db.NewBatch()
	if err := writeBatch.Set([]byte("write-noise"), []byte("value")); err != nil {
		_ = writeBatch.Close()
		t.Fatalf("noise batch Set: %v", err)
	}
	if err := writeBatch.Write(); err != nil {
		_ = writeBatch.Close()
		t.Fatalf("noise batch Write: %v", err)
	}
	if err := writeBatch.Close(); err != nil {
		t.Fatalf("noise batch Close: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := statMapUint64(t, db.Stats(), prefix+"calls_total"); got != 0 {
		t.Fatalf("phase calls after non-WriteSync traffic=%d, want 0", got)
	}

	payloadBatch := db.NewBatch()
	if err := payloadBatch.Set([]byte("payload"), []byte("one")); err != nil {
		_ = payloadBatch.Close()
		t.Fatalf("payload batch Set: %v", err)
	}
	if err := payloadBatch.WriteSync(); err != nil {
		_ = payloadBatch.Close()
		t.Fatalf("payload batch WriteSync: %v", err)
	}
	if err := payloadBatch.Close(); err != nil {
		t.Fatalf("payload batch Close: %v", err)
	}

	entryScanBatch, ok := db.NewBatch().(*commandWALPublicBatch)
	if !ok {
		t.Fatalf("NewBatch type=%T, want *commandWALPublicBatch", db.NewBatch())
	}
	if err := entryScanBatch.SetView([]byte("entry-scan"), []byte("two")); err != nil {
		_ = entryScanBatch.Close()
		t.Fatalf("entry-scan batch SetView: %v", err)
	}
	if err := entryScanBatch.WriteSync(); err != nil {
		_ = entryScanBatch.Close()
		t.Fatalf("entry-scan batch WriteSync: %v", err)
	}
	if err := entryScanBatch.Close(); err != nil {
		t.Fatalf("entry-scan batch Close: %v", err)
	}

	// Empty WriteSync still has a request-scoped command barrier. Keep it in a
	// distinct leaf so command_other remains honest for dirty callbacks.
	emptyBatch := db.NewBatch()
	if err := emptyBatch.WriteSync(); err != nil {
		_ = emptyBatch.Close()
		t.Fatalf("empty batch WriteSync: %v", err)
	}
	if err := emptyBatch.Close(); err != nil {
		t.Fatalf("empty batch Close: %v", err)
	}

	after := db.Stats()
	requirePublicStatDelta(t, before, after, "treedb.public.batch.write_sync.calls_total", 3)
	if got := statMapUint64(t, after, prefix+"calls_total"); got != 3 {
		t.Fatalf("phase calls=%d, want 3 (stats=%#v)", got, after)
	}
	if got := statMapUint64(t, after, prefix+"errors_total"); got != 0 {
		t.Fatalf("phase errors=%d, want 0 (stats=%#v)", got, after)
	}
	if got := statMapUint64(t, after, prefix+"command_append.calls_total"); got != 2 {
		t.Fatalf("phase command append calls=%d, want 2 (stats=%#v)", got, after)
	}
	if got := statMapUint64(t, after, prefix+"command_group_commit_wait.calls_total"); got != 2 {
		t.Fatalf("phase command group wait calls=%d, want 2 (stats=%#v)", got, after)
	}
	if got := statMapUint64(t, after, prefix+"command_group_commit_wait.ns_total"); got == 0 {
		t.Fatalf("phase command group wait ns=0, want measured wait (stats=%#v)", after)
	}
	if got := statMapUint64(t, after, prefix+"command_sync.calls_total"); got != 0 {
		t.Fatalf("phase command sync calls=%d, want 0 because shared sync is group-scoped (stats=%#v)", got, after)
	}
	if got := statMapUint64(t, after, prefix+"command_public_payload_entry_scan_preparation.calls_total"); got != 2 {
		t.Fatalf("phase public preparation calls=%d, want 2 (stats=%#v)", got, after)
	}
	if got := statMapUint64(t, after, prefix+"command_publish_lock_barrier_wait.calls_total"); got != 2 {
		t.Fatalf("phase publish lock/barrier calls=%d, want 2 (stats=%#v)", got, after)
	}
	if got := statMapUint64(t, after, prefix+"command_backend_intent_planning_serialization.calls_total"); got != 2 {
		t.Fatalf("phase backend planning calls=%d, want 2 (stats=%#v)", got, after)
	}
	if got := statMapUint64(t, after, prefix+"command_post_append_pending_lsn_bookkeeping.calls_total"); got != 2 {
		t.Fatalf("phase post-append bookkeeping calls=%d, want 2 (stats=%#v)", got, after)
	}
	if got := statMapUint64(t, after, prefix+"command_empty_barrier.calls_total"); got != 1 {
		t.Fatalf("phase empty barrier calls=%d, want 1 (stats=%#v)", got, after)
	}
	if got := statMapUint64(t, after, "treedb.command_wal.append.count_total"); got <= 2 {
		t.Fatalf("global command append count=%d, want > request-scoped 2 after noise (stats=%#v)", got, after)
	}
	requirePublicBatchWriteSyncPhasePartitions(t, after)
}

func TestPublicCommandWALBatchWriteSyncPhaseStatsEnvironmentOverride(t *testing.T) {
	t.Setenv(envPublicBatchWriteSyncPhaseStats, "true")
	db, err := Open(commandWALDurabilityProofOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	if got := db.Stats()["treedb.public.batch.write_sync.phase.enabled"]; got != "true" {
		t.Fatalf("phase enabled=%q, want true from %s", got, envPublicBatchWriteSyncPhaseStats)
	}
}

func TestPublicCommandWALBatchWriteSyncPhaseStatsLabelsRelaxedExplicitSync(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	ApplyProfile(&opts, ProfileCommandWALRelaxed)
	opts.PublicBatchWriteSyncPhaseStats = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.commandWALGroupCommit.testBeforeSync = func(int) {}

	b := db.NewBatch()
	if err := b.Set([]byte("relaxed"), []byte("value")); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}

	stats := db.Stats()
	prefix := "treedb.public.batch.write_sync.phase."
	if got := statMapUint64(t, stats, prefix+"command_group_commit_wait.calls_total"); got != 1 {
		t.Fatalf("phase command group wait calls=%d, want 1 (stats=%#v)", got, stats)
	}
	if got := statMapUint64(t, stats, prefix+"command_sync.calls_total"); got != 0 {
		t.Fatalf("phase command sync calls=%d, want 0 because shared sync is group-scoped (stats=%#v)", got, stats)
	}
	requirePublicBatchWriteSyncPhasePartitions(t, stats)
}

func TestPublicCommandWALBatchWriteSyncExternalRefOrderingPhaseStats(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	opts.PublicBatchWriteSyncPhaseStats = true
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.commandWALGroupCommit.testBeforeSync = func(int) {}
	before := db.Stats()
	value := bytes.Repeat([]byte("v"), 4096)

	b := db.NewBatch()
	if err := b.Set([]byte("external-ref"), value); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}

	stats := db.Stats()
	prefix := "treedb.public.batch.write_sync.phase."
	if got := statMapUint64(t, stats, prefix+"command_external_ref_ordering.calls_total"); got != 1 {
		t.Fatalf("phase external-ref planning calls=%d, want 1 observed no-op planning phase (stats=%#v)", got, stats)
	}
	requirePublicStatDelta(t, before, stats, "treedb.command_wal.sync.count_total", 1)
	// The mutation frame and the durable-prefix barrier are distinct WAL
	// writes, but the group still owns exactly one shared file sync.
	requirePublicStatDelta(t, before, stats, "treedb.command_wal.write.syscalls_total", 2)
	requirePublicStatDelta(t, before, stats, "treedb.command_wal.file_sync.calls_total", 1)
	requirePublicStatDelta(t, before, stats, "treedb.cache.value_log.sync.calls_total", 0)
	requirePublicStatDelta(t, before, stats, "treedb.cache.value_log.sync.materialization.calls_total", 0)
	requirePublicStatDelta(t, before, stats, "treedb.cache.value_log.sync.external_ref.calls_total", 0)
	requirePublicStatDelta(t, before, stats, "treedb.cache.value_log.file_sync.calls_total", 0)
	requirePublicBatchWriteSyncPhasePartitions(t, stats)

	r, err := commitlog.NewReader(filepath.Join(backenddb.WALDirPath(opts.Dir), "commit-l0-000001.log"))
	if err != nil {
		t.Fatalf("NewReader materialized command frame: %v", err)
	}
	env, err := r.ReadCommandFrame()
	if err != nil {
		_ = r.Close()
		t.Fatalf("ReadCommandFrame materialized command frame: %v", err)
	}
	if env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV2 {
		_ = r.Close()
		t.Fatalf("payload format=%d, want RawKVBatchV2", env.PayloadFormat)
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(env.Payload)
	if err != nil {
		_ = r.Close()
		t.Fatalf("DecodeRawKVBatchPayload materialized command frame: %v", err)
	}
	if len(ops) != 1 || ops[0].Op != commitlog.RawKVOpSetMaterializedRID || ops[0].RID == 0 ||
		string(ops[0].Key) != "external-ref" || !bytes.Equal(ops[0].Value, value) {
		_ = r.Close()
		t.Fatalf("materialized command ops=%+v", ops)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close materialized command reader: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint materialized command: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close materialized command DB: %v", err)
	}
	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen materialized command DB: %v", err)
	}
	defer reopen.Close()
	requireRawKVValue(t, reopen, []byte("external-ref"), value)
}

func TestPublicCommandWALMaterializedRIDRequiresStrictDurableWrite(t *testing.T) {
	tests := []struct {
		name    string
		relaxed bool
		write   func(Batch) error
	}{
		{
			name:    "relaxed-ordinary-write",
			relaxed: true,
			write:   func(b Batch) error { return b.Write() },
		},
		{
			name:    "relaxed-write-sync",
			relaxed: true,
			write:   func(b Batch) error { return b.WriteSync() },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := commandWALDurabilityProofOptions(t.TempDir())
			if tc.relaxed {
				ApplyProfile(&opts, ProfileCommandWALRelaxed)
			}
			opts.ValueLog.PointerThreshold = 1
			opts.ValueLog.ForcePointers = true
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("Open command WAL: %v", err)
			}
			defer func() { _ = db.Close() }()

			b := db.NewBatch()
			if err := b.Set([]byte("external-ref"), bytes.Repeat([]byte("v"), 4096)); err != nil {
				_ = b.Close()
				t.Fatalf("batch Set: %v", err)
			}
			if err := tc.write(b); err != nil {
				_ = b.Close()
				t.Fatalf("batch write: %v", err)
			}
			if err := b.Close(); err != nil {
				t.Fatalf("batch Close: %v", err)
			}

			r, err := commitlog.NewReader(filepath.Join(backenddb.WALDirPath(opts.Dir), "commit-l0-000001.log"))
			if err != nil {
				t.Fatalf("NewReader command frame: %v", err)
			}
			defer r.Close()
			env, err := r.ReadCommandFrame()
			if err != nil {
				t.Fatalf("ReadCommandFrame: %v", err)
			}
			if env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 {
				t.Fatalf("payload format=%d, want RawKVBatchV1 outside strict durable writes", env.PayloadFormat)
			}
			ops, err := commitlog.DecodeRawKVBatchPayload(env.Payload)
			if err != nil {
				t.Fatalf("DecodeRawKVBatchPayload: %v", err)
			}
			if len(ops) != 1 || ops[0].Op != commitlog.RawKVOpSetRID {
				t.Fatalf("command ops=%+v, want one SetRID dependency", ops)
			}
		})
	}
}

func TestPublicCommandWALMaterializedRIDTotalOperationBoundFallsBackAtomically(t *testing.T) {
	for _, tc := range []struct {
		name       string
		totalOps   int
		wantFormat commitlog.PayloadFormat
		wantFirst  commitlog.RawKVOp
	}{
		{name: "at-bound", totalOps: 256, wantFormat: commitlog.PayloadFormatRawKVBatchV2, wantFirst: commitlog.RawKVOpSetMaterializedRID},
		{name: "over-bound", totalOps: 257, wantFormat: commitlog.PayloadFormatRawKVBatchV1, wantFirst: commitlog.RawKVOpSetRID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := commandWALDurabilityProofOptions(t.TempDir())
			opts.ValueLog.PointerThreshold = 1024
			opts.ValueLog.ForcePointers = false
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("Open command WAL: %v", err)
			}
			defer func() { _ = db.Close() }()

			b := db.NewBatchWithSize(tc.totalOps)
			if err := b.Set([]byte("pointer"), bytes.Repeat([]byte("p"), 4096)); err != nil {
				_ = b.Close()
				t.Fatalf("batch pointer Set: %v", err)
			}
			for i := 1; i < tc.totalOps; i++ {
				if err := b.Set([]byte(fmt.Sprintf("inline-%03d", i)), []byte("i")); err != nil {
					_ = b.Close()
					t.Fatalf("batch inline Set %d: %v", i, err)
				}
			}
			if err := b.WriteSync(); err != nil {
				_ = b.Close()
				t.Fatalf("batch WriteSync: %v", err)
			}
			if err := b.Close(); err != nil {
				t.Fatalf("batch Close: %v", err)
			}

			r, err := commitlog.NewReader(filepath.Join(backenddb.WALDirPath(opts.Dir), "commit-l0-000001.log"))
			if err != nil {
				t.Fatalf("NewReader operation bound: %v", err)
			}
			defer r.Close()
			env, err := r.ReadCommandFrame()
			if err != nil {
				t.Fatalf("ReadCommandFrame operation bound: %v", err)
			}
			if env.PayloadFormat != tc.wantFormat {
				t.Fatalf("operation-bound payload format=%d, want %d", env.PayloadFormat, tc.wantFormat)
			}
			ops, err := commitlog.DecodeRawKVBatchPayload(env.Payload)
			if err != nil {
				t.Fatalf("DecodeRawKVBatchPayload operation bound: %v", err)
			}
			if len(ops) != tc.totalOps {
				t.Fatalf("operation-bound ops len=%d, want %d", len(ops), tc.totalOps)
			}
			if ops[0].Op != tc.wantFirst {
				t.Fatalf("operation-bound first op=%+v, want op %d", ops[0], tc.wantFirst)
			}
			for i := 1; i < len(ops); i++ {
				if ops[i].Op != commitlog.RawKVOpSet {
					t.Fatalf("operation-bound op[%d]=%+v, want inline Set", i, ops[i])
				}
			}
		})
	}
}

func TestPublicCommandWALMaterializedRIDRecoveryPreservesCachedAllocatorHighWater(t *testing.T) {
	dir := t.TempDir()
	opts := commandWALDurabilityProofOptions(dir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true

	bootstrap, err := Open(opts)
	if err != nil {
		t.Fatalf("Open bootstrap command WAL: %v", err)
	}
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap command WAL: %v", err)
	}

	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	valuePath := filepath.Join(backenddb.ValueLogDirPath(dir), "value-l0-000001.log")
	valueWriter, err := valuelog.NewWriter(valuePath, fileID)
	if err != nil {
		t.Fatalf("NewWriter existing high RID: %v", err)
	}
	if _, err := valueWriter.Append(0, nil, 200, []byte("existing-high-water")); err != nil {
		_ = valueWriter.Close()
		t.Fatalf("Append existing high RID: %v", err)
	}
	if err := valueWriter.Sync(); err != nil {
		_ = valueWriter.Close()
		t.Fatalf("Sync existing high RID: %v", err)
	}
	if err := valueWriter.Close(); err != nil {
		t.Fatalf("Close existing high RID: %v", err)
	}

	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op:    commitlog.RawKVOpSetMaterializedRID,
		Key:   []byte("recovered"),
		Value: []byte("materialized-older-rid"),
		RID:   42,
	}})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	commandPath := filepath.Join(backenddb.WALDirPath(dir), "commit-l0-000001.log")
	commandWriter, err := commitlog.NewWriter(commandPath)
	if err != nil {
		t.Fatalf("NewWriter materialized command: %v", err)
	}
	if err := commandWriter.AppendCommand(commitlog.CommandEnvelope{
		Version:         commitlog.CommandFrameVersionV2,
		DurabilityClass: commitlog.CommandDurabilityDurable,
		LSN:             1,
		Kind:            commitlog.CommandKindRawKVBatch,
		Scope:           commitlog.CommandScopeRawKV,
		PayloadFormat:   commitlog.PayloadFormatRawKVBatchV2,
		Payload:         payload,
	}); err != nil {
		_ = commandWriter.Close()
		t.Fatalf("AppendCommand materialized command: %v", err)
	}
	if err := commandWriter.Sync(); err != nil {
		_ = commandWriter.Close()
		t.Fatalf("Sync materialized command: %v", err)
	}
	if err := commandWriter.Close(); err != nil {
		t.Fatalf("Close materialized command: %v", err)
	}

	recovered, err := Open(opts)
	if err != nil {
		t.Fatalf("Open materialized recovery: %v", err)
	}
	requireRawKVValue(t, recovered, []byte("recovered"), []byte("materialized-older-rid"))
	startRID, err := recovered.cached.ReserveValueLogRIDs(1)
	if err != nil {
		_ = recovered.Close()
		t.Fatalf("ReserveValueLogRIDs after materialized recovery: %v", err)
	}
	if startRID <= 200 {
		_ = recovered.Close()
		t.Fatalf("cached allocator start RID=%d, want a RID above the all-segment high-water 200", startRID)
	}
	if err := recovered.SetSync([]byte("foreground"), bytes.Repeat([]byte("f"), 4096)); err != nil {
		_ = recovered.Close()
		t.Fatalf("foreground pointer SetSync: %v", err)
	}
	if err := recovered.Checkpoint(); err != nil {
		_ = recovered.Close()
		t.Fatalf("Checkpoint after foreground pointer: %v", err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatalf("Close after foreground pointer: %v", err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen after foreground pointer: %v", err)
	}
	defer reopened.Close()
	requireRawKVValue(t, reopened, []byte("recovered"), []byte("materialized-older-rid"))
	requireRawKVValue(t, reopened, []byte("foreground"), bytes.Repeat([]byte("f"), 4096))
}

func TestPublicCommandWALStateShapedDurabilityLedger(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	ApplyProfile(&opts, ProfileCommandWALRelaxed)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	before := db.Stats()
	if err := db.Set([]byte("state/unsynced"), []byte("u")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.SetSync([]byte("state/synced"), []byte("s")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	b, ok := db.NewBatch().(*commandWALPublicBatch)
	if !ok {
		t.Fatalf("NewBatch type=%T, want *commandWALPublicBatch", db.NewBatch())
	}
	if err := b.SetView([]byte("state/batch"), []byte("b")); err != nil {
		_ = b.Close()
		t.Fatalf("batch SetView: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}
	after := db.Stats()

	for key, want := range map[string]uint64{
		"treedb.command_wal.append.count_total":            3,
		"treedb.command_wal.append.point.count_total":      2,
		"treedb.command_wal.append.entry_scan.count_total": 1,
		"treedb.command_wal.flush.count_total":             1,
		"treedb.command_wal.flush.point.count_total":       1,
		"treedb.command_wal.sync.count_total":              2,
		"treedb.command_wal.sync.point.count_total":        1,
		"treedb.command_wal.sync.entry_scan.count_total":   1,
		"treedb.command_wal.sync.barrier.count_total":      0,
		"treedb.command_wal.write.syscalls_total":          3,
		"treedb.command_wal.write.errors_total":            0,
		"treedb.command_wal.file_sync.calls_total":         2,
		"treedb.command_wal.file_sync.errors_total":        0,
		"treedb.public.batch.write_sync.calls_total":       1,
		"treedb.cache.value_log.sync.calls_total":          0,
		"treedb.cache.value_log.file_sync.calls_total":     0,
	} {
		requirePublicStatDelta(t, before, after, key, want)
	}
	for key, value := range map[string]string{
		"state/unsynced": "u",
		"state/synced":   "s",
		"state/batch":    "b",
	} {
		requireRawKVValue(t, db, []byte(key), []byte(value))
	}
}

func TestPublicCommandWALPointerEmptyWriteSyncSweepsPriorUnsyncedWrite(t *testing.T) {
	dir := t.TempDir()
	opts := commandWALDurabilityProofOptions(dir)
	ApplyProfile(&opts, ProfileCommandWALRelaxed)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}

	before := db.Stats()
	want := bytes.Repeat([]byte("p"), 2048)
	dirty := db.NewBatch()
	if err := dirty.Set([]byte("pointer/prior"), want); err != nil {
		_ = dirty.Close()
		_ = db.Close()
		t.Fatalf("dirty Set: %v", err)
	}
	if err := dirty.Write(); err != nil {
		_ = dirty.Close()
		_ = db.Close()
		t.Fatalf("dirty Write: %v", err)
	}
	if err := dirty.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("dirty Close: %v", err)
	}
	empty := db.NewBatch()
	if err := empty.WriteSync(); err != nil {
		_ = empty.Close()
		_ = db.Close()
		t.Fatalf("empty WriteSync: %v", err)
	}
	if err := empty.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("empty Close: %v", err)
	}
	after := db.Stats()
	for key, wantDelta := range map[string]uint64{
		// The relaxed mutation and the explicit durable-prefix barrier are
		// distinct V2 frames.
		"treedb.command_wal.append.count_total":       2,
		"treedb.command_wal.flush.count_total":        1,
		"treedb.command_wal.sync.count_total":         1,
		"treedb.command_wal.sync.barrier.count_total": 1,
		"treedb.command_wal.write.syscalls_total":     2,
		"treedb.command_wal.file_sync.calls_total":    1,
		"treedb.cache.value_log.sync.calls_total":     0,
		// Durable-prefix publication syncs the pinned value-log resource
		// directly from command-WAL dependency debt. It must not route
		// through the cache's pending-barrier sweep.
		"treedb.cache.value_log.sync.pending_barrier.calls_total": 0,
		"treedb.cache.value_log.file_sync.calls_total":            0,
		"treedb.public.batch.write.calls_total":                   1,
		"treedb.public.batch.write_sync.calls_total":              1,
	} {
		requirePublicStatDelta(t, before, after, key, wantDelta)
	}
	requireRawKVValue(t, db, []byte("pointer/prior"), want)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	requireRawKVValue(t, reopen, []byte("pointer/prior"), want)
}

func TestPublicCommandWALWriteThenDirtyWriteSyncDurabilityLedger(t *testing.T) {
	for _, tc := range []struct {
		name           string
		forcedPointers bool
	}{
		{name: "inline"},
		{name: "forced_pointer", forcedPointers: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := commandWALDurabilityProofOptions(dir)
			ApplyProfile(&opts, ProfileCommandWALRelaxed)
			value := []byte("inline-value")
			if tc.forcedPointers {
				opts.ValueLog.PointerThreshold = 1
				opts.ValueLog.ForcePointers = true
				value = bytes.Repeat([]byte("p"), 2048)
			}
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("Open command WAL: %v", err)
			}

			before := db.Stats()
			prior := db.NewBatch()
			if err := prior.Set([]byte("prior"), value); err != nil {
				_ = prior.Close()
				_ = db.Close()
				t.Fatalf("prior Set: %v", err)
			}
			if err := prior.Write(); err != nil {
				_ = prior.Close()
				_ = db.Close()
				t.Fatalf("prior Write: %v", err)
			}
			if err := prior.Close(); err != nil {
				_ = db.Close()
				t.Fatalf("prior Close: %v", err)
			}

			durable := db.NewBatch()
			if err := durable.Set([]byte("durable"), value); err != nil {
				_ = durable.Close()
				_ = db.Close()
				t.Fatalf("durable Set: %v", err)
			}
			if err := durable.WriteSync(); err != nil {
				_ = durable.Close()
				_ = db.Close()
				t.Fatalf("durable WriteSync: %v", err)
			}
			if err := durable.Close(); err != nil {
				_ = db.Close()
				t.Fatalf("durable Close: %v", err)
			}

			after := db.Stats()
			for key, want := range publicCommandWALDurableShapeExpectedCounters("write_then_dirty_write_sync", tc.forcedPointers, DurabilityWALOnRelaxed) {
				requirePublicStatDelta(t, before, after, key, want)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			reopen, err := Open(opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = reopen.Close() }()
			requireRawKVValue(t, reopen, []byte("prior"), value)
			requireRawKVValue(t, reopen, []byte("durable"), value)
		})
	}
}

func TestPublicCommandWALDurableOrdinaryAndExplicitSyncBoundaryMatrixDoesNotCheckpoint(t *testing.T) {
	db, err := Open(commandWALDurabilityProofOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open command WAL durable: %v", err)
	}
	defer func() { _ = db.Close() }()

	before := db.Stats()
	for _, key := range []string{
		"treedb.command_wal.append.count_total",
		"treedb.command_wal.append.point.count_total",
		"treedb.command_wal.append.payload.count_total",
		"treedb.command_wal.append.entry_scan.count_total",
		"treedb.command_wal.flush.count_total",
		"treedb.command_wal.sync.count_total",
		"treedb.public.batch.write.calls_total",
		"treedb.public.batch.write_sync.calls_total",
		"treedb.public.checkpoint.calls_total",
	} {
		if got := statMapUint64(t, before, key); got != 0 {
			t.Fatalf("%s before writes=%d, want 0 (stats=%#v)", key, got, before)
		}
	}
	if got := before["treedb.cache.redo_log.mode"]; got != "external_command_wal" {
		t.Fatalf("redo_log.mode=%q, want external_command_wal", got)
	}
	if got := before["treedb.cache.redo_log.enabled"]; got != "false" {
		t.Fatalf("redo_log.enabled=%q, want false", got)
	}
	if got := before["treedb.cache.command_wal.external_durability"]; got != "true" {
		t.Fatalf("command_wal.external_durability=%q, want true", got)
	}

	if err := db.SetSync([]byte("point-sync"), []byte("v1")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	afterSetSync := db.Stats()
	requirePublicStatDelta(t, before, afterSetSync, "treedb.command_wal.append.count_total", 1)
	requirePublicStatDelta(t, before, afterSetSync, "treedb.command_wal.append.point.count_total", 1)
	requirePublicStatDelta(t, before, afterSetSync, "treedb.command_wal.sync.count_total", 1)
	requirePublicCommandWALNoCheckpointSince(t, db, before)

	if err := db.DeleteSync([]byte("point-sync")); err != nil {
		t.Fatalf("DeleteSync: %v", err)
	}
	afterDeleteSync := db.Stats()
	requirePublicStatDelta(t, before, afterDeleteSync, "treedb.command_wal.append.count_total", 2)
	requirePublicStatDelta(t, before, afterDeleteSync, "treedb.command_wal.append.point.count_total", 2)
	requirePublicStatDelta(t, before, afterDeleteSync, "treedb.command_wal.sync.count_total", 2)
	requirePublicCommandWALNoCheckpointSince(t, db, before)

	writeBatch := db.NewBatch()
	if err := writeBatch.Set([]byte("batch-write"), []byte("v2")); err != nil {
		_ = writeBatch.Close()
		t.Fatalf("batch Write Set: %v", err)
	}
	if err := writeBatch.Write(); err != nil {
		_ = writeBatch.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := writeBatch.Close(); err != nil {
		t.Fatalf("batch Write Close: %v", err)
	}
	afterBatchWrite := db.Stats()
	requirePublicStatDelta(t, before, afterBatchWrite, "treedb.command_wal.append.count_total", 3)
	requirePublicStatDelta(t, before, afterBatchWrite, "treedb.command_wal.append.payload.count_total", 1)
	requirePublicStatDelta(t, before, afterBatchWrite, "treedb.command_wal.flush.count_total", 0)
	requirePublicStatDelta(t, before, afterBatchWrite, "treedb.command_wal.sync.count_total", 3)
	requirePublicStatDelta(t, before, afterBatchWrite, "treedb.public.batch.write.calls_total", 1)
	requirePublicCommandWALNoCheckpointSince(t, db, before)

	syncBatch := db.NewBatch()
	if err := syncBatch.Set([]byte("batch-sync"), []byte("v3")); err != nil {
		_ = syncBatch.Close()
		t.Fatalf("batch WriteSync Set: %v", err)
	}
	if err := syncBatch.WriteSync(); err != nil {
		_ = syncBatch.Close()
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := syncBatch.Close(); err != nil {
		t.Fatalf("batch WriteSync Close: %v", err)
	}
	afterBatchWriteSync := db.Stats()
	requirePublicStatDelta(t, before, afterBatchWriteSync, "treedb.command_wal.append.count_total", 4)
	requirePublicStatDelta(t, before, afterBatchWriteSync, "treedb.command_wal.append.payload.count_total", 2)
	requirePublicStatDelta(t, before, afterBatchWriteSync, "treedb.command_wal.flush.count_total", 0)
	requirePublicStatDelta(t, before, afterBatchWriteSync, "treedb.command_wal.sync.count_total", 4)
	requirePublicStatDelta(t, before, afterBatchWriteSync, "treedb.public.batch.write_sync.calls_total", 1)
	requirePublicCommandWALNoCheckpointSince(t, db, before)

	if err := db.DeleteRange([]byte("batch-"), []byte("batch-\xff")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	afterRangeDelete := db.Stats()
	requirePublicStatDelta(t, before, afterRangeDelete, "treedb.command_wal.append.count_total", 5)
	requirePublicStatDelta(t, before, afterRangeDelete, "treedb.command_wal.append.entry_scan.count_total", 1)
	requirePublicStatDelta(t, before, afterRangeDelete, "treedb.command_wal.flush.count_total", 0)
	requirePublicStatDelta(t, before, afterRangeDelete, "treedb.command_wal.sync.count_total", 5)
	requirePublicCommandWALNoCheckpointSince(t, db, before)

	for _, key := range []string{"point-sync", "batch-write", "batch-sync"} {
		has, err := db.Has([]byte(key))
		if err != nil {
			t.Fatalf("Has(%s): %v", key, err)
		}
		if has {
			t.Fatalf("%s remains visible after delete/delete-range", key)
		}
	}
	assertPublicCommandWALFrames(t, db, 5)
}

func TestPublicCommandWALBatchIngressStatsDistinguishViews(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	b, ok := db.NewBatch().(*commandWALPublicBatch)
	if !ok {
		t.Fatalf("NewBatch type=%T, want *commandWALPublicBatch", db.NewBatch())
	}
	if err := b.Set([]byte("plain-set"), []byte("one")); err != nil {
		_ = b.Close()
		t.Fatalf("Set: %v", err)
	}
	if err := b.SetView([]byte("view-set"), []byte("two")); err != nil {
		_ = b.Close()
		t.Fatalf("SetView: %v", err)
	}
	if err := b.Delete([]byte("plain-delete")); err != nil {
		_ = b.Close()
		t.Fatalf("Delete: %v", err)
	}
	if err := b.DeleteView([]byte("view-delete")); err != nil {
		_ = b.Close()
		t.Fatalf("DeleteView: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	assertPublicCommandWALFrames(t, db, 1)

	tests := []struct {
		key  string
		want uint64
	}{
		{"treedb.command_wal.public_batch.set.calls_total", 1},
		{"treedb.command_wal.public_batch.set.bytes_total", uint64(len("plain-set") + len("one"))},
		{"treedb.command_wal.public_batch.set_view.calls_total", 1},
		{"treedb.command_wal.public_batch.set_view.bytes_total", uint64(len("view-set") + len("two"))},
		{"treedb.command_wal.public_batch.delete.calls_total", 1},
		{"treedb.command_wal.public_batch.delete.bytes_total", uint64(len("plain-delete"))},
		{"treedb.command_wal.public_batch.delete_view.calls_total", 1},
		{"treedb.command_wal.public_batch.delete_view.bytes_total", uint64(len("view-delete"))},
	}
	for _, tt := range tests {
		if got := publicStatUint64(t, db, tt.key); got != tt.want {
			t.Fatalf("%s=%d want %d", tt.key, got, tt.want)
		}
	}
	if got := publicStatUint64(t, db, "treedb.public.batch.write.calls_total"); got != 1 {
		t.Fatalf("public batch write calls=%d want 1", got)
	}
	// Very small writes can round to 0 on coarse platform timers; parsing the
	// stat still verifies that the timed public batch path exported it.
	_ = publicStatUint64(t, db, "treedb.public.batch.write.ns_total")
	if got := publicStatUint64(t, db, "treedb.public.batch.write_sync.calls_total"); got != 0 {
		t.Fatalf("public batch write_sync calls=%d want 0", got)
	}
	cachedStats := db.cached.Stats()
	for _, tt := range tests {
		if got := statMapUint64(t, cachedStats, tt.key); got != tt.want {
			t.Fatalf("cached stats %s=%d want %d", tt.key, got, tt.want)
		}
	}
	if got := statMapUint64(t, cachedStats, "treedb.public.batch.write.calls_total"); got != 1 {
		t.Fatalf("cached stats public batch write calls=%d want 1", got)
	}
}

func TestPublicCommandWALLiveCountersDoNotRequireStatsScan(t *testing.T) {
	db, err := Open(Options{
		Dir:               t.TempDir(),
		Durability:        DurabilityWALOnRelaxed,
		CommandWAL:        true,
		DisableSideStores: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("k2"), []byte("v2")); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.command_wal.stats_scan"]; got != "false" {
		t.Fatalf("stats_scan=%q, want false (stats=%#v)", got, stats)
	}
	for key, want := range map[string]string{
		"treedb.command_wal.live_accepted_frames":  "2",
		"treedb.command_wal.live_accepted_max_lsn": "2",
		"treedb.command_wal.live_covered_frames":   "2",
		"treedb.command_wal.live_covered_max_lsn":  "2",
		"treedb.applied_command_lsn":               "2",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("stats[%q]=%q, want %q (stats=%#v)", key, got, want, stats)
		}
	}
}

func TestPublicCommandWALPointWritesSerializeLSNWithCachedMutation(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	aAppended := make(chan struct{})
	bAppended := make(chan struct{})
	releaseA := make(chan struct{})
	var aOnce, bOnce, releaseOnce sync.Once
	testAfterPublicCommandWALPointAppend = func(op commitlog.RawKVOperation) {
		switch string(op.Value) {
		case "A":
			aOnce.Do(func() { close(aAppended) })
			<-releaseA
		case "B":
			bOnce.Do(func() { close(bAppended) })
		}
	}
	defer func() { testAfterPublicCommandWALPointAppend = nil }()

	errA := make(chan error, 1)
	go func() {
		errA <- db.Set([]byte("same-key"), []byte("A"))
	}()
	select {
	case <-aAppended:
	case <-time.After(5 * time.Second):
		t.Fatal("first command append did not reach test hook")
	}

	errB := make(chan error, 1)
	bStarted := make(chan struct{})
	go func() {
		close(bStarted)
		errB <- db.Set([]byte("same-key"), []byte("B"))
	}()
	<-bStarted
	select {
	case <-bAppended:
		releaseOnce.Do(func() { close(releaseA) })
		t.Fatal("second same-key command appended before first cached mutation was released")
	case <-time.After(100 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(releaseA) })
	if err := recvTestErr(t, errA); err != nil {
		t.Fatalf("first Set: %v", err)
	}
	if err := recvTestErr(t, errB); err != nil {
		t.Fatalf("second Set: %v", err)
	}
	select {
	case <-bAppended:
	default:
		t.Fatal("second command append hook did not run")
	}
	got, err := db.Get([]byte("same-key"))
	if err != nil {
		t.Fatalf("Get(same-key): %v", err)
	}
	if string(got) != "B" {
		t.Fatalf("Get(same-key)=%q, want B", got)
	}
	assertPublicCommandWALFrames(t, db, 2)
}

func TestPublicCommandWALPointWritesUseVisibleRevision(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	var pointOps []commitlog.RawKVOperation
	prevHook := testAfterPublicCommandWALPointAppend
	testAfterPublicCommandWALPointAppend = func(op commitlog.RawKVOperation) {
		pointOps = append(pointOps, op)
	}
	defer func() { testAfterPublicCommandWALPointAppend = prevHook }()

	if err := db.Set([]byte("k"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	val, revision, err := db.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	if len(pointOps) != 1 || pointOps[0].Revision == 0 {
		t.Fatalf("point ops after Set=%+v, want one revision-bearing op", pointOps)
	}
	if !bytes.Equal(val, []byte("value")) || revision != EntryRevision(pointOps[0].Revision) {
		t.Fatalf("GetVersioned after Set=(%q,%d), command revision=%d", val, revision, pointOps[0].Revision)
	}

	if err := db.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	val, revision, err = db.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("GetVersioned after Delete: %v", err)
	}
	if len(pointOps) != 2 || pointOps[1].Revision == 0 {
		t.Fatalf("point ops after Delete=%+v, want second revision-bearing op", pointOps)
	}
	if val != nil || revision != EntryRevision(pointOps[1].Revision) || revision <= EntryRevision(pointOps[0].Revision) {
		t.Fatalf("GetVersioned after Delete=(%q,%d), command revisions=(%d,%d)", val, revision, pointOps[0].Revision, pointOps[1].Revision)
	}
}

func TestPublicCommandWALBatchWritesPreserveVisibleRevisionOnReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}

	b := db.NewBatch()
	if err := b.Set([]byte("k"), []byte("value")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	val, revision, err := db.GetVersioned([]byte("k"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("GetVersioned: %v", err)
	}
	if !bytes.Equal(val, []byte("value")) || revision == LegacyEntryRevision {
		_ = db.Close()
		t.Fatalf("GetVersioned=(%q,%d), want (value,non-legacy)", val, revision)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true})
	if err != nil {
		t.Fatalf("Reopen command WAL: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	val, reopenedRevision, err := reopen.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("reopen GetVersioned: %v", err)
	}
	if !bytes.Equal(val, []byte("value")) || reopenedRevision != revision {
		t.Fatalf("reopen GetVersioned=(%q,%d), want (value,%d)", val, reopenedRevision, revision)
	}
}

func TestPublicCommandWALDurableBatchWriteSyncDoesNotCheckpoint(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileCommandWALDurable, dir)
	opts.CommandWALStatsScan = true
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.DisableBackgroundPrune = true
	opts.FlushThreshold = 1 << 30
	opts.MaxQueuedMemtables = -1
	opts.WriterFlushMaxMemtables = 0
	opts.WriterFlushMaxDuration = 0
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}

	commitSeqBefore, err := strconv.ParseUint(db.Stats()["treedb.commit_seq"], 10, 64)
	if err != nil {
		_ = db.Close()
		t.Fatalf("parse commit_seq before: %v", err)
	}

	b := db.NewBatchWithSize(1)
	key := []byte("durable-batch-sync")
	want := []byte("visible-after-command-wal-replay")
	if err := b.Set(key, want); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		_ = db.Close()
		t.Fatalf("immediate Get: %v", err)
	}
	if !bytes.Equal(got, want) {
		_ = db.Close()
		t.Fatalf("immediate Get=%q, want %q", got, want)
	}

	commitSeqAfterSync, err := strconv.ParseUint(db.Stats()["treedb.commit_seq"], 10, 64)
	if err != nil {
		_ = db.Close()
		t.Fatalf("parse commit_seq after sync: %v", err)
	}
	if commitSeqAfterSync != commitSeqBefore {
		_ = db.Close()
		t.Fatalf("commit_seq after batch WriteSync=%d want %d before explicit checkpoint", commitSeqAfterSync, commitSeqBefore)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen command WAL: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	requireRawKVValue(t, reopen, key, want)
}

func TestPublicCommandWALDurableEmptyBatchWriteSyncDoesNotManufactureBarrier(t *testing.T) {
	db, err := Open(commandWALDurabilityProofOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open command WAL durable: %v", err)
	}
	defer func() { _ = db.Close() }()

	before := db.Stats()
	b := db.NewBatchWithSize(1)
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("empty batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("empty batch Close: %v", err)
	}

	after := db.Stats()
	requirePublicStatDelta(t, before, after, "treedb.public.batch.write_sync.calls_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.sync.count_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.append.count_total", 0)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.sync.barrier.count_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.write.syscalls_total", 0)
	requirePublicCommandWALNoCheckpointSince(t, db, before)
}

func TestPublicCommandWALBatchCloseDiscardsDirtyPayload(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := db.NewBatch()
	if err := b.Set([]byte("discarded"), []byte("value")); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}
	has, err := db.Has([]byte("discarded"))
	if err != nil {
		t.Fatalf("Has(discarded): %v", err)
	}
	if has {
		t.Fatal("closed dirty batch became visible without Write")
	}
	stats := db.Stats()
	frames, err := strconv.ParseUint(stats["treedb.command_wal.frames"], 10, 64)
	if err != nil {
		t.Fatalf("parse command_wal.frames=%q: %v", stats["treedb.command_wal.frames"], err)
	}
	if frames != 0 {
		t.Fatalf("command_wal.frames=%d, want 0 after Close without Write", frames)
	}
}

func TestPublicCommandWALBatchWriteFailureDoesNotAppendFrame(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	b, ok := db.NewBatch().(*commandWALPublicBatch)
	if !ok {
		t.Fatalf("NewBatch type=%T, want *commandWALPublicBatch", db.NewBatch())
	}
	defer func() { _ = b.Close() }()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.inner.Close(); err != nil {
		t.Fatalf("inner Close: %v", err)
	}
	if err := b.Write(); err == nil {
		t.Fatal("batch Write succeeded after inner batch was closed")
	}

	stats := db.Stats()
	frames, err := strconv.ParseUint(stats["treedb.command_wal.frames"], 10, 64)
	if err != nil {
		t.Fatalf("parse command_wal.frames=%q: %v", stats["treedb.command_wal.frames"], err)
	}
	if frames != 0 {
		t.Fatalf("command_wal.frames=%d, want 0 after failed batch Write", frames)
	}
}

func TestPublicCommandWALBatchPayloadSoftCapWritesAndReopens(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	b, ok := db.NewBatch().(*commandWALPublicBatch)
	if !ok {
		_ = db.Close()
		t.Fatalf("NewBatch type=%T, want *commandWALPublicBatch", db.NewBatch())
	}
	b.payloadSoftCapBytes = 1
	for _, kv := range []struct {
		key   string
		value string
	}{
		{"alpha", "one"},
		{"bravo", "two"},
	} {
		if err := b.SetView([]byte(kv.key), []byte(kv.value)); err != nil {
			_ = b.Close()
			_ = db.Close()
			t.Fatalf("SetView(%s): %v", kv.key, err)
		}
	}
	if !b.payloadBypass || b.payload.Count() != 0 || b.opCount != 2 {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("pre-Write bypass=%t payload_count=%d opCount=%d, want true/0/2", b.payloadBypass, b.payload.Count(), b.opCount)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	requireRawKVValue(t, db, []byte("alpha"), []byte("one"))
	requireRawKVValue(t, db, []byte("bravo"), []byte("two"))
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{
		Dir:                 dir,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	requireRawKVValue(t, reopen, []byte("alpha"), []byte("one"))
	requireRawKVValue(t, reopen, []byte("bravo"), []byte("two"))
}

func TestPublicCommandWALBatchPayloadSoftCapRangePointerReopens(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                          dir,
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	}
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	if err := db.Set([]byte("b"), []byte("old-pointer-value")); err != nil {
		_ = db.Close()
		t.Fatalf("Set old value: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint old value: %v", err)
	}

	b, ok := db.NewBatch().(*commandWALPublicBatch)
	if !ok {
		_ = db.Close()
		t.Fatalf("NewBatch type=%T, want *commandWALPublicBatch", db.NewBatch())
	}
	b.payloadSoftCapBytes = 1
	want := bytes.Repeat([]byte("p"), 2048)
	if err := b.DeleteRange([]byte("a"), []byte("z")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch DeleteRange: %v", err)
	}
	if err := b.Set([]byte("m"), want); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	requireRawKVValue(t, db, []byte("m"), want)
	hasOld, err := db.Has([]byte("b"))
	if err != nil || hasOld {
		_ = db.Close()
		t.Fatalf("Has(b)=(%t,%v), want false,nil before reopen", hasOld, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true, DisableSideStores: true, BackgroundCheckpointInterval: -1})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopen.Close()
	requireRawKVValue(t, reopen, []byte("m"), want)
	hasOld, err = reopen.Has([]byte("b"))
	if err != nil || hasOld {
		t.Fatalf("reopen Has(b)=(%t,%v), want false,nil", hasOld, err)
	}
}

func TestPublicCommandWALCheckpointPublishesOnlyCoveredLSNs(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("covered"), []byte("v1")); err != nil {
		t.Fatalf("covered Set: %v", err)
	}

	var hookOnce sync.Once
	testAfterCachedCheckpoint = func() {
		hookOnce.Do(func() {
			if err := db.Set([]byte("post-cut"), []byte("v2")); err != nil {
				t.Errorf("post-cut Set: %v", err)
			}
		})
	}
	defer func() { testAfterCachedCheckpoint = nil }()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	if got := db.backend.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after checkpoint=%d, want covered mutation LSN 1", got)
	}
	first, last := db.publicCommandWALPendingRange()
	if first != 2 || last != 2 {
		t.Fatalf("pending command WAL range=(%d,%d), want post-cut mutation range (2,2)", first, last)
	}
	got, err := db.Get([]byte("post-cut"))
	if err != nil {
		t.Fatalf("Get(post-cut): %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("Get(post-cut)=%q, want v2", got)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("second Checkpoint: %v", err)
	}
	if got := db.backend.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN after second checkpoint=%d, want 2", got)
	}
}

func TestPublicCommandWALCheckpointPublishCapsAtCutoverLSN(t *testing.T) {
	db, err := Open(Options{
		Dir:               t.TempDir(),
		Durability:        DurabilityWALOnRelaxed,
		CommandWAL:        true,
		DisableSideStores: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("covered"), []byte("v1")); err != nil {
		t.Fatalf("covered Set: %v", err)
	}
	db.snapshotPublicCommandWALCheckpointCutover()
	db.recordPublicCommandWALPendingLSN(2)

	applied, ranges, err := db.preparePublicCommandWALPendingPublish(false)
	if err != nil {
		t.Fatalf("preparePublicCommandWALPendingPublish: %v", err)
	}
	if applied != 1 {
		t.Fatalf("prepared AppliedCommandLSN=%d, want cutover mutation LSN 1", applied)
	}
	if len(ranges) != 1 || ranges[0].First != 1 || ranges[0].Last != 1 {
		t.Fatalf("prepared ranges=%+v, want [{First:1 Last:1}]", ranges)
	}

	// The synthetic post-cutover LSN is not backed by a cached mutation in this
	// test. Clear it so the close-time checkpoint does not try to publish it.
	db.clearPublicCommandWALPendingThrough(2)
}

func TestPublicCommandWALCheckpointCleansCoveredCommandJournalSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	before := publicCommandWALSegmentNames(t, dir)
	if len(before) == 0 {
		t.Fatal("expected command WAL segment before checkpoint")
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	// The checkpointed root is durable, but the other recovery-selectable slot
	// can still require the covered command. Publish one durable successor so
	// cleanup can reclaim the original segment from the exact two-slot minimum.
	if err := db.Set([]byte("fallback-advance"), []byte("value")); err != nil {
		t.Fatalf("Set fallback-advance: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint fallback-advance: %v", err)
	}
	after := publicCommandWALSegmentNames(t, dir)
	beforeSet := make(map[string]struct{}, len(before))
	for _, name := range before {
		beforeSet[name] = struct{}{}
	}
	for _, name := range after {
		if _, ok := beforeSet[name]; ok {
			t.Fatalf("checkpoint retained covered command WAL segment %s; before=%v after=%v", name, before, after)
		}
	}
	if len(after) == 0 {
		t.Fatalf("checkpoint removed all command WAL segments; before=%v after=%v", before, after)
	}
	stats := db.Stats()
	if got := statMapUint64(t, stats, "treedb.command_wal.cleanup.removed_segments"); got == 0 {
		t.Fatalf("cleanup.removed_segments=0, want >0 (stats=%#v)", stats)
	}
	if got := statMapUint64(t, stats, "treedb.command_wal.cleanup.scan.count_total"); got == 0 {
		t.Fatalf("cleanup.scan.count_total=0, want >0")
	}
	// Very short cleanup scans can complete below the clock's observable
	// granularity on Windows; count/byte/frame stats below prove the scan ran.
	_ = statMapUint64(t, stats, "treedb.command_wal.cleanup.scan.ns_total")
	for _, key := range []string{
		"treedb.command_wal.cleanup.scanned_bytes_total",
		"treedb.command_wal.cleanup.scanned_frames_total",
	} {
		if got := statMapUint64(t, stats, key); got == 0 {
			t.Fatalf("%s=0, want >0", key)
		}
	}
	if got := stats["treedb.command_wal.segments.active"]; got != "1" {
		t.Fatalf("segments.active=%q, want 1 (stats=%#v)", got, stats)
	}
}

func TestPublicCommandWALEmptyCheckpointReclaimsCoveredBenchmarkEpochs(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, Durability: DurabilityWALOnRelaxed, CommandWAL: true, CommandWALStatsScan: true, MaxWALBytes: -1, BackgroundCheckpointInterval: -1, BackgroundCheckpointIdleDuration: -1, DisableSideStores: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	cleanupCalls := 0
	db.cached.SetCommandWALCheckpointCleanupHook(func(sync bool) error {
		cleanupCalls++
		if cleanupCalls <= 2 {
			return nil
		}
		return db.cleanupPublicCommandWALCheckpoint(sync)
	})

	for _, epoch := range []string{"first", "second"} {
		batch := db.NewBatch()
		for i := 0; i < 64; i++ {
			if err := batch.Set([]byte(fmt.Sprintf("%s-%03d", epoch, i)), []byte("value")); err != nil {
				t.Fatalf("%s batch Set: %v", epoch, err)
			}
		}
		if err := batch.Write(); err != nil {
			t.Fatalf("%s batch Write: %v", epoch, err)
		}
		if err := batch.Close(); err != nil {
			t.Fatalf("%s batch Close: %v", epoch, err)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("%s Checkpoint: %v", epoch, err)
		}
	}
	before := publicCommandWALSegmentNames(t, dir)
	if len(before) < 2 {
		t.Fatalf("segments after two checkpointed epochs=%v, want closed command WAL generations", before)
	}
	if cleanupCalls != 2 {
		t.Fatalf("suppressed cleanup calls=%d, want 2", cleanupCalls)
	}
	db.cached.SetCommandWALCheckpointCleanupHook(db.cleanupPublicCommandWALCheckpoint)
	stateBefore := db.backend.State()
	nextLSNBefore := db.backend.CommandWALNextLSN()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("empty post-run Checkpoint: %v", err)
	}
	if got := db.backend.State().AppliedCommandLSN; got != stateBefore.AppliedCommandLSN {
		t.Fatalf("AppliedCommandLSN after empty checkpoint=%d, want unchanged %d", got, stateBefore.AppliedCommandLSN)
	}
	if got := db.backend.CommandWALNextLSN(); got != nextLSNBefore {
		t.Fatalf("next command WAL LSN after empty checkpoint=%d, want unchanged %d", got, nextLSNBefore)
	}
	after := publicCommandWALSegmentNames(t, dir)
	if len(after) != 1 {
		t.Fatalf("empty checkpoint retained covered benchmark epochs: before=%v after=%v", before, after)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close after cleanup: %v", err)
	}
	db, err = Open(Options{Dir: dir, Durability: DurabilityWALOnRelaxed, CommandWAL: true, CommandWALStatsScan: true, DisableSideStores: true})
	if err != nil {
		t.Fatalf("reopen after cleanup: %v", err)
	}
	for _, key := range [][]byte{[]byte("first-000"), []byte("second-000")} {
		got, err := db.Get(key)
		if err != nil || string(got) != "value" {
			t.Fatalf("Get(%q)=(%q, %v), want (value, nil)", key, got, err)
		}
	}
}

func TestPublicCommandWALAutoCheckpointUsesCommandWALBytes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                              dir,
		Durability:                       DurabilityWALOnRelaxed,
		CommandWAL:                       true,
		CommandWALStatsScan:              true,
		CommandWALSegmentTargetBytes:     1,
		MaxWALBytes:                      1,
		BackgroundCheckpointInterval:     -1,
		BackgroundCheckpointIdleDuration: -1,
		DisableSideStores:                true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	value := make([]byte, 8<<10)
	for i := range value {
		value[i] = byte(i)
	}
	if err := db.Set([]byte("auto-checkpoint"), value); err != nil {
		t.Fatalf("Set: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		stats := db.cached.Stats()
		count, err := strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
		if err != nil {
			t.Fatalf("parse auto_checkpoint.count=%q: %v", stats["treedb.cache.auto_checkpoint.count"], err)
		}
		applied := uint64(0)
		if db.backend != nil {
			applied = db.backend.State().AppliedCommandLSN
		}
		if count > 0 && stats["treedb.cache.auto_checkpoint.last_reason"] == "size" && applied >= 1 {
			// One auto-checkpoint establishes the applied frontier, while the older
			// durable slot can still require LSN 1. Advance the fallback slot before
			// requiring physical cleanup convergence.
			if err := db.Set([]byte("auto-checkpoint/fallback-advance"), []byte("value")); err != nil {
				t.Fatalf("Set fallback advance after size auto-checkpoint: %v", err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint fallback advance after size auto-checkpoint: %v", err)
			}
			commandStats := db.Stats()
			if got := statMapUint64(t, commandStats, "treedb.command_wal.cleanup.removed_segments"); got == 0 {
				t.Fatalf("cleanup.removed_segments=0, want >0 after fallback advance (stats=%#v)", commandStats)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for size auto-checkpoint: cache=%#v command=%#v applied=%d", stats, db.Stats(), applied)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPublicCommandWALAutoCheckpointBytesIncludesRotatedSegments(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                              dir,
		Durability:                       DurabilityWALOnRelaxed,
		CommandWAL:                       true,
		CommandWALSegmentTargetBytes:     1,
		BackgroundCheckpointInterval:     -1,
		BackgroundCheckpointIdleDuration: -1,
		DisableSideStores:                true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("pressure-a"), []byte("a")); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	if err := db.Set([]byte("pressure-b"), []byte("b")); err != nil {
		t.Fatalf("Set b: %v", err)
	}
	if got := publicCommandWALSegmentNames(t, dir); len(got) < 2 {
		t.Fatalf("segments=%v, want rotation before pressure accounting check", got)
	}
	active := db.backend.CommandWALActiveBytes()
	pressure := db.publicCommandWALAutoCheckpointBytes()
	if active <= 0 {
		t.Fatalf("active command WAL bytes=%d, want >0", active)
	}
	if pressure <= active {
		t.Fatalf("auto-checkpoint pressure bytes=%d, active bytes=%d; want pressure to include rotated non-active bytes", pressure, active)
	}
}

func TestPublicCommandWALCheckpointPiggybacksAppliedLSN(t *testing.T) {
	db, err := Open(Options{
		Dir:               t.TempDir(),
		Durability:        DurabilityWALOnRelaxed,
		CommandWAL:        true,
		DisableSideStores: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := db.backend.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want mutation LSN 1", got)
	}
	stats := db.cached.Stats()
	if got := stats["treedb.cache.command_wal.checkpoint_publish.piggybacked"]; got != "1" {
		t.Fatalf("piggybacked checkpoint publishes=%q, want 1", got)
	}
	if got := stats["treedb.cache.command_wal.checkpoint_publish.separate"]; got != "0" {
		t.Fatalf("separate checkpoint publishes=%q, want 0", got)
	}
}

func TestPublicCommandWALNoopCheckpointRunsPublishHook(t *testing.T) {
	db, err := Open(Options{
		Dir:               t.TempDir(),
		Durability:        DurabilityWALOnRelaxed,
		CommandWAL:        true,
		DisableSideStores: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	called := false
	db.cached.SetCommandWALCheckpointPublishHook(func(sync bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		called = true
		return 1, []backenddb.CommandWALLSNRange{{First: 1, Last: 1}}, nil
	})
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if !called {
		t.Fatal("checkpoint publish hook was not called on no-op checkpoint")
	}
	if got := db.backend.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1 from no-op checkpoint publish", got)
	}
}

func TestPublicCommandWALCheckpointSeparatesCleanupStage(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOnRelaxed, CommandWAL: true, DisableSideStores: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.cached.SetCommandWALCheckpointPublishHook(func(bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		return 1, []backenddb.CommandWALLSNRange{{First: 1, Last: 1}}, nil
	})
	db.cached.SetCommandWALCheckpointCleanupHook(func(bool) error {
		time.Sleep(time.Millisecond)
		return nil
	})
	if err := db.Set([]byte("cleanup-stage"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stats := db.cached.Stats()
	if got := stats["treedb.cache.checkpoint.stage.command_wal_cleanup.samples"]; got != "1" {
		t.Fatalf("cleanup samples=%q want 1", got)
	}
	if got := stats["treedb.cache.checkpoint.stage.command_wal_cleanup.last_ns"]; got == "0" || got == "" {
		t.Fatalf("cleanup duration=%q want nonzero independently timed stage", got)
	}
}

func publicCommandWALSegmentNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(backenddb.WALDirPath(dir))
	if err != nil {
		t.Fatalf("ReadDir(wal): %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !commitlog.IsCommandSegmentName(entry.Name()) {
			continue
		}
		names = append(names, entry.Name())
	}
	return names
}

func TestPublicCommandWALCheckpointHookUsesSyncIntent(t *testing.T) {
	tests := []struct {
		name     string
		profile  Profile
		wantSync bool
	}{
		{name: "durable", profile: ProfileCommandWALDurable, wantSync: true},
		{name: "relaxed", profile: ProfileCommandWALRelaxed, wantSync: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := OptionsFor(tt.profile, t.TempDir())
			opts.DisableSideStores = true
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("Open command WAL: %v", err)
			}
			defer func() { _ = db.Close() }()
			if err := db.Set([]byte("k"), []byte("v")); err != nil {
				t.Fatalf("Set: %v", err)
			}

			called := false
			db.cached.SetCommandWALCheckpointPublishHook(func(sync bool) (uint64, []backenddb.CommandWALLSNRange, error) {
				called = true
				if sync != tt.wantSync {
					t.Fatalf("checkpoint hook sync=%t, want %t", sync, tt.wantSync)
				}
				return 0, nil, nil
			})
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if !called {
				t.Fatal("checkpoint publish hook was not called")
			}
		})
	}
}

func TestPublicCommandWALCloseReportsFinalCheckpointError(t *testing.T) {
	checkpointErr := errors.New("forced checkpoint failure")
	var notified []error
	db, err := Open(Options{
		Dir:               t.TempDir(),
		Durability:        DurabilityWALOnRelaxed,
		CommandWAL:        true,
		DisableSideStores: true,
		NotifyError: func(err error) {
			notified = append(notified, err)
		},
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	db.cached.SetCommandWALCheckpointPublishHook(func(sync bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		return 0, nil, checkpointErr
	})
	err = db.Close()
	if !errors.Is(err, checkpointErr) {
		t.Fatalf("Close error=%v, want checkpoint error", err)
	}
	found := false
	for _, err := range notified {
		if errors.Is(err, checkpointErr) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("NotifyError=%v, want checkpoint error", notified)
	}
}

func recvTestErr(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for goroutine")
		return nil
	}
}

func TestPublicCommandWALPendingClearPreservesNewerRange(t *testing.T) {
	var db DB
	db.recordPublicCommandWALPendingLSN(1)
	db.recordPublicCommandWALPendingLSN(3)
	db.clearPublicCommandWALPendingThrough(1)
	first, last := db.publicCommandWALPendingRange()
	if first != 2 || last != 3 {
		t.Fatalf("pending range after partial clear=(%d,%d), want (2,3)", first, last)
	}
	db.clearPublicCommandWALPendingThrough(3)
	first, last = db.publicCommandWALPendingRange()
	if first != 0 || last != 0 {
		t.Fatalf("pending range after full clear=(%d,%d), want (0,0)", first, last)
	}

	db.recordPublicCommandWALPendingLSN(4)
	db.clearPublicCommandWALPendingThrough(3)
	first, last = db.publicCommandWALPendingRange()
	if first != 4 || last != 4 {
		t.Fatalf("pending range after stale clear=(%d,%d), want newer LSN (4,4)", first, last)
	}

	db.clearPublicCommandWALPendingThrough(4)
	db.recordPublicCommandWALPendingLSN(5)
	first, last = db.publicCommandWALPendingRange()
	if first != 5 || last != 5 {
		t.Fatalf("pending range after record following full clear=(%d,%d), want new LSN (5,5)", first, last)
	}
}

func TestPublicCommandWALBatchResetFallbackKeepsWrapperUsable(t *testing.T) {
	inner := &commandWALNoResetBatch{}
	wrapped := &commandWALPublicBatch{inner: inner}
	_ = wrapped.payload.ResetWithHint(0, 0)
	wrapped.dirty = true
	wrapped.closed = true

	wrapped.Reset()
	if wrapped.closed {
		t.Fatal("Reset fallback marked command WAL batch closed")
	}
	if wrapped.dirty {
		t.Fatal("Reset fallback left command WAL batch dirty")
	}
	if err := wrapped.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set after Reset fallback: %v", err)
	}
	if len(inner.entries) != 1 {
		t.Fatalf("inner entries=%d, want 1 after Set", len(inner.entries))
	}
}

func TestPublicCommandWALBatchReplayBytesSurviveWriteUntilReset(t *testing.T) {
	inner := &commandWALHookBatch{}
	wrapped := newCommandWALPublicBatch(nil, inner, 2)
	key := []byte("alpha")
	value := []byte("value")
	keyView, valueView, err := wrapped.SetViewWithReplayBytes(key, value)
	if err != nil {
		t.Fatalf("SetViewWithReplayBytes: %v", err)
	}
	copy(key, "omega")
	copy(value, "xxxxx")
	if string(keyView) != "alpha" || string(valueView) != "value" {
		t.Fatalf("replay views changed before Write: key=%q value=%q", keyView, valueView)
	}
	if err := wrapped.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if inner.writeAfterCalls != 1 {
		t.Fatalf("WriteAfterCommandWALAppend calls=%d want 1", inner.writeAfterCalls)
	}
	if !wrapped.retainPayloadAfterWrite {
		t.Fatal("Write did not retain replay-view payload")
	}
	if wrapped.dirty || wrapped.opCount != 0 {
		t.Fatalf("post-Write dirty=%t opCount=%d, want clean", wrapped.dirty, wrapped.opCount)
	}
	if string(keyView) != "alpha" || string(valueView) != "value" {
		t.Fatalf("replay views changed after Write: key=%q value=%q", keyView, valueView)
	}
	wrapped.Reset()
	if wrapped.retainPayloadAfterWrite {
		t.Fatal("Reset left replay-view payload retained")
	}
}

func TestPublicCommandWALBatchReplayBytesAppendOnlyLeaseAvoidsDirectArena(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
		MemtableMode:        "append_only",
		MemtableShards:      1,
		FlushThreshold:      1 << 30,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b, ok := db.NewBatchWithSize(1).(*commandWALPublicBatch)
	if !ok {
		t.Fatalf("NewBatchWithSize type=%T, want *commandWALPublicBatch", db.NewBatchWithSize(1))
	}
	keyView, valueView, err := b.SetViewWithReplayBytes([]byte("alpha"), bytes.Repeat([]byte{0x33}, 128))
	if err != nil {
		t.Fatalf("SetViewWithReplayBytes: %v", err)
	}
	wantKey := bytes.Clone(keyView)
	wantValue := bytes.Clone(valueView)
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !b.payloadLeasedToMemtable {
		t.Fatal("command-WAL payload was not marked leased to the append-only memtable")
	}
	if !b.retainPayloadAfterWrite {
		t.Fatal("replay payload was not retained after write")
	}
	stats := db.Stats()
	if got := stats["treedb.cache.append_only_direct_arena.active_used_bytes"]; got != "0" {
		t.Fatalf("direct arena active used bytes=%q want 0", got)
	}
	if got := stats["treedb.cache.batch_arena.leased_bytes"]; got == "0" || got == "" {
		t.Fatalf("batch arena leased bytes=%q want >0 for external payload lease", got)
	}
	got, err := db.Get(wantKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, wantValue) {
		t.Fatalf("stored value=%x want %x", got, wantValue)
	}

	b.Reset()
	if b.payloadLeasedToMemtable {
		t.Fatal("Reset left payload marked leased to memtable")
	}

	secondKeyView, secondValueView, err := b.SetViewWithReplayBytes([]byte("bravo"), bytes.Repeat([]byte{0x44}, 128))
	if err != nil {
		t.Fatalf("second SetViewWithReplayBytes: %v", err)
	}
	secondWantKey := bytes.Clone(secondKeyView)
	secondWantValue := bytes.Clone(secondValueView)
	if err := b.Write(); err != nil {
		t.Fatalf("second Write: %v", err)
	}
	got, err = db.Get(wantKey)
	if err != nil {
		t.Fatalf("Get original after batch reuse: %v", err)
	}
	if !bytes.Equal(got, wantValue) {
		t.Fatalf("original stored value after batch reuse=%x want %x", got, wantValue)
	}
	got, err = db.Get(secondWantKey)
	if err != nil {
		t.Fatalf("Get second: %v", err)
	}
	if !bytes.Equal(got, secondWantValue) {
		t.Fatalf("second stored value=%x want %x", got, secondWantValue)
	}
}

func TestPublicCommandWALBatchHashSortedLeaseProtectsCallerMutation(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
		MemtableMode:        "hash_sorted",
		MemtableShards:      1,
		FlushThreshold:      1 << 30,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	const entries = 32
	b, ok := db.NewBatchWithSize(entries).(*commandWALPublicBatch)
	if !ok {
		t.Fatalf("NewBatchWithSize type=%T, want *commandWALPublicBatch", db.NewBatchWithSize(entries))
	}
	wantKeys := make([][]byte, entries)
	values := make([][]byte, entries)
	wantValues := make([][]byte, entries)
	for i := 0; i < entries; i++ {
		key := []byte(fmt.Sprintf("hash-sorted-stable-key-%02d", i))
		values[i] = bytes.Repeat([]byte{byte(0x51 + i)}, 128)
		keyView, valueView, err := b.SetViewWithReplayBytes(key, values[i])
		if err != nil {
			_ = b.Close()
			t.Fatalf("SetViewWithReplayBytes(%d): %v", i, err)
		}
		wantKeys[i] = bytes.Clone(keyView)
		wantValues[i] = bytes.Clone(valueView)
		key[0] = 'X'
		values[i][0] = 0x7f
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("WriteSync: %v", err)
	}
	if !b.payloadLeasedToMemtable {
		_ = b.Close()
		t.Fatalf("command-WAL payload was not marked leased to the hash-sorted memtable: attached_mask=%d leased_mask=%d", b.payloadLeaseAttachedMask, b.payloadLeasedBufferMask)
	}
	if !b.retainPayloadAfterWrite {
		_ = b.Close()
		t.Fatal("replay payload was not retained after write")
	}
	if got := db.Stats()["treedb.cache.batch_arena.leased_bytes"]; got == "0" || got == "" {
		_ = b.Close()
		t.Fatalf("batch arena leased bytes=%q want >0 for hash-sorted payload lease", got)
	}
	for i := 0; i < entries; i++ {
		got, err := db.Get(wantKeys[i])
		if err != nil {
			_ = b.Close()
			t.Fatalf("Get(%d): %v", i, err)
		}
		if !bytes.Equal(got, wantValues[i]) {
			_ = b.Close()
			t.Fatalf("stored value %d after caller mutation=%x want %x", i, got, wantValues[i])
		}
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := db.Stats()["treedb.cache.batch_arena.leased_bytes"]; got != "0" {
		t.Fatalf("batch arena leased bytes after checkpoint=%q want 0", got)
	}
	for i := 0; i < entries; i++ {
		got, err := db.Get(wantKeys[i])
		if err != nil {
			t.Fatalf("Get(%d) after checkpoint: %v", i, err)
		}
		if !bytes.Equal(got, wantValues[i]) {
			t.Fatalf("stored value %d after checkpoint=%x want %x", i, got, wantValues[i])
		}
	}
}

func TestPublicCommandWALBatchZeroValueAppendOnlyLeaseAvoidsDirectArena(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
		MemtableMode:        "append_only",
		MemtableShards:      1,
		FlushThreshold:      1 << 30,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatchWithSize(1)
	value := make([]byte, 128)
	if err := b.Set([]byte("zero-value-key"), value); err != nil {
		_ = b.Close()
		t.Fatalf("Set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.append_only_direct_arena.active_used_bytes"]; got != "0" {
		t.Fatalf("direct arena active used bytes=%q want 0", got)
	}
	if got := stats["treedb.cache.batch_arena.leased_bytes"]; got != "0" {
		t.Fatalf("batch arena leased bytes=%q want 0 for shared zero view", got)
	}
	got, err := db.Get([]byte("zero-value-key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("stored value=%x want %x", got, value)
	}
}

func TestPublicCommandWALBatchResetPreservesCompactZeroScanFallback(t *testing.T) {
	wrapped := newCommandWALPublicBatch(nil, &commandWALResetBatch{}, 8000)
	zeroValue := make([]byte, 128)
	key := []byte("k")

	if err := wrapped.SetView(key, zeroValue); err != nil {
		t.Fatalf("SetView before reset: %v", err)
	}
	payload, err := wrapped.commandWALPayload()
	if err != nil {
		t.Fatalf("commandWALPayload before reset: %v", err)
	}
	if len(payload) >= 6+9+len(key)+len(zeroValue) {
		t.Fatalf("commandWALPayload before reset len=%d, want compact zero payload below expanded size", len(payload))
	}
	visits := 0
	if err := commitlog.ScanRawKVBatchPayload(payload, func(op commitlog.RawKVOp, gotKey, gotValue []byte) error {
		visits++
		if op != commitlog.RawKVOpSet || string(gotKey) != string(key) || len(gotValue) != len(zeroValue) {
			t.Fatalf("scan before reset op=%v key=%q value_len=%d", op, gotKey, len(gotValue))
		}
		return nil
	}); err != nil {
		t.Fatalf("scan before reset: %v", err)
	}
	if visits != 1 {
		t.Fatalf("scan visits before reset=%d, want 1", visits)
	}

	wrapped.Reset()
	if err := wrapped.SetView(key, zeroValue); err != nil {
		t.Fatalf("SetView after reset: %v", err)
	}
	payload, err = wrapped.commandWALPayload()
	if err != nil {
		t.Fatalf("commandWALPayload after reset: %v", err)
	}
	if len(payload) >= 6+9+len(key)+len(zeroValue) {
		t.Fatalf("commandWALPayload after reset len=%d, want compact zero payload below expanded size", len(payload))
	}
	visits = 0
	if err := commitlog.ScanRawKVBatchPayload(payload, func(op commitlog.RawKVOp, gotKey, gotValue []byte) error {
		visits++
		if op != commitlog.RawKVOpSet || string(gotKey) != string(key) || len(gotValue) != len(zeroValue) {
			t.Fatalf("scan after reset op=%v key=%q value_len=%d", op, gotKey, len(gotValue))
		}
		return nil
	}); err != nil {
		t.Fatalf("scan after reset: %v", err)
	}
	if visits != 1 {
		t.Fatalf("scan visits after reset=%d, want 1", visits)
	}
}

func TestPublicCommandWALBatchRevisionPayloadSetKeepsMutableAllZeroValue(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}

	b, ok := db.NewBatch().(*commandWALPublicBatch)
	if !ok {
		_ = db.Close()
		t.Fatalf("NewBatch type=%T, want *commandWALPublicBatch", db.NewBatch())
	}
	zeroValue := make([]byte, 128)
	if err := b.Set([]byte("k"), zeroValue); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("Set zero value: %v", err)
	}
	for i := range zeroValue {
		zeroValue[i] = byte(i + 1)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}

	val, revision, err := db.GetVersioned([]byte("k"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("GetVersioned: %v", err)
	}
	if len(val) != 128 || !commandWALPublicAllZeroBytes(val) || revision == LegacyEntryRevision {
		_ = db.Close()
		t.Fatalf("GetVersioned len=%d zero=%t revision=%d, want zero/non-legacy", len(val), commandWALPublicAllZeroBytes(val), revision)
	}

	r, err := commitlog.NewReader(filepath.Join(backenddb.WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewReader: %v", err)
	}
	env, err := r.ReadCommandFrame()
	if err != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	var gotRevision uint64
	visits := 0
	if err := commitlog.ScanRawKVBatchPayloadWithRevision(env.Payload, func(op commitlog.RawKVOp, gotKey, gotValue []byte, got uint64) error {
		visits++
		if op != commitlog.RawKVOpSet || string(gotKey) != "k" || len(gotValue) != 128 || !commandWALPublicAllZeroBytes(gotValue) {
			t.Fatalf("decoded op=%v key=%q value_len=%d zero=%t", op, gotKey, len(gotValue), commandWALPublicAllZeroBytes(gotValue))
		}
		gotRevision = got
		return nil
	}); err != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("ScanRawKVBatchPayloadWithRevision: %v", err)
	}
	if err := r.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("reader Close: %v", err)
	}
	if visits != 1 || gotRevision != uint64(revision) {
		_ = db.Close()
		t.Fatalf("command WAL visits=%d revision=%d, want 1/%d", visits, gotRevision, revision)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPublicCommandWALBatchLegacyCompactZeroPinsMutableValueIdentity(t *testing.T) {
	wrapped := newCommandWALPublicBatch(nil, &commandWALResetBatch{}, 2)
	zeroValue := make([]byte, 4)

	if err := wrapped.Set([]byte("zero"), zeroValue); err != nil {
		t.Fatalf("Set zero: %v", err)
	}
	for i := range zeroValue {
		zeroValue[i] = byte(i + 1)
	}
	if err := wrapped.Set([]byte("mutated"), zeroValue); err != nil {
		t.Fatalf("Set mutated: %v", err)
	}
	payload, err := wrapped.commandWALPayload()
	if err != nil {
		t.Fatalf("commandWALPayload: %v", err)
	}
	var got []batch.Entry
	if err := commitlog.ScanRawKVBatchPayload(payload, func(op commitlog.RawKVOp, key, value []byte) error {
		if op != commitlog.RawKVOpSet {
			t.Fatalf("decoded op=%v, want set", op)
		}
		got = append(got, batch.Entry{Type: batch.OpPut, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
		return nil
	}); err != nil {
		t.Fatalf("ScanRawKVBatchPayload: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("decoded entries=%d, want 2", len(got))
	}
	if string(got[0].Key) != "zero" || len(got[0].Value) != 4 || !commandWALPublicAllZeroBytes(got[0].Value) {
		t.Fatalf("first decoded entry key=%q value=%v, want zero value", got[0].Key, got[0].Value)
	}
	if string(got[1].Key) != "mutated" || !bytes.Equal(got[1].Value, []byte{1, 2, 3, 4}) {
		t.Fatalf("second decoded entry key=%q value=%v, want mutated value", got[1].Key, got[1].Value)
	}
}

func TestPublicCommandWALBatchOrdinarySetUsesPayloadBuilder(t *testing.T) {
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(nil, inner, 0)
	wrapped.payloadSoftCapBytes = 64

	firstKey := []byte("a")
	firstValue := []byte("b")
	if err := wrapped.Set(firstKey, firstValue); err != nil {
		t.Fatalf("Set first: %v", err)
	}
	firstKey[0] = 'A'
	firstValue[0] = 'B'
	if wrapped.payloadBypass || wrapped.payload.Count() != 1 || wrapped.payload.Len() == 0 {
		t.Fatalf("payload after first set: bypass=%t count=%d len=%d, want false/1/>0", wrapped.payloadBypass, wrapped.payload.Count(), wrapped.payload.Len())
	}
	if inner.setViewCalls != 1 || inner.setCalls != 0 {
		t.Fatalf("inner calls after first set: setView=%d set=%d, want 1/0", inner.setViewCalls, inner.setCalls)
	}
	if got := inner.entries[0]; string(got.Key) != "a" || string(got.Value) != "b" {
		t.Fatalf("payload-backed entry mutated or not copied: key=%q value=%q", got.Key, got.Value)
	}

	key := []byte("second")
	value := bytes.Repeat([]byte("x"), 128)
	if err := wrapped.Set(key, value); err != nil {
		t.Fatalf("Set second: %v", err)
	}
	key[0] = 'X'
	value[0] = 'Y'
	if !wrapped.payloadBypass || wrapped.payload.Count() != 1 || wrapped.opCount != 2 {
		t.Fatalf("payload state after soft-cap bypass: bypass=%t count=%d opCount=%d, want true/1/2", wrapped.payloadBypass, wrapped.payload.Count(), wrapped.opCount)
	}
	if inner.setViewCalls != 1 || inner.setCalls != 1 {
		t.Fatalf("inner calls: setView=%d set=%d, want 1/1", inner.setViewCalls, inner.setCalls)
	}
	if got := inner.entries[1]; string(got.Key) != "second" || string(got.Value) != strings.Repeat("x", 128) {
		t.Fatalf("bypassed entry mutated or not copied: key=%q value_prefix=%q", got.Key, got.Value[:1])
	}

	payload, err := wrapped.commandWALPayload()
	if err != nil {
		t.Fatalf("commandWALPayload after bypass: %v", err)
	}
	var got []batch.Entry
	if err := commitlog.ScanRawKVBatchPayload(payload, func(op commitlog.RawKVOp, key, value []byte) error {
		got = append(got, batch.Entry{Type: batch.OpPut, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
		return nil
	}); err != nil {
		t.Fatalf("ScanRawKVBatchPayload: %v", err)
	}
	if len(got) != 2 || string(got[0].Key) != "a" || string(got[0].Value) != "b" || string(got[1].Key) != "second" || string(got[1].Value) != strings.Repeat("x", 128) {
		t.Fatalf("replayed payload mismatch: %+v", got)
	}
}

func TestPublicCommandWALBatchFallbackPreservesEntryRevisions(t *testing.T) {
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(nil, inner, 0)
	inner.entries = append(inner.entries,
		batch.Entry{Type: batch.OpPut, Key: []byte("alpha"), Value: []byte("one"), Revision: 41},
		batch.Entry{Type: batch.OpDelete, Key: []byte("bravo"), Revision: 42},
		batch.Entry{Type: batch.OpDeleteRange, Key: []byte("range-a"), Value: []byte("range-z"), Revision: 99},
	)
	wrapped.payloadBypass = true
	wrapped.opCount = len(inner.entries)

	payload, err := wrapped.commandWALPayload()
	if err != nil {
		t.Fatalf("commandWALPayload: %v", err)
	}
	type gotOp struct {
		op       commitlog.RawKVOp
		key      string
		value    string
		revision uint64
	}
	var got []gotOp
	if err := commitlog.ScanRawKVBatchPayloadWithRevision(payload, func(op commitlog.RawKVOp, key, value []byte, revision uint64) error {
		got = append(got, gotOp{op: op, key: string(key), value: string(value), revision: revision})
		return nil
	}); err != nil {
		t.Fatalf("ScanRawKVBatchPayloadWithRevision: %v", err)
	}
	want := []gotOp{
		{op: commitlog.RawKVOpSet, key: "alpha", value: "one", revision: 41},
		{op: commitlog.RawKVOpDelete, key: "bravo", revision: 42},
		{op: commitlog.RawKVOpDeleteRange, key: "range-a", value: "range-z"},
	}
	if len(got) != len(want) {
		t.Fatalf("decoded ops=%+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("decoded op %d=%+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestPublicCommandWALBatchAppendUsesStablePayload(t *testing.T) {
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(nil, inner, 0)

	if _, _, err := wrapped.SetViewWithReplayBytes([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("SetViewWithReplayBytes alpha: %v", err)
	}
	if _, _, err := wrapped.SetViewWithReplayBytes([]byte("bravo"), []byte("two")); err != nil {
		t.Fatalf("SetViewWithReplayBytes bravo: %v", err)
	}
	if wrapped.payloadBypass || wrapped.payload.Count() != wrapped.opCount || wrapped.payload.Count() != 2 {
		t.Fatalf("payload state bypass=%t count=%d opCount=%d, want stable count 2", wrapped.payloadBypass, wrapped.payload.Count(), wrapped.opCount)
	}
	savedScratch := wrapped.beginGroupPublicationTicketHandoff()
	appendErr := wrapped.appendCommandWAL(false)
	publication := wrapped.finishGroupPublicationTicketHandoff(savedScratch)
	if wrapped.db != nil {
		wrapped.db.finishPublicCommandWALGroupPublication(publication, appendErr)
	}
	if appendErr != nil {
		t.Fatalf("appendCommandWAL: %v", appendErr)
	}
	if inner.replayCalls != 0 {
		t.Fatalf("inner replay calls=%d, want 0 for stable payload append", inner.replayCalls)
	}
}

func TestPublicCommandWALBatchReplayBytesPayloadPreservesAssignedRevision(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}

	b, ok := db.NewBatch().(*commandWALPublicBatch)
	if !ok {
		_ = db.Close()
		t.Fatalf("NewBatch type=%T, want *commandWALPublicBatch", db.NewBatch())
	}
	keyView, valueView, err := b.SetViewWithReplayBytes([]byte("k"), []byte("value"))
	if err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("SetViewWithReplayBytes: %v", err)
	}
	if b.payloadBypass || b.payload.Count() != b.opCount || b.payload.Count() != 1 {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("payload state bypass=%t count=%d opCount=%d, want stable count 1", b.payloadBypass, b.payload.Count(), b.opCount)
	}
	if string(keyView) != "k" || string(valueView) != "value" {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("replay views key=%q value=%q, want k/value", keyView, valueView)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}

	val, revision, err := db.GetVersioned([]byte("k"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("GetVersioned: %v", err)
	}
	if !bytes.Equal(val, []byte("value")) || revision == LegacyEntryRevision {
		_ = db.Close()
		t.Fatalf("GetVersioned=(%q,%d), want (value,non-legacy)", val, revision)
	}

	r, err := commitlog.NewReader(filepath.Join(backenddb.WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewReader: %v", err)
	}
	env, err := r.ReadCommandFrame()
	if err != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	var gotRevision uint64
	visits := 0
	if err := commitlog.ScanRawKVBatchPayloadWithRevision(env.Payload, func(op commitlog.RawKVOp, gotKey, gotValue []byte, revision uint64) error {
		visits++
		if op != commitlog.RawKVOpSet || string(gotKey) != "k" || string(gotValue) != "value" {
			t.Fatalf("decoded op=%v key=%q value=%q", op, gotKey, gotValue)
		}
		gotRevision = revision
		return nil
	}); err != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("ScanRawKVBatchPayloadWithRevision: %v", err)
	}
	if err := r.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("reader Close: %v", err)
	}
	if visits != 1 || gotRevision != uint64(revision) {
		_ = db.Close()
		t.Fatalf("command WAL visits=%d revision=%d, want 1/%d", visits, gotRevision, revision)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true, DisableSideStores: true})
	if err != nil {
		t.Fatalf("Reopen command WAL: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	val, reopenedRevision, err := reopen.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("reopen GetVersioned: %v", err)
	}
	if !bytes.Equal(val, []byte("value")) || reopenedRevision != revision {
		t.Fatalf("reopen GetVersioned=(%q,%d), want (value,%d)", val, reopenedRevision, revision)
	}
}

func TestPublicCommandWALBatchSetViewBypassesPayloadBuilderWithoutInnerCopy(t *testing.T) {
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(nil, inner, 0)
	wrapped.payloadSoftCapBytes = 64
	initialPayloadLen := wrapped.payload.Len()
	initialPayloadCap := wrapped.payload.RetainedCap()

	if err := wrapped.SetView([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("SetView first: %v", err)
	}
	if !wrapped.payloadBypass || wrapped.payload.Count() != 0 || wrapped.payload.Len() != initialPayloadLen || wrapped.payload.RetainedCap() != initialPayloadCap {
		t.Fatalf("payload after first setView: bypass=%t count=%d len/cap=%d/%d, want true/0/%d/%d", wrapped.payloadBypass, wrapped.payload.Count(), wrapped.payload.Len(), wrapped.payload.RetainedCap(), initialPayloadLen, initialPayloadCap)
	}

	key := []byte("second")
	value := bytes.Repeat([]byte("x"), 128)
	if err := wrapped.SetView(key, value); err != nil {
		t.Fatalf("SetView second: %v", err)
	}
	if wrapped.payload.Count() != 0 || wrapped.opCount != 2 || wrapped.payload.Len() != initialPayloadLen || wrapped.payload.RetainedCap() != initialPayloadCap {
		t.Fatalf("payload count=%d opCount=%d len/cap=%d/%d, want 0/2/%d/%d", wrapped.payload.Count(), wrapped.opCount, wrapped.payload.Len(), wrapped.payload.RetainedCap(), initialPayloadLen, initialPayloadCap)
	}
	if inner.setViewCalls != 2 || inner.setCalls != 0 {
		t.Fatalf("inner calls: setView=%d set=%d, want 2/0", inner.setViewCalls, inner.setCalls)
	}
	if got := inner.entries[1]; !sameTestBytesBacking(got.Key, key) || !sameTestBytesBacking(got.Value, value) {
		t.Fatalf("SetView did not pass inner views: key_alias=%t value_alias=%t", sameTestBytesBacking(got.Key, key), sameTestBytesBacking(got.Value, value))
	}
}

func TestPublicCommandWALBatchDeleteViewBypassesPayloadBuilderWithoutInnerCopy(t *testing.T) {
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(nil, inner, 0)
	wrapped.payloadSoftCapBytes = 64
	initialPayloadLen := wrapped.payload.Len()
	initialPayloadCap := wrapped.payload.RetainedCap()

	key := []byte("second")
	if err := wrapped.DeleteView(key); err != nil {
		t.Fatalf("DeleteView: %v", err)
	}
	if !wrapped.payloadBypass || wrapped.payload.Count() != 0 || wrapped.opCount != 1 || wrapped.payload.Len() != initialPayloadLen || wrapped.payload.RetainedCap() != initialPayloadCap {
		t.Fatalf("payload after deleteView: bypass=%t count=%d opCount=%d len/cap=%d/%d, want true/0/1/%d/%d", wrapped.payloadBypass, wrapped.payload.Count(), wrapped.opCount, wrapped.payload.Len(), wrapped.payload.RetainedCap(), initialPayloadLen, initialPayloadCap)
	}
	if inner.deleteViewCalls != 1 || inner.deleteCalls != 0 {
		t.Fatalf("inner calls: deleteView=%d delete=%d, want 1/0", inner.deleteViewCalls, inner.deleteCalls)
	}
	if got := inner.entries[0]; got.Type != batch.OpDelete || !sameTestBytesBacking(got.Key, key) {
		t.Fatalf("DeleteView did not pass inner view: type=%v key_alias=%t", got.Type, sameTestBytesBacking(got.Key, key))
	}
}

func sameTestBytesBacking(a, b []byte) bool {
	return len(a) > 0 && len(b) > 0 && &a[0] == &b[0]
}

func TestPublicCommandWALBatchBypassStreamsReplayToCommandWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(db, inner, 0)
	wrapped.payloadSoftCapBytes = 64

	if err := wrapped.SetView([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("SetView alpha: %v", err)
	}
	if err := wrapped.SetView([]byte("bravo"), bytes.Repeat([]byte("v"), 128)); err != nil {
		t.Fatalf("SetView bravo: %v", err)
	}
	if !wrapped.payloadBypass {
		t.Fatal("batch did not take payload-bypass path")
	}
	savedScratch := wrapped.beginGroupPublicationTicketHandoff()
	appendErr := wrapped.appendCommandWAL(false)
	publication := wrapped.finishGroupPublicationTicketHandoff(savedScratch)
	db.finishPublicCommandWALGroupPublication(publication, appendErr)
	if appendErr != nil {
		t.Fatalf("appendCommandWAL: %v", appendErr)
	}
	if inner.replayCalls != 2 {
		t.Fatalf("inner replay calls=%d, want 2 planning/writing scans", inner.replayCalls)
	}

	r, err := commitlog.NewReader(filepath.Join(backenddb.WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewReader: %v", err)
	}
	env, err := r.ReadCommandFrame()
	if err != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	var got []batch.Entry
	if err := commitlog.ScanRawKVBatchPayload(env.Payload, func(op commitlog.RawKVOp, key, value []byte) error {
		got = append(got, batch.Entry{Type: batch.OpPut, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
		return nil
	}); err != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("ScanRawKVBatchPayload: %v", err)
	}
	if err := r.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("reader Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(got) != 2 || string(got[0].Key) != "alpha" || string(got[0].Value) != "one" || string(got[1].Key) != "bravo" || string(got[1].Value) != strings.Repeat("v", 128) {
		t.Fatalf("decoded streamed command WAL ops=%+v", got)
	}
}

func TestPublicCommandWALBatchStablePayloadAppendsWithoutReplay(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(db, inner, 0)
	wrapped.payloadSoftCapBytes = 64 << 10

	if _, _, err := wrapped.SetViewWithReplayBytes([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("SetViewWithReplayBytes alpha: %v", err)
	}
	if _, _, err := wrapped.SetViewWithReplayBytes([]byte("bravo"), []byte("two")); err != nil {
		t.Fatalf("SetViewWithReplayBytes bravo: %v", err)
	}
	if wrapped.payloadBypass || wrapped.payload.Count() != wrapped.opCount {
		t.Fatalf("payload state bypass=%t count=%d opCount=%d, want stable payload", wrapped.payloadBypass, wrapped.payload.Count(), wrapped.opCount)
	}
	savedScratch := wrapped.beginGroupPublicationTicketHandoff()
	appendErr := wrapped.appendCommandWAL(false)
	publication := wrapped.finishGroupPublicationTicketHandoff(savedScratch)
	db.finishPublicCommandWALGroupPublication(publication, appendErr)
	if appendErr != nil {
		_ = db.Close()
		t.Fatalf("appendCommandWAL: %v", appendErr)
	}
	if inner.replayCalls != 0 {
		_ = db.Close()
		t.Fatalf("inner replay calls=%d, want 0 for stable payload append", inner.replayCalls)
	}

	r, err := commitlog.NewReader(filepath.Join(backenddb.WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewReader: %v", err)
	}
	env, err := r.ReadCommandFrame()
	if err != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	var got []batch.Entry
	if err := commitlog.ScanRawKVBatchPayload(env.Payload, func(op commitlog.RawKVOp, key, value []byte) error {
		got = append(got, batch.Entry{Type: batch.OpPut, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
		return nil
	}); err != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("ScanRawKVBatchPayload: %v", err)
	}
	if err := r.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("reader Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(got) != 2 || string(got[0].Key) != "alpha" || string(got[0].Value) != "one" || string(got[1].Key) != "bravo" || string(got[1].Value) != "two" {
		t.Fatalf("decoded payload command WAL ops=%+v", got)
	}
}

func TestPublicCommandWALBatchOrdinarySetStablePayloadAppendsWithoutReplay(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(db, inner, 0)
	wrapped.payloadSoftCapBytes = 64 << 10

	alphaKey, alphaValue := []byte("alpha"), []byte("one")
	bravoKey, bravoValue := []byte("bravo"), []byte("two")
	if err := wrapped.Set(alphaKey, alphaValue); err != nil {
		t.Fatalf("Set alpha: %v", err)
	}
	if err := wrapped.Set(bravoKey, bravoValue); err != nil {
		t.Fatalf("Set bravo: %v", err)
	}
	alphaKey[0], alphaValue[0] = 'A', 'O'
	bravoKey[0], bravoValue[0] = 'B', 'T'
	if wrapped.payloadBypass || wrapped.payload.Count() != wrapped.opCount {
		t.Fatalf("payload state bypass=%t count=%d opCount=%d, want stable payload", wrapped.payloadBypass, wrapped.payload.Count(), wrapped.opCount)
	}
	if inner.setViewCalls != 2 || inner.setCalls != 0 {
		t.Fatalf("inner calls: setView=%d set=%d, want 2/0", inner.setViewCalls, inner.setCalls)
	}
	savedScratch := wrapped.beginGroupPublicationTicketHandoff()
	appendErr := wrapped.appendCommandWAL(false)
	publication := wrapped.finishGroupPublicationTicketHandoff(savedScratch)
	db.finishPublicCommandWALGroupPublication(publication, appendErr)
	if appendErr != nil {
		_ = db.Close()
		t.Fatalf("appendCommandWAL: %v", appendErr)
	}
	if inner.replayCalls != 0 {
		_ = db.Close()
		t.Fatalf("inner replay calls=%d, want 0 for ordinary stable payload append", inner.replayCalls)
	}

	r, err := commitlog.NewReader(filepath.Join(backenddb.WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewReader: %v", err)
	}
	env, err := r.ReadCommandFrame()
	if err != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	var got []batch.Entry
	if err := commitlog.ScanRawKVBatchPayload(env.Payload, func(op commitlog.RawKVOp, key, value []byte) error {
		got = append(got, batch.Entry{Type: batch.OpPut, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
		return nil
	}); err != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("ScanRawKVBatchPayload: %v", err)
	}
	if err := r.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("reader Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(got) != 2 || string(got[0].Key) != "alpha" || string(got[0].Value) != "one" || string(got[1].Key) != "bravo" || string(got[1].Value) != "two" {
		t.Fatalf("decoded payload command WAL ops=%+v", got)
	}
}

func TestPublicCommandWALBatchOrdinaryRetainedCapDoesNotGrow(t *testing.T) {
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(nil, inner, 0)
	wrapped.payloadSoftCapBytes = 100
	initialPayloadLen := wrapped.payload.Len()
	initialPayloadCap := wrapped.payload.RetainedCap()

	if err := wrapped.SetView([]byte("a"), bytes.Repeat([]byte("x"), 70)); err != nil {
		t.Fatalf("SetView first: %v", err)
	}
	if !wrapped.payloadBypass || wrapped.payload.Len() != initialPayloadLen || wrapped.payload.RetainedCap() != initialPayloadCap {
		t.Fatalf("payload after first ordinary set: bypass=%t len/cap=%d/%d, want true/%d/%d", wrapped.payloadBypass, wrapped.payload.Len(), wrapped.payload.RetainedCap(), initialPayloadLen, initialPayloadCap)
	}

	if err := wrapped.SetView([]byte("b"), []byte("y")); err != nil {
		t.Fatalf("SetView second: %v", err)
	}
	if wrapped.payload.Count() != 0 || wrapped.payload.Len() != initialPayloadLen || wrapped.payload.RetainedCap() != initialPayloadCap || wrapped.opCount != 2 {
		t.Fatalf("payload after second ordinary set: count=%d len/cap=%d/%d opCount=%d, want 0/%d/%d/2", wrapped.payload.Count(), wrapped.payload.Len(), wrapped.payload.RetainedCap(), wrapped.opCount, initialPayloadLen, initialPayloadCap)
	}
	if inner.setViewCalls != 2 || inner.setCalls != 0 {
		t.Fatalf("inner calls: setView=%d set=%d, want 2/0", inner.setViewCalls, inner.setCalls)
	}
}

func TestPublicCommandWALBatchOrdinaryCompactZeroDoesNotRetainPayload(t *testing.T) {
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(nil, inner, 0)
	wrapped.payloadSoftCapBytes = 96
	initialPayloadLen := wrapped.payload.Len()
	initialPayloadCap := wrapped.payload.RetainedCap()

	for i := 0; i < 3; i++ {
		key := []byte(fmt.Sprintf("zero-%03d", i))
		if err := wrapped.SetView(key, make([]byte, 32)); err != nil {
			t.Fatalf("SetView compact zero %d: %v", i, err)
		}
	}
	if !wrapped.payloadBypass || wrapped.payload.Count() != 0 || wrapped.payload.Len() != initialPayloadLen || wrapped.payload.RetainedCap() != initialPayloadCap {
		t.Fatalf("payload after compact zeros: bypass=%t count=%d len/cap=%d/%d, want true/0/%d/%d", wrapped.payloadBypass, wrapped.payload.Count(), wrapped.payload.Len(), wrapped.payload.RetainedCap(), initialPayloadLen, initialPayloadCap)
	}

	if err := wrapped.SetView([]byte("nz"), []byte("x")); err != nil {
		t.Fatalf("SetView non-zero after compact zeros: %v", err)
	}
	if wrapped.payload.Count() != 0 || wrapped.opCount != 4 || wrapped.payload.Len() != initialPayloadLen || wrapped.payload.RetainedCap() != initialPayloadCap {
		t.Fatalf("payload count=%d opCount=%d len/cap=%d/%d, want 0/4/%d/%d", wrapped.payload.Count(), wrapped.opCount, wrapped.payload.Len(), wrapped.payload.RetainedCap(), initialPayloadLen, initialPayloadCap)
	}
	if inner.setViewCalls != 4 || inner.setCalls != 0 {
		t.Fatalf("inner calls after compact zeros: setView=%d set=%d, want 4/0", inner.setViewCalls, inner.setCalls)
	}
}

func TestPublicCommandWALBatchPayloadSoftCapAccountsForCompactZeroRetainedBuffers(t *testing.T) {
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(nil, inner, 0)
	wrapped.payloadSoftCapBytes = 64

	key := bytes.Repeat([]byte("k"), 80)
	value := []byte{0}
	if err := wrapped.SetView(key, value); err != nil {
		t.Fatalf("SetView large compact-zero key: %v", err)
	}
	if !wrapped.payloadBypass {
		t.Fatal("compact-zero append did not bypass retained compact buffers")
	}
	if wrapped.payload.Count() != 0 || wrapped.opCount != 1 {
		t.Fatalf("payload count=%d opCount=%d, want 0/1", wrapped.payload.Count(), wrapped.opCount)
	}
	if inner.setViewCalls != 1 || inner.setCalls != 0 {
		t.Fatalf("inner calls after compact buffer bypass: setView=%d set=%d, want 1/0", inner.setViewCalls, inner.setCalls)
	}
	if got := wrapped.payload.RetainedCap(); got > wrapped.payloadSoftCapBytes {
		t.Fatalf("retained canonical payload cap=%d, soft_cap=%d", got, wrapped.payloadSoftCapBytes)
	}
}

func TestPublicCommandWALBatchPayloadSoftCapBoundsLargeValueBuilder(t *testing.T) {
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(nil, inner, 0)
	wrapped.payloadSoftCapBytes = 256 << 10

	value := bytes.Repeat([]byte("v"), 512<<10)
	for i := 0; i < 8; i++ {
		key := []byte(fmt.Sprintf("large-%02d", i))
		if err := wrapped.SetView(key, value); err != nil {
			t.Fatalf("SetView large %d: %v", i, err)
		}
	}
	if !wrapped.payloadBypass {
		t.Fatal("large-value batch did not bypass retained payload")
	}
	if got := wrapped.payload.RetainedCap(); got > wrapped.payloadSoftCapBytes {
		t.Fatalf("retained payload cap=%d, soft_cap=%d", got, wrapped.payloadSoftCapBytes)
	}
	if wrapped.payload.Count() != 0 || wrapped.opCount != 8 {
		t.Fatalf("payload count=%d opCount=%d, want 0/8", wrapped.payload.Count(), wrapped.opCount)
	}
	if inner.setViewCalls != 8 || inner.setCalls != 0 {
		t.Fatalf("inner calls after large values: setView=%d set=%d, want 8/0", inner.setViewCalls, inner.setCalls)
	}
	totalViewed := 0
	for _, entry := range inner.entries {
		totalViewed += len(entry.Value)
	}
	if totalViewed != 8*len(value) {
		t.Fatalf("inner viewed value bytes=%d, want %d", totalViewed, 8*len(value))
	}
}

func TestPublicCommandWALBatchPayloadSoftCapPreservesReplayViews(t *testing.T) {
	inner := &commandWALPayloadSoftCapBatch{}
	wrapped := newCommandWALPublicBatch(nil, inner, 0)
	wrapped.payloadSoftCapBytes = 1

	key := []byte("alpha")
	value := []byte("value")
	keyView, valueView, err := wrapped.SetViewWithReplayBytes(key, value)
	if err != nil {
		t.Fatalf("SetViewWithReplayBytes: %v", err)
	}
	key[0] = 'X'
	value[0] = 'Y'
	if wrapped.payloadBypass {
		t.Fatal("replay-view append used payload bypass")
	}
	if inner.setViewCalls != 1 || inner.setCalls != 0 {
		t.Fatalf("inner calls=%d/%d, want replay views through SetViewValidated", inner.setViewCalls, inner.setCalls)
	}
	if string(keyView) != "alpha" || string(valueView) != "value" {
		t.Fatalf("replay views changed: key=%q value=%q", keyView, valueView)
	}
	if wrapped.payload.Count() != 1 || wrapped.opCount != 1 {
		t.Fatalf("payload count=%d opCount=%d, want 1/1", wrapped.payload.Count(), wrapped.opCount)
	}
}

type commandWALNoResetBatch struct {
	entries     []batch.Entry
	replayCalls int
}

type commandWALResetBatch struct {
	commandWALNoResetBatch
}

type commandWALHookBatch struct {
	commandWALResetBatch
	writeAfterCalls int
}

type commandWALPayloadSoftCapBatch struct {
	commandWALResetBatch
	setCalls        int
	setViewCalls    int
	deleteCalls     int
	deleteViewCalls int
}

func (b *commandWALPayloadSoftCapBatch) AssignExternalCommandWALPointRevisions() page.EntryRevision {
	revision := page.EntryRevision(1)
	for i := range b.entries {
		entry := &b.entries[i]
		if entry.Type != batch.OpDeleteRange && entry.Revision == page.LegacyEntryRevision {
			entry.Revision = revision
		}
	}
	return revision
}

func (b *commandWALHookBatch) WriteAfterCommandWALAppend(_ bool, appendCommand func() error) error {
	if err := appendCommand(); err != nil {
		return err
	}
	b.writeAfterCalls++
	b.Reset()
	return nil
}

func (b *commandWALHookBatch) AssignExternalCommandWALPointRevisions() page.EntryRevision {
	revision := page.EntryRevision(1)
	for i := range b.entries {
		entry := &b.entries[i]
		if entry.Type != batch.OpDeleteRange && entry.Revision == page.LegacyEntryRevision {
			entry.Revision = revision
		}
	}
	return revision
}

func (b *commandWALHookBatch) WriteAfterCommandWALAppendWithPreparedRevision(_ bool, appendCommand func() error) error {
	if err := appendCommand(); err != nil {
		return err
	}
	b.writeAfterCalls++
	b.Reset()
	return nil
}

func (b *commandWALPayloadSoftCapBatch) Set(key, value []byte) error {
	b.setCalls++
	return b.commandWALResetBatch.Set(key, value)
}

func (b *commandWALPayloadSoftCapBatch) SetViewValidated(key, value []byte) error {
	b.setViewCalls++
	b.entries = append(b.entries, batch.Entry{Type: batch.OpPut, Key: key, Value: value})
	return nil
}

func (b *commandWALPayloadSoftCapBatch) Delete(key []byte) error {
	b.deleteCalls++
	return b.commandWALResetBatch.Delete(key)
}

func (b *commandWALPayloadSoftCapBatch) DeleteViewValidated(key []byte) error {
	b.deleteViewCalls++
	b.entries = append(b.entries, batch.Entry{Type: batch.OpDelete, Key: key})
	return nil
}

func (b *commandWALResetBatch) Reset() {
	b.entries = b.entries[:0]
}

func (b *commandWALNoResetBatch) Set(key, value []byte) error {
	b.entries = append(b.entries, batch.Entry{Type: batch.OpPut, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
	return nil
}

func (b *commandWALNoResetBatch) Delete(key []byte) error {
	b.entries = append(b.entries, batch.Entry{Type: batch.OpDelete, Key: append([]byte(nil), key...)})
	return nil
}

func (b *commandWALNoResetBatch) DeleteRange(start, end []byte) error {
	var startCopy, endCopy []byte
	if start != nil {
		startCopy = append([]byte(nil), start...)
	}
	if end != nil {
		endCopy = append([]byte(nil), end...)
	}
	b.entries = append(b.entries, batch.Entry{Type: batch.OpDeleteRange, Key: startCopy, Value: endCopy})
	return nil
}

func (b *commandWALNoResetBatch) Write() error { return nil }

func (b *commandWALNoResetBatch) WriteSync() error { return nil }

func (b *commandWALNoResetBatch) Close() error { return nil }

func (b *commandWALNoResetBatch) Replay(fn func(batch.Entry) error) error {
	b.replayCalls++
	for _, entry := range b.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *commandWALNoResetBatch) GetByteSize() (int, error) { return len(b.entries), nil }

func BenchmarkPublicCommandWALRawKVSet(b *testing.B) {
	for _, commandWAL := range []bool{false, true} {
		b.Run(fmt.Sprintf("command_wal=%t", commandWAL), func(b *testing.B) {
			db, err := Open(Options{
				Dir:                 b.TempDir(),
				Durability:          DurabilityWALOnRelaxed,
				CommandWAL:          commandWAL,
				CommandWALStatsScan: commandWAL,
				DisableSideStores:   true,
			})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer func() { _ = db.Close() }()

			value := []byte("public-command-wal-value")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("k%09d", i))
				if err := db.Set(key, value); err != nil {
					b.Fatalf("Set: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "sets/s")
			if commandWAL {
				assertPublicCommandWALFramesB(b, db, uint64(b.N))
			}
		})
	}
}

func BenchmarkPublicCommandWALRawKVBatchWrite(b *testing.B) {
	for _, batchSize := range []int{64, 1024} {
		b.Run(fmt.Sprintf("batch_size=%d", batchSize), func(b *testing.B) {
			for _, commandWAL := range []bool{false, true} {
				b.Run(fmt.Sprintf("command_wal=%t", commandWAL), func(b *testing.B) {
					db, err := Open(Options{
						Dir:                 b.TempDir(),
						Durability:          DurabilityWALOnRelaxed,
						CommandWAL:          commandWAL,
						CommandWALStatsScan: commandWAL,
						DisableSideStores:   true,
					})
					if err != nil {
						b.Fatalf("Open: %v", err)
					}
					defer func() { _ = db.Close() }()

					value := []byte("public-command-wal-value")
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						batch := db.NewBatchWithSize(batchSize)
						base := i * batchSize
						for j := 0; j < batchSize; j++ {
							var keyBuf [32]byte
							key := strconv.AppendInt(keyBuf[:0], int64(base+j), 10)
							if err := batch.Set(key, value); err != nil {
								_ = batch.Close()
								b.Fatalf("batch Set: %v", err)
							}
						}
						if err := batch.Write(); err != nil {
							_ = batch.Close()
							b.Fatalf("batch Write: %v", err)
						}
						if err := batch.Close(); err != nil {
							b.Fatalf("batch Close: %v", err)
						}
					}
					b.StopTimer()
					totalSets := float64(b.N * batchSize)
					b.ReportMetric(totalSets/b.Elapsed().Seconds(), "sets/s")
					b.ReportMetric(float64(batchSize), "sets/batch")
					if commandWAL {
						assertPublicCommandWALFramesB(b, db, uint64(b.N))
					}
				})
			}
		})
	}
}

func publicCommandWALSequentialDecimalKeyBytes(count int) uint64 {
	if count <= 0 {
		return 0
	}
	total := uint64(1) // key "0"
	for lower, digits := 1, uint64(1); lower < count; digits++ {
		upper := count
		if lower <= count/10 {
			upper = lower * 10
		}
		total += uint64(upper-lower) * digits
		lower = upper
	}
	return total
}

func TestPublicCommandWALDurableTinyBatchWriteSyncByteAccounting(t *testing.T) {
	for count, want := range map[int]uint64{
		0:    0,
		1:    1,
		10:   10,
		11:   12,
		100:  190,
		101:  193,
		1000: 2890,
		1001: 2894,
	} {
		if got := publicCommandWALSequentialDecimalKeyBytes(count); got != want {
			t.Fatalf("sequential decimal key bytes for count=%d: got %d, want %d", count, got, want)
		}
	}
}

func BenchmarkPublicCommandWALDurableTinyBatchWriteSync(b *testing.B) {
	type shape struct {
		name      string
		batchSize int
	}
	shapes := []shape{
		{name: "dirty_batch", batchSize: 1},
		{name: "dirty_batch", batchSize: 8},
		{name: "dirty_batch", batchSize: 32},
		{name: "write_then_dirty_write_sync", batchSize: 1},
		{name: "empty_write_sync_after_write", batchSize: 1},
		{name: "state_point_point_sync_batch_sync", batchSize: 1},
	}
	for _, placement := range []struct {
		name           string
		forcedPointers bool
	}{
		{name: "inline"},
		{name: "forced_pointer", forcedPointers: true},
	} {
		for _, benchmarkShape := range shapes {
			name := fmt.Sprintf("placement=%s/shape=%s/ops=%d", placement.name, benchmarkShape.name, benchmarkShape.batchSize)
			b.Run(name, func(b *testing.B) {
				benchmarkPublicCommandWALDurableShape(b, placement.forcedPointers, benchmarkShape.name, benchmarkShape.batchSize)
			})
		}
	}
}

func benchmarkPublicCommandWALDurableShape(b *testing.B, forcedPointers bool, shape string, batchSize int) {
	opts := commandWALDurabilityProofOptions(b.TempDir())
	opts.PublicBatchWriteSyncPhaseStats = true
	if forcedPointers {
		opts.ValueLog.PointerThreshold = 1
		opts.ValueLog.ForcePointers = true
	}
	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	value := []byte("public-command-wal-value")
	if forcedPointers {
		value = bytes.Repeat([]byte("p"), 4096)
	}
	before := db.Stats()
	latencyCapacity := b.N
	if shape == "state_point_point_sync_batch_sync" {
		latencyCapacity *= 2
	}
	latencies := make([]time.Duration, 0, latencyCapacity)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := i * max(batchSize, 1)
		switch shape {
		case "dirty_batch":
			latencies = append(latencies, benchmarkPublicCommandWALBatch(b, db, "dirty/", base, batchSize, value, true))
		case "write_then_dirty_write_sync":
			_ = benchmarkPublicCommandWALBatch(b, db, "prior/", base, 1, value, false)
			latencies = append(latencies, benchmarkPublicCommandWALBatch(b, db, "dirty/", base, 1, value, true))
		case "empty_write_sync_after_write":
			_ = benchmarkPublicCommandWALBatch(b, db, "prior/", base, 1, value, false)
			latencies = append(latencies, benchmarkPublicCommandWALBatch(b, db, "empty/", base, 0, value, true))
		case "state_point_point_sync_batch_sync":
			var keyBuf [64]byte
			key := strconv.AppendInt(append(keyBuf[:0], "state/unsynced/"...), int64(base), 10)
			if err := db.Set(key, value); err != nil {
				b.Fatalf("state Set: %v", err)
			}
			key = strconv.AppendInt(append(keyBuf[:0], "state/synced/"...), int64(base), 10)
			start := time.Now()
			if err := db.SetSync(key, value); err != nil {
				b.Fatalf("state SetSync: %v", err)
			}
			latencies = append(latencies, time.Since(start))
			latencies = append(latencies, benchmarkPublicCommandWALBatchWithSetView(b, db, "state/batch/", base, value))
		default:
			b.Fatalf("unknown durable shape %q", shape)
		}
	}
	b.StopTimer()

	perIteration := publicCommandWALDurableShapeExpectedCounters(shape, forcedPointers, DurabilityDurable)
	after := db.Stats()
	for key, want := range perIteration {
		requireBenchmarkStatDelta(b, before, after, key, want*uint64(b.N))
	}
	requireBenchmarkStatDelta(b, before, after, "treedb.public.checkpoint.calls_total", 0)
	if got := statMapUint64B(b, after, "treedb.cache.checkpoint.runs"); got != 0 {
		b.Fatalf("checkpoint.runs=%d, want 0 for isolated durable shape", got)
	}

	elapsed := b.Elapsed()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "iterations/s")
	}
	b.ReportMetric(float64(batchSize), "batch_keys/iteration")
	reportWriteSyncLatencyDistribution(b, latencies)
	reportCommandWALBenchmarkDeltas(b, before, after, uint64(b.N))
}

func benchmarkPublicCommandWALBatch(b *testing.B, db *DB, prefix string, base, count int, value []byte, syncWrite bool) time.Duration {
	b.Helper()
	batch := db.NewBatchWithSize(max(count, 1))
	for j := 0; j < count; j++ {
		var keyBuf [64]byte
		key := strconv.AppendInt(append(keyBuf[:0], prefix...), int64(base+j), 10)
		if err := batch.Set(key, value); err != nil {
			_ = batch.Close()
			b.Fatalf("batch Set(%d): %v", j, err)
		}
	}
	start := time.Now()
	var err error
	if syncWrite {
		err = batch.WriteSync()
	} else {
		err = batch.Write()
	}
	elapsed := time.Since(start)
	if err != nil {
		_ = batch.Close()
		b.Fatalf("batch write (sync=%t): %v", syncWrite, err)
	}
	if err := batch.Close(); err != nil {
		b.Fatalf("batch Close: %v", err)
	}
	return elapsed
}

func benchmarkPublicCommandWALBatchWithSetView(b *testing.B, db *DB, prefix string, base int, value []byte) time.Duration {
	b.Helper()
	batch, ok := db.NewBatchWithSize(1).(*commandWALPublicBatch)
	if !ok {
		b.Fatalf("NewBatchWithSize type=%T, want *commandWALPublicBatch", db.NewBatchWithSize(1))
	}
	var keyBuf [64]byte
	key := strconv.AppendInt(append(keyBuf[:0], prefix...), int64(base), 10)
	if err := batch.SetView(key, value); err != nil {
		_ = batch.Close()
		b.Fatalf("batch SetView: %v", err)
	}
	start := time.Now()
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		b.Fatalf("batch WriteSync: %v", err)
	}
	elapsed := time.Since(start)
	if err := batch.Close(); err != nil {
		b.Fatalf("batch Close: %v", err)
	}
	return elapsed
}

func publicCommandWALDurableShapeExpectedCounters(shape string, forcedPointers bool, durability DurabilityMode) map[string]uint64 {
	want := map[string]uint64{
		"treedb.command_wal.write.errors_total":                         0,
		"treedb.command_wal.file_sync.errors_total":                     0,
		"treedb.cache.value_log.sync.errors_total":                      0,
		"treedb.cache.value_log.file_sync.errors_total":                 0,
		"treedb.cache.value_log.file_sync.rotated_segment.calls_total":  0,
		"treedb.cache.value_log.file_sync.rotated_segment.errors_total": 0,
	}
	var appendCalls, flushCalls, syncCalls, writeSyscalls, fileSyncCalls uint64
	var batchWriteCalls, batchWriteSyncCalls, barrierSyncCalls uint64
	var valueLogMaterializationSyncs, valueLogExternalRefSyncs, valueLogPendingBarrierSyncs uint64
	switch shape {
	case "dirty_batch":
		appendCalls, syncCalls, writeSyscalls, fileSyncCalls = 1, 1, 1, 1
		batchWriteSyncCalls = 1
	case "write_then_dirty_write_sync":
		appendCalls, flushCalls, syncCalls, writeSyscalls, fileSyncCalls = 2, 1, 1, 2, 1
		batchWriteCalls, batchWriteSyncCalls = 1, 1
		// In the relaxed profile the first write remains replay-self-contained
		// in the command WAL; the following directly synced mutation closes
		// that prefix without forcing deferred value-log materialization.
	case "empty_write_sync_after_write":
		appendCalls, flushCalls, syncCalls, writeSyscalls, fileSyncCalls = 2, 1, 1, 2, 1
		batchWriteCalls, batchWriteSyncCalls, barrierSyncCalls = 1, 1, 1
	case "state_point_point_sync_batch_sync":
		appendCalls, flushCalls, syncCalls, writeSyscalls, fileSyncCalls = 3, 1, 2, 3, 2
		batchWriteSyncCalls = 1
	}
	if durability == DurabilityDurable {
		switch shape {
		case "dirty_batch":
			appendCalls, flushCalls, writeSyscalls, barrierSyncCalls = 1, 0, 1, 0
		case "write_then_dirty_write_sync":
			appendCalls, flushCalls, syncCalls, writeSyscalls, fileSyncCalls, barrierSyncCalls = 2, 0, 2, 2, 2, 0
		case "empty_write_sync_after_write":
			appendCalls, flushCalls, syncCalls, writeSyscalls, fileSyncCalls = 1, 0, 2, 1, 2
		case "state_point_point_sync_batch_sync":
			appendCalls, flushCalls, syncCalls, writeSyscalls, fileSyncCalls, barrierSyncCalls = 3, 0, 3, 3, 3, 0
		}
	}
	valueLogSyncs := valueLogMaterializationSyncs + valueLogExternalRefSyncs + valueLogPendingBarrierSyncs
	want["treedb.command_wal.append.count_total"] = appendCalls
	want["treedb.command_wal.flush.count_total"] = flushCalls
	want["treedb.command_wal.sync.count_total"] = syncCalls
	want["treedb.command_wal.sync.barrier.count_total"] = barrierSyncCalls
	want["treedb.command_wal.write.syscalls_total"] = writeSyscalls
	want["treedb.command_wal.file_sync.calls_total"] = fileSyncCalls
	want["treedb.public.batch.write.calls_total"] = batchWriteCalls
	want["treedb.public.batch.write_sync.calls_total"] = batchWriteSyncCalls
	want["treedb.cache.value_log.sync.calls_total"] = valueLogSyncs
	want["treedb.cache.value_log.sync.materialization.calls_total"] = valueLogMaterializationSyncs
	want["treedb.cache.value_log.sync.external_ref.calls_total"] = valueLogExternalRefSyncs
	want["treedb.cache.value_log.sync.pending_barrier.calls_total"] = valueLogPendingBarrierSyncs
	want["treedb.cache.value_log.file_sync.calls_total"] = valueLogSyncs
	return want
}

func BenchmarkPublicCommandWALCheckpointOverlapWriteSync(b *testing.B) {
	opts := commandWALDurabilityProofOptions(b.TempDir())
	opts.FlushThreshold = 64 << 10
	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	defer db.cached.SetCommandWALCheckpointPublishHook(db.preparePublicCommandWALPendingPublish)

	// Seed one pre-cut command so every measured checkpoint has a real mutable
	// frontier and an AppliedCommandLSN publication boundary. Each measured
	// write then becomes the next iteration's pre-cut command.
	if err := db.SetSync([]byte("tx-index/overlap/seed"), []byte("value")); err != nil {
		b.Fatalf("seed SetSync: %v", err)
	}

	const checkpointDwell = 20 * time.Millisecond
	before := db.Stats()
	latencies := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		publishReached := make(chan struct{})
		releasePublish := make(chan struct{})
		var publishOnce sync.Once
		db.cached.SetCommandWALCheckpointPublishHook(func(sync bool) (uint64, []backenddb.CommandWALLSNRange, error) {
			publishOnce.Do(func() { close(publishReached) })
			<-releasePublish
			return db.preparePublicCommandWALPendingPublish(sync)
		})

		checkpointDone := make(chan error, 1)
		go func() { checkpointDone <- db.cached.Checkpoint() }()
		if err := waitForPublicCommandWALCheckpointPhase(publishReached, "benchmark checkpoint publish hook"); err != nil {
			b.Fatal(err)
		}

		batch := db.NewBatchWithSize(1)
		var keyBuf [64]byte
		key := append(keyBuf[:0], "tx-index/overlap/"...)
		key = strconv.AppendInt(key, int64(i), 10)
		if err := batch.Set(key, []byte("value")); err != nil {
			_ = batch.Close()
			close(releasePublish)
			b.Fatalf("batch Set: %v", err)
		}

		releaseTimer := time.AfterFunc(checkpointDwell, func() { close(releasePublish) })
		writeStart := time.Now()
		writeErr := batch.WriteSync()
		latencies = append(latencies, time.Since(writeStart))
		if writeErr != nil {
			_ = batch.Close()
			if releaseTimer.Stop() {
				close(releasePublish)
			}
			b.Fatalf("batch WriteSync: %v", writeErr)
		}
		if err := batch.Close(); err != nil {
			if releaseTimer.Stop() {
				close(releasePublish)
			}
			b.Fatalf("batch Close: %v", err)
		}
		if err := <-checkpointDone; err != nil {
			b.Fatalf("Checkpoint: %v", err)
		}
		_ = releaseTimer.Stop()
	}
	b.StopTimer()

	after := db.Stats()
	reportWriteSyncLatencyDistribution(b, latencies)
	b.ReportMetric(float64(checkpointDwell.Nanoseconds()), "checkpoint_dwell_ns")
	for _, key := range []string{
		"treedb.cache.write.wait_for_checkpoint.count_total",
		"treedb.cache.write.wait_for_checkpoint.ns_total",
		"treedb.cache.write.wait.frontier_cutover.count_total",
		"treedb.cache.write.wait.frontier_cutover.ns_total",
		"treedb.cache.write.wait.checkpoint_drain.count_total",
		"treedb.cache.write.wait.checkpoint_drain.ns_total",
		"treedb.cache.write.wait.maintenance.count_total",
		"treedb.cache.write.wait.maintenance.ns_total",
		"treedb.cache.write.post_frontier_admission.count_total",
	} {
		beforeValue, beforeOK := benchmarkOptionalStatUint64(before, key)
		afterValue, afterOK := benchmarkOptionalStatUint64(after, key)
		if !beforeOK || !afterOK {
			continue
		}
		name := strings.TrimPrefix(key, "treedb.cache.")
		b.ReportMetric(float64(afterValue-beforeValue)/float64(b.N), name+"/op")
	}
}

func benchmarkOptionalStatUint64(stats map[string]string, key string) (uint64, bool) {
	raw, ok := stats[key]
	if !ok || raw == "" {
		return 0, false
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	return value, err == nil
}

func BenchmarkPublicCommandWALTinyBatchWriteSyncPhaseStatsOverhead(b *testing.B) {
	for _, durability := range []struct {
		name string
		mode DurabilityMode
	}{
		{name: "durable", mode: DurabilityDurable},
		{name: "relaxed", mode: DurabilityWALOnRelaxed},
	} {
		b.Run("durability="+durability.name, func(b *testing.B) {
			for _, enabled := range []bool{false, true} {
				b.Run(fmt.Sprintf("enabled=%t", enabled), func(b *testing.B) {
					opts := commandWALDurabilityProofOptions(b.TempDir())
					opts.Durability = durability.mode
					opts.PublicBatchWriteSyncPhaseStats = enabled
					db, err := Open(opts)
					if err != nil {
						b.Fatalf("Open: %v", err)
					}
					defer func() { _ = db.Close() }()

					value := []byte("public-command-wal-value")
					before := db.Stats()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						batch := db.NewBatchWithSize(1)
						var keyBuf [32]byte
						key := strconv.AppendInt(keyBuf[:0], int64(i), 10)
						if err := batch.Set(key, value); err != nil {
							_ = batch.Close()
							b.Fatalf("batch Set: %v", err)
						}
						if err := batch.WriteSync(); err != nil {
							_ = batch.Close()
							b.Fatalf("batch WriteSync: %v", err)
						}
						if err := batch.Close(); err != nil {
							b.Fatalf("batch Close: %v", err)
						}
					}
					b.StopTimer()

					after := db.Stats()
					wantPhaseCalls := uint64(0)
					if enabled {
						wantPhaseCalls = uint64(b.N)
					}
					requireBenchmarkStatDelta(b, before, after, "treedb.public.batch.write_sync.phase.calls_total", wantPhaseCalls)
				})
			}
		})
	}
}

func TestPublicCommandWALDurableTinyBatchWriteSyncShapes(t *testing.T) {
	for _, batchSize := range []int{1, 8, 32} {
		t.Run(fmt.Sprintf("ops=%d", batchSize), func(t *testing.T) {
			db, err := Open(commandWALDurabilityProofOptions(t.TempDir()))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = db.Close() }()

			before := db.Stats()
			batch := db.NewBatchWithSize(batchSize)
			for i := 0; i < batchSize; i++ {
				if err := batch.Set([]byte(fmt.Sprintf("tx-index/%02d", i)), []byte("value")); err != nil {
					_ = batch.Close()
					t.Fatalf("batch Set(%d): %v", i, err)
				}
			}
			if err := batch.WriteSync(); err != nil {
				_ = batch.Close()
				t.Fatalf("batch WriteSync: %v", err)
			}
			if err := batch.Close(); err != nil {
				t.Fatalf("batch Close: %v", err)
			}

			after := db.Stats()
			requirePublicStatDelta(t, before, after, "treedb.command_wal.append.count_total", 1)
			requirePublicStatDelta(t, before, after, "treedb.command_wal.append.payload.count_total", 1)
			requirePublicStatDelta(t, before, after, "treedb.command_wal.flush.count_total", 0)
			requirePublicStatDelta(t, before, after, "treedb.command_wal.sync.count_total", 1)
			requirePublicStatDelta(t, before, after, "treedb.public.batch.write_sync.calls_total", 1)
			requirePublicCommandWALNoCheckpointSince(t, db, before)
			assertPublicCommandWALFrames(t, db, 1)
		})
	}
}

func TestPublicCommandWALAutoCheckpointOverlapAdmitsPostFrontierWrites(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	ApplyProfile(&opts, ProfileCommandWALRelaxed)
	opts.BackgroundCheckpointInterval = time.Hour // Starts the existing loop; the test triggers its pass explicitly.
	opts.FlushThreshold = 64 << 10
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	frontierValue := bytes.Repeat([]byte("f"), 32<<10)
	for i := 0; i < 32; i++ {
		batch := db.NewBatchWithSize(16)
		for j := 0; j < 16; j++ {
			key := []byte(fmt.Sprintf("tx-index/frontier/%03d/%02d", i, j))
			if err := batch.Set(key, frontierValue); err != nil {
				_ = batch.Close()
				t.Fatalf("seed Set(%d,%d): %v", i, j, err)
			}
		}
		if err := batch.Write(); err != nil {
			_ = batch.Close()
			t.Fatalf("seed Write(%d): %v", i, err)
		}
		if err := batch.Close(); err != nil {
			t.Fatalf("seed Close(%d): %v", i, err)
		}
	}

	checkpointPublish := make(chan struct{})
	releaseCheckpointPublish := make(chan struct{})
	checkpointComplete := make(chan struct{})
	var checkpointPublishOnce, releaseCheckpointPublishOnce, checkpointCompleteOnce sync.Once
	defer releaseCheckpointPublishOnce.Do(func() { close(releaseCheckpointPublish) })
	db.cached.SetCommandWALCheckpointCutoverHook(func() {
		db.snapshotPublicCommandWALCheckpointCutover()
	})
	db.cached.SetCommandWALCheckpointPublishHook(func(sync bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		checkpointPublishOnce.Do(func() {
			close(checkpointPublish)
		})
		if err := waitForPublicCommandWALCheckpointPhase(releaseCheckpointPublish, "checkpoint publish release"); err != nil {
			return 0, nil, err
		}
		return db.preparePublicCommandWALPendingPublish(sync)
	})
	db.cached.SetCommandWALCheckpointCleanupHook(func(sync bool) error {
		err := db.cleanupPublicCommandWALCheckpoint(sync)
		if err == nil {
			checkpointCompleteOnce.Do(func() { close(checkpointComplete) })
		}
		return err
	})

	before := db.Stats()
	db.triggerAutoCheckpointForTest()
	if err := waitForPublicCommandWALCheckpointPhase(checkpointPublish, "checkpoint publish hook"); err != nil {
		t.Fatal(err)
	}

	overlapValue := bytes.Repeat([]byte("p"), 64<<10)
	type writeResult struct {
		index int
		err   error
	}
	overlapBatches := make([]Batch, 16)
	for i := range overlapBatches {
		batch := db.NewBatchWithSize(1)
		if err := batch.Set([]byte(fmt.Sprintf("tx-index/overlap/%02d", i)), overlapValue); err != nil {
			_ = batch.Close()
			for _, pending := range overlapBatches[:i] {
				_ = pending.Close()
			}
			t.Fatalf("overlap Set(%d): %v", i, err)
		}
		overlapBatches[i] = batch
	}
	writeResults := make(chan writeResult, 16)
	for i, batch := range overlapBatches {
		go func(index int, batch Batch) {
			var writeErr error
			if index%2 == 0 {
				writeErr = batch.Write()
			} else {
				writeErr = batch.WriteSync()
			}
			if closeErr := batch.Close(); writeErr == nil {
				writeErr = closeErr
			}
			writeResults <- writeResult{index: index, err: writeErr}
		}(i, batch)
	}
	// The publish hook is after the command-LSN/mutable frontier cut and before
	// the checkpoint can complete. Post-cut Write and WriteSync calls must finish
	// in the fresh command-WAL/mutable generation while this old frontier remains
	// latched.
	writeCompletionTimer := time.NewTimer(publicCommandWALCheckpointTestTimeout)
	defer writeCompletionTimer.Stop()
	for i := 0; i < len(overlapBatches); i++ {
		select {
		case result := <-writeResults:
			if result.err != nil {
				t.Fatalf("overlap Write(%d): %v", result.index, result.err)
			}
		case <-checkpointComplete:
			t.Fatalf("checkpoint completed before publish latch released after %d/%d overlap writes", i, len(overlapBatches))
		case <-writeCompletionTimer.C:
			t.Fatalf("timed out waiting for post-frontier write completion %d/%d", i+1, len(overlapBatches))
		}
	}
	writeCompletionTimer.Stop()
	select {
	case <-checkpointComplete:
		t.Fatal("checkpoint completed while publish hook remained latched")
	default:
	}
	releaseCheckpointPublishOnce.Do(func() { close(releaseCheckpointPublish) })
	if err := waitForPublicCommandWALCheckpointPhase(checkpointComplete, "checkpoint cleanup hook"); err != nil {
		t.Fatal(err)
	}

	wantAutoCheckpointCount := statMapUint64(t, before, "treedb.cache.auto_checkpoint.count") + 1
	if err := waitForPublicCommandWALAutoCheckpointCount(db, wantAutoCheckpointCount); err != nil {
		t.Fatal(err)
	}

	after := db.Stats()
	if got := statMapUint64(t, after, "treedb.cache.auto_checkpoint.count"); got != 1 {
		t.Fatalf("auto_checkpoint.count=%d, want 1", got)
	}
	if got := after["treedb.cache.auto_checkpoint.last_reason"]; got != "force" {
		t.Fatalf("auto_checkpoint.last_reason=%q, want force", got)
	}
	if got := statMapUint64(t, after, "treedb.cache.checkpoint.frontier.drained_units_last"); got == 0 {
		t.Fatal("checkpoint frontier drained no units")
	}
	requirePublicStatDelta(t, before, after, "treedb.cache.write.wait_for_checkpoint.count_total", 0)
	requirePublicStatDelta(t, before, after, "treedb.cache.write.post_frontier_admission.count_total", 16)
	requirePublicStatDelta(t, before, after, "treedb.cache.write.wait.checkpoint_drain.count_total", 0)
	requirePublicStatDelta(t, before, after, "treedb.public.batch.write.calls_total", 8)
	requirePublicStatDelta(t, before, after, "treedb.public.batch.write_sync.calls_total", 8)
	groupCount := statMapUint64(t, after, "treedb.command_wal.group_commit.groups_total") -
		statMapUint64(t, before, "treedb.command_wal.group_commit.groups_total")
	forcedGroups := statMapUint64(t, after, "treedb.command_wal.group_commit.forced_total") -
		statMapUint64(t, before, "treedb.command_wal.group_commit.forced_total")
	if groupCount < forcedGroups {
		t.Fatalf("group commits=%d forced groups=%d, want every forced group represented", groupCount, forcedGroups)
	}
	durableGroups := groupCount - forcedGroups
	if durableGroups > 8 {
		t.Fatalf("durable overlap groups=%d, want at most one per durable caller", durableGroups)
	}
	groupedCommits := statMapUint64(t, after, "treedb.command_wal.group_commit.commits_total") -
		statMapUint64(t, before, "treedb.command_wal.group_commit.commits_total")
	soloDurable := statMapUint64(t, after, "treedb.command_wal.group_commit.bypass.reason.solo_durable_sync_total") -
		statMapUint64(t, before, "treedb.command_wal.group_commit.bypass.reason.solo_durable_sync_total")
	if groupedCommits+soloDurable != 8 {
		t.Fatalf("durable overlap publications: grouped commits=%d solo durable=%d, want 8 total", groupedCommits, soloDurable)
	}
	// Each of the sixteen writes contributes one mutation frame. The eight
	// durable writes routed through the coordinator contribute one barrier per
	// group. An uncontended durable caller may instead sync its mutation frame
	// directly, so its publication contributes no barrier.
	requirePublicStatDelta(t, before, after, "treedb.command_wal.append.count_total", 16+durableGroups)
	// The auto-checkpoint itself contributes one direct durable-prefix sync in
	// addition to the coordinator-owned groups. Each solo durable publication
	// contributes one more direct sync.
	requirePublicStatDelta(t, before, after, "treedb.command_wal.sync.count_total", groupCount+1+soloDurable)
	requirePublicStatDelta(t, before, after, "treedb.public.checkpoint.calls_total", 0)
}

func TestPublicCommandWALCheckpointPostFrontierRangeWritesWaitForDrain(t *testing.T) {
	tests := []struct {
		name  string
		write func(*DB) error
	}{
		{
			name: "delete_range",
			write: func(db *DB) error {
				return db.DeleteRange([]byte("range/"), []byte("range0"))
			},
		},
		{
			name: "pure_range_batch",
			write: func(db *DB) error {
				b := db.NewBatch()
				if err := b.DeleteRange([]byte("range/"), []byte("range0")); err != nil {
					_ = b.Close()
					return err
				}
				err := b.WriteSync()
				if closeErr := b.Close(); err == nil {
					err = closeErr
				}
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(commandWALDurabilityProofOptions(t.TempDir()))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = db.Close() }()

			for _, key := range []string{"range/a", "range/b", "outside"} {
				if err := db.SetSync([]byte(key), []byte("value")); err != nil {
					t.Fatalf("seed SetSync(%q): %v", key, err)
				}
			}

			publishEntered := make(chan struct{})
			releasePublish := make(chan struct{})
			var publishOnce, releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(releasePublish) })
			db.cached.SetCommandWALCheckpointPublishHook(func(syncWrite bool) (uint64, []backenddb.CommandWALLSNRange, error) {
				publishOnce.Do(func() { close(publishEntered) })
				if err := waitForPublicCommandWALCheckpointPhase(releasePublish, "range checkpoint publish release"); err != nil {
					return 0, nil, err
				}
				return db.preparePublicCommandWALPendingPublish(syncWrite)
			})

			before := db.Stats()
			checkpointDone := make(chan error, 1)
			go func() { checkpointDone <- db.cached.Checkpoint() }()
			if err := waitForPublicCommandWALCheckpointPhase(publishEntered, "range checkpoint publish hook"); err != nil {
				t.Fatal(err)
			}
			if got := db.cached.Stats()["treedb.cache.checkpoint.post_frontier_admission.active"]; got != "true" {
				t.Fatalf("post-frontier admission active=%q, want true", got)
			}

			writeDone := make(chan error, 1)
			go func() { writeDone <- tc.write(db) }()
			if err := waitForPublicCommandWALCheckpointWriterWaiters(db, 1); err != nil {
				t.Fatal(err)
			}
			blocked := db.Stats()
			requirePublicStatDelta(t, before, blocked, "treedb.command_wal.append.count_total", 0)
			requirePublicStatDelta(t, before, blocked, "treedb.cache.write.post_frontier_admission.count_total", 0)
			select {
			case err := <-writeDone:
				t.Fatalf("range write completed while checkpoint publish remained latched: %v", err)
			default:
			}

			releaseOnce.Do(func() { close(releasePublish) })
			if err := <-checkpointDone; err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if err := <-writeDone; err != nil {
				t.Fatalf("range write after checkpoint drain: %v", err)
			}

			after := db.Stats()
			requirePublicStatDelta(t, before, after, "treedb.cache.write.wait_for_checkpoint.count_total", 1)
			requirePublicStatDelta(t, before, after, "treedb.cache.write.wait.checkpoint_drain.count_total", 1)
			requirePublicStatDelta(t, before, after, "treedb.cache.write.post_frontier_admission.count_total", 0)
			requirePublicStatDelta(t, before, after, "treedb.command_wal.append.count_total", 1)
			for _, key := range []string{"range/a", "range/b"} {
				if got, err := db.Get([]byte(key)); err != nil || got != nil {
					t.Fatalf("Get(%q) after range write=(%q, %v), want missing", key, got, err)
				}
			}
			requireRawKVValue(t, db, []byte("outside"), []byte("value"))
		})
	}
}

func TestPublicCommandWALCheckpointPostFrontierAdmissionPropagatesPublishError(t *testing.T) {
	db, err := Open(commandWALDurabilityProofOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.SetSync([]byte("publish-error/pre-cut"), []byte("pre-value")); err != nil {
		t.Fatalf("pre-cut SetSync: %v", err)
	}

	publishErr := errors.New("forced checkpoint publish failure")
	publishEntered := make(chan struct{})
	releasePublish := make(chan struct{})
	var publishOnce sync.Once
	db.cached.SetCommandWALCheckpointPublishHook(func(bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		publishOnce.Do(func() { close(publishEntered) })
		if err := waitForPublicCommandWALCheckpointPhase(releasePublish, "publish-error release"); err != nil {
			return 0, nil, err
		}
		return 0, nil, publishErr
	})
	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- db.Checkpoint() }()
	if err := waitForPublicCommandWALCheckpointPhase(publishEntered, "publish-error hook"); err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync([]byte("publish-error/post-cut"), []byte("post-value")); err != nil {
		t.Fatalf("post-cut SetSync: %v", err)
	}
	close(releasePublish)
	if err := <-checkpointDone; !errors.Is(err, publishErr) {
		t.Fatalf("Checkpoint error=%v, want %v", err, publishErr)
	}
	// SetSync publishes into the cache before a background flush finishes its
	// backend commit post-work. Drain serializes with that flush so the Stats
	// snapshot below cannot overlap leaf-generation finalization.
	if err := db.cached.Drain(); err != nil {
		t.Fatalf("Drain after failed checkpoint: %v", err)
	}
	if first, last := db.publicCommandWALPendingRange(); first != 1 || last != 2 {
		t.Fatalf("pending command-WAL range after failed publish=(%d,%d), want direct mutation prefix (1,2)", first, last)
	}
	requireRawKVValue(t, db, []byte("publish-error/pre-cut"), []byte("pre-value"))
	requireRawKVValue(t, db, []byte("publish-error/post-cut"), []byte("post-value"))
	if got := db.cached.Stats()["treedb.cache.checkpoint.active"]; got != "false" {
		t.Fatalf("checkpoint active after error=%q, want false", got)
	}

	// Restore the public owner hooks and prove the failed checkpoint left both
	// command generations available for a later strict boundary.
	db.cached.SetCommandWALCheckpointPublishHook(db.preparePublicCommandWALPendingPublish)
	db.cached.SetCommandWALCheckpointCleanupHook(db.cleanupPublicCommandWALCheckpoint)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("retry Checkpoint: %v", err)
	}
	if got := db.backend.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN after retry=%d, want 2", got)
	}
}

func TestPublicCommandWALCheckpointDefaultCutoverAdmitsPostFrontierWriteSync(t *testing.T) {
	db, err := Open(commandWALDurabilityProofOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.SetSync([]byte("default-cutover/pre"), []byte("pre-value")); err != nil {
		t.Fatalf("pre-cut SetSync: %v", err)
	}

	publishEntered := make(chan struct{})
	releasePublish := make(chan struct{})
	var publishOnce sync.Once
	db.cached.SetCommandWALCheckpointPublishHook(func(syncWrite bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		publishOnce.Do(func() { close(publishEntered) })
		if err := waitForPublicCommandWALCheckpointPhase(releasePublish, "default cutover publish release"); err != nil {
			return 0, nil, err
		}
		return db.preparePublicCommandWALPendingPublish(syncWrite)
	})
	before := db.Stats()
	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- db.cached.Checkpoint() }()
	if err := waitForPublicCommandWALCheckpointPhase(publishEntered, "default cutover publish hook"); err != nil {
		t.Fatal(err)
	}
	if got := db.cached.Stats()["treedb.cache.checkpoint.post_frontier_admission.active"]; got != "true" {
		t.Fatalf("post-frontier admission active=%q, want true", got)
	}
	if err := db.SetSync([]byte("default-cutover/post"), []byte("post-value")); err != nil {
		t.Fatalf("post-cut SetSync: %v", err)
	}
	close(releasePublish)
	if err := <-checkpointDone; err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	after := db.Stats()
	requirePublicStatDelta(t, before, after, "treedb.cache.write.post_frontier_admission.count_total", 1)
}

func TestHelperPublicCommandWALCheckpointPostFrontierCrashWriter(t *testing.T) {
	if os.Getenv("TREEDB_CHECKPOINT_FRONTIER_CRASH_HELPER") != "1" {
		t.Skip("helper")
	}
	dir := os.Getenv("TREEDB_CHECKPOINT_FRONTIER_CRASH_DIR")
	if dir == "" {
		t.Fatal("missing TREEDB_CHECKPOINT_FRONTIER_CRASH_DIR")
	}

	opts := commandWALDurabilityProofOptions(dir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	preValue := bytes.Repeat([]byte("a"), 64<<10)
	postValue := bytes.Repeat([]byte("b"), 64<<10)
	if err := db.SetSync([]byte("frontier/pre-cut"), preValue); err != nil {
		t.Fatalf("pre-cut SetSync: %v", err)
	}
	segmentsBefore := publicCommandWALSegmentNames(t, dir)
	if len(segmentsBefore) == 0 {
		t.Fatal("missing pre-cut command-WAL segment")
	}

	publishEntered := make(chan struct{})
	releasePublish := make(chan struct{})
	cleanupComplete := make(chan struct{})
	var publishOnce, cleanupOnce sync.Once
	db.cached.SetCommandWALCheckpointCutoverHook(db.snapshotPublicCommandWALCheckpointCutover)
	db.cached.SetCommandWALCheckpointPublishHook(func(syncWrite bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		publishOnce.Do(func() { close(publishEntered) })
		if err := waitForPublicCommandWALCheckpointPhase(releasePublish, "crash helper publish release"); err != nil {
			return 0, nil, err
		}
		return db.preparePublicCommandWALPendingPublish(syncWrite)
	})
	db.cached.SetCommandWALCheckpointCleanupHook(func(syncWrite bool) error {
		err := db.cleanupPublicCommandWALCheckpoint(syncWrite)
		if err == nil {
			cleanupOnce.Do(func() { close(cleanupComplete) })
		}
		return err
	})

	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- db.Checkpoint() }()
	if err := waitForPublicCommandWALCheckpointPhase(publishEntered, "crash helper checkpoint publish"); err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync([]byte("frontier/post-cut"), postValue); err != nil {
		t.Fatalf("post-cut SetSync while checkpoint publish latched: %v", err)
	}
	select {
	case err := <-checkpointDone:
		t.Fatalf("checkpoint completed while publish remained latched: %v", err)
	default:
	}
	close(releasePublish)
	if err := <-checkpointDone; err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := waitForPublicCommandWALCheckpointPhase(cleanupComplete, "crash helper checkpoint cleanup"); err != nil {
		t.Fatal(err)
	}
	if got := db.backend.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want pre-cut mutation LSN 1", got)
	}
	if first, last := db.publicCommandWALPendingRange(); first != 2 || last != 2 {
		t.Fatalf("pending command-WAL range=(%d,%d), want post-cut mutation range (2,2)", first, last)
	}
	segmentsAfter := publicCommandWALSegmentNames(t, dir)
	if len(segmentsAfter) == 0 {
		t.Fatalf("checkpoint cleanup removed the post-cut command-WAL generation; before=%v", segmentsBefore)
	}
	// The older durable slot may still need the pre-cut command generation.
	// Retaining covered segments here is therefore valid; reopen must tolerate
	// the duplicate covered prefix and replay the post-cut generation.

	// Simulate a process crash after cleanup without the close-time checkpoint.
	// The only durable owner of the post-cut generation is its fresh command-WAL
	// segment (plus the synced value-log external reference).
	os.Exit(0)
}

func TestPublicCommandWALCheckpointPostFrontierGenerationSurvivesCrashReopen(t *testing.T) {
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.Command(exe, "-test.run=^TestHelperPublicCommandWALCheckpointPostFrontierCrashWriter$", "-test.v")
	cmd.Env = append(os.Environ(),
		"TREEDB_CHECKPOINT_FRONTIER_CRASH_HELPER=1",
		"TREEDB_CHECKPOINT_FRONTIER_CRASH_DIR="+dir,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("frontier crash helper failed: %v\n%s", err, out)
	}

	opts := commandWALDurabilityProofOptions(dir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	reopen, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen after post-frontier crash: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	preValue := bytes.Repeat([]byte("a"), 64<<10)
	postValue := bytes.Repeat([]byte("b"), 64<<10)
	requireRawKVValue(t, reopen, []byte("frontier/pre-cut"), preValue)
	requireRawKVValue(t, reopen, []byte("frontier/post-cut"), postValue)
	if got := reopen.backend.State().AppliedCommandLSN; got < 2 {
		t.Fatalf("reopened AppliedCommandLSN=%d, want post-cut mutation LSN 2 replayed", got)
	}
	if _, err := reopen.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC after post-frontier replay: %v", err)
	}
	requireRawKVValue(t, reopen, []byte("frontier/pre-cut"), preValue)
	requireRawKVValue(t, reopen, []byte("frontier/post-cut"), postValue)
}

// triggerAutoCheckpointForTest is test-only coordination for an already-running
// auto-checkpoint loop. It does not expose a production control surface.
func (db *DB) triggerAutoCheckpointForTest() {
	db.cached.TriggerAutoCheckpoint()
}

const publicCommandWALCheckpointTestTimeout = 10 * time.Second

func waitForPublicCommandWALCheckpointPhase(ch <-chan struct{}, phase string) error {
	timer := time.NewTimer(publicCommandWALCheckpointTestTimeout)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-timer.C:
		return fmt.Errorf("timed out waiting for %s", phase)
	}
}

func waitForPublicCommandWALAutoCheckpointCount(db *DB, want uint64) error {
	timer := time.NewTimer(publicCommandWALCheckpointTestTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		stats := db.cached.Stats()
		got, err := strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
		if err != nil {
			return fmt.Errorf("parse auto-checkpoint count %q: %w", stats["treedb.cache.auto_checkpoint.count"], err)
		}
		if got == want {
			return nil
		}
		if got > want {
			return fmt.Errorf("auto-checkpoint count=%d, want %d", got, want)
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			return fmt.Errorf("timed out waiting for auto-checkpoint count=%d (last=%d)", want, got)
		}
	}
}

func waitForPublicCommandWALCheckpointWriterWaiters(db *DB, want uint64) error {
	timer := time.NewTimer(publicCommandWALCheckpointTestTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		stats := db.Stats()
		got, err := strconv.ParseUint(stats["treedb.cache.write.wait_for_checkpoint.active"], 10, 64)
		if err != nil {
			return fmt.Errorf("parse checkpoint writer waiters %q: %w", stats["treedb.cache.write.wait_for_checkpoint.active"], err)
		}
		if got == want {
			return nil
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			return fmt.Errorf("timed out waiting for checkpoint writer waiters=%d (last=%d)", want, got)
		}
	}
}

func requireBenchmarkStatDelta(b *testing.B, before, after map[string]string, key string, want uint64) {
	b.Helper()
	got := statMapUint64B(b, after, key) - statMapUint64B(b, before, key)
	if got != want {
		b.Fatalf("%s delta=%d, want %d", key, got, want)
	}
}

func statMapUint64B(b *testing.B, stats map[string]string, key string) uint64 {
	b.Helper()
	got, err := strconv.ParseUint(stats[key], 10, 64)
	if err != nil {
		b.Fatalf("parse %s=%q: %v", key, stats[key], err)
	}
	return got
}

func reportWriteSyncLatencyDistribution(b *testing.B, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	for _, percentile := range []struct {
		name string
		pct  int
	}{{"p50", 50}, {"p95", 95}, {"p99", 99}, {"max", 100}} {
		idx := len(samples) - 1
		if percentile.pct < 100 {
			idx = (len(samples)*percentile.pct - 1) / 100
		}
		b.ReportMetric(float64(samples[idx].Nanoseconds()), "writesync_"+percentile.name+"_ns")
	}
}

func reportCommandWALBenchmarkDeltas(b *testing.B, before, after map[string]string, operations uint64) {
	b.Helper()
	for _, key := range []string{
		"treedb.command_wal.append.count_total",
		"treedb.command_wal.flush.count_total",
		"treedb.command_wal.sync.count_total",
		"treedb.command_wal.write.syscalls_total",
		"treedb.command_wal.write.bytes_total",
		"treedb.command_wal.write.ns_total",
		"treedb.command_wal.file_sync.calls_total",
		"treedb.command_wal.file_sync.ns_total",
		"treedb.command_wal.directory_sync.calls_total",
		"treedb.command_wal.directory_sync.ns_total",
		"treedb.command_wal.append.ns_total",
		"treedb.command_wal.flush.ns_total",
		"treedb.command_wal.sync.ns_total",
		"treedb.cache.value_log.sync.calls_total",
		"treedb.cache.value_log.sync.ns_total",
		"treedb.cache.value_log.sync.wait_ns_total",
		"treedb.cache.value_log.sync.materialization.calls_total",
		"treedb.cache.value_log.sync.materialization.ns_total",
		"treedb.cache.value_log.sync.materialization.wait_ns_total",
		"treedb.cache.value_log.sync.external_ref.calls_total",
		"treedb.cache.value_log.sync.external_ref.ns_total",
		"treedb.cache.value_log.sync.external_ref.wait_ns_total",
		"treedb.cache.value_log.sync.pending_barrier.calls_total",
		"treedb.cache.value_log.sync.pending_barrier.ns_total",
		"treedb.cache.value_log.sync.pending_barrier.wait_ns_total",
		"treedb.cache.value_log.file_sync.calls_total",
		"treedb.cache.value_log.file_sync.ns_total",
		"treedb.cache.value_log.file_sync.rotated_segment.calls_total",
		"treedb.cache.value_log.file_sync.rotated_segment.ns_total",
		"treedb.cache.vlog_io.bytes",
		"treedb.public.batch.write_sync.phase.wall.ns_total",
		"treedb.public.batch.write_sync.phase.checkpoint_gate.ns_total",
		"treedb.public.batch.write_sync.phase.preflight_materialization.ns_total",
		"treedb.public.batch.write_sync.phase.command_callback.ns_total",
		"treedb.public.batch.write_sync.phase.memtable_publication_reset.ns_total",
		"treedb.public.batch.write_sync.phase.residual.ns_total",
		"treedb.public.batch.write_sync.phase.command_external_ref_ordering.ns_total",
		"treedb.public.batch.write_sync.phase.command_append.ns_total",
		"treedb.public.batch.write_sync.phase.command_sync.ns_total",
		"treedb.public.batch.write_sync.phase.command_empty_barrier.ns_total",
		"treedb.cache.checkpoint.runs",
		"treedb.cache.checkpoint.barrier_wait_ns_total",
		"treedb.cache.checkpoint.stage.cutover.total_ns",
		"treedb.cache.checkpoint.stage.wal_rotate.total_ns",
		"treedb.cache.checkpoint.stage.value_log_flush.total_ns",
		"treedb.cache.checkpoint.stage.command_wal_publish.total_ns",
		"treedb.cache.checkpoint.stage.flush_all.total_ns",
		"treedb.cache.checkpoint.stage.backend_boundary.total_ns",
		"treedb.cache.checkpoint.stage.wal_cleanup.total_ns",
	} {
		delta := statMapUint64B(b, after, key) - statMapUint64B(b, before, key)
		name := strings.TrimPrefix(strings.TrimSuffix(key, "_total"), "treedb.")
		b.ReportMetric(float64(delta)/float64(operations), name+"/op")
	}
}

func assertPublicCommandWALFramesB(b *testing.B, db *DB, minFrames uint64) {
	b.Helper()
	stats := db.Stats()
	frames, err := strconv.ParseUint(stats["treedb.command_wal.frames"], 10, 64)
	if err != nil {
		b.Fatalf("parse command_wal.frames=%q: %v", stats["treedb.command_wal.frames"], err)
	}
	if frames < minFrames {
		b.Fatalf("command_wal.frames=%d, want at least %d", frames, minFrames)
	}
}

func TestPublicCommandWALDeleteRangeCachedReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                          dir,
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"d", "vd"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	before := publicCommandWALFrameCount(t, db)
	if err := db.DeleteRange([]byte("b"), []byte("d")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if got := publicCommandWALFrameCount(t, db); got != before+1 {
		t.Fatalf("command_wal.frames=%d, want %d after directly synced DeleteRange mutation", got, before+1)
	}
	for _, key := range []string{"b", "c"} {
		has, err := db.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil before reopen", key, has, err)
		}
	}
	requireRawKVValue(t, db, []byte("a"), []byte("va"))
	requireRawKVValue(t, db, []byte("d"), []byte("vd"))

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true, DisableSideStores: true, BackgroundCheckpointInterval: -1})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopen.Close()
	for _, key := range []string{"b", "c"} {
		has, err := reopen.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil after replay", key, has, err)
		}
	}
	requireRawKVValue(t, reopen, []byte("a"), []byte("va"))
	requireRawKVValue(t, reopen, []byte("d"), []byte("vd"))
	if got := reopen.backend.State().AppliedCommandLSN; got < before+1 {
		t.Fatalf("AppliedCommandLSN=%d, want at least %d after reopen", got, before+1)
	}
}

func TestPublicCommandWALDeleteRangeReplaysUnappliedFrame(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileCommandWALDurable, dir)
	opts.CommandWALStatsScan = true
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"d", "vd"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			_ = db.Close()
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	baseApplied := db.backend.State().AppliedCommandLSN
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		CommandWAL:             true,
		Durability:             backenddb.DurabilityDurable,
		ResolvedProfile:        backenddb.ProfileCommandWALDurable,
		DisableBackgroundPrune: true,
		ValueLog: backenddb.ValueLogOptions{
			ReadIntegrity: backenddb.IntegrityVerify,
		},
	})
	if err != nil {
		t.Fatalf("backend Open for manual command append: %v", err)
	}
	var payload commitlog.RawKVBatchPayloadBuilder
	_ = payload.ResetWithHint(1, len("b")+len("d"))
	if _, err := payload.AppendDeleteRange([]byte("b"), []byte("d")); err != nil {
		_ = backend.Close()
		t.Fatalf("AppendDeleteRange payload: %v", err)
	}
	lsn, err := backend.AppendRawKVBatchPayloadCommandWALTrusted(payload.Payload(), true)
	if err != nil {
		_ = backend.Close()
		t.Fatalf("AppendRawKVBatchPayloadCommandWALTrusted DeleteRange: %v", err)
	}
	if lsn <= baseApplied {
		_ = backend.Close()
		t.Fatalf("DeleteRange frame lsn=%d, want > base applied %d", lsn, baseApplied)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("backend Close after manual command append: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true, DisableSideStores: true, BackgroundCheckpointInterval: -1})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopen.Close()
	for _, key := range []string{"b", "c"} {
		has, err := reopen.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil after replay", key, has, err)
		}
	}
	requireRawKVValue(t, reopen, []byte("a"), []byte("va"))
	requireRawKVValue(t, reopen, []byte("d"), []byte("vd"))
	if got := reopen.backend.State().AppliedCommandLSN; got < lsn {
		t.Fatalf("AppliedCommandLSN=%d, want at least replayed lsn %d", got, lsn)
	}
}

func TestPublicCommandWALDeleteRangeBoundsNoopFramesAndCheckpointLSN(t *testing.T) {
	db, err := Open(Options{
		Dir:                          t.TempDir(),
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"z", "vz"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	baseFrames := publicCommandWALFrameCount(t, db)
	for _, bounds := range []struct{ start, end []byte }{
		{start: []byte("c"), end: []byte("c")},
		{start: []byte("z"), end: []byte("a")},
		{start: nil, end: []byte{}},
	} {
		if err := db.DeleteRange(bounds.start, bounds.end); err != nil {
			t.Fatalf("noop DeleteRange(%q,%q): %v", bounds.start, bounds.end, err)
		}
		if got := publicCommandWALFrameCount(t, db); got != baseFrames {
			t.Fatalf("noop DeleteRange(%q,%q) frames=%d, want unchanged %d", bounds.start, bounds.end, got, baseFrames)
		}
	}

	if err := db.DeleteRange(nil, []byte("b")); err != nil {
		t.Fatalf("DeleteRange nil,b: %v", err)
	}
	if got := publicCommandWALFrameCount(t, db); got != baseFrames+1 {
		t.Fatalf("frames after lower-unbounded DeleteRange=%d, want %d", got, baseFrames+1)
	}
	for _, key := range []string{"a"} {
		has, err := db.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil", key, has, err)
		}
	}
	requireRawKVValue(t, db, []byte("b"), []byte("vb"))
	requireRawKVValue(t, db, []byte("c"), []byte("vc"))
	requireRawKVValue(t, db, []byte("z"), []byte("vz"))

	if err := db.DeleteRange([]byte("z"), nil); err != nil {
		t.Fatalf("DeleteRange z,nil: %v", err)
	}
	if got := publicCommandWALFrameCount(t, db); got != baseFrames+2 {
		t.Fatalf("frames after upper-unbounded DeleteRange=%d, want %d", got, baseFrames+2)
	}
	has, err := db.Has([]byte("z"))
	if err != nil || has {
		t.Fatalf("Has(z)=(%t,%v), want false,nil", has, err)
	}

	first, last := db.publicCommandWALPendingRange()
	if first == 0 || last != baseFrames+2 {
		t.Fatalf("pending command WAL range=(%d,%d), want non-empty ending at %d", first, last, baseFrames+2)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := db.backend.State().AppliedCommandLSN; got != baseFrames+2 {
		t.Fatalf("AppliedCommandLSN=%d, want %d", got, baseFrames+2)
	}
}

func TestPublicCommandWALDeleteRangeFullRangeInMemoryCheckpoint(t *testing.T) {
	db, err := Open(Options{
		Dir:                          t.TempDir(),
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	before := publicCommandWALFrameCount(t, db)
	if err := db.DeleteRange(nil, nil); err != nil {
		t.Fatalf("DeleteRange nil,nil: %v", err)
	}
	if got := publicCommandWALFrameCount(t, db); got != before+1 {
		t.Fatalf("frames after full-range DeleteRange=%d, want %d", got, before+1)
	}
	for _, key := range []string{"a", "b"} {
		has, err := db.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil after full range", key, has, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := db.backend.State().AppliedCommandLSN; got != before+1 {
		t.Fatalf("AppliedCommandLSN=%d, want %d", got, before+1)
	}
}

func TestPublicCommandWALDeleteRangeSnapshotIsolation(t *testing.T) {
	db, err := Open(Options{
		Dir:                          t.TempDir(),
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"d", "vd"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	if err := db.DeleteRange([]byte("b"), []byte("d")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	for _, key := range []string{"b", "c"} {
		has, err := snap.Has([]byte(key))
		if err != nil || !has {
			t.Fatalf("snapshot Has(%s)=(%t,%v), want true,nil", key, has, err)
		}
		has, err = db.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("db Has(%s)=(%t,%v), want false,nil after DeleteRange", key, has, err)
		}
	}
	requireRawKVValue(t, db, []byte("a"), []byte("va"))
	requireRawKVValue(t, db, []byte("d"), []byte("vd"))
}

func TestPublicCommandWALDeleteRangeValueLogPointersReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                          dir,
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	}
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	for _, kv := range []struct{ k, v string }{{"a", "left-pointer-value"}, {"b", "deleted-pointer-value"}, {"c", "right-pointer-value"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			_ = db.Close()
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	if err := db.DeleteRange([]byte("b"), []byte("c")); err != nil {
		_ = db.Close()
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.range_span.spans_flushed_total"]; got != "1" {
		_ = db.Close()
		t.Fatalf("range spans flushed=%s want 1", got)
	}
	if got, want := stats["treedb.command_wal.live_covered_max_lsn"], stats["treedb.command_wal.live_accepted_max_lsn"]; got != want {
		_ = db.Close()
		t.Fatalf("covered command WAL LSN=%s want accepted max %s", got, want)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true, DisableSideStores: true, BackgroundCheckpointInterval: -1})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopen.Close()
	requireRawKVValue(t, reopen, []byte("a"), []byte("left-pointer-value"))
	requireRawKVValue(t, reopen, []byte("c"), []byte("right-pointer-value"))
	if _, err := reopen.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC after range checkpoint: %v", err)
	}
	requireRawKVValue(t, reopen, []byte("a"), []byte("left-pointer-value"))
	requireRawKVValue(t, reopen, []byte("c"), []byte("right-pointer-value"))
	has, err := reopen.Has([]byte("b"))
	if err != nil || has {
		t.Fatalf("Has(b)=(%t,%v), want false,nil", has, err)
	}
}

func TestPublicCommandWALBatchValueLogPointersStayBelowFrameCap(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                          dir,
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
		WALMaxSegmentBytes:           4096,
	}
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}

	values := make(map[string][]byte)
	b := db.NewBatch()
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("large-%02d", i)
		value := bytes.Repeat([]byte{byte('a' + i)}, 2048)
		values[key] = value
		if err := b.Set([]byte(key), value); err != nil {
			_ = b.Close()
			_ = db.Close()
			t.Fatalf("batch Set %s: %v", key, err)
		}
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write should encode pointer ops as SetRID below the command frame cap: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	assertPublicCommandWALFrames(t, db, 1)
	if frames := publicCommandWALFrameCount(t, db); frames != 1 {
		_ = db.Close()
		t.Fatalf("command_wal.frames=%d, want one directly synced mutation", frames)
	}
	r, err := commitlog.NewReader(filepath.Join(backenddb.WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewReader fallback command frame: %v", err)
	}
	env, err := r.ReadCommandFrame()
	if err != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("ReadCommandFrame fallback command frame: %v", err)
	}
	if env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("fallback payload format=%d, want RawKVBatchV1", env.PayloadFormat)
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(env.Payload)
	if err != nil {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("DecodeRawKVBatchPayload fallback command frame: %v", err)
	}
	for i := range ops {
		if ops[i].Op != commitlog.RawKVOpSetRID || ops[i].RID == 0 || ops[i].Value != nil {
			_ = r.Close()
			_ = db.Close()
			t.Fatalf("fallback command op[%d]=%+v, want SetRID", i, ops[i])
		}
	}
	if len(ops) != len(values) {
		_ = r.Close()
		_ = db.Close()
		t.Fatalf("fallback command op count=%d, want %d", len(ops), len(values))
	}
	if err := r.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("Close fallback command reader: %v", err)
	}
	for key, value := range values {
		requireRawKVValue(t, db, []byte(key), value)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true, DisableSideStores: true, BackgroundCheckpointInterval: -1})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopen.Close()
	for key, value := range values {
		requireRawKVValue(t, reopen, []byte(key), value)
	}
}

func TestPublicCommandWALBatchValueLogPointersFlushMultiLaneRefs(t *testing.T) {
	dir := t.TempDir()
	opts := OptionsFor(ProfileCommandWALRelaxed, dir)
	opts.CommandWALStatsScan = true
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.FlushThreshold = 1 << 30
	opts.WALMaxSegmentBytes = 1 << 20
	opts.JournalLanes = 4
	opts.MemtableShards = 16
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	const entries = 1100
	b := db.NewBatchWithSize(entries)
	for i := 0; i < entries; i++ {
		key := []byte(fmt.Sprintf("multi-lane-%06d", i))
		value := bytes.Repeat([]byte{byte(i%251 + 1)}, 2048)
		if err := b.Set(key, value); err != nil {
			_ = b.Close()
			t.Fatalf("batch Set %d: %v", i, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}

	lanes := publicCommandWALValueLogLanes(t, dir)
	if len(lanes) < 2 {
		t.Fatalf("value-log lanes used=%v, want at least 2 to cover multi-lane SetRID flushing", lanes)
	}
	for _, i := range []int{0, entries / 2, entries - 1} {
		key := []byte(fmt.Sprintf("multi-lane-%06d", i))
		want := bytes.Repeat([]byte{byte(i%251 + 1)}, 2048)
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get %q: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get %q len=%d, want len=%d", key, len(got), len(want))
		}
	}
}

func publicCommandWALValueLogLanes(t *testing.T, dir string) map[int]struct{} {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "value_vlog", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob value-log segments: %v", err)
	}
	lanes := make(map[int]struct{})
	for _, path := range paths {
		var lane, seq int
		if n, _ := fmt.Sscanf(filepath.Base(path), "value-l%d-%d.log", &lane, &seq); n == 2 {
			lanes[lane] = struct{}{}
		}
	}
	return lanes
}

func TestPublicCommandWALBatchDeleteRangeWithPointerUsesCompactRangeFrame(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                          dir,
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
		WALMaxSegmentBytes:           4096,
	}
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	for i := 0; i < 400; i++ {
		key := []byte(fmt.Sprintf("range-%04d", i))
		if err := db.Set(key, []byte("seed")); err != nil {
			_ = db.Close()
			t.Fatalf("seed Set %q: %v", key, err)
		}
	}
	before := publicCommandWALFrameCount(t, db)

	b := db.NewBatch()
	if err := b.DeleteRange([]byte("range-"), []byte("range-\xff")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch DeleteRange: %v", err)
	}
	wantValue := bytes.Repeat([]byte("z"), 2048)
	if err := b.Set([]byte("z-pointer"), wantValue); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write should log compact DeleteRange instead of materialized point deletes: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	if frames := publicCommandWALFrameCount(t, db); frames != before+1 {
		_ = db.Close()
		t.Fatalf("command_wal.frames=%d want %d", frames, before+1)
	}
	has, err := db.Has([]byte("range-0000"))
	if err != nil || has {
		_ = db.Close()
		t.Fatalf("Has(range-0000)=(%t,%v), want false,nil", has, err)
	}
	requireRawKVValue(t, db, []byte("z-pointer"), wantValue)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPublicCommandWALBatchDeleteRangeCachedReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, Durability: DurabilityWALOnRelaxed, CommandWAL: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"d", "vd"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	b := db.NewBatch()
	if err := b.DeleteRange([]byte("a"), []byte("d")); err != nil {
		t.Fatalf("batch DeleteRange: %v", err)
	}
	if err := b.Set([]byte("b"), []byte("after")); err != nil {
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("batch WriteSync: %v", err)
	}
	_ = b.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopen.Close()
	for _, key := range []string{"a", "c"} {
		has, err := reopen.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil", key, has, err)
		}
	}
	got, err := reopen.Get([]byte("b"))
	if err != nil || string(got) != "after" {
		t.Fatalf("Get(b)=(%q,%v), want after,nil", got, err)
	}
	got, err = reopen.Get([]byte("d"))
	if err != nil || string(got) != "vd" {
		t.Fatalf("Get(d)=(%q,%v), want vd,nil", got, err)
	}
}

func TestPublicCommandWALCloseRejectsLateSetSyncWithErrClosed(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	if err := db.Set([]byte("seed"), []byte("v1")); err != nil {
		_ = db.Close()
		t.Fatalf("Set(seed): %v", err)
	}

	started := make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	testDuringPublicCloseAfterCheckpoint = func() {
		once.Do(func() {
			go func() {
				close(started)
				done <- db.SetSync([]byte("late-shutdown"), []byte("v2"))
			}()
			<-started
			select {
			case err := <-done:
				t.Fatalf("late SetSync completed before Close released lifecycle lock: %v", err)
			default:
			}
		})
	}
	defer func() { testDuringPublicCloseAfterCheckpoint = nil }()

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	err = <-done
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("late SetSync error=%v, want ErrClosed", err)
	}
	if err != nil && strings.Contains(err.Error(), "command wal journal unavailable") {
		t.Fatalf("late SetSync leaked command journal shutdown error: %v", err)
	}
}

func TestPublicCommandWALBatchWriteAfterCloseReturnsErrClosed(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	b := db.NewBatch()
	if b == nil {
		_ = db.Close()
		t.Fatalf("NewBatch returned nil")
	}
	if err := b.Set([]byte("late-batch"), []byte("v1")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := db.Close(); err != nil {
		_ = b.Close()
		t.Fatalf("Close: %v", err)
	}
	err = b.WriteSync()
	if !errors.Is(err, ErrClosed) {
		_ = b.Close()
		t.Fatalf("batch WriteSync after close error=%v, want ErrClosed", err)
	}
	if err != nil && strings.Contains(err.Error(), "command wal journal unavailable") {
		_ = b.Close()
		t.Fatalf("batch WriteSync leaked command journal shutdown error: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}
}

func TestPublicCommandWALBatchPayloadPoolAcquisitionKeepsClosedHandlesIsolated(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	var seededPayload commitlog.RawKVBatchPayloadBuilder
	if err := seededPayload.ResetWithHint(0, 0); err != nil {
		t.Fatalf("seeded ResetWithHint: %v", err)
	}
	if _, _, err := seededPayload.AppendSet([]byte("seed"), bytes.Repeat([]byte{7}, 128)); err != nil {
		t.Fatalf("seeded AppendSet: %v", err)
	}
	seededRetained := seededPayload.RetainedCap()
	db.commandWALPublicPayloadPool.New = func() any {
		return seededPayload
	}

	const entries = 128
	value := bytes.Repeat([]byte{7}, 128)
	first, ok := db.NewBatchWithSize(entries).(*commandWALPublicBatch)
	if !ok {
		t.Fatalf("NewBatchWithSize type=%T, want *commandWALPublicBatch", first)
	}
	if got := first.payload.RetainedCap(); got != seededRetained {
		_ = first.Close()
		t.Fatalf("first payload retained cap=%d, want seeded cap %d", got, seededRetained)
	}
	for i := 0; i < entries; i++ {
		key := []byte(fmt.Sprintf("first-%03d", i))
		if err := first.Set(key, value); err != nil {
			_ = first.Close()
			t.Fatalf("first Set %d: %v", i, err)
		}
	}
	retained := first.payload.RetainedCap()
	if retained <= commandWALRawKVBatchHeaderSize {
		_ = first.Close()
		t.Fatalf("first payload retained cap=%d, want payload allocation", retained)
	}
	if err := first.WriteSync(); err != nil {
		_ = first.Close()
		t.Fatalf("first WriteSync: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := first.Set([]byte("stale"), value); !errors.Is(err, ErrClosed) {
		t.Fatalf("first Set after Close error=%v, want ErrClosed", err)
	}

	second, ok := db.NewBatchWithSize(entries).(*commandWALPublicBatch)
	if !ok {
		t.Fatalf("second NewBatchWithSize type=%T, want *commandWALPublicBatch", second)
	}
	if err := second.Set([]byte("second"), []byte("value")); err != nil {
		_ = second.Close()
		t.Fatalf("second Set: %v", err)
	}
	if err := second.WriteSync(); err != nil {
		_ = second.Close()
		t.Fatalf("second WriteSync: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	got, err := db.Get([]byte("second"))
	if err != nil {
		t.Fatalf("Get(second): %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("Get(second)=%q, want value", got)
	}
	if err := first.WriteSync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("first WriteSync after second batch error=%v, want ErrClosed", err)
	}
}

func TestPublicCommandWALBatchPayloadPoolDropsOversizeBuffer(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	oversizeHint := commandWALPublicBatchPayloadPoolRetainMaxBytes/commandWALPublicBatchEstimatedKeyValueBytes + 128
	large, ok := db.NewBatchWithSize(oversizeHint).(*commandWALPublicBatch)
	if !ok {
		t.Fatalf("NewBatchWithSize type=%T, want *commandWALPublicBatch", large)
	}
	if err := large.Set([]byte("large"), bytes.Repeat([]byte{9}, 128)); err != nil {
		_ = large.Close()
		t.Fatalf("large Set: %v", err)
	}
	if got := large.payload.RetainedCap(); got <= commandWALPublicBatchPayloadPoolRetainMaxBytes {
		_ = large.Close()
		t.Fatalf("large payload retained cap=%d, want > pool retain max %d", got, commandWALPublicBatchPayloadPoolRetainMaxBytes)
	}
	if err := large.WriteSync(); err != nil {
		_ = large.Close()
		t.Fatalf("large WriteSync: %v", err)
	}
	if err := large.Close(); err != nil {
		t.Fatalf("large Close: %v", err)
	}

	small, ok := db.NewBatchWithSize(1).(*commandWALPublicBatch)
	if !ok {
		t.Fatalf("small NewBatchWithSize type=%T, want *commandWALPublicBatch", small)
	}
	defer func() { _ = small.Close() }()
	if got := small.payload.RetainedCap(); got > commandWALPublicBatchPayloadPoolRetainMaxBytes {
		t.Fatalf("small payload retained cap=%d, oversized buffer leaked through pool", got)
	}
}
