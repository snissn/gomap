package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func relaxedCommandWALDurablePrefixOptions(dir string) Options {
	opts := commandWALDurabilityProofOptions(dir)
	ApplyProfile(&opts, ProfileCommandWALRelaxed)
	return opts
}

func scanPublicCommandWALV2(t *testing.T, dir string) []commitlog.CommandEnvelope {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(dir), "commit-l*.log"))
	if err != nil {
		t.Fatalf("glob command WAL: %v", err)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		t.Fatal("no command WAL segments")
	}
	var frames []commitlog.CommandEnvelope
	for _, path := range paths {
		segmentFrames, err := commitlog.ScanCommandFramesV2(path, commitlog.Options{})
		if err != nil {
			t.Fatalf("scan strict V2 command WAL %q: %v", path, err)
		}
		frames = append(frames, segmentFrames...)
	}
	sort.Slice(frames, func(i, j int) bool { return frames[i].LSN < frames[j].LSN })
	return frames
}

func writeEmptyPublicCommandWALSync(db *DB) error {
	b := db.NewBatch()
	writeErr := b.WriteSync()
	return errors.Join(writeErr, b.Close())
}

func TestPublicCommandWALRelaxedExplicitSyncPersistsGroupedDurablePrefix(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(relaxedCommandWALDurablePrefixOptions(dir))
	if err != nil {
		t.Fatalf("Open relaxed command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	// This test certifies the grouped durable-prefix representation rather than
	// the production solo-sync optimization.
	db.commandWALGroupCommit.testBeforeSync = func(int) {}

	if err := db.SetSync([]byte("durable"), []byte("value")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	frames := scanPublicCommandWALV2(t, dir)
	if len(frames) != 2 {
		t.Fatalf("frames=%d, want relaxed mutation plus grouped durable-prefix barrier: %+v", len(frames), frames)
	}
	if got := frames[0].DurabilityClass; got != commitlog.CommandDurabilityRelaxed {
		t.Fatalf("mutation durability class=%v, want relaxed group member", got)
	}
	if got := frames[0].Kind; got != commitlog.CommandKindRawKVBatch {
		t.Fatalf("kind=%v, want RawKVBatch", got)
	}
	if got := frames[1].DurabilityClass; got != commitlog.CommandDurabilityDurable {
		t.Fatalf("barrier durability class=%v, want durable", got)
	}
	if got := frames[1].Kind; got != commitlog.CommandKindDurablePrefixBarrier {
		t.Fatalf("barrier kind=%v, want DurablePrefixBarrier", got)
	}
}

func TestPublicCommandWALRelaxedEmptyWriteSyncPersistsDurablePrefixBarrier(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(relaxedCommandWALDurablePrefixOptions(dir))
	if err != nil {
		t.Fatalf("Open relaxed command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("relaxed"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	b := db.NewBatch()
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("empty WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("empty batch Close: %v", err)
	}

	frames := scanPublicCommandWALV2(t, dir)
	if len(frames) != 2 {
		t.Fatalf("frames=%d, want relaxed mutation plus durable barrier: %+v", len(frames), frames)
	}
	if got := frames[0].DurabilityClass; got != commitlog.CommandDurabilityRelaxed {
		t.Fatalf("mutation durability class=%v, want relaxed", got)
	}
	barrier := frames[1]
	if got := barrier.DurabilityClass; got != commitlog.CommandDurabilityDurable {
		t.Fatalf("barrier durability class=%v, want durable", got)
	}
	if got := barrier.Kind; got != commitlog.CommandKindDurablePrefixBarrier {
		t.Fatalf("barrier kind=%v, want DurablePrefixBarrier", got)
	}
	if got := barrier.PayloadFormat; got != commitlog.PayloadFormatDurablePrefixBarrierV1 {
		t.Fatalf("barrier payload format=%v, want DurablePrefixBarrierV1", got)
	}
	if barrier.LSN != frames[0].LSN+1 {
		t.Fatalf("barrier LSN=%d, want mutation LSN+1=%d", barrier.LSN, frames[0].LSN+1)
	}
}

func TestPublicCommandWALRelaxedEmptyWriteSyncPromotedBarrierRemainsContiguous(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(relaxedCommandWALDurablePrefixOptions(dir))
	if err != nil {
		t.Fatalf("Open relaxed command WAL: %v", err)
	}

	if err := db.Set([]byte("before"), []byte("value-before")); err != nil {
		_ = db.Close()
		t.Fatalf("Set before: %v", err)
	}
	if err := writeEmptyPublicCommandWALSync(db); err != nil {
		_ = db.Close()
		t.Fatalf("empty WriteSync: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint promoted prefix: %v", err)
	}
	if got := db.Stats()["treedb.applied_command_lsn"]; got != "2" {
		_ = db.Close()
		t.Fatalf("applied_command_lsn after promoted-prefix checkpoint=%q, want 2", got)
	}

	if err := db.Set([]byte("after"), []byte("value-after")); err != nil {
		_ = db.Close()
		t.Fatalf("Set after: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint after promoted prefix: %v", err)
	}
	if got := db.Stats()["treedb.applied_command_lsn"]; got != "3" {
		_ = db.Close()
		t.Fatalf("applied_command_lsn after contiguous checkpoint=%q, want 3", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(relaxedCommandWALDurablePrefixOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	for key, want := range map[string]string{
		"before": "value-before",
		"after":  "value-after",
	} {
		got, err := reopen.Get([]byte(key))
		if err != nil {
			t.Fatalf("reopen Get(%q): %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("reopen Get(%q)=%q, want %q", key, got, want)
		}
	}
}

func TestPublicCommandWALRelaxedEmptyWriteSyncDoesNotCreateAppliedLSNGap(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, *DB)
	}{
		{name: "fresh prefix"},
		{
			name: "already durable applied prefix",
			setup: func(t *testing.T, db *DB) {
				t.Helper()
				if err := db.Set([]byte("already-applied"), []byte("value")); err != nil {
					t.Fatalf("Set setup value: %v", err)
				}
				if err := db.Checkpoint(); err != nil {
					t.Fatalf("Checkpoint setup value: %v", err)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(relaxedCommandWALDurablePrefixOptions(dir))
			if err != nil {
				t.Fatalf("Open relaxed command WAL: %v", err)
			}
			defer func() { _ = db.Close() }()
			if tc.setup != nil {
				tc.setup(t, db)
			}

			before := db.Stats()
			beforeNextLSN := statMapUint64(t, before, "treedb.command_wal.next_lsn")
			beforeDurableLSN := statMapUint64(t, before, "treedb.command_wal.durable_wal_lsn")
			b := db.NewBatch()
			if err := b.WriteSync(); err != nil {
				_ = b.Close()
				t.Fatalf("empty WriteSync: %v", err)
			}
			if err := b.Close(); err != nil {
				t.Fatalf("empty batch Close: %v", err)
			}
			after := db.Stats()
			if got := statMapUint64(t, after, "treedb.command_wal.next_lsn"); got != beforeNextLSN {
				t.Fatalf("next LSN after empty sync=%d, want unchanged %d", got, beforeNextLSN)
			}
			if got := statMapUint64(t, after, "treedb.command_wal.durable_wal_lsn"); got != beforeDurableLSN {
				t.Fatalf("durable LSN after empty sync=%d, want unchanged %d", got, beforeDurableLSN)
			}

			if err := db.Set([]byte("after-empty-sync"), []byte("value")); err != nil {
				t.Fatalf("Set after empty sync: %v", err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint after empty sync: %v", err)
			}
		})
	}
}

func TestPublicCommandWALRelaxedPointerDebtStatsCloseOnSetSync(t *testing.T) {
	dir := t.TempDir()
	opts := relaxedCommandWALDurablePrefixOptions(dir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open relaxed command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := db.NewBatch()
	// Keep this above the bounded materialized-RID value limit so the test
	// continues to exercise #3920's external dependency-debt path.
	if err := b.Set([]byte("relaxed-pointer"), bytes.Repeat([]byte("r"), 65<<10)); err != nil {
		_ = b.Close()
		t.Fatalf("relaxed pointer batch Set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("relaxed pointer batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("relaxed pointer batch Close: %v", err)
	}
	stats := db.Stats()
	if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "1" {
		t.Fatalf("pending debt entries=%q, want 1", got)
	}
	kindPrefix := "treedb.command_wal.dependency_debt.kind.command-wal-external-rid."
	if got := stats[kindPrefix+"pending_count"]; got != "1" {
		t.Fatalf("external-RID pending count=%q, want 1 (stats=%#v)", got, stats)
	}
	if got := stats[kindPrefix+"pending_bytes"]; got == "" || got == "0" {
		t.Fatalf("external-RID pending bytes=%q, want non-zero", got)
	}
	if _, ok := stats[kindPrefix+"max_age_ns"]; !ok {
		t.Fatalf("external-RID pending age missing (stats=%#v)", stats)
	}

	if err := db.SetSync([]byte("durable-inline"), []byte("value")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}
	stats = db.Stats()
	if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "2" {
		t.Fatalf("durable_wal_lsn=%q, want directly synced mutation LSN 2", got)
	}
	if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "0" {
		t.Fatalf("pending debt entries=%q, want 0 after SetSync", got)
	}
}

func TestPublicCommandWALRelaxedPointerDebtEmitsExactDependencySyncCuts(t *testing.T) {
	dir := t.TempDir()
	opts := relaxedCommandWALDurablePrefixOptions(dir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open relaxed command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		events = append(events, event)
		return nil
	})
	defer restore()

	b := db.NewBatch()
	// Keep this above the bounded materialized-RID value limit so the test
	// continues to exercise #3920's external dependency-sync path.
	if err := b.Set([]byte("relaxed-pointer"), bytes.Repeat([]byte("r"), 65<<10)); err != nil {
		_ = b.Close()
		t.Fatalf("relaxed pointer batch Set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("relaxed pointer batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("relaxed pointer batch Close: %v", err)
	}

	var relaxedValuePath string
	for _, event := range events {
		switch {
		case event.Resource == durabilitycut.ResourceValueLog && event.Point == durabilitycut.AfterDependencyAppend && event.Path != "":
			relaxedValuePath = event.Path
		case event.Resource == durabilitycut.ResourceValueLog && event.Namespace == durabilitycut.NamespaceCreate && event.NewPath != "":
			relaxedValuePath = event.NewPath
		}
	}
	if relaxedValuePath == "" {
		t.Fatalf("relaxed pointer write emitted no exact value-log append path: %#v", events)
	}
	beforeDurable := len(events)
	if err := db.SetSync([]byte("durable-inline"), []byte("value")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	containsPath := func(event durabilitycut.Event, want string) bool {
		if event.Path == want {
			return true
		}
		for _, path := range event.Paths {
			if path == want {
				return true
			}
		}
		return false
	}
	beforeSync, afterSync, durableAppend := -1, -1, -1
	for i := beforeDurable; i < len(events); i++ {
		event := events[i]
		switch {
		case event.Resource == durabilitycut.ResourceAuxiliary && event.Point == durabilitycut.BeforeDependencyFileSync && containsPath(event, relaxedValuePath):
			beforeSync = i
		case event.Resource == durabilitycut.ResourceAuxiliary && event.Point == durabilitycut.AfterDependencyFileSync && containsPath(event, relaxedValuePath):
			afterSync = i
		case event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyAppend && event.LSN == 2:
			durableAppend = i
		}
	}
	if beforeSync < 0 || afterSync <= beforeSync || durableAppend <= afterSync {
		t.Fatalf("exact dependency sync cuts must bracket the value-log fsync before durable frame append: before=%d after=%d append=%d path=%q events=%#v",
			beforeSync, afterSync, durableAppend, relaxedValuePath, events)
	}
}

func TestPublicCommandWALRelaxedPointerDependencySyncCutsRetainDebtForRetry(t *testing.T) {
	for _, point := range []durabilitycut.Point{
		durabilitycut.BeforeDependencyFileSync,
		durabilitycut.AfterDependencyFileSync,
	} {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := t.TempDir()
			opts := relaxedCommandWALDurablePrefixOptions(dir)
			opts.ValueLog.PointerThreshold = 1
			opts.ValueLog.ForcePointers = true
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("Open relaxed command WAL: %v", err)
			}
			defer func() { _ = db.Close() }()

			b := db.NewBatch()
			// Keep this above the bounded materialized-RID value limit so the test
			// continues to exercise #3920's external dependency retry path.
			if err := b.Set([]byte("relaxed-pointer"), bytes.Repeat([]byte("r"), 65<<10)); err != nil {
				_ = b.Close()
				t.Fatalf("batch Set relaxed pointer: %v", err)
			}
			if err := b.Write(); err != nil {
				_ = b.Close()
				t.Fatalf("batch Write relaxed pointer: %v", err)
			}
			if err := b.Close(); err != nil {
				t.Fatalf("batch Close relaxed pointer: %v", err)
			}
			cutErr := fmt.Errorf("injected pointer dependency cut at %s", point)
			cut := false
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if !cut && event.Resource == durabilitycut.ResourceAuxiliary && event.Point == point {
					cut = true
					return cutErr
				}
				return nil
			})
			err = writeEmptyPublicCommandWALSync(db)
			restore()
			if !errors.Is(err, cutErr) || !cut {
				t.Fatalf("empty WriteSync error=%v cut=%t, want injected %s cut", err, cut, point)
			}
			stats := db.Stats()
			if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "0" {
				t.Fatalf("durable_wal_lsn after pre-append cut=%q, want 0", got)
			}
			if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "1" {
				t.Fatalf("pending debt entries after pre-append cut=%q, want retained 1", got)
			}
			if got := stats["treedb.command_wal.dependency_debt.retries_total"]; got != "1" {
				t.Fatalf("pending debt retries after pre-append cut=%q, want 1", got)
			}
			if frames := scanPublicCommandWALV2(t, dir); len(frames) != 1 {
				t.Fatalf("frames after pre-append cut=%+v, want only relaxed frame", frames)
			}

			if err := writeEmptyPublicCommandWALSync(db); err != nil {
				t.Fatalf("retry empty WriteSync barrier: %v", err)
			}
			stats = db.Stats()
			if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "2" {
				t.Fatalf("durable_wal_lsn after retry=%q, want 2", got)
			}
			if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "0" {
				t.Fatalf("pending debt entries after retry=%q, want 0", got)
			}
		})
	}
}

func TestPublicCommandWALRelaxedRotationsStabilizeExactPrefixBeforeBarrierAppend(t *testing.T) {
	dir := t.TempDir()
	opts := relaxedCommandWALDurablePrefixOptions(dir)
	opts.CommandWALSegmentTargetBytes = 1
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open relaxed command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		events = append(events, event)
		return nil
	})
	defer restore()

	if err := db.Set([]byte("relaxed-a"), []byte("value-a")); err != nil {
		t.Fatalf("Set relaxed-a: %v", err)
	}
	if err := db.Set([]byte("relaxed-b"), []byte("value-b")); err != nil {
		t.Fatalf("Set relaxed-b: %v", err)
	}
	for _, event := range events {
		if event.Resource != durabilitycut.ResourceCommandWAL {
			continue
		}
		if event.Point == durabilitycut.BeforeDependencyFileSync || event.Point == durabilitycut.AfterDependencyFileSync ||
			event.Point == durabilitycut.BeforeNewFileDirectorySync || event.Point == durabilitycut.AfterNewFileDirectorySync {
			t.Fatalf("ordinary relaxed rotation emitted stable sync event before barrier: %#v", event)
		}
	}
	statsBeforeBarrier := db.Stats()
	if got := statsBeforeBarrier["treedb.command_wal.dependency_debt.entries"]; got != "2" {
		t.Fatalf("pending debt entries before barrier=%q, want 2", got)
	}
	rotationKindPrefix := "treedb.command_wal.dependency_debt.kind.command-wal."
	if got := statMapUint64(t, statsBeforeBarrier, rotationKindPrefix+"pending_count"); got == 0 {
		t.Fatalf("command-WAL rotation pending count=%d, want non-zero", got)
	}
	if got := statMapUint64(t, statsBeforeBarrier, rotationKindPrefix+"pending_bytes"); got == 0 {
		t.Fatalf("command-WAL rotation pending bytes=%d, want non-zero", got)
	}
	if _, ok := statsBeforeBarrier[rotationKindPrefix+"max_age_ns"]; !ok {
		t.Fatalf("command-WAL rotation pending age missing (stats=%#v)", statsBeforeBarrier)
	}

	if err := writeEmptyPublicCommandWALSync(db); err != nil {
		t.Fatalf("empty WriteSync barrier: %v", err)
	}

	barrierAppend := -1
	fileSyncBeforeBarrier := false
	namespaceSyncBeforeBarrier := false
	for i, event := range events {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyAppend && event.LSN == 3 {
			barrierAppend = i
			break
		}
	}
	if barrierAppend < 0 {
		t.Fatalf("missing durable barrier append event in %#v", events)
	}
	for _, event := range events[:barrierAppend] {
		if event.Resource != durabilitycut.ResourceCommandWAL {
			continue
		}
		switch event.Point {
		case durabilitycut.AfterDependencyFileSync:
			fileSyncBeforeBarrier = true
		case durabilitycut.AfterNewFileDirectorySync:
			namespaceSyncBeforeBarrier = true
		}
	}
	if !fileSyncBeforeBarrier || !namespaceSyncBeforeBarrier {
		t.Fatalf("barrier append index=%d lacks completed old-file/new-name persistence before append: file=%t namespace=%t events=%#v", barrierAppend, fileSyncBeforeBarrier, namespaceSyncBeforeBarrier, events)
	}
	stats := db.Stats()
	if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "3" {
		t.Fatalf("durable_wal_lsn=%q, want 3", got)
	}
	if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "0" {
		t.Fatalf("pending debt entries after barrier=%q, want 0", got)
	}
	frames := scanPublicCommandWALV2(t, dir)
	if len(frames) != 3 || frames[2].Kind != commitlog.CommandKindDurablePrefixBarrier || frames[2].DurabilityClass != commitlog.CommandDurabilityDurable {
		t.Fatalf("frames=%+v, want two relaxed mutations followed by durable barrier", frames)
	}
}

func TestPublicCommandWALCheckpointReleasesCoveredRotationDebtBeforeNextBarrier(t *testing.T) {
	dir := t.TempDir()
	opts := relaxedCommandWALDurablePrefixOptions(dir)
	opts.CommandWALSegmentTargetBytes = 1
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open relaxed command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("relaxed-a"), []byte("value-a")); err != nil {
		t.Fatalf("Set relaxed-a: %v", err)
	}
	if err := db.Set([]byte("relaxed-b"), []byte("value-b")); err != nil {
		t.Fatalf("Set relaxed-b: %v", err)
	}
	if got := db.Stats()["treedb.command_wal.dependency_debt.entries"]; got != "2" {
		t.Fatalf("pending debt entries before checkpoint=%q, want 2", got)
	}

	before := publicCommandWALSegmentNames(t, dir)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stats := db.Stats()
	if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "2" {
		t.Fatalf("durable_wal_lsn after checkpoint=%q, want covered LSN 2", got)
	}
	if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "0" {
		t.Fatalf("pending debt entries after checkpoint=%q, want 0", got)
	}
	if err := db.Set([]byte("fallback-advance"), []byte("value")); err != nil {
		t.Fatalf("Set fallback-advance: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint fallback-advance: %v", err)
	}
	after := publicCommandWALSegmentNames(t, dir)
	afterSet := make(map[string]struct{}, len(after))
	for _, name := range after {
		afterSet[name] = struct{}{}
	}
	for _, name := range before {
		if _, ok := afterSet[name]; ok {
			t.Fatalf("command WAL retained covered segment %s after fallback advance; before=%v after=%v", name, before, after)
		}
	}
	if err := writeEmptyPublicCommandWALSync(db); err != nil {
		t.Fatalf("empty WriteSync after checkpoint cleanup: %v", err)
	}
}

func TestPublicCommandWALRelaxedRotationSyncCutsRetainDebtForBarrierRetry(t *testing.T) {
	points := []durabilitycut.Point{
		durabilitycut.BeforeDependencyFileSync,
		durabilitycut.AfterDependencyFileSync,
		durabilitycut.BeforeNewFileDirectorySync,
		durabilitycut.AfterNewFileDirectorySync,
	}
	for _, point := range points {
		point := point
		t.Run(string(point), func(t *testing.T) {
			dir := t.TempDir()
			opts := relaxedCommandWALDurablePrefixOptions(dir)
			opts.CommandWALSegmentTargetBytes = 1
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("Open relaxed command WAL: %v", err)
			}
			defer func() { _ = db.Close() }()

			if err := db.Set([]byte("relaxed-a"), []byte("value-a")); err != nil {
				t.Fatalf("Set relaxed-a: %v", err)
			}
			if err := db.Set([]byte("relaxed-b"), []byte("value-b")); err != nil {
				t.Fatalf("Set relaxed-b: %v", err)
			}

			cutErr := fmt.Errorf("injected command-WAL rotation cut at %s", point)
			cut := false
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if !cut && event.Resource == durabilitycut.ResourceCommandWAL && event.Point == point {
					cut = true
					return cutErr
				}
				return nil
			})
			err = writeEmptyPublicCommandWALSync(db)
			restore()
			if !errors.Is(err, cutErr) {
				t.Fatalf("empty WriteSync error=%v, want injected cut", err)
			}
			if !cut {
				t.Fatalf("injected command-WAL rotation cut at %s was not reached", point)
			}
			stats := db.Stats()
			if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "0" {
				t.Fatalf("durable_wal_lsn after pre-append cut=%q, want 0", got)
			}
			if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "2" {
				t.Fatalf("pending debt entries after pre-append cut=%q, want retained 2", got)
			}
			if got := stats["treedb.command_wal.dependency_debt.retries_total"]; got != "2" {
				t.Fatalf("pending debt retries after pre-append cut=%q, want one retry recorded for each of two entries", got)
			}
			if frames := scanPublicCommandWALV2(t, dir); len(frames) != 2 {
				t.Fatalf("frames after pre-append cut=%+v, want only two relaxed frames", frames)
			}

			if err := writeEmptyPublicCommandWALSync(db); err != nil {
				t.Fatalf("retry empty WriteSync barrier: %v", err)
			}
			stats = db.Stats()
			if got := stats["treedb.command_wal.durable_wal_lsn"]; got != "3" {
				t.Fatalf("durable_wal_lsn after retry=%q, want 3", got)
			}
			if got := stats["treedb.command_wal.dependency_debt.entries"]; got != "0" {
				t.Fatalf("pending debt entries after retry=%q, want 0", got)
			}
		})
	}
}

func BenchmarkPublicCommandWALRelaxedDurablePrefixBarrier(b *testing.B) {
	for _, groupSize := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("group=%d", groupSize), func(b *testing.B) {
			benchmarkRoot := b.TempDir()
			var exactFileSyncs atomic.Uint64
			var namespaceSyncs atomic.Uint64
			var barrierWALSyncs atomic.Uint64
			var relaxedStableSyncs atomic.Uint64

			latencies := make([]time.Duration, 0, b.N)
			var coalescedDependencies uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				opts := relaxedCommandWALDurablePrefixOptions(filepath.Join(benchmarkRoot, strconv.Itoa(i)))
				opts.CommandWALSegmentTargetBytes = 1
				db, err := Open(opts)
				if err != nil {
					b.Fatalf("Open relaxed command WAL: %v", err)
				}
				func() {
					defer func() {
						if err := db.Close(); err != nil {
							b.Fatalf("Close relaxed command WAL: %v", err)
						}
					}()
					var barrierActive atomic.Bool
					var barrierAppended atomic.Bool
					restore := durabilitycut.Install(func(event durabilitycut.Event) error {
						if event.Resource != durabilitycut.ResourceCommandWAL {
							return nil
						}
						stable := event.Point == durabilitycut.AfterDependencyFileSync || event.Point == durabilitycut.AfterNewFileDirectorySync
						if !barrierActive.Load() {
							if stable {
								relaxedStableSyncs.Add(1)
							}
							return nil
						}
						if event.Point == durabilitycut.AfterDependencyAppend {
							barrierAppended.Store(true)
							return nil
						}
						if barrierAppended.Load() {
							if event.Point == durabilitycut.AfterDependencyFileSync {
								barrierWALSyncs.Add(1)
							}
							return nil
						}
						switch event.Point {
						case durabilitycut.AfterDependencyFileSync:
							exactFileSyncs.Add(1)
						case durabilitycut.AfterNewFileDirectorySync:
							namespaceSyncs.Add(1)
						}
						return nil
					})
					defer restore()
					for j := 0; j < groupSize; j++ {
						var keyBuf [64]byte
						key := strconv.AppendInt(append(keyBuf[:0], "durable-prefix/"...), int64(i*groupSize+j), 10)
						if err := db.Set(key, []byte("value")); err != nil {
							b.Fatalf("relaxed Set: %v", err)
						}
					}
					stats := db.Stats()
					if got := statMapUint64B(b, stats, "treedb.command_wal.dependency_debt.entries"); got != uint64(groupSize) {
						b.Fatalf("debt entries=%d, want barrier group size %d", got, groupSize)
					}
					coalescedDependencies += statMapUint64B(b, stats, "treedb.command_wal.dependency_debt.pending_count")
					barrierAppended.Store(false)
					barrierActive.Store(true)
					b.StartTimer()
					start := time.Now()
					err := writeEmptyPublicCommandWALSync(db)
					b.StopTimer()
					latencies = append(latencies, time.Since(start))
					barrierActive.Store(false)
					if err != nil {
						b.Fatalf("empty WriteSync barrier: %v", err)
					}
					if !barrierAppended.Load() {
						b.Fatal("durable barrier append was not observed")
					}
				}()
			}
			if got := relaxedStableSyncs.Load(); got != 0 {
				b.Fatalf("ordinary relaxed writes emitted %d stable sync events, want 0", got)
			}
			b.ReportMetric(float64(groupSize), "barrier_group_entries/op")
			b.ReportMetric(float64(coalescedDependencies)/float64(b.N), "coalesced_dependency_entries/op")
			b.ReportMetric(float64(exactFileSyncs.Load())/float64(b.N), "exact_file_syncs_before_barrier_append/op")
			b.ReportMetric(float64(namespaceSyncs.Load())/float64(b.N), "namespace_syncs_before_barrier_append/op")
			b.ReportMetric(float64(barrierWALSyncs.Load())/float64(b.N), "wal_file_syncs_after_barrier_append/op")
			reportWriteSyncLatencyDistribution(b, latencies)
		})
	}
}
