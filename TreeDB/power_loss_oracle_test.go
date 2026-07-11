package treedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
)

const powerLossOracleSeed = uint64(3674)

func powerLossOptions(dir string) treedb.Options {
	return treedb.Options{
		Dir:                          dir,
		ChunkSize:                    64 * 1024,
		DisableSideStores:            true,
		DisableBackgroundPrune:       true,
		BackgroundCheckpointInterval: -1,
	}
}

func seedPowerLossImage(t *testing.T) (*powerlossoracle.Model, treedb.Options) {
	t.Helper()
	dir := t.TempDir()
	opts := powerLossOptions(dir)
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open seed: %v", err)
	}
	if err := db.SetSync([]byte("stable/old"), []byte("old-value")); err != nil {
		_ = db.Close()
		t.Fatalf("SetSync seed: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint seed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close seed: %v", err)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatalf("Capture seed: %v", err)
	}
	return model, opts
}

func requirePublicReopen(t *testing.T, model *powerlossoracle.Model, opts treedb.Options, readOnly bool) powerlossreopen.Result {
	t.Helper()
	result, db, closeFn, err := powerlossreopen.Stable(model, opts, readOnly)
	if err != nil {
		t.Fatalf("materialize/public Open: %v", err)
	}
	if db != nil {
		value, getErr := db.Get([]byte("stable/old"))
		if getErr != nil {
			_ = closeFn()
			t.Fatalf("public-open stable/old: %v", getErr)
		}
		if !bytes.Equal(value, []byte("old-value")) {
			_ = closeFn()
			t.Fatalf("public-open stable/old=%q want old-value", value)
		}
	}
	if err := closeFn(); err != nil {
		t.Fatalf("close public reopen: %v", err)
	}
	return result
}

// TestPowerLossOracleEnumerateCutPoints is the one-command deterministic cut
// enumerator used by later durability children. Failure output always includes
// the replayable seed and stable cut-point identifier.
func TestPowerLossOracleEnumerateCutPoints(t *testing.T) {
	dir := t.TempDir()
	opts := powerLossOptions(dir)
	opts.CommandWAL = true
	opts.CommandWALSegmentTargetBytes = 4096
	opts.WALMaxSegmentBytes = 64 * 1024
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.IndexOuterLeavesInValueLog = true
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open actual-cut fixture: %v", err)
	}
	if err := db.SetSync([]byte("stable/old"), []byte("old-value")); err != nil {
		t.Fatalf("seed SetSync: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("seed Checkpoint: %v", err)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatalf("Capture live stable baseline: %v", err)
	}
	snapshots := make(map[powerlossoracle.CutPoint]*powerlossoracle.Model, len(powerlossoracle.CutPoints))
	events := make(map[powerlossoracle.CutPoint][]durabilitycut.Event, len(powerlossoracle.CutPoints))
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		events[event.Point] = append(events[event.Point], event)
		if snapshots[event.Point] == nil {
			snapshots[event.Point] = model.Clone()
		}
		return nil
	})
	batch := db.NewBatch()
	defer batch.Close()
	for i := 0; i < 64; i++ {
		key := []byte(fmt.Sprintf("actual/%03d", i))
		value := bytes.Repeat([]byte{byte(i + 1)}, 2048)
		if err := batch.Set(key, value); err != nil {
			restore()
			_ = db.Close()
			t.Fatalf("actual batch Set %d: %v", i, err)
		}
	}
	if err := batch.Write(); err != nil {
		restore()
		_ = db.Close()
		t.Fatalf("actual batch Write: %v", err)
	}
	if err := db.SetSync([]byte("actual/durable"), bytes.Repeat([]byte("d"), 2048)); err != nil {
		restore()
		_ = db.Close()
		t.Fatalf("actual durable SetSync: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		restore()
		_ = db.Close()
		t.Fatalf("actual Checkpoint: %v", err)
	}
	restore()
	if err := db.Close(); err != nil {
		t.Fatalf("Close actual-cut fixture: %v", err)
	}
	for _, cut := range powerlossoracle.CutPoints {
		cut := cut
		t.Run(string(cut), func(t *testing.T) {
			t.Logf("power-loss-oracle seed=%d cut=%s events=%d", powerLossOracleSeed, cut, len(events[cut]))
			snapshot := snapshots[cut]
			if snapshot == nil {
				t.Fatalf("seed=%d cut=%s not emitted by actual TreeDB workload", powerLossOracleSeed, cut)
			}
			result := requirePublicReopen(t, snapshot, opts, false)
			if result.Rejected {
				t.Fatalf("seed=%d cut=%s stable-only public Open rejected: %v", powerLossOracleSeed, cut, result.Err)
			}
		})
	}
}

// This family is the stable witness for finalizeCommitLockedWithOptions and
// flushFinalizeCommitDurability publishing meta ahead of dependency closure.
func TestPowerLossOracleCounterexampleNewMetaMissingClosure(t *testing.T) {
	dir := t.TempDir()
	opts := powerLossOptions(dir)
	opts.Durability = treedb.DurabilityWALOffRelaxed
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.IndexOuterLeavesInValueLog = true
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open actual relaxed fixture: %v", err)
	}
	if err := db.SetSync([]byte("stable/old"), []byte("old-value")); err != nil {
		t.Fatalf("seed SetSync: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("seed Checkpoint: %v", err)
	}
	base, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatalf("capture stable baseline: %v", err)
	}
	cutErr := errors.New("power-loss-oracle: stop after actual meta write")
	var snapshot *powerlossoracle.Model
	var meta durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := base.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterMetaWrite {
			meta = event
			snapshot = base.Clone()
			return cutErr
		}
		return nil
	})
	for i := 0; i < 400; i++ {
		key := []byte(fmt.Sprintf("new/pointer/%03d", i))
		if err := db.Set(key, bytes.Repeat([]byte{byte(i + 1)}, 4096)); err != nil {
			restore()
			t.Fatalf("stage actual cached write %d: %v", i, err)
		}
	}
	err = db.Checkpoint()
	restore()
	if !errors.Is(err, cutErr) || snapshot == nil {
		t.Fatalf("actual relaxed finalize did not stop at AfterMetaWrite: err=%v", err)
	}
	_ = db.Close()
	relIndex, err := filepath.Rel(dir, meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := snapshot.ChangedRanges(relIndex)
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) == 0 {
		t.Fatal("actual relaxed finalize changed no index bytes")
	}

	images := map[powerlossoracle.ResourceKind]*powerlossoracle.Model{}
	missingIndex := snapshot.Clone()
	if err := missingIndex.PromoteRange(relIndex, meta.Offset, meta.Length); err != nil {
		t.Fatal(err)
	}
	images[powerlossoracle.ResourceIndex] = missingIndex
	missingVlog := snapshot.Clone()
	for _, r := range changed {
		if err := missingVlog.PromoteRange(relIndex, r.Offset, r.Length); err != nil {
			t.Fatal(err)
		}
	}
	images[powerlossoracle.ResourceValueLog] = missingVlog
	missingOuter := missingVlog.Clone()
	leafChanged := false
	for _, path := range missingOuter.VolatilePaths() {
		if strings.HasPrefix(filepath.ToSlash(path), "leaf_vlog/") {
			ranges, err := missingOuter.ChangedRanges(path)
			if err != nil {
				t.Fatal(err)
			}
			leafChanged = leafChanged || len(ranges) > 0
		}
		if strings.Contains(path, "value_vlog") {
			if err := missingOuter.SyncFile(path); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !leafChanged {
		t.Fatal("actual fixture generated no changed outer-leaf record")
	}
	images[powerlossoracle.ResourceOuterLeaf] = missingOuter

	for _, missing := range []powerlossoracle.ResourceKind{
		powerlossoracle.ResourceIndex,
		powerlossoracle.ResourceValueLog,
		powerlossoracle.ResourceOuterLeaf,
	} {
		missing := missing
		t.Run(string(missing), func(t *testing.T) {
			result, reopened, closeFn, err := powerlossreopen.Stable(images[missing], opts, false)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = closeFn() }()
			if result.Rejected {
				t.Logf("public Open rejected stable-only image as permitted evidence: %v", result.Err)
				return
			}
			got, getErr := reopened.Get([]byte("new/pointer/399"))
			want := bytes.Repeat([]byte{144}, 4096)
			if getErr == nil && bytes.Equal(got, want) {
				t.Fatalf("public read unexpectedly resolved %s closure at commit=%d", missing, result.CommitSeq)
			}
		})
	}
}

// This family is the stable witness for relaxed command-WAL external-RID replay
// accepting a checksum-valid but dependency-incomplete frame.
func TestPowerLossOracleCounterexampleRelaxedCommandFrameMissingRID(t *testing.T) {
	dir := t.TempDir()
	opts := powerLossOptions(dir)
	opts.CommandWAL = true
	opts.Durability = treedb.DurabilityWALOnRelaxed
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	cutErr := errors.New("power-loss-oracle: stop after actual command frame flush")
	var snapshot *powerlossoracle.Model
	var walEvent durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterUserspaceFlush && event.Resource == durabilitycut.ResourceCommandWAL {
			snapshot = model.Clone()
			walEvent = event
			return cutErr
		}
		return nil
	})
	b := db.NewBatch()
	if err := b.Set([]byte("rid/missing"), bytes.Repeat([]byte("r"), 4096)); err != nil {
		t.Fatal(err)
	}
	err = b.Write()
	_ = b.Close()
	restore()
	if !errors.Is(err, cutErr) || snapshot == nil {
		t.Fatalf("actual external-RID cut err=%v", err)
	}
	_ = db.Close()
	if walEvent.Path == "" {
		t.Fatal("actual command-WAL flush event omitted path")
	}
	walPath, err := filepath.Rel(dir, walEvent.Path)
	if err != nil {
		t.Fatal(err)
	}
	ranges, err := snapshot.ChangedRanges(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) == 0 {
		t.Fatal("actual relaxed command frame changed no command-WAL bytes")
	}
	for _, r := range ranges {
		if err := snapshot.PromoteRange(walPath, r.Offset, r.Length); err != nil {
			t.Fatal(err)
		}
	}
	result, reopened, closeFn, err := powerlossreopen.Stable(snapshot, opts, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closeFn() }()
	if result.Rejected {
		t.Logf("public Open rejected checksum-valid frame with missing RID: %v", result.Err)
		return
	}
	got, getErr := reopened.Get([]byte("rid/missing"))
	if getErr == nil && bytes.Equal(got, bytes.Repeat([]byte("r"), 4096)) {
		t.Fatalf("public recovery resolved command frame whose actual RID bytes were not stable")
	}
}

// This family is the stable witness for Checkpoint, flushSyncRequested, and
// chunked cached flush apply exposing an intermediate incomplete root.
func TestPowerLossOracleCounterexampleChunkedSyncIntermediateRoot(t *testing.T) {
	dir := t.TempDir()
	opts := powerLossOptions(dir)
	opts.Durability = treedb.DurabilityWALOffRelaxed
	opts.FlushBackendMaxEntries = 4
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync([]byte("stable/old"), []byte("old-value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		if err := db.Set([]byte(fmt.Sprintf("chunk/%03d", i)), []byte(fmt.Sprintf("value-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	cutErr := errors.New("power-loss-oracle: stop after intermediate chunk meta")
	var snapshot *powerlossoracle.Model
	var meta durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterMetaWrite {
			meta, snapshot = event, model.Clone()
			return cutErr
		}
		return nil
	})
	err = db.Checkpoint()
	restore()
	if !errors.Is(err, cutErr) || snapshot == nil {
		t.Fatalf("actual chunk checkpoint cut err=%v", err)
	}
	_ = db.Close()
	rel, err := filepath.Rel(dir, meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	ranges, err := snapshot.ChangedRanges(rel)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range ranges {
		if err := snapshot.PromoteRange(rel, r.Offset, r.Length); err != nil {
			t.Fatal(err)
		}
	}
	result, reopened, closeFn, err := powerlossreopen.Stable(snapshot, opts, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closeFn() }()
	if result.Rejected {
		t.Logf("public Open rejected actual intermediate image: %v", result.Err)
		return
	}
	got, getErr := reopened.Get([]byte("chunk/063"))
	if getErr == nil && bytes.Equal(got, []byte("value-063")) {
		t.Fatalf("actual first chunk unexpectedly exposed complete checkpoint at commit=%d", result.CommitSeq)
	}
}

func TestPowerLossOracleFixtureInventoryReopensStableOnly(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*treedb.Options)
		entries    int
		valueSize  int
		outerLeaf  bool
		valueLanes int
		rotatedWAL bool
	}{
		{name: "inline-values", entries: 12, valueSize: 32, configure: func(opts *treedb.Options) { opts.ValueLog.PointerThreshold = 1 << 20 }},
		{name: "forced-value-pointers", entries: 12, valueSize: 2048, configure: func(opts *treedb.Options) { opts.ValueLog.PointerThreshold = 1; opts.ValueLog.ForcePointers = true }},
		{name: "forced-outer-leaves", entries: 400, valueSize: 48, outerLeaf: true, configure: func(opts *treedb.Options) { opts.IndexOuterLeavesInValueLog = true }},
		{name: "value-pointers-plus-outer-leaves", entries: 400, valueSize: 1024, outerLeaf: true, configure: func(opts *treedb.Options) {
			opts.IndexOuterLeavesInValueLog = true
			opts.ValueLog.PointerThreshold = 1
			opts.ValueLog.ForcePointers = true
		}},
		{name: "multi-lane-value-log", entries: 320, valueSize: 1024, valueLanes: 4, configure: func(opts *treedb.Options) {
			opts.JournalLanes = 4
			opts.MemtableShards = 16
			opts.ValueLog.PointerThreshold = 1
			opts.ValueLog.ForcePointers = true
		}},
		{name: "segment-rotation", entries: 80, valueSize: 1024, rotatedWAL: true, configure: func(opts *treedb.Options) {
			opts.CommandWAL = true
			opts.WALMaxSegmentBytes = 4096
			opts.ValueLog.PointerThreshold = 1
			opts.ValueLog.ForcePointers = true
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := powerLossOptions(dir)
			test.configure(&opts)
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatalf("Open fixture: %v", err)
			}
			for i := 0; i < test.entries; i++ {
				key := []byte(fmt.Sprintf("fixture/%06d", i))
				value := bytes.Repeat([]byte{byte(i%251 + 1)}, test.valueSize)
				if err := db.Set(key, value); err != nil {
					_ = db.Close()
					t.Fatalf("Set fixture %d: %v", i, err)
				}
			}
			if err := db.Checkpoint(); err != nil {
				_ = db.Close()
				t.Fatalf("Checkpoint fixture: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close fixture: %v", err)
			}
			model, err := powerlossoracle.Capture(dir)
			if err != nil {
				t.Fatalf("Capture fixture: %v", err)
			}
			paths := model.StablePaths()
			if test.outerLeaf && !stablePathHasPrefix(paths, "leaf_vlog/") {
				t.Fatalf("fixture did not create forced outer-leaf storage: %v", paths)
			}
			if test.valueLanes > 0 && stableValueLogLaneCount(paths) < test.valueLanes {
				t.Fatalf("fixture value-log lanes=%d want at least %d: %v", stableValueLogLaneCount(paths), test.valueLanes, paths)
			}
			if test.rotatedWAL && !stableWALHasSequenceAfterOne(paths) {
				t.Fatalf("fixture did not rotate a WAL segment: %v", paths)
			}
			result, reopened, closeFn, err := powerlossreopen.Stable(model, opts, true)
			if err != nil {
				t.Fatalf("public read-only Open fixture: %v", err)
			}
			if result.Rejected {
				_ = closeFn()
				t.Fatalf("public read-only Open rejected fixture: %v", result.Err)
			}
			for _, index := range []int{0, test.entries / 2, test.entries - 1} {
				key := []byte(fmt.Sprintf("fixture/%06d", index))
				value, err := reopened.Get(key)
				if err != nil {
					_ = closeFn()
					t.Fatalf("Get fixture %d: %v", index, err)
				}
				if len(value) != test.valueSize {
					_ = closeFn()
					t.Fatalf("Get fixture %d len=%d want %d", index, len(value), test.valueSize)
				}
			}
			if err := closeFn(); err != nil {
				t.Fatalf("Close fixture stable image: %v", err)
			}
		})
	}
}

func stablePathHasPrefix(paths []string, prefix string) bool {
	for _, path := range paths {
		if strings.HasPrefix(filepath.ToSlash(path), prefix) {
			return true
		}
	}
	return false
}

func stableValueLogLaneCount(paths []string) int {
	lanes := make(map[int]struct{})
	for _, path := range paths {
		var lane, sequence int
		if _, err := fmt.Sscanf(filepath.Base(path), "value-l%d-%d.log", &lane, &sequence); err == nil && strings.HasPrefix(filepath.ToSlash(path), "value_vlog/") {
			lanes[lane] = struct{}{}
		}
	}
	return len(lanes)
}

func stableWALHasSequenceAfterOne(paths []string) bool {
	for _, path := range paths {
		var lane, sequence int
		if _, err := fmt.Sscanf(filepath.Base(path), "commit-l%d-%d.log", &lane, &sequence); err == nil && sequence > 1 {
			return true
		}
	}
	return false
}

func TestPowerLossOracleStableNamesArePortable(t *testing.T) {
	for _, cut := range powerlossoracle.CutPoints {
		if strings.TrimSpace(string(cut)) == "" || strings.ContainsAny(string(cut), " /\\") {
			t.Fatalf("cut point %q is not a portable stable identifier", cut)
		}
	}
}
