package treedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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
	type cutSnapshot struct {
		model                  *powerlossoracle.Model
		generations            []powerlossoracle.Generation
		latestSealedSequence   uint64
		expectedBySequence     map[uint64]map[string]map[string]string
		durableAcknowledged    bool
		durableAcknowledgement uint64
		expectedInvariant      string
	}
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
	baseSequence := publicCommitSequence(t, db)
	baseState := expectedPowerLossState(nil, false)
	batchState := expectedPowerLossState(nil, false)
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("actual/%03d", i)
		batchState["actual/"][key] = string(bytes.Repeat([]byte{byte(i + 1)}, 2048))
	}
	durableState := cloneExpectedPowerLossState(batchState)
	durableState["actual/"]["actual/durable"] = string(bytes.Repeat([]byte("d"), 2048))
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatalf("Capture live stable baseline: %v", err)
	}
	snapshots := make(map[powerlossoracle.CutPoint][]cutSnapshot, len(powerlossoracle.CutPoints))
	events := make(map[powerlossoracle.CutPoint][]durabilitycut.Event, len(powerlossoracle.CutPoints))
	durableAcknowledged := false
	durableSequence := uint64(0)
	phase := "baseline"
	currentTargetSequence := baseSequence
	currentTargetState := baseState
	latestSealedSequence := baseSequence
	currentAppliedLSN := uint64(1)
	sealWriteObserved := false
	var dependencyPaths []string
	generations := []powerlossoracle.Generation{{
		Sequence:    baseSequence,
		Recoverable: true,
		Resources:   completePowerLossClosure("baseline"),
		AppliedLSN:  currentAppliedLSN,
	}}
	expectedBySequence := map[uint64]map[string]map[string]string{
		baseSequence: cloneExpectedPowerLossState(baseState),
	}
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterDependencyAppend && (event.Resource == durabilitycut.ResourceValueLog || event.Resource == durabilitycut.ResourceOuterLeaf) {
			dependencyPaths = appendUniquePaths(dependencyPaths, event.Path, event.Paths...)
		}
		if event.Point == durabilitycut.AfterDependencyFileSync {
			dependencyPaths = appendUniquePaths(dependencyPaths, event.Path, event.Paths...)
		}
		if event.Point == durabilitycut.AfterAppliedLSNAdvance {
			currentAppliedLSN = event.LSN
		}
		if event.Point == durabilitycut.AfterPublicationSealWrite {
			sealWriteObserved = true
		}
		if event.Point == durabilitycut.AfterMetaSync && currentTargetSequence > latestSealedSequence {
			resources, err := observedPowerLossClosure(model, dir, event.Path, dependencyPaths, sealWriteObserved, currentAppliedLSN, true)
			if err != nil {
				return err
			}
			latestSealedSequence = currentTargetSequence
			generations = append(generations, powerlossoracle.Generation{
				Sequence:    currentTargetSequence,
				Recoverable: true,
				Resources:   resources,
				AppliedLSN:  currentAppliedLSN,
			})
			expectedBySequence[currentTargetSequence] = cloneExpectedPowerLossState(currentTargetState)
		}
		expectedInvariant := ""
		switch {
		case phase == "relaxed-batch":
			expectedInvariant = powerlossoracle.InvariantKeyStateMismatch
		case phase == "durable-set-sync" && (event.Point == durabilitycut.BeforeNewFileDirectorySync || event.Point == durabilitycut.AfterNewFileDirectorySync):
			expectedInvariant = powerlossoracle.InvariantKeyStateMismatch
		case phase == "checkpoint" && latestSealedSequence < currentTargetSequence:
			expectedInvariant = powerlossoracle.InvariantDurableAckLost
		}
		events[event.Point] = append(events[event.Point], event)
		snapshots[event.Point] = append(snapshots[event.Point], cutSnapshot{
			model:                  model.Clone(),
			generations:            clonePowerLossGenerations(generations),
			latestSealedSequence:   latestSealedSequence,
			expectedBySequence:     cloneExpectedPowerLossStates(expectedBySequence),
			durableAcknowledged:    durableAcknowledged,
			durableAcknowledgement: durableSequence,
			expectedInvariant:      expectedInvariant,
		})
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
	// A relaxed command-WAL batch does not publish a new index generation.
	// Its keys become part of the next synchronous publication.
	phase = "relaxed-batch"
	currentTargetSequence = baseSequence
	currentTargetState = batchState
	if err := batch.Write(); err != nil {
		restore()
		_ = db.Close()
		t.Fatalf("actual batch Write: %v", err)
	}
	batchSequence := publicCommitSequence(t, db)
	if batchSequence != currentTargetSequence {
		restore()
		_ = db.Close()
		t.Fatalf("actual batch sequence=%d want modeled target=%d", batchSequence, currentTargetSequence)
	}
	currentTargetSequence = batchSequence
	currentTargetState = durableState
	phase = "durable-set-sync"
	if err := db.SetSync([]byte("actual/durable"), bytes.Repeat([]byte("d"), 2048)); err != nil {
		restore()
		_ = db.Close()
		t.Fatalf("actual durable SetSync: %v", err)
	}
	if sequence := publicCommitSequence(t, db); sequence != currentTargetSequence {
		restore()
		_ = db.Close()
		t.Fatalf("actual durable index sequence=%d want unchanged command-WAL base=%d", sequence, currentTargetSequence)
	}
	// Acknowledgement sequences are logical operation order, independent of
	// whether recovery obtains the operation from a newer root or WAL replay.
	durableSequence = batchSequence + 1
	durableAcknowledged = true
	currentTargetState = durableState
	currentTargetSequence = batchSequence + 1
	phase = "checkpoint"
	sealWriteObserved = false
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
			t.Logf("power-loss-oracle seed=%d cut=%s occurrences=%d", powerLossOracleSeed, cut, len(events[cut]))
			cutSnapshots := snapshots[cut]
			if len(cutSnapshots) == 0 {
				t.Fatalf("seed=%d cut=%s not emitted by actual TreeDB workload", powerLossOracleSeed, cut)
			}
			for occurrence, snapshot := range cutSnapshots {
				validateActualCutReopen(t, snapshot.model, opts, cut, occurrence, false, snapshot.generations, snapshot.latestSealedSequence, snapshot.expectedBySequence, snapshot.durableAcknowledgement, snapshot.durableAcknowledged, snapshot.expectedInvariant)
				validateActualCutReopen(t, snapshot.model, opts, cut, occurrence, true, snapshot.generations, snapshot.latestSealedSequence, snapshot.expectedBySequence, snapshot.durableAcknowledgement, snapshot.durableAcknowledged, snapshot.expectedInvariant)
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
	baseSequence := publicCommitSequence(t, db)
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
			result, _, closeFn, err := powerlossreopen.Stable(images[missing], opts, false)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = closeFn() }()
			dependencyPaths := make([]string, 0)
			for _, path := range images[missing].VolatilePaths() {
				slashed := filepath.ToSlash(path)
				if strings.Contains(slashed, "value_vlog/") || strings.Contains(slashed, "leaf_vlog/") {
					dependencyPaths = append(dependencyPaths, filepath.Join(dir, filepath.FromSlash(path)))
				}
			}
			newResources, err := observedPowerLossClosure(images[missing], dir, meta.Path, dependencyPaths, true, 0, false)
			if err != nil {
				t.Fatalf("derive actual %s closure: %v", missing, err)
			}
			scenario := powerlossoracle.Scenario{
				Name:                 "actual-new-meta-missing-" + string(missing),
				Cut:                  powerlossoracle.AfterMetaWrite,
				LatestSealedSequence: baseSequence + 1,
				SelectedSequence:     result.CommitSeq,
				OpenedSequence:       result.CommitSeq,
				OpenedAppliedLSN:     result.AppliedLSN,
				ReopenAttempted:      true,
				ReopenRejected:       result.Rejected,
				Generations: []powerlossoracle.Generation{
					{Sequence: baseSequence, Recoverable: true, Resources: completePowerLossClosure("old")},
					{Sequence: baseSequence + 1, Recoverable: true, Resources: newResources},
				},
			}
			if err := powerlossoracle.RequireViolation(scenario.Validate(), powerlossoracle.InvariantIncompleteRecoverableRoot); err != nil {
				t.Fatalf("actual %s image did not produce named diagnosis: %v (open=%v)", missing, err, result.Err)
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
	var appendedLSN uint64
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterDependencyAppend && event.Resource == durabilitycut.ResourceCommandWAL {
			appendedLSN = event.LSN
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
	if appendedLSN == 0 {
		t.Fatal("actual command-WAL append emitted no logical sequence")
	}
	got, getErr := reopened.Get([]byte("rid/missing"))
	if getErr == nil && bytes.Equal(got, bytes.Repeat([]byte("r"), 4096)) {
		t.Fatalf("public recovery resolved command frame whose actual RID bytes were not stable")
	}
	scenario := powerlossoracle.Scenario{
		Name:            "actual-relaxed-frame-missing-rid",
		Cut:             powerlossoracle.AfterUserspaceFlush,
		ReopenAttempted: true,
		CommandFrames: []powerlossoracle.CommandFrame{{
			LSN:           appendedLSN,
			ChecksumValid: true,
			Dependencies: []powerlossoracle.Resource{{
				Kind:   powerlossoracle.ResourceValueLog,
				ID:     fmt.Sprintf("rid/%d", appendedLSN),
				Stable: false,
				Live:   true,
			}},
			Applied: result.AppliedLSN >= appendedLSN,
		}},
	}
	if err := powerlossoracle.RequireViolation(scenario.Validate(), powerlossoracle.InvariantCommandReplayHole); err != nil {
		t.Fatalf("successful public Open did not produce missing-RID replay diagnosis: %v (commit=%d applied=%d get=%v)", err, result.CommitSeq, result.AppliedLSN, getErr)
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
		t.Logf("public Open rejected actual intermediate chunk image: %v", result.Err)
		return
	}
	first, firstErr := reopened.Get([]byte("chunk/000"))
	if firstErr != nil || !bytes.Equal(first, []byte("value-000")) {
		t.Fatalf("actual cut selected the wholly old root instead of an intermediate chunk: first=%q err=%v commit=%d", first, firstErr, result.CommitSeq)
	}
	expected := map[string]map[string]string{"chunk/": {}}
	observed := map[string]map[string]string{"chunk/": {}}
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("chunk/%03d", i)
		value := fmt.Sprintf("value-%03d", i)
		expected["chunk/"][key] = value
		if got, err := reopened.Get([]byte(key)); err == nil {
			observed["chunk/"][key] = string(got)
		}
	}
	scenario := powerlossoracle.Scenario{
		Name:                      "actual-chunked-sync-intermediate-root",
		Cut:                       powerlossoracle.AfterMetaWrite,
		Generations:               []powerlossoracle.Generation{{Sequence: result.CommitSeq, Recoverable: true, Resources: completePowerLossClosure("chunk"), AppliedLSN: result.AppliedLSN}},
		LatestSealedSequence:      result.CommitSeq,
		SelectedSequence:          result.CommitSeq,
		OpenedSequence:            result.CommitSeq,
		OpenedAppliedLSN:          result.AppliedLSN,
		ExpectedKeyValuesByPrefix: expected,
		ObservedKeyValuesByPrefix: observed,
		ReopenAttempted:           true,
	}
	if err := powerlossoracle.RequireViolation(scenario.Validate(), powerlossoracle.InvariantKeyStateMismatch); err != nil {
		t.Fatalf("successful public Open did not produce intermediate-root diagnosis: %v", err)
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

func TestPowerLossOracleProductionOuterLeafAppendCuts(t *testing.T) {
	dir := t.TempDir()
	opts := powerLossOptions(dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.PointerThreshold = 1 << 20
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open outer-leaf fixture: %v", err)
	}
	var outerBefore, outerAfter, valueAppend, pathlessOuterAfter int
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		switch {
		case event.Resource == durabilitycut.ResourceOuterLeaf && event.Point == durabilitycut.BeforeDependencyAppend:
			outerBefore++
		case event.Resource == durabilitycut.ResourceOuterLeaf && event.Point == durabilitycut.AfterDependencyAppend:
			outerAfter++
			if len(event.Paths) == 0 || !strings.Contains(filepath.ToSlash(event.Paths[0]), "/leaf_vlog/") {
				pathlessOuterAfter++
			}
		case event.Resource == durabilitycut.ResourceValueLog && (event.Point == durabilitycut.BeforeDependencyAppend || event.Point == durabilitycut.AfterDependencyAppend):
			valueAppend++
		}
		return nil
	})
	for i := 0; i < 400; i++ {
		key := []byte(fmt.Sprintf("outer/%06d", i))
		if err := db.Set(key, bytes.Repeat([]byte{byte(i%251 + 1)}, 48)); err != nil {
			restore()
			_ = db.Close()
			t.Fatalf("Set outer-leaf fixture %d: %v", i, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		restore()
		_ = db.Close()
		t.Fatalf("Checkpoint outer-leaf fixture: %v", err)
	}
	restore()
	if err := db.Close(); err != nil {
		t.Fatalf("Close outer-leaf fixture: %v", err)
	}
	if outerBefore == 0 || outerAfter != outerBefore {
		t.Fatalf("outer-leaf append cuts before=%d after=%d", outerBefore, outerAfter)
	}
	if pathlessOuterAfter != 0 {
		t.Fatalf("outer-leaf after-append cuts without exact segment paths=%d", pathlessOuterAfter)
	}
	if valueAppend != 0 {
		t.Fatalf("inline-value outer-leaf fixture emitted %d user value-log append cuts", valueAppend)
	}
}

func TestObservedPowerLossClosureRequiresAppendedDependencySync(t *testing.T) {
	root := t.TempDir()
	indexPath := filepath.Join(root, "index.db")
	if err := os.WriteFile(indexPath, []byte("stable-index"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	valuePath := filepath.Join(root, "value_vlog", "value-l0-000001.log")
	if err := os.MkdirAll(filepath.Dir(valuePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(valuePath, []byte("volatile-value"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{Resource: durabilitycut.ResourceValueLog, Namespace: durabilitycut.NamespaceCreate, NewPath: valuePath}); err != nil {
		t.Fatal(err)
	}
	resources, err := observedPowerLossClosure(model, root, indexPath, []string{valuePath}, true, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, resource := range resources {
		if resource.Kind == powerlossoracle.ResourceValueLog && resource.Stable {
			t.Fatal("unsynced appended value-log dependency was reported stable")
		}
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

func publicCommitSequence(t *testing.T, db *treedb.DB) uint64 {
	t.Helper()
	sequence, err := strconv.ParseUint(db.Stats()["treedb.commit_seq"], 10, 64)
	if err != nil {
		t.Fatalf("parse public commit sequence: %v", err)
	}
	return sequence
}

func validateActualCutReopen(t *testing.T, model *powerlossoracle.Model, opts treedb.Options, cut powerlossoracle.CutPoint, occurrence int, readOnly bool, generations []powerlossoracle.Generation, latestSealedSequence uint64, expectedBySequence map[uint64]map[string]map[string]string, durableSequence uint64, durableAcknowledged bool, expectedInvariant string) {
	t.Helper()
	result, reopened, closeFn, err := powerlossreopen.Stable(model, opts, readOnly)
	if err != nil {
		t.Fatalf("seed=%d cut=%s occurrence=%d readOnly=%t materialize/public Open: %v", powerLossOracleSeed, cut, occurrence, readOnly, err)
	}
	defer func() {
		if err := closeFn(); err != nil {
			t.Fatalf("seed=%d cut=%s occurrence=%d readOnly=%t close: %v", powerLossOracleSeed, cut, occurrence, readOnly, err)
		}
	}()
	if result.Rejected {
		rejected := powerlossoracle.Scenario{
			Name:                 "actual-cut-public-rejection",
			Cut:                  cut,
			Generations:          clonePowerLossGenerations(generations),
			LatestSealedSequence: latestSealedSequence,
			ReopenAttempted:      true,
			ReopenRejected:       true,
		}
		if err := rejected.Validate(); err != nil {
			t.Fatalf("seed=%d cut=%s occurrence=%d readOnly=%t public Open rejected (%v), diagnosis: %v", powerLossOracleSeed, cut, occurrence, readOnly, result.Err, err)
		}
		return
	}

	expected, knownSequence := expectedBySequence[result.CommitSeq]
	if !knownSequence {
		t.Fatalf("seed=%d cut=%s occurrence=%d readOnly=%t public Open selected unmodeled generation=%d", powerLossOracleSeed, cut, occurrence, readOnly, result.CommitSeq)
	}
	observed := map[string]map[string]string{
		"stable/": {},
		"actual/": {},
	}
	for _, key := range expectedPowerLossFixtureKeys() {
		value, getErr := reopened.Get([]byte(key))
		if getErr == nil {
			prefix := "actual/"
			if strings.HasPrefix(key, "stable/") {
				prefix = "stable/"
			}
			observed[prefix][key] = string(value)
		}
	}
	var acknowledgements []powerlossoracle.Acknowledgement
	var recoveredAcknowledgements []uint64
	if durableAcknowledged {
		acknowledgements = []powerlossoracle.Acknowledgement{{Sequence: durableSequence, Durable: true}}
		if value, ok := observed["actual/"]["actual/durable"]; ok && value == string(bytes.Repeat([]byte("d"), 2048)) {
			recoveredAcknowledgements = []uint64{durableSequence}
		}
	}
	scenario := powerlossoracle.Scenario{
		Name:                      "actual-cut-stable-image",
		Cut:                       cut,
		Generations:               clonePowerLossGenerations(generations),
		Acknowledged:              acknowledgements,
		RecoveredAcknowledgements: recoveredAcknowledgements,
		LatestSealedSequence:      latestSealedSequence,
		SelectedSequence:          result.CommitSeq,
		OpenedSequence:            result.CommitSeq,
		OpenedAppliedLSN:          result.AppliedLSN,
		ExpectedKeyValuesByPrefix: expected,
		ObservedKeyValuesByPrefix: observed,
		ReopenAttempted:           true,
	}
	validationErr := scenario.Validate()
	if expectedInvariant != "" {
		if err := powerlossoracle.RequireViolation(validationErr, expectedInvariant); err != nil {
			t.Fatalf("seed=%d cut=%s occurrence=%d readOnly=%t expected diagnosis %s: %v", powerLossOracleSeed, cut, occurrence, readOnly, expectedInvariant, err)
		}
		t.Logf("known counterexample seed=%d cut=%s occurrence=%d: %v", powerLossOracleSeed, cut, occurrence, validationErr)
		return
	}
	if validationErr != nil {
		t.Fatalf("seed=%d cut=%s occurrence=%d readOnly=%t scenario: %v", powerLossOracleSeed, cut, occurrence, readOnly, validationErr)
	}
}

func appendUniquePaths(paths []string, path string, more ...string) []string {
	seen := make(map[string]struct{}, len(paths)+len(more)+1)
	for _, existing := range paths {
		seen[existing] = struct{}{}
	}
	for _, candidate := range append([]string{path}, more...) {
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		paths = append(paths, candidate)
	}
	return paths
}

func observedPowerLossClosure(model *powerlossoracle.Model, root, indexPath string, dependencyPaths []string, sealWriteObserved bool, appliedLSN uint64, commandWALRequired bool) ([]powerlossoracle.Resource, error) {
	indexStable, err := model.PathStable(root, indexPath)
	if err != nil {
		return nil, err
	}
	kindStable := map[powerlossoracle.ResourceKind]bool{
		powerlossoracle.ResourceIndex:      indexStable,
		powerlossoracle.ResourceFreelist:   indexStable,
		powerlossoracle.ResourceValueLog:   true,
		powerlossoracle.ResourceOuterLeaf:  true,
		powerlossoracle.ResourceAuxiliary:  true,
		powerlossoracle.ResourceDirectory:  true,
		powerlossoracle.ResourceSeal:       indexStable && sealWriteObserved,
		powerlossoracle.ResourceCommandWAL: !commandWALRequired || indexStable && appliedLSN > 0,
	}
	for _, path := range dependencyPaths {
		stable, err := model.PathStable(root, path)
		if err != nil {
			return nil, err
		}
		slashed := filepath.ToSlash(path)
		switch {
		case strings.Contains(slashed, "/value_vlog/"):
			kindStable[powerlossoracle.ResourceValueLog] = kindStable[powerlossoracle.ResourceValueLog] && stable
		case strings.Contains(slashed, "/leaf_vlog/"):
			kindStable[powerlossoracle.ResourceOuterLeaf] = kindStable[powerlossoracle.ResourceOuterLeaf] && stable
		default:
			kindStable[powerlossoracle.ResourceAuxiliary] = kindStable[powerlossoracle.ResourceAuxiliary] && stable
		}
		kindStable[powerlossoracle.ResourceDirectory] = kindStable[powerlossoracle.ResourceDirectory] && stable
	}
	resources := completePowerLossClosure("observed")
	for i := range resources {
		resources[i].Stable = kindStable[resources[i].Kind]
	}
	return resources, nil
}

func expectedPowerLossState(actual map[string]string, durable bool) map[string]map[string]string {
	state := map[string]map[string]string{
		"stable/": {"stable/old": "old-value"},
		"actual/": {},
	}
	for key, value := range actual {
		state["actual/"][key] = value
	}
	if durable {
		state["actual/"]["actual/durable"] = string(bytes.Repeat([]byte("d"), 2048))
	}
	return state
}

func expectedPowerLossFixtureKeys() []string {
	keys := make([]string, 0, 66)
	keys = append(keys, "stable/old")
	for i := 0; i < 64; i++ {
		keys = append(keys, fmt.Sprintf("actual/%03d", i))
	}
	return append(keys, "actual/durable")
}

func cloneExpectedPowerLossState(state map[string]map[string]string) map[string]map[string]string {
	clone := make(map[string]map[string]string, len(state))
	for prefix, values := range state {
		clone[prefix] = make(map[string]string, len(values))
		for key, value := range values {
			clone[prefix][key] = value
		}
	}
	return clone
}

func cloneExpectedPowerLossStates(states map[uint64]map[string]map[string]string) map[uint64]map[string]map[string]string {
	clone := make(map[uint64]map[string]map[string]string, len(states))
	for sequence, state := range states {
		clone[sequence] = cloneExpectedPowerLossState(state)
	}
	return clone
}

func clonePowerLossGenerations(generations []powerlossoracle.Generation) []powerlossoracle.Generation {
	clone := make([]powerlossoracle.Generation, len(generations))
	copy(clone, generations)
	for i := range clone {
		clone[i].Resources = append([]powerlossoracle.Resource(nil), generations[i].Resources...)
	}
	return clone
}

func completePowerLossClosure(id string) []powerlossoracle.Resource {
	return []powerlossoracle.Resource{
		{Kind: powerlossoracle.ResourceIndex, ID: id + "/index", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceFreelist, ID: id + "/freelist", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceValueLog, ID: id + "/value-log", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceOuterLeaf, ID: id + "/outer-leaf", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceAuxiliary, ID: id + "/auxiliary", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceDirectory, ID: id + "/directory", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceSeal, ID: id + "/seal", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceCommandWAL, ID: id + "/command-wal", Stable: true, Live: true},
	}
}

func TestPowerLossOracleStableNamesArePortable(t *testing.T) {
	for _, cut := range powerlossoracle.CutPoints {
		if strings.TrimSpace(string(cut)) == "" || strings.ContainsAny(string(cut), " /\\") {
			t.Fatalf("cut point %q is not a portable stable identifier", cut)
		}
	}
}
