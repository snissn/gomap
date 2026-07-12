package treedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

const powerLossOracleSeed = uint64(3674)

var retainedPowerLossCounterexamples = []string{
	"new-meta-before-index-closure",
	"new-meta-before-value-log-closure",
	"new-meta-before-outer-leaf-closure",
	"new-file-bytes-before-namespace",
	"torn-target-meta",
	"relaxed-command-frame-before-rid",
	"chunked-sync-intermediate-root",
	"older-meta-live-page-reused",
}

type observedPowerLossCommandFrame struct {
	LSN  uint64
	Path string
}

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
		phase                  string
		event                  durabilitycut.Event
		generations            []powerlossoracle.Generation
		latestSealedSequence   uint64
		expectedByAppliedLSN   map[uint64]map[string]map[string]string
		commandFrames          []observedPowerLossCommandFrame
		durableAcknowledged    bool
		durableAcknowledgement uint64
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
	var commandFrames []observedPowerLossCommandFrame
	generations := []powerlossoracle.Generation{{
		Sequence:    baseSequence,
		Recoverable: true,
		Resources:   completePowerLossClosure("baseline"),
		AppliedLSN:  currentAppliedLSN,
	}}
	expectedByAppliedLSN := map[uint64]map[string]map[string]string{
		currentAppliedLSN: cloneExpectedPowerLossState(baseState),
	}
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterDependencyAppend && (event.Resource == durabilitycut.ResourceValueLog || event.Resource == durabilitycut.ResourceOuterLeaf) {
			dependencyPaths = appendUniquePaths(dependencyPaths, event.Path, event.Paths...)
		}
		if event.Point == durabilitycut.AfterDependencyAppend && event.Resource == durabilitycut.ResourceCommandWAL {
			if event.Path == "" {
				return fmt.Errorf("command-WAL append LSN %d omitted exact segment path", event.LSN)
			}
			var err error
			commandFrames, err = appendPowerLossCommandFrame(commandFrames, observedPowerLossCommandFrame{LSN: event.LSN, Path: event.Path})
			if err != nil {
				return err
			}
			switch phase {
			case "relaxed-batch":
				expectedByAppliedLSN[event.LSN] = cloneExpectedPowerLossState(batchState)
			case "durable-set-sync":
				durableSequence = event.LSN
				expectedByAppliedLSN[event.LSN] = cloneExpectedPowerLossState(durableState)
			}
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
			expectedByAppliedLSN[currentAppliedLSN] = cloneExpectedPowerLossState(currentTargetState)
		}
		events[event.Point] = append(events[event.Point], event)
		snapshots[event.Point] = append(snapshots[event.Point], cutSnapshot{
			model:                  model.Clone(),
			phase:                  phase,
			event:                  event,
			generations:            clonePowerLossGenerations(generations),
			latestSealedSequence:   latestSealedSequence,
			expectedByAppliedLSN:   cloneExpectedPowerLossStates(expectedByAppliedLSN),
			commandFrames:          cloneObservedPowerLossCommandFrames(commandFrames),
			durableAcknowledged:    durableAcknowledged,
			durableAcknowledgement: durableSequence,
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
	if durableSequence == 0 {
		restore()
		_ = db.Close()
		t.Fatal("durable SetSync emitted no command-WAL LSN")
	}
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
				t.Logf("cut occurrence=%d phase=%s resource=%s path=%s lsn=%d", occurrence, snapshot.phase, snapshot.event.Resource, snapshot.event.Path, snapshot.event.LSN)
				validateActualCutReopen(t, snapshot.model, opts, cut, occurrence, snapshot.phase, false, snapshot.generations, snapshot.latestSealedSequence, snapshot.expectedByAppliedLSN, snapshot.commandFrames, snapshot.durableAcknowledgement, snapshot.durableAcknowledged)
				validateActualCutReopen(t, snapshot.model, opts, cut, occurrence, snapshot.phase, true, snapshot.generations, snapshot.latestSealedSequence, snapshot.expectedByAppliedLSN, snapshot.commandFrames, snapshot.durableAcknowledgement, snapshot.durableAcknowledged)
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

	stablePaths := make(map[string]bool)
	for _, path := range snapshot.StablePaths() {
		stablePaths[path] = true
	}
	dependencies := []powerlossoracle.DirtyResource{{
		Kind:   powerlossoracle.ResourceIndex,
		ID:     "index-generation-2",
		Path:   filepath.ToSlash(relIndex),
		Ranges: changed,
	}}
	dependencyOrdinal := map[powerlossoracle.ResourceKind]int{}
	leafChanged := false
	for _, path := range snapshot.VolatilePaths() {
		slashed := filepath.ToSlash(path)
		kind := powerlossoracle.ResourceKind("")
		switch {
		case strings.Contains(slashed, "value_vlog/"):
			kind = powerlossoracle.ResourceValueLog
		case strings.Contains(slashed, "leaf_vlog/"):
			kind = powerlossoracle.ResourceOuterLeaf
		default:
			continue
		}
		ranges, err := snapshot.ChangedRanges(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(ranges) == 0 {
			continue
		}
		dependencyOrdinal[kind]++
		resource := powerlossoracle.DirtyResource{
			Kind:   kind,
			ID:     fmt.Sprintf("%s-generation-2-%03d", kind, dependencyOrdinal[kind]),
			Path:   slashed,
			Ranges: ranges,
		}
		if !stablePaths[slashed] {
			resource.NewName = true
			resource.NamespaceDirs = []string{filepath.ToSlash(filepath.Dir(path))}
		}
		dependencies = append(dependencies, resource)
		leafChanged = leafChanged || kind == powerlossoracle.ResourceOuterLeaf
	}
	if !leafChanged {
		t.Fatal("actual fixture generated no changed outer-leaf record")
	}
	target := powerlossoracle.DirtyResource{
		Kind:   powerlossoracle.ResourceIndex,
		ID:     "target-meta-generation-2",
		Path:   filepath.ToSlash(relIndex),
		Ranges: []powerlossoracle.ByteRange{{Offset: meta.Offset, Length: meta.Length}},
	}
	if meta.Length > 1 {
		target.Torn = []powerlossoracle.TornBoundary{{ID: "meta-body", Format: powerlossoracle.FormatMeta, Offset: meta.Offset, Length: meta.Length, Persisted: meta.Length / 2}}
	}
	variants, coverage, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{
		ID:               "checkpoint-generation-2",
		Point:            powerlossoracle.AfterMetaWrite,
		Occurrence:       1,
		Model:            snapshot,
		TargetMeta:       &target,
		Dependencies:     dependencies,
		RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantSyncedOnly, powerlossoracle.VariantTargetMetaOnly, powerlossoracle.VariantOneMissingDependency, powerlossoracle.VariantFullWriteback, powerlossoracle.VariantTornFormat},
		ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
			powerlossoracle.VariantSyncedOnly:           powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantTargetMetaOnly:       powerlossoracle.ExpectedCorruption,
			powerlossoracle.VariantOneMissingDependency: powerlossoracle.ExpectedCorruption,
			powerlossoracle.VariantDataWithoutNamespace: powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantNamespaceWithoutData: powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantFullWriteback:        powerlossoracle.ExpectedNewRoot,
			powerlossoracle.VariantTornFormat:           powerlossoracle.ExpectedTypedError,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	variants, err = powerlossoracle.SelectReplayVariant(variants, selector)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	var peakBytes int64
	validated := map[powerlossoracle.ResourceKind]bool{}
	for _, variant := range variants {
		variant := variant
		t.Run(variant.ID, func(t *testing.T) {
			result, _, closeFn, err := powerlossreopen.Stable(variant.Model, opts, false)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = closeFn() }()
			if size := variant.Model.StableSizeBytes(); size > peakBytes {
				peakBytes = size
			}
			missing := powerlossoracle.ResourceKind("")
			switch variant.Family {
			case powerlossoracle.VariantTargetMetaOnly:
				missing = powerlossoracle.ResourceIndex
			case powerlossoracle.VariantOneMissingDependency:
				for _, kind := range []powerlossoracle.ResourceKind{powerlossoracle.ResourceValueLog, powerlossoracle.ResourceOuterLeaf} {
					if strings.HasPrefix(variant.Qualifier, string(kind)+"/") && !validated[kind] {
						missing = kind
					}
				}
			}
			if missing == "" {
				return
			}
			validated[missing] = true
			dependencyPaths := make([]string, 0)
			for _, path := range variant.Model.VolatilePaths() {
				slashed := filepath.ToSlash(path)
				if strings.Contains(slashed, "value_vlog/") || strings.Contains(slashed, "leaf_vlog/") {
					dependencyPaths = append(dependencyPaths, filepath.Join(dir, filepath.FromSlash(path)))
				}
			}
			newResources, err := observedPowerLossClosure(variant.Model, dir, meta.Path, dependencyPaths, true, 0, false)
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
				t.Fatalf("actual %s image %s seed=%d did not produce named diagnosis: %v (open=%v)", missing, variant.ID, variant.Seed, err, result.Err)
			}
		})
	}
	if selector == (powerlossoracle.ReplaySelector{}) {
		for _, missing := range []powerlossoracle.ResourceKind{powerlossoracle.ResourceIndex, powerlossoracle.ResourceValueLog, powerlossoracle.ResourceOuterLeaf} {
			if !validated[missing] {
				t.Fatalf("generator did not retain named %s witness", missing)
			}
		}
	}
	t.Logf("adversarial crash images: cut=%s count=%d runtime=%s peak_temp_storage_bytes=%d family_coverage=%v", coverage.CutID, len(variants), time.Since(started), peakBytes, coverage.ByFamily)
}

func TestPowerLossOracleAdversarialNewFileNamespaceMismatch(t *testing.T) {
	model, opts := seedPowerLossImage(t)
	const path = "maindb/adversarial-namespace/new-dependency.bin"
	if err := model.Create(path, []byte("new dependency bytes")); err != nil {
		t.Fatal(err)
	}
	variants, coverage, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{
		ID:         "new-auxiliary-namespace",
		Point:      powerlossoracle.AfterNewFileDirectorySync,
		Occurrence: 1,
		Model:      model,
		Dependencies: []powerlossoracle.DirtyResource{{
			Kind:          powerlossoracle.ResourceAuxiliary,
			ID:            "new-asset-generation-2",
			Path:          path,
			NewName:       true,
			NamespaceDirs: []string{".", "maindb", "maindb/adversarial-namespace"},
		}},
		RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantSyncedOnly, powerlossoracle.VariantDataWithoutNamespace, powerlossoracle.VariantNamespaceWithoutData, powerlossoracle.VariantFullWriteback},
		ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
			powerlossoracle.VariantSyncedOnly:           powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantDataWithoutNamespace: powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantNamespaceWithoutData: powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantFullWriteback:        powerlossoracle.ExpectedOldRoot,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	variants, err = powerlossoracle.SelectReplayVariant(variants, selector)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range variants {
		variant := variant
		t.Run(variant.ID, func(t *testing.T) {
			requirePublicReopen(t, variant.Model, opts, true)
			stablePaths := variant.Model.StablePaths()
			position := sort.SearchStrings(stablePaths, path)
			present := position < len(stablePaths) && stablePaths[position] == path
			switch variant.Family {
			case powerlossoracle.VariantDataWithoutNamespace:
				if present {
					t.Fatal("data-without-namespace persisted the new name")
				}
			case powerlossoracle.VariantNamespaceWithoutData:
				if !present {
					t.Fatal("namespace-without-data omitted the new name")
				}
			}
		})
	}
	t.Logf("adversarial crash images: cut=%s count=%d family_coverage=%v", coverage.CutID, len(variants), coverage.ByFamily)
}

func TestPowerLossOracleCounterexampleLedger(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "power_loss_counterexamples.json"))
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := powerlossoracle.ParseCounterexampleLedger(data)
	if err != nil {
		t.Fatal(err)
	}
	generated := powerLossLedgerGeneratedVariants(t)
	if err := powerlossoracle.ValidateCounterexampleLedger(ledger, generated); err != nil {
		t.Fatal(err)
	}
	if err := powerlossoracle.RequireRetainedCounterexamples(ledger, retainedPowerLossCounterexamples); err != nil {
		t.Fatal(err)
	}
	covered := make(map[powerlossoracle.VariantFamily]bool)
	for _, entry := range ledger.Entries {
		for _, family := range entry.VariantFamilies {
			covered[family] = true
		}
	}
	for _, family := range powerlossoracle.VariantFamilies {
		if !covered[family] {
			t.Fatalf("machine-readable ledger does not cover required family %s", family)
		}
	}
}

func powerLossLedgerGeneratedVariants(t *testing.T) map[string][]powerlossoracle.Variant {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "maindb", "assets"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"maindb/index.db", "maindb/outer.leaf", "maindb/command.wal", "maindb/chunk.db", "maindb/reuse.db"} {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(path)), bytes.Repeat([]byte("0"), 64), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	model, err := powerlossoracle.Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"maindb/index.db", "maindb/outer.leaf", "maindb/command.wal", "maindb/chunk.db", "maindb/reuse.db"} {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(path)), bytes.Repeat([]byte("1"), 64), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "maindb", "assets", "value-0002.vlog"), bytes.Repeat([]byte("v"), 64), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := model.Overlay(root); err != nil {
		t.Fatal(err)
	}
	target := powerlossoracle.DirtyResource{
		Kind:   powerlossoracle.ResourceIndex,
		ID:     "target-meta-generation-2",
		Path:   "maindb/index.db",
		Ranges: []powerlossoracle.ByteRange{{Offset: 0, Length: 8}},
		Torn:   []powerlossoracle.TornBoundary{{ID: "meta-body", Format: powerlossoracle.FormatMeta, Offset: 0, Length: 8, Persisted: 4}},
	}
	specs := []powerlossoracle.CutSpec{
		{
			ID:         "checkpoint-generation-2",
			Point:      powerlossoracle.AfterMetaWrite,
			Occurrence: 1,
			Model:      model,
			TargetMeta: &target,
			Dependencies: []powerlossoracle.DirtyResource{
				{Kind: powerlossoracle.ResourceIndex, ID: "index-generation-2", Path: "maindb/index.db", Ranges: []powerlossoracle.ByteRange{{Offset: 8, Length: 56}}},
				{Kind: powerlossoracle.ResourceOuterLeaf, ID: "outer-leaf-generation-2-001", Path: "maindb/outer.leaf"},
				{Kind: powerlossoracle.ResourceValueLog, ID: "value-log-generation-2-001", Path: "maindb/assets/value-0002.vlog", NewName: true, NamespaceDirs: []string{"maindb/assets"}},
			},
			RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantSyncedOnly, powerlossoracle.VariantTargetMetaOnly, powerlossoracle.VariantOneMissingDependency, powerlossoracle.VariantDataWithoutNamespace, powerlossoracle.VariantNamespaceWithoutData, powerlossoracle.VariantFullWriteback, powerlossoracle.VariantTornFormat},
			ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
				powerlossoracle.VariantSyncedOnly:           powerlossoracle.ExpectedOldRoot,
				powerlossoracle.VariantTargetMetaOnly:       powerlossoracle.ExpectedCorruption,
				powerlossoracle.VariantOneMissingDependency: powerlossoracle.ExpectedCorruption,
				powerlossoracle.VariantDataWithoutNamespace: powerlossoracle.ExpectedOldRoot,
				powerlossoracle.VariantNamespaceWithoutData: powerlossoracle.ExpectedOldRoot,
				powerlossoracle.VariantFullWriteback:        powerlossoracle.ExpectedNewRoot,
				powerlossoracle.VariantTornFormat:           powerlossoracle.ExpectedTypedError,
			},
		},
		{
			ID:               "new-auxiliary-namespace",
			Point:            powerlossoracle.AfterNewFileDirectorySync,
			Occurrence:       1,
			Model:            model,
			Dependencies:     []powerlossoracle.DirtyResource{{Kind: powerlossoracle.ResourceAuxiliary, ID: "new-asset-generation-2", Path: "maindb/assets/value-0002.vlog", NewName: true, NamespaceDirs: []string{"maindb/assets"}}},
			RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantDataWithoutNamespace, powerlossoracle.VariantNamespaceWithoutData},
			ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
				powerlossoracle.VariantSyncedOnly:           powerlossoracle.ExpectedOldRoot,
				powerlossoracle.VariantDataWithoutNamespace: powerlossoracle.ExpectedOldRoot,
				powerlossoracle.VariantNamespaceWithoutData: powerlossoracle.ExpectedOldRoot,
				powerlossoracle.VariantFullWriteback:        powerlossoracle.ExpectedOldRoot,
			},
		},
		{
			ID:               "relaxed-command-frame-external-rid",
			Point:            powerlossoracle.AfterUserspaceFlush,
			Occurrence:       1,
			Model:            model,
			Dependencies:     []powerlossoracle.DirtyResource{{Kind: powerlossoracle.ResourceCommandWAL, ID: "relaxed-frame-1", Path: "maindb/command.wal"}},
			RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantFullWriteback},
			ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
				powerlossoracle.VariantSyncedOnly:    powerlossoracle.ExpectedOldRoot,
				powerlossoracle.VariantFullWriteback: powerlossoracle.ExpectedSuffixDiscard,
			},
		},
		{
			ID:               "chunked-checkpoint-intermediate-root",
			Point:            powerlossoracle.AfterMetaWrite,
			Occurrence:       1,
			Model:            model,
			TargetMeta:       &powerlossoracle.DirtyResource{Kind: powerlossoracle.ResourceIndex, ID: "chunked-intermediate-root", Path: "maindb/chunk.db"},
			RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantTargetMetaOnly},
			ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
				powerlossoracle.VariantSyncedOnly:     powerlossoracle.ExpectedOldRoot,
				powerlossoracle.VariantTargetMetaOnly: powerlossoracle.ExpectedCorruption,
				powerlossoracle.VariantFullWriteback:  powerlossoracle.ExpectedCorruption,
			},
		},
		{
			ID:               "older-meta-live-page-reuse",
			Point:            powerlossoracle.AfterMetaWrite,
			Occurrence:       1,
			Model:            model,
			OldPageWrites:    []powerlossoracle.DirtyResource{{Kind: powerlossoracle.ResourceIndex, ID: "first-reused-old-live-page", Path: "maindb/reuse.db"}},
			RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantOldPageReuse},
			ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
				powerlossoracle.VariantSyncedOnly:   powerlossoracle.ExpectedOldRoot,
				powerlossoracle.VariantOldPageReuse: powerlossoracle.ExpectedCorruption,
			},
		},
	}
	generated := make(map[string][]powerlossoracle.Variant, len(specs))
	for _, spec := range specs {
		variants, coverage, err := powerlossoracle.GenerateVariants(spec)
		if err != nil {
			t.Fatal(err)
		}
		generated[coverage.CutID] = variants
	}
	return generated
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
	variants, coverage, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{
		ID:         "relaxed-command-frame-external-rid",
		Point:      powerlossoracle.AfterUserspaceFlush,
		Occurrence: 1,
		Model:      snapshot,
		Dependencies: []powerlossoracle.DirtyResource{{
			Kind:   powerlossoracle.ResourceCommandWAL,
			ID:     "relaxed-frame-1",
			Path:   filepath.ToSlash(walPath),
			Ranges: ranges,
		}},
		RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantSyncedOnly, powerlossoracle.VariantFullWriteback},
		ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
			powerlossoracle.VariantSyncedOnly:    powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantFullWriteback: powerlossoracle.ExpectedSuffixDiscard,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	variants, err = powerlossoracle.SelectReplayVariant(variants, selector)
	if err != nil {
		t.Fatal(err)
	}
	if appendedLSN == 0 {
		t.Fatal("actual command-WAL append emitted no logical sequence")
	}
	for _, variant := range variants {
		variant := variant
		t.Run(variant.ID, func(t *testing.T) {
			result, reopened, closeFn, err := powerlossreopen.Stable(variant.Model, opts, false)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = closeFn() }()
			if variant.Family != powerlossoracle.VariantFullWriteback {
				return
			}
			if result.Rejected {
				t.Logf("public Open rejected checksum-valid frame with missing RID: %v", result.Err)
				return
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
		})
	}
	t.Logf("adversarial crash images: cut=%s count=%d family_coverage=%v", coverage.CutID, len(variants), coverage.ByFamily)
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
	target := powerlossoracle.DirtyResource{Kind: powerlossoracle.ResourceIndex, ID: "chunked-intermediate-root", Path: filepath.ToSlash(rel), Ranges: ranges}
	variants, coverage, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{
		ID:               "chunked-checkpoint-intermediate-root",
		Point:            powerlossoracle.AfterMetaWrite,
		Occurrence:       1,
		Model:            snapshot,
		TargetMeta:       &target,
		RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantSyncedOnly, powerlossoracle.VariantTargetMetaOnly, powerlossoracle.VariantFullWriteback},
		ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
			powerlossoracle.VariantSyncedOnly:     powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantTargetMetaOnly: powerlossoracle.ExpectedCorruption,
			powerlossoracle.VariantFullWriteback:  powerlossoracle.ExpectedCorruption,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	variants, err = powerlossoracle.SelectReplayVariant(variants, selector)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range variants {
		variant := variant
		t.Run(variant.ID, func(t *testing.T) {
			result, reopened, closeFn, err := powerlossreopen.Stable(variant.Model, opts, false)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = closeFn() }()
			if variant.Family != powerlossoracle.VariantTargetMetaOnly {
				return
			}
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
		})
	}
	t.Logf("adversarial crash images: cut=%s count=%d family_coverage=%v", coverage.CutID, len(variants), coverage.ByFamily)
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

func validateActualCutReopen(t *testing.T, model *powerlossoracle.Model, opts treedb.Options, cut powerlossoracle.CutPoint, occurrence int, phase string, readOnly bool, generations []powerlossoracle.Generation, latestSealedSequence uint64, expectedByAppliedLSN map[uint64]map[string]map[string]string, observedCommandFrames []observedPowerLossCommandFrame, durableSequence uint64, durableAcknowledged bool) {
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
		selectedAppliedLSN, selectedAppliedOK := newestCompleteGenerationAppliedLSN(generations, latestSealedSequence)
		commandFrames, frameErr := buildPowerLossCommandFrames(model, opts.Dir, observedCommandFrames, 0)
		if frameErr != nil {
			t.Fatalf("seed=%d cut=%s occurrence=%d model command frames: %v", powerLossOracleSeed, cut, occurrence, frameErr)
		}
		stableFrameLSN := contiguousStablePowerLossCommandLSN(commandFrames, selectedAppliedLSN)
		completeCommandLSN := contiguousCompletePowerLossCommandLSN(commandFrames, selectedAppliedLSN)
		if readOnly && selectedAppliedOK && allRecoverableGenerationsComplete(generations, latestSealedSequence) && stableFrameLSN > selectedAppliedLSN && errors.Is(result.Err, treedb.ErrRecoveryRequired) {
			if completeCommandLSN > selectedAppliedLSN {
				t.Logf("expected read-only recovery-required seed=%d cut=%s occurrence=%d: %v", powerLossOracleSeed, cut, occurrence, result.Err)
			} else {
				t.Logf("known counterexample rejected by read-only Open seed=%d cut=%s occurrence=%d: checksum-valid command through LSN %d has incomplete RID closure", powerLossOracleSeed, cut, occurrence, stableFrameLSN)
			}
			return
		}
		rejected := powerlossoracle.Scenario{
			Name:                 "actual-cut-public-rejection",
			Cut:                  cut,
			Generations:          clonePowerLossGenerations(generations),
			LatestSealedSequence: latestSealedSequence,
			ReopenAttempted:      true,
			ReopenRejected:       true,
		}
		diagnosis := rejected.Validate()
		if !readOnly && strings.Contains(result.Err.Error(), "command wal missing value-log rid") {
			if err := powerlossoracle.RequireViolation(diagnosis, powerlossoracle.InvariantSelectedRootInvalid); err != nil {
				t.Fatalf("seed=%d cut=%s occurrence=%d public Open rejected (%v), expected missing-RID diagnosis: %v", powerLossOracleSeed, cut, occurrence, result.Err, err)
			}
			t.Logf("known counterexample seed=%d cut=%s occurrence=%d: %v", powerLossOracleSeed, cut, occurrence, diagnosis)
			return
		}
		if diagnosis != nil {
			t.Fatalf("seed=%d cut=%s occurrence=%d readOnly=%t public Open rejected (%v), diagnosis: %v", powerLossOracleSeed, cut, occurrence, readOnly, result.Err, diagnosis)
		}
		return
	}

	expected, knownAppliedLSN := expectedByAppliedLSN[result.AppliedLSN]
	if !knownAppliedLSN {
		t.Fatalf("seed=%d cut=%s occurrence=%d readOnly=%t public Open reached unmodeled applied LSN=%d (commit sequence=%d)", powerLossOracleSeed, cut, occurrence, readOnly, result.AppliedLSN, result.CommitSeq)
	}
	selectedSequence, err := inferSelectedSequence(generations, latestSealedSequence, result.CommitSeq, result.AppliedLSN)
	if err != nil {
		t.Fatalf("seed=%d cut=%s occurrence=%d readOnly=%t infer selected root: %v", powerLossOracleSeed, cut, occurrence, readOnly, err)
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
	commandFrames, err := buildPowerLossCommandFrames(model, opts.Dir, observedCommandFrames, result.AppliedLSN)
	if err != nil {
		t.Fatalf("seed=%d cut=%s occurrence=%d model command frames: %v", powerLossOracleSeed, cut, occurrence, err)
	}
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
		SelectedSequence:          selectedSequence,
		OpenedSequence:            result.CommitSeq,
		OpenedAppliedLSN:          result.AppliedLSN,
		ExpectedKeyValuesByPrefix: expected,
		ObservedKeyValuesByPrefix: observed,
		CommandFrames:             commandFrames,
		ReopenAttempted:           true,
	}
	validationErr := scenario.Validate()
	if (phase == "relaxed-batch" || phase == "durable-set-sync") && validationErr != nil {
		if err := powerlossoracle.RequireViolation(validationErr, powerlossoracle.InvariantKeyStateMismatch); err == nil {
			t.Logf("known counterexample seed=%d cut=%s occurrence=%d: %v", powerLossOracleSeed, cut, occurrence, validationErr)
			return
		}
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

func appendPowerLossCommandFrame(frames []observedPowerLossCommandFrame, candidate observedPowerLossCommandFrame) ([]observedPowerLossCommandFrame, error) {
	if candidate.LSN == 0 {
		return frames, errors.New("command-WAL append emitted zero LSN")
	}
	for _, frame := range frames {
		if frame.LSN != candidate.LSN {
			continue
		}
		if frame.Path != candidate.Path {
			return frames, fmt.Errorf("command-WAL LSN %d observed in segments %q and %q", candidate.LSN, frame.Path, candidate.Path)
		}
		return frames, nil
	}
	return append(frames, candidate), nil
}

func cloneObservedPowerLossCommandFrames(frames []observedPowerLossCommandFrame) []observedPowerLossCommandFrame {
	clone := make([]observedPowerLossCommandFrame, len(frames))
	copy(clone, frames)
	return clone
}

func buildPowerLossCommandFrames(model *powerlossoracle.Model, root string, observed []observedPowerLossCommandFrame, openedAppliedLSN uint64) ([]powerlossoracle.CommandFrame, error) {
	stableRoot, err := os.MkdirTemp("", "treedb-powerloss-command-frames-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(stableRoot)
	if err := model.MaterializeStable(stableRoot); err != nil {
		return nil, err
	}
	stableEnvelopesByPath := make(map[string]map[uint64]commitlog.CommandEnvelope)
	for _, observedFrame := range observed {
		if _, scanned := stableEnvelopesByPath[observedFrame.Path]; scanned {
			continue
		}
		stableEnvelopesByPath[observedFrame.Path] = make(map[uint64]commitlog.CommandEnvelope)
		rel, err := filepath.Rel(root, observedFrame.Path)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
			return nil, fmt.Errorf("command-WAL segment %q is outside root %q", observedFrame.Path, root)
		}
		stablePath := filepath.Join(stableRoot, rel)
		if _, err := os.Stat(stablePath); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		envelopes, err := commitlog.ScanCommandFrames(stablePath, commitlog.Options{})
		if err != nil {
			return nil, fmt.Errorf("scan stable command-WAL segment %q: %w", observedFrame.Path, err)
		}
		for _, envelope := range envelopes {
			stableEnvelopesByPath[observedFrame.Path][envelope.LSN] = envelope
		}
	}
	stableRIDs, err := scanStablePowerLossValueLogRIDs(stableRoot)
	if err != nil {
		return nil, err
	}
	frames := make([]powerlossoracle.CommandFrame, 0, len(observed))
	for _, observedFrame := range observed {
		envelope, checksumValid := stableEnvelopesByPath[observedFrame.Path][observedFrame.LSN]
		frame := powerlossoracle.CommandFrame{
			LSN:           observedFrame.LSN,
			ChecksumValid: checksumValid,
			Applied:       observedFrame.LSN <= openedAppliedLSN,
		}
		if checksumValid && envelope.Kind == commitlog.CommandKindRawKVBatch && envelope.Scope == commitlog.CommandScopeRawKV && envelope.PayloadFormat == commitlog.PayloadFormatRawKVBatchV1 {
			operations, err := commitlog.DecodeRawKVBatchPayload(envelope.Payload)
			if err != nil {
				return nil, fmt.Errorf("decode stable command-WAL LSN %d dependencies: %w", observedFrame.LSN, err)
			}
			seenRIDs := make(map[uint64]struct{})
			for _, operation := range operations {
				if operation.Op != commitlog.RawKVOpSetRID {
					continue
				}
				if _, seen := seenRIDs[operation.RID]; seen {
					continue
				}
				seenRIDs[operation.RID] = struct{}{}
				frame.Dependencies = append(frame.Dependencies, powerlossoracle.Resource{
					Kind:   powerlossoracle.ResourceValueLog,
					ID:     fmt.Sprintf("rid/%d", operation.RID),
					Stable: stableRIDs[operation.RID],
					Live:   true,
				})
			}
		}
		frames = append(frames, frame)
	}
	sort.Slice(frames, func(i, j int) bool { return frames[i].LSN < frames[j].LSN })
	return frames, nil
}

func scanStablePowerLossValueLogRIDs(stableRoot string) (map[uint64]bool, error) {
	paths, err := filepath.Glob(filepath.Join(backenddb.ValueLogDirPath(stableRoot), "value-l*-*.log"))
	if err != nil {
		return nil, err
	}
	stable := make(map[uint64]bool)
	for _, path := range paths {
		name := filepath.Base(path)
		stem := strings.TrimSuffix(strings.TrimPrefix(name, "value-l"), ".log")
		parts := strings.Split(stem, "-")
		if len(parts) != 2 {
			continue
		}
		lane, laneErr := strconv.ParseUint(parts[0], 10, 32)
		sequence, sequenceErr := strconv.ParseUint(parts[1], 10, 32)
		if laneErr != nil || sequenceErr != nil {
			continue
		}
		fileID, err := valuelog.EncodeFileID(uint32(lane), uint32(sequence))
		if err != nil {
			return nil, fmt.Errorf("value-log segment %q: %w", path, err)
		}
		reader, err := valuelog.NewReaderWithBufferSize(path, fileID, 64<<10)
		if err != nil {
			return nil, err
		}
		reader.DisableValueDecode()
		for {
			rid, _, readErr := reader.ReadNextMeta()
			if readErr == nil {
				stable[rid] = true
				continue
			}
			if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
				break
			}
			_ = reader.Close()
			return nil, fmt.Errorf("scan stable value-log segment %q: %w", path, readErr)
		}
		if err := reader.Close(); err != nil {
			return nil, err
		}
	}
	return stable, nil
}

func contiguousCompletePowerLossCommandLSN(frames []powerlossoracle.CommandFrame, baseAppliedLSN uint64) uint64 {
	frontier := baseAppliedLSN
	for _, frame := range frames {
		if frame.LSN <= frontier {
			continue
		}
		if frame.LSN != frontier+1 || !frame.ChecksumValid {
			break
		}
		complete := true
		for _, dependency := range frame.Dependencies {
			if !dependency.Stable || !dependency.Live {
				complete = false
				break
			}
		}
		if !complete {
			break
		}
		frontier = frame.LSN
	}
	return frontier
}

func contiguousStablePowerLossCommandLSN(frames []powerlossoracle.CommandFrame, baseAppliedLSN uint64) uint64 {
	frontier := baseAppliedLSN
	for _, frame := range frames {
		if frame.LSN <= frontier {
			continue
		}
		if frame.LSN != frontier+1 || !frame.ChecksumValid {
			break
		}
		frontier = frame.LSN
	}
	return frontier
}

var requiredPowerLossRootKinds = []powerlossoracle.ResourceKind{
	powerlossoracle.ResourceIndex,
	powerlossoracle.ResourceFreelist,
	powerlossoracle.ResourceValueLog,
	powerlossoracle.ResourceOuterLeaf,
	powerlossoracle.ResourceAuxiliary,
	powerlossoracle.ResourceDirectory,
	powerlossoracle.ResourceSeal,
	powerlossoracle.ResourceCommandWAL,
}

func powerLossGenerationComplete(generation powerlossoracle.Generation) bool {
	present := make(map[powerlossoracle.ResourceKind]bool, len(generation.Resources))
	for _, resource := range generation.Resources {
		if !resource.Stable || !resource.Live {
			return false
		}
		present[resource.Kind] = true
	}
	for _, kind := range requiredPowerLossRootKinds {
		if !present[kind] {
			return false
		}
	}
	return true
}

func newestCompleteGenerationAppliedLSN(generations []powerlossoracle.Generation, latestSealedSequence uint64) (uint64, bool) {
	var selectedSequence uint64
	var selectedAppliedLSN uint64
	found := false
	for _, generation := range generations {
		if !generation.Recoverable || generation.Sequence > latestSealedSequence || !powerLossGenerationComplete(generation) {
			continue
		}
		if !found || generation.Sequence > selectedSequence {
			selectedSequence = generation.Sequence
			selectedAppliedLSN = generation.AppliedLSN
			found = true
		}
	}
	return selectedAppliedLSN, found
}

func allRecoverableGenerationsComplete(generations []powerlossoracle.Generation, latestSealedSequence uint64) bool {
	for _, generation := range generations {
		if generation.Recoverable && generation.Sequence <= latestSealedSequence && !powerLossGenerationComplete(generation) {
			return false
		}
	}
	return true
}

func TestNewestCompleteGenerationAppliedLSN(t *testing.T) {
	completeOlder := powerlossoracle.Generation{
		Sequence:    2,
		Recoverable: true,
		Resources:   completePowerLossClosure("older"),
		AppliedLSN:  20,
	}
	completeNewer := powerlossoracle.Generation{
		Sequence:    3,
		Recoverable: true,
		Resources:   completePowerLossClosure("newer"),
		AppliedLSN:  30,
	}

	t.Run("fully-covered-clean-image", func(t *testing.T) {
		generations := []powerlossoracle.Generation{completeOlder, completeNewer}
		got, ok := newestCompleteGenerationAppliedLSN(generations, 3)
		if !ok || got != 30 {
			t.Fatalf("selected applied LSN=(%d,%t), want (30,true)", got, ok)
		}
		if !allRecoverableGenerationsComplete(generations, 3) {
			t.Fatal("fully covered clean image reported incomplete")
		}
	})

	t.Run("fallback-does-not-mask-incomplete-sealed-generation", func(t *testing.T) {
		incompleteNewer := completeNewer
		incompleteNewer.Resources = append([]powerlossoracle.Resource(nil), completeNewer.Resources...)
		incompleteNewer.Resources[0].Stable = false
		generations := []powerlossoracle.Generation{completeOlder, incompleteNewer}
		got, ok := newestCompleteGenerationAppliedLSN(generations, 3)
		if !ok || got != 20 {
			t.Fatalf("fallback applied LSN=(%d,%t), want (20,true)", got, ok)
		}
		if allRecoverableGenerationsComplete(generations, 3) {
			t.Fatal("incomplete sealed generation must not be hidden by read-only recovery exception")
		}
	})
}

func TestBuildPowerLossCommandFramesRequiresStableSegmentName(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "command-wal")
	valueDir := filepath.Join(root, "value_vlog")
	for _, dir := range []string{walDir, valueDir} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	model, err := powerlossoracle.Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	segmentPath := filepath.Join(walDir, "commit-l0-2.log")
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("key"), Value: []byte("value")}})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := commitlog.NewWriter(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.AppendCommand(commitlog.CommandEnvelope{
		LSN:            2,
		BaseAppliedLSN: 1,
		Kind:           commitlog.CommandKindRawKVBatch,
		Scope:          commitlog.CommandScopeRawKV,
		PayloadFormat:  commitlog.PayloadFormatRawKVBatchV1,
		Payload:        payload,
	}); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Resource:  durabilitycut.ResourceCommandWAL,
		Namespace: durabilitycut.NamespaceCreate,
		NewPath:   segmentPath,
	}); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Point:    durabilitycut.AfterDependencyFileSync,
		Resource: durabilitycut.ResourceCommandWAL,
		Path:     segmentPath,
	}); err != nil {
		t.Fatal(err)
	}
	observed := []observedPowerLossCommandFrame{{LSN: 2, Path: segmentPath}}
	frames, err := buildPowerLossCommandFrames(model, root, observed, 1)
	if err != nil {
		t.Fatal(err)
	}
	if frames[0].ChecksumValid {
		t.Fatal("file-synced frame with volatile segment name reported complete")
	}
	if got := contiguousCompletePowerLossCommandLSN(frames, 1); got != 1 {
		t.Fatalf("pre-directory-sync frontier=%d, want 1", got)
	}

	if err := model.Observe(root, durabilitycut.Event{
		Point:    durabilitycut.AfterNewFileDirectorySync,
		Resource: durabilitycut.ResourceCommandWAL,
		Path:     walDir,
	}); err != nil {
		t.Fatal(err)
	}
	frames, err = buildPowerLossCommandFrames(model, root, observed, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !frames[0].ChecksumValid {
		t.Fatal("file-and-directory-synced frame reported incomplete")
	}
	if got := contiguousCompletePowerLossCommandLSN(frames, 1); got != 2 {
		t.Fatalf("post-directory-sync frontier=%d, want 2", got)
	}

	writer, err = commitlog.NewWriter(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.AppendCommand(commitlog.CommandEnvelope{
		LSN:            3,
		BaseAppliedLSN: 2,
		Kind:           commitlog.CommandKindRawKVBatch,
		Scope:          commitlog.CommandScopeRawKV,
		PayloadFormat:  commitlog.PayloadFormatRawKVBatchV1,
		Payload:        payload,
	}); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Point:    durabilitycut.AfterUserspaceFlush,
		Resource: durabilitycut.ResourceCommandWAL,
		Path:     segmentPath,
	}); err != nil {
		t.Fatal(err)
	}
	observed = append(observed, observedPowerLossCommandFrame{LSN: 3, Path: segmentPath})
	frames, err = buildPowerLossCommandFrames(model, root, observed, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !frames[0].ChecksumValid || frames[1].ChecksumValid {
		t.Fatalf("stable-prefix frames=%+v, want LSN 2 complete and appended LSN 3 incomplete", frames)
	}
	if got := contiguousCompletePowerLossCommandLSN(frames, 1); got != 2 {
		t.Fatalf("stable-prefix frontier=%d, want 2", got)
	}

	valuePath := filepath.Join(valueDir, "value-l0-000001.log")
	valueFileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	valueWriter, err := valuelog.NewWriter(valuePath, valueFileID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := valueWriter.Append(0, nil, 2, []byte("stable-rid-2")); err != nil {
		_ = valueWriter.Close()
		t.Fatal(err)
	}
	if err := valueWriter.Sync(); err != nil {
		_ = valueWriter.Close()
		t.Fatal(err)
	}
	if err := valueWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Resource:  durabilitycut.ResourceValueLog,
		Namespace: durabilitycut.NamespaceCreate,
		NewPath:   valuePath,
	}); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Point:    durabilitycut.AfterDependencyFileSync,
		Resource: durabilitycut.ResourceValueLog,
		Path:     valuePath,
	}); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Point:    durabilitycut.AfterNewFileDirectorySync,
		Resource: durabilitycut.ResourceValueLog,
		Path:     valueDir,
	}); err != nil {
		t.Fatal(err)
	}
	valueWriter, err = valuelog.NewWriter(valuePath, valueFileID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := valueWriter.Append(0, nil, 3, []byte("volatile-rid-3")); err != nil {
		_ = valueWriter.Close()
		t.Fatal(err)
	}
	if err := valueWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Point:    durabilitycut.AfterUserspaceFlush,
		Resource: durabilitycut.ResourceValueLog,
		Path:     valuePath,
	}); err != nil {
		t.Fatal(err)
	}
	writer, err = commitlog.NewWriter(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range []struct {
		lsn uint64
		rid uint64
	}{{lsn: 4, rid: 2}, {lsn: 5, rid: 3}} {
		payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{Op: commitlog.RawKVOpSetRID, Key: []byte(fmt.Sprintf("rid-%d", command.rid)), RID: command.rid}})
		if err != nil {
			_ = writer.Close()
			t.Fatal(err)
		}
		if err := writer.AppendCommand(commitlog.CommandEnvelope{
			LSN:            command.lsn,
			BaseAppliedLSN: command.lsn - 1,
			Kind:           commitlog.CommandKindRawKVBatch,
			Scope:          commitlog.CommandScopeRawKV,
			PayloadFormat:  commitlog.PayloadFormatRawKVBatchV1,
			Payload:        payload,
		}); err != nil {
			_ = writer.Close()
			t.Fatal(err)
		}
		observed = append(observed, observedPowerLossCommandFrame{LSN: command.lsn, Path: segmentPath})
	}
	if err := writer.Sync(); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Point:    durabilitycut.AfterDependencyFileSync,
		Resource: durabilitycut.ResourceCommandWAL,
		Path:     segmentPath,
	}); err != nil {
		t.Fatal(err)
	}
	frames, err = buildPowerLossCommandFrames(model, root, observed, 5)
	if err != nil {
		t.Fatal(err)
	}
	if !frames[2].ChecksumValid || len(frames[2].Dependencies) != 1 || !frames[2].Dependencies[0].Stable || frames[2].Dependencies[0].ID != "rid/2" {
		t.Fatalf("stable value-log RID dependency reported incomplete: %+v", frames[2])
	}
	if !frames[3].ChecksumValid || len(frames[3].Dependencies) != 1 || frames[3].Dependencies[0].Stable || frames[3].Dependencies[0].ID != "rid/3" {
		t.Fatalf("volatile value-log RID dependency reported complete: %+v", frames[3])
	}
	if got := contiguousCompletePowerLossCommandLSN(frames, 1); got != 4 {
		t.Fatalf("value-log stable-RID frontier=%d, want 4", got)
	}
}

// The raw-KV fixture publishes one commit sequence per replayed command frame.
// Use the public post-open counters plus each modeled root's applied frontier to
// infer which sealed root production selected before replay.
func TestInferSelectedSequenceTracksGenerationZero(t *testing.T) {
	t.Run("selects generation zero", func(t *testing.T) {
		got, err := inferSelectedSequence([]powerlossoracle.Generation{{
			Sequence:    0,
			AppliedLSN:  0,
			Recoverable: true,
		}}, 0, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got != 0 {
			t.Fatalf("selected sequence=%d, want 0", got)
		}
	})

	t.Run("detects ambiguity after generation zero", func(t *testing.T) {
		_, err := inferSelectedSequence([]powerlossoracle.Generation{
			{Sequence: 0, AppliedLSN: 0, Recoverable: true},
			{Sequence: 1, AppliedLSN: 1, Recoverable: true},
		}, 1, 1, 1)
		if err == nil || !strings.Contains(err.Error(), "ambiguous candidates 0 and 1") {
			t.Fatalf("error=%v, want generation-zero ambiguity", err)
		}
	})
}

func inferSelectedSequence(generations []powerlossoracle.Generation, latestSealedSequence, openedSequence, openedAppliedLSN uint64) (uint64, error) {
	var selected uint64
	found := false
	for _, generation := range generations {
		if !generation.Recoverable || generation.Sequence > latestSealedSequence || generation.AppliedLSN > openedAppliedLSN {
			continue
		}
		replayed := openedAppliedLSN - generation.AppliedLSN
		if generation.Sequence > ^uint64(0)-replayed || generation.Sequence+replayed != openedSequence {
			continue
		}
		if found {
			return 0, fmt.Errorf("ambiguous candidates %d and %d for public-open sequence=%d applied-lsn=%d", selected, generation.Sequence, openedSequence, openedAppliedLSN)
		}
		selected = generation.Sequence
		found = true
	}
	if !found {
		return 0, fmt.Errorf("no candidate at-or-below seal=%d for public-open sequence=%d applied-lsn=%d", latestSealedSequence, openedSequence, openedAppliedLSN)
	}
	return selected, nil
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
