package treedb_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
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

func requirePublicReopen(t *testing.T, model *powerlossoracle.Model, opts treedb.Options, readOnly bool) powerlossoracle.ReopenResult {
	t.Helper()
	result, db, closeFn, err := powerlossoracle.ReopenStable(model, opts, readOnly)
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
	base, opts := seedPowerLossImage(t)
	for index, cut := range powerlossoracle.CutPoints {
		cut := cut
		t.Run(string(cut), func(t *testing.T) {
			t.Logf("power-loss-oracle seed=%d cut=%s", powerLossOracleSeed, cut)
			model := base.Clone()
			if index >= 1 {
				if err := model.Create("oracle-dependency.tmp", []byte("dependency")); err != nil {
					t.Fatalf("seed=%d cut=%s create dependency: %v", powerLossOracleSeed, cut, err)
				}
			}
			if index >= 2 {
				if err := model.Flush("oracle-dependency.tmp"); err != nil {
					t.Fatalf("seed=%d cut=%s flush dependency: %v", powerLossOracleSeed, cut, err)
				}
			}
			if index >= 3 {
				if err := model.SyncFile("oracle-dependency.tmp"); err != nil {
					t.Fatalf("seed=%d cut=%s sync dependency: %v", powerLossOracleSeed, cut, err)
				}
			}
			if index >= 4 {
				if err := model.SyncDir("."); err != nil {
					t.Fatalf("seed=%d cut=%s sync directory: %v", powerLossOracleSeed, cut, err)
				}
			}
			result := requirePublicReopen(t, model, opts, index%2 == 1)
			if result.Rejected {
				t.Fatalf("seed=%d cut=%s valid old-root image rejected by public Open: %v", powerLossOracleSeed, cut, result.Err)
			}
		})
	}
}

// This family is the stable witness for finalizeCommitLockedWithOptions and
// flushFinalizeCommitDurability publishing meta ahead of dependency closure.
func TestPowerLossOracleCounterexampleNewMetaMissingClosure(t *testing.T) {
	base, opts := seedPowerLossImage(t)
	updatedDir := t.TempDir()
	updatedOpts := powerLossOptions(updatedDir)
	updatedOpts.ValueLog.PointerThreshold = 1
	updatedOpts.ValueLog.ForcePointers = true
	db, err := treedb.Open(updatedOpts)
	if err != nil {
		t.Fatalf("Open updated image: %v", err)
	}
	if err := db.SetSync([]byte("stable/old"), []byte("old-value")); err != nil {
		_ = db.Close()
		t.Fatalf("SetSync old in updated image: %v", err)
	}
	if err := db.SetSync([]byte("new/pointer"), bytes.Repeat([]byte("p"), 4096)); err != nil {
		_ = db.Close()
		t.Fatalf("SetSync pointer in updated image: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint updated image: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close updated image: %v", err)
	}
	if err := base.Overlay(updatedDir); err != nil {
		t.Fatalf("Overlay updated image: %v", err)
	}
	for _, path := range base.VolatilePaths() {
		if filepath.Base(path) == "index.db" {
			if err := base.SyncFile(path); err != nil {
				t.Fatalf("sync newer index/meta %s: %v", path, err)
			}
		}
	}
	result, _, closeFn, err := powerlossoracle.ReopenStable(base, opts, false)
	if err != nil {
		t.Fatalf("stable-only public Open: %v", err)
	}
	if err := closeFn(); err != nil {
		t.Fatalf("close stable-only public Open: %v", err)
	}

	for _, missing := range []powerlossoracle.ResourceKind{
		powerlossoracle.ResourceIndex,
		powerlossoracle.ResourceValueLog,
		powerlossoracle.ResourceOuterLeaf,
	} {
		missing := missing
		t.Run(string(missing), func(t *testing.T) {
			scenario := powerlossoracle.Scenario{
				Name:            "TestPowerLossOracleCounterexampleNewMetaMissingClosure/" + string(missing),
				Cut:             powerlossoracle.AfterMetaSync,
				ReopenAttempted: true,
				ReopenRejected:  result.Rejected,
				Generations: []powerlossoracle.Generation{
					{Sequence: 1, Recoverable: true, Complete: true, Resources: completeRootResources("old")},
					{Sequence: 2, Recoverable: true, Complete: true, Resources: resourcesWithMissing("new", missing)},
				},
			}
			if result.Rejected {
				t.Logf("public Open rejected stable-only image as permitted evidence: %v", result.Err)
				return
			}
			if err := powerlossoracle.RequireViolation(scenario.Validate(), powerlossoracle.InvariantIncompleteRecoverableRoot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// This family is the stable witness for graveyard/freelist reuse before the
// two-recoverable-root horizon advances.
func TestPowerLossOracleCounterexampleRecoverablePageReuse(t *testing.T) {
	model, opts := seedPowerLossImage(t)
	result := requirePublicReopen(t, model, opts, true)
	scenario := powerlossoracle.Scenario{
		Name:            "TestPowerLossOracleCounterexampleRecoverablePageReuse",
		Cut:             powerlossoracle.AfterIndexDataSync,
		ReopenAttempted: true,
		ReopenRejected:  result.Rejected,
		Generations: []powerlossoracle.Generation{
			{Sequence: 8, Recoverable: true, Complete: true, Resources: completeRootResources("old"), LivePages: []uint64{41, 42}},
			{Sequence: 9, Recoverable: true, Complete: true, Resources: completeRootResources("new"), LivePages: []uint64{42, 43}},
		},
		ReusedPages: []uint64{41},
	}
	if err := powerlossoracle.RequireViolation(scenario.Validate(), powerlossoracle.InvariantRecoverablePageReused); err != nil {
		t.Fatal(err)
	}
}

// This family is the stable witness for relaxed command-WAL external-RID replay
// accepting a checksum-valid but dependency-incomplete frame.
func TestPowerLossOracleCounterexampleRelaxedCommandFrameMissingRID(t *testing.T) {
	model, opts := seedPowerLossImage(t)
	result := requirePublicReopen(t, model, opts, false)
	scenario := powerlossoracle.Scenario{
		Name:            "TestPowerLossOracleCounterexampleRelaxedCommandFrameMissingRID",
		Cut:             powerlossoracle.AfterDependencyFileSync,
		ReopenAttempted: true,
		ReopenRejected:  result.Rejected,
		Generations:     []powerlossoracle.Generation{{Sequence: 3, Recoverable: true, Complete: true, Resources: completeRootResources("root")}},
		CommandFrames: []powerlossoracle.CommandFrame{
			{LSN: 11, ChecksumValid: true, Applied: true, Dependencies: []powerlossoracle.Resource{{Kind: powerlossoracle.ResourceValueLog, ID: "rid-11", Stable: true, Live: true}}},
			{LSN: 12, ChecksumValid: true, Applied: true, Dependencies: []powerlossoracle.Resource{{Kind: powerlossoracle.ResourceValueLog, ID: "rid-12", Stable: false, Live: false}}},
		},
	}
	if err := powerlossoracle.RequireViolation(scenario.Validate(), powerlossoracle.InvariantCommandReplayHole); err != nil {
		t.Fatal(err)
	}
}

// This family is the stable witness for publishCommandWALCheckpointApplied and
// cleanup removing a WAL/asset before a sealed root proves coverage.
func TestPowerLossOracleCounterexampleSourceDeletionBeforeStableCoverage(t *testing.T) {
	model, opts := seedPowerLossImage(t)
	result := requirePublicReopen(t, model, opts, true)
	scenario := powerlossoracle.Scenario{
		Name:             "TestPowerLossOracleCounterexampleSourceDeletionBeforeStableCoverage",
		Cut:              powerlossoracle.AfterWALOrAssetUnlink,
		ReopenAttempted:  true,
		ReopenRejected:   result.Rejected,
		Generations:      []powerlossoracle.Generation{{Sequence: 5, Recoverable: true, Complete: true, Resources: completeRootResources("retained")}},
		RemovedResources: []powerlossoracle.Resource{{Kind: powerlossoracle.ResourceCommandWAL, ID: "retained-wal", Stable: false, Live: false}},
	}
	if err := powerlossoracle.RequireViolation(scenario.Validate(), powerlossoracle.InvariantEarlySourceDeletion); err != nil {
		t.Fatal(err)
	}
}

// This family is the stable witness for Checkpoint, flushSyncRequested, and
// chunked cached flush apply exposing an intermediate incomplete root.
func TestPowerLossOracleCounterexampleChunkedSyncIntermediateRoot(t *testing.T) {
	model, opts := seedPowerLossImage(t)
	result := requirePublicReopen(t, model, opts, false)
	scenario := powerlossoracle.Scenario{
		Name:            "TestPowerLossOracleCounterexampleChunkedSyncIntermediateRoot",
		Cut:             powerlossoracle.AfterMetaSync,
		ReopenAttempted: true,
		ReopenRejected:  result.Rejected,
		Generations: []powerlossoracle.Generation{
			{Sequence: 21, Recoverable: true, Complete: true, Resources: completeRootResources("old")},
			{Sequence: 22, Recoverable: true, Complete: true, Resources: resourcesWithMissing("chunk-2", powerlossoracle.ResourceIndex)},
		},
	}
	if err := powerlossoracle.RequireViolation(scenario.Validate(), powerlossoracle.InvariantIncompleteRecoverableRoot); err != nil {
		t.Fatal(err)
	}
}

func TestPowerLossOracleAcknowledgementRules(t *testing.T) {
	model, opts := seedPowerLossImage(t)
	result := requirePublicReopen(t, model, opts, true)
	tests := []struct {
		name      string
		scenario  powerlossoracle.Scenario
		invariant string
	}{
		{
			name: "durable-ack-survival",
			scenario: powerlossoracle.Scenario{
				Generations:  []powerlossoracle.Generation{{Sequence: 4, Recoverable: true, Complete: true, Resources: completeRootResources("root")}},
				Acknowledged: []powerlossoracle.Acknowledgement{{Sequence: 5, Durable: true}},
			},
			invariant: powerlossoracle.InvariantDurableAckLost,
		},
		{
			name: "relaxed-loss-is-suffix",
			scenario: powerlossoracle.Scenario{
				Generations:               []powerlossoracle.Generation{{Sequence: 6, Recoverable: true, Complete: true, Resources: completeRootResources("root")}},
				Acknowledged:              []powerlossoracle.Acknowledgement{{Sequence: 1}, {Sequence: 2}, {Sequence: 3}},
				RecoveredAcknowledgements: []uint64{1, 3},
			},
			invariant: powerlossoracle.InvariantRelaxedNonSuffixLoss,
		},
		{
			name: "selected-root-is-complete-candidate",
			scenario: powerlossoracle.Scenario{
				Generations:      []powerlossoracle.Generation{{Sequence: 6, Recoverable: true, Complete: true, Resources: completeRootResources("root")}},
				SelectedSequence: 7,
			},
			invariant: powerlossoracle.InvariantSelectedRootInvalid,
		},
		{
			name: "selected-root-key-prefix-state",
			scenario: powerlossoracle.Scenario{
				Generations:               []powerlossoracle.Generation{{Sequence: 6, Recoverable: true, Complete: true, Resources: completeRootResources("root")}},
				SelectedSequence:          6,
				ExpectedKeyValuesByPrefix: map[string]map[string]string{"user/": {"user/a": "old"}},
				ObservedKeyValuesByPrefix: map[string]map[string]string{"user/": {"user/a": "new"}},
			},
			invariant: powerlossoracle.InvariantKeyStateMismatch,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.scenario.Name = "TestPowerLossOracleAcknowledgementRules/" + test.name
			test.scenario.Cut = powerlossoracle.AfterMetaSync
			test.scenario.ReopenAttempted = true
			test.scenario.ReopenRejected = result.Rejected
			if err := powerlossoracle.RequireViolation(test.scenario.Validate(), test.invariant); err != nil {
				t.Fatal(err)
			}
		})
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
			result, reopened, closeFn, err := powerlossoracle.ReopenStable(model, opts, true)
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

func completeRootResources(prefix string) []powerlossoracle.Resource {
	return []powerlossoracle.Resource{
		{Kind: powerlossoracle.ResourceIndex, ID: prefix + "-index", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceFreelist, ID: prefix + "-freelist", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceValueLog, ID: prefix + "-vlog", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceOuterLeaf, ID: prefix + "-leaf", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceAuxiliary, ID: prefix + "-asset", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceDirectory, ID: prefix + "-dir", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceSeal, ID: prefix + "-seal", Stable: true, Live: true},
		{Kind: powerlossoracle.ResourceCommandWAL, ID: prefix + "-wal", Stable: true, Live: true},
	}
}

func resourcesWithMissing(prefix string, missing powerlossoracle.ResourceKind) []powerlossoracle.Resource {
	resources := completeRootResources(prefix)
	for index := range resources {
		if resources[index].Kind == missing {
			resources[index].Stable = false
			resources[index].Live = false
		}
	}
	return resources
}

func TestPowerLossOracleStableNamesArePortable(t *testing.T) {
	for _, cut := range powerlossoracle.CutPoints {
		if strings.TrimSpace(string(cut)) == "" || strings.ContainsAny(string(cut), " /\\") {
			t.Fatalf("cut point %q is not a portable stable identifier", cut)
		}
	}
	if err := powerlossoracle.RequireViolation(nil, powerlossoracle.InvariantCommandReplayHole); err == nil {
		t.Fatal("RequireViolation must reject a missing counterexample")
	}
}
