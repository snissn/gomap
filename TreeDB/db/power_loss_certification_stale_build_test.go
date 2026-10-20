package db_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	dbpkg "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	staleBuildDataCutID     = "cut/public-stale-build-base-retry-data-current/after-meta-sync/000"
	staleBuildDataVariantID = staleBuildDataCutID + "/variant/full-writeback"
	staleBuildDataSeed      = uint64(15748367916217096863)
)

// TestPowerLossCertificationStaleBuildBaseDATAPublicReopen binds a fresh DATA
// producer to the original stale-base publication rendezvous. The private test
// hooks force a completed successor build to lose its basis before acceptance;
// only its distinguishable retry may survive checkpoint and public crash reopen.
// This is a new witness, separate from the archived historical DATA V1 evidence.
func TestPowerLossCertificationStaleBuildBaseDATAPublicReopen(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	// This frozen witness includes an external USER leaf dependency. Keep its
	// placement explicit rather than inheriting the fresh-database default.
	opts.IndexOuterLeavesInValueLog = true
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	backend, closeBackend, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = closeBackend()
		}
	}()
	if _, err := os.Stat(filepath.Join(dir, "index.db.primary")); !os.IsNotExist(err) {
		t.Fatalf("DATA producer created retired PRIMARY namespace: %v", err)
	}
	if err := backend.SetSync([]byte("stale-build/public-baseline"), []byte("stable")); err != nil {
		t.Fatal(err)
	}

	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	requireStaleBuildSelector(t)
	metaSyncs := 0
	frozen := false
	checkpointArmed := false
	var observeMu sync.Mutex
	restoreCut := durabilitycut.Install(func(event durabilitycut.Event) error {
		observeMu.Lock()
		defer observeMu.Unlock()
		if frozen {
			return nil
		}
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		terminal := event.Point == durabilitycut.AfterMetaSync
		if checkpointArmed && terminal {
			metaSyncs++
			frozen = true
		}
		return nil
	})

	firstReleased := make(chan struct{})
	releaseFirst := make(chan struct{})
	restoreHooks := backend.SetOrderedRootStaleBuildHooksForTest(dbpkg.OrderedRootStaleBuildHooksForTest{
		AfterFinalizeRootSerializationRelease: func() {
			close(firstReleased)
			<-releaseFirst
		},
	})
	publishDelta := func(key, value string) error {
		delta := mustFrozenStaleBuildDelta(t, key, value)
		_, _, err := backend.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
			return delta.NewIterator(nil, nil), nil
		})
		return err
	}

	firstErr := make(chan error, 1)
	go func() { firstErr <- publishDelta("sys/certification/first", "first") }()
	select {
	case <-firstReleased:
	case <-time.After(5 * time.Second):
		restoreHooks()
		restoreCut()
		t.Fatal("predecessor did not release root serialization")
	}
	restoreHooks()

	secondAtFinalize := make(chan struct{})
	releaseSecond := make(chan struct{})
	restoreHooks = backend.SetOrderedRootStaleBuildHooksForTest(dbpkg.OrderedRootStaleBuildHooksForTest{
		BeforeFinalizeCommit: func() {
			close(secondAtFinalize)
			<-releaseSecond
		},
	})
	secondErr := make(chan error, 1)
	go func() { secondErr <- publishDelta("sys/certification/second", "stale-second") }()
	select {
	case <-secondAtFinalize:
	case <-time.After(5 * time.Second):
		restoreHooks()
		restoreCut()
		t.Fatal("successor did not finish its stale root build")
	}
	close(releaseFirst)
	if err := <-firstErr; err != nil {
		restoreHooks()
		restoreCut()
		t.Fatalf("predecessor publication: %v", err)
	}
	predecessor, ok := backend.StateToken()
	if !ok {
		restoreHooks()
		restoreCut()
		t.Fatal("predecessor state token unavailable")
	}
	close(releaseSecond)
	staleErr := <-secondErr
	restoreHooks()
	if staleErr == nil || !strings.Contains(staleErr.Error(), "durable-root candidate base changed") {
		restoreCut()
		t.Fatalf("stale successor error=%v", staleErr)
	}
	if dbpkg.CommitPublicationAccepted(staleErr) {
		restoreCut()
		t.Fatalf("stale successor was marked accepted: %v", staleErr)
	}
	assertRejectedStaleBuildAbsent(t, backend, predecessor)

	// Close the predecessor's publication before arming the terminal cut. On a
	// loaded runner, PublishOrderedRootDeltaGroupWithSystemDeltaBuilder can
	// return while its relaxed publication is still queued; otherwise the first
	// observed meta sync below can belong to the predecessor rather than the
	// distinguishable retry.
	if err := backend.Checkpoint(); err != nil {
		restoreCut()
		t.Fatalf("checkpoint predecessor before retry: %v", err)
	}
	retryModel, err := powerlossoracle.Capture(dir)
	if err != nil {
		restoreCut()
		t.Fatalf("capture retry durability baseline: %v", err)
	}
	observeMu.Lock()
	// The stale-build rendezvous above is asserted behaviorally. Rebase the
	// byte model after its predecessor checkpoint so the terminal evidence
	// trace addresses only the distinguishable retry's meta-sync occurrence.
	// Otherwise the predecessor checkpoint and retry both appear as occurrence
	// zero candidates and the evidence writer correctly rejects the trace.
	model = retryModel
	checkpointArmed = true
	observeMu.Unlock()
	if err := publishDelta("sys/certification/second", "retry-second"); err != nil {
		restoreCut()
		t.Fatalf("successor retry: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		restoreCut()
		t.Fatalf("checkpoint predecessor and retry: %v", err)
	}
	restoreCut()
	if !frozen {
		t.Fatalf("checkpoint emitted %d completed meta syncs, want one", metaSyncs)
	}
	if err := closeBackend(); err != nil {
		t.Fatal(err)
	}
	closed = true
	terminalMetaSyncs := 0
	for _, event := range model.Trace() {
		if strings.HasPrefix(event, "cut:after-meta-sync:") {
			terminalMetaSyncs++
		}
	}
	if terminalMetaSyncs != 1 {
		t.Fatalf("terminal retry trace contains %d after-meta-sync events, want one", terminalMetaSyncs)
	}

	result, recovered, closeRecovered, err := powerlossreopen.Stable(model, opts, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closeRecovered() }()
	if result.Rejected || recovered == nil {
		t.Fatalf("public Open rejected stale-build stable image: %v", result.Err)
	}
	assertStaleBuildRecoveryStats(t, result)
	value, err := recovered.Get([]byte("stale-build/public-baseline"))
	if err != nil || string(value) != "stable" {
		t.Fatalf("recovered public baseline=%q err=%v want stable", value, err)
	}
	if err := closeRecovered(); err != nil {
		t.Fatal(err)
	}
	assertStableStaleBuildSystemValues(t, model, opts)
	t.Logf("recovered rejected-stale counterexample at commit %d", result.CommitSeq)
}

func assertStaleBuildRecoveryStats(t *testing.T, result powerlossreopen.Result) {
	t.Helper()
	if result.CommitSeq != 4 || result.AppliedLSN != 0 {
		t.Fatalf("recovered horizon=(commit=%d applied_lsn=%d), want (commit=4 applied_lsn=0)", result.CommitSeq, result.AppliedLSN)
	}
	// Commit sequence and allocator generation are separate authorities. Fresh
	// DATA starts at commit 1 / generation 2. Baseline SetSync combines its
	// visible and seal publication at commit 2 / generation 3. The ordered
	// predecessor and retry each prepare visible and seal generations:
	// predecessor is commit 3 / generation 5, retry is commit 4 / generation 7.
	// Rejected work has no acceptance; Checkpoint waits for the accepted prefix
	// rather than adding a logical commit. The two recovery slots remain
	// commits 3 and 4, with immediate parents 2 and 3 respectively.
	t.Logf("fresh DATA recovery stats: %+v", result.Stats)
	want := []struct {
		key   string
		value string
	}{
		{key: "treedb.profile.resolved", value: "no_wal_fast"},
		{key: "treedb.applied_command_lsn", value: "0"},
		{key: "treedb.command_wal.durable_wal_lsn", value: "0"},
		{key: "treedb.durable_root.selected_slot", value: "1"},
		{key: "treedb.durable_root.commit_seq", value: "4"},
		{key: "treedb.durable_root.slot0.commit_seq", value: "3"},
		{key: "treedb.durable_root.slot1.commit_seq", value: "4"},
		{key: "treedb.durable_root.freelist.generation", value: "7"},
		{key: "treedb.durable_root.manifest.entries", value: "1"},
		{key: "treedb.durable_root.durable_seq", value: "4"},
	}
	for _, item := range want {
		got, ok := result.Stats[item.key]
		if !ok {
			t.Fatalf("recovery stats missing %q", item.key)
		}
		if got != item.value {
			t.Fatalf("recovery stats[%q]=%q, want %q", item.key, got, item.value)
		}
	}
}

func mustFrozenStaleBuildDelta(t *testing.T, key, value string) memtable.Table {
	t.Helper()
	delta, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		delta.SetEntry([]byte(key), []byte(value), page.ValuePtr{}, 0)
	}
	delta.Freeze()
	return delta
}

func assertRejectedStaleBuildAbsent(t *testing.T, backend *dbpkg.DB, predecessor dbpkg.StateToken) {
	t.Helper()
	current, ok := backend.StateToken()
	if !ok {
		t.Fatal("post-rejection state token unavailable")
	}
	if current != predecessor {
		t.Fatalf("stale rejection changed state token: got=%+v want predecessor=%+v", current, predecessor)
	}
	snapshot := backend.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("post-rejection snapshot unavailable")
	}
	defer func() { _ = snapshot.Close() }()
	first, err := snapshot.GetAtRoot(current.SystemRootPageID, []byte("sys/certification/first"))
	if err != nil || string(first) != "first" {
		t.Fatalf("predecessor value=%q err=%v want first", first, err)
	}
	second, err := snapshot.GetAtRoot(current.SystemRootPageID, []byte("sys/certification/second"))
	if !errors.Is(err, tree.ErrKeyNotFound) || len(second) != 0 {
		t.Fatalf("rejected stale successor became visible as %q", second)
	}
}

func assertStableStaleBuildSystemValues(t *testing.T, model *powerlossoracle.Model, sourceOpts treedb.Options) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "backend-recovery")
	if err := model.MaterializeStable(root); err != nil {
		t.Fatal(err)
	}
	release, err := model.InstallStableIdentityOverrides(root)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	opts := sourceOpts
	opts.Dir = root
	opts.ReadOnly = true
	backend, closeBackend, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatalf("public OpenBackend rejected stable stale-build image: %v", err)
	}
	defer func() { _ = closeBackend() }()
	snapshot := backend.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("recovered backend snapshot unavailable")
	}
	defer func() { _ = snapshot.Close() }()
	state, ok := snapshot.StateToken()
	if !ok {
		t.Fatal("recovered backend state token unavailable")
	}
	for key, want := range map[string]string{
		"sys/certification/first":  "first",
		"sys/certification/second": "retry-second",
	} {
		value, err := snapshot.GetAtRoot(state.SystemRootPageID, []byte(key))
		if err != nil || string(value) != want {
			t.Fatalf("recovered system value %q=%q err=%v want=%q", key, value, err, want)
		}
	}
}

func requireStaleBuildSelector(t *testing.T) {
	t.Helper()
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if selector == (powerlossoracle.ReplaySelector{}) {
		return
	}
	if err := validateStaleBuildSelector(selector); err != nil {
		t.Fatal(err)
	}
}

func validateStaleBuildSelector(selector powerlossoracle.ReplaySelector) error {
	if selector.CutID != staleBuildDataCutID || selector.VariantID != staleBuildDataVariantID || selector.Seed != staleBuildDataSeed {
		return fmt.Errorf("replay selector=(%q,%q,%d) want=(%q,%q,%d)", selector.CutID, selector.VariantID, selector.Seed, staleBuildDataCutID, staleBuildDataVariantID, staleBuildDataSeed)
	}
	return nil
}

func TestStaleBuildDATASelectorRejectsHistoricalAndRetiredFormats(t *testing.T) {
	correct := powerlossoracle.ReplaySelector{CutID: staleBuildDataCutID, VariantID: staleBuildDataVariantID, Seed: staleBuildDataSeed}
	if err := validateStaleBuildSelector(correct); err != nil {
		t.Fatal(err)
	}
	seedMismatch := correct
	seedMismatch.Seed++
	variantMismatch := correct
	variantMismatch.VariantID += "/foreign"
	for _, refused := range []powerlossoracle.ReplaySelector{
		{CutID: "cut/public-stale-build-base-retry-stable-image/after-meta-sync/000", VariantID: "cut/public-stale-build-base-retry-stable-image/after-meta-sync/000/variant/full-writeback", Seed: 12505447533306515078},
		{CutID: "cut/public-stale-build-base-retry-saved-primary-v5/after-meta-sync/000", VariantID: "cut/public-stale-build-base-retry-saved-primary-v5/after-meta-sync/000/variant/full-writeback", Seed: 13546396532373078556},
		{CutID: "cut/public-stale-build-base-retry-capsule-v6/after-index-data-sync/000", VariantID: "cut/public-stale-build-base-retry-capsule-v6/after-index-data-sync/000/variant/full-writeback", Seed: 13362006158980588834},
		seedMismatch, variantMismatch,
	} {
		if err := validateStaleBuildSelector(refused); err == nil {
			t.Fatalf("DATA accepted foreign selector %+v", refused)
		}
	}
}
