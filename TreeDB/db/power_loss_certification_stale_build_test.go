package db_test

import (
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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
	staleBuildBaseCutID        = "cut/public-stale-build-base-retry-saved-primary-v5/after-meta-sync/000"
	staleBuildBaseVariantID    = "cut/public-stale-build-base-retry-saved-primary-v5/after-meta-sync/000/variant/full-writeback"
	staleBuildBaseSeed         = uint64(13546396532373078556)
	staleBuildCapsuleCutID     = "cut/public-stale-build-base-retry-capsule-v6/after-index-data-sync/000"
	staleBuildCapsuleVariantID = staleBuildCapsuleCutID + "/variant/full-writeback"
	staleBuildCapsuleSeed      = uint64(13362006158980588834)
)

// TestPowerLossCertificationStaleBuildBaseSavedPrimaryV5PublicReopen retains the #3865
// counterexample at a real publication rendezvous. Its only hook installer is
// compiled into the db test binary; production builds expose no mutable hook
// API. A successor finishes against an old base, the predecessor becomes
// durable, the stale successor is rejected, and a distinguishable retry is
// reopened through public treedb.Open.
func TestPowerLossCertificationStaleBuildBaseSavedPrimaryV5PublicReopen(t *testing.T) {
	runStaleBuildBasePublicReopen(t, false)
}

func TestPowerLossCertificationStaleBuildBasePrimaryCapsulePublicReopenV6(t *testing.T) {
	runStaleBuildBasePublicReopen(t, true)
}

func runStaleBuildBasePublicReopen(t *testing.T, capsule bool) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	if capsule {
		opts.IndexPrimaryDirectory = true
	}
	if !capsule {
		// Construct the separately versioned saved V5 format. The original DATA V1
		// witness and generation8 evidence are archived with zero current coverage.
		// Public Open admits this real preexisting V5 arena and uses DATA META.
		dbpkg.CreateSavedPrimaryFormatV5ForTest(t, dir, dbpkg.Options{
			ResolvedProfile:            dbpkg.ProfileNoWALFast,
			IndexOuterLeavesInValueLog: opts.IndexOuterLeavesInValueLog,
			LeafPrefixCompression:      opts.LeafPrefixCompression,
			IndexColumnarLeaves:        opts.IndexColumnarLeaves,
			IndexPackedValuePtr:        opts.IndexPackedValuePtr,
			IndexInternalBaseDelta:     opts.IndexInternalBaseDelta,
			IndexAdaptiveLeafEncoding:  opts.IndexAdaptiveLeafEncoding,
		})
	}
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
	header, err := os.ReadFile(filepath.Join(dir, "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	wantMagic := "TDPRBANK"
	if capsule {
		wantMagic = "TDPRCAPB"
	}
	if len(header) < page.PageSize || string(header[16:24]) != wantMagic || !page.VerifyChecksumNonMutating(header[:page.PageSize]) {
		t.Fatal("public producer format differs from bound witness")
	}
	if err := backend.SetSync([]byte("stale-build/public-baseline"), []byte("stable")); err != nil {
		t.Fatal(err)
	}

	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	requireStaleBuildSelector(t, capsule)
	metaSyncs := 0
	var capsuleIdentity rootpublication.StableIdentity
	var sealedCapsule rootpublication.PrimaryCapsuleViewV6
	capsuleSealed := false
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
		if checkpointArmed && capsule && event.Point == durabilitycut.AfterPublicationSealWrite && filepath.Base(event.Path) == "index.db.primary" {
			if event.StablePath == nil || event.Length != rootpublication.PrimaryCapsuleSizeV6 || (event.Offset != 2*page.PageSize && event.Offset != 5*page.PageSize) {
				return errors.New("retry capsule seal lacks exact original slot")
			}
			image := make([]byte, rootpublication.PrimaryCapsuleSizeV6)
			if _, err := event.StablePath.ReadAt(image, event.Offset); err != nil {
				return err
			}
			var uuid [16]byte
			copy(uuid[:], image[32:48])
			view, err := rootpublication.DecodePrimaryCapsuleV6(image, uint64((event.Offset/page.PageSize-2)/3), uuid)
			if err != nil {
				return err
			}
			if view.Current().Record.CommitSeq != 4 || view.Parent().Record.CommitSeq != 3 {
				return errors.New("retry capsule seal has wrong current/parent horizon")
			}
			capsuleIdentity, err = rootpublication.StableIdentityFromFile(event.StablePath)
			if err != nil {
				return err
			}
			sealedCapsule = view
			capsuleSealed = true
			if os.Getenv(powerlossoracle.EnvEvidenceDir) != "" {
				if os.Getenv(powerlossoracle.EnvEvidenceReplayWindow) != staleBuildCapsuleVariantID {
					return errors.New("capsule evidence requires its exact seal replay window")
				}
				if err := model.BeginReplayWindow(staleBuildCapsuleVariantID); err != nil {
					return err
				}
			}
		}
		terminal := !capsule && event.Point == durabilitycut.AfterMetaSync
		if checkpointArmed && capsule && capsuleSealed && event.Point == durabilitycut.AfterIndexDataSync && filepath.Base(event.Path) == "index.db.primary" {
			if event.StablePath == nil {
				return errors.New("capsule fence lacks original handle")
			}
			identity, err := rootpublication.StableIdentityFromFile(event.StablePath)
			if err != nil {
				return err
			}
			if !rootpublication.SamePhysicalIdentity(identity, capsuleIdentity) {
				return errors.New("capsule fence changed original identity")
			}
			terminal = true
		}
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
		if (!capsule && strings.HasPrefix(event, "cut:after-meta-sync:")) || (capsule && strings.HasPrefix(event, "cut:after-index-data-sync:")) {
			terminalMetaSyncs++
		}
	}
	if capsule {
		trace := model.Trace()
		if !capsuleSealed || metaSyncs != 1 || len(trace) == 0 || trace[len(trace)-1] != "cut:after-index-data-sync:index" {
			t.Fatalf("capsule terminal trace lacks exact completed original fence: %v", trace)
		}
	}
	if !capsule && terminalMetaSyncs != 1 {
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
	if capsule {
		assertStaleBuildCapsuleRecoveryStats(t, result, sealedCapsule)
	} else {
		assertStaleBuildRecoveryStats(t, result)
	}
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
		{key: "treedb.durable_root.freelist.generation", value: "4"},
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

func requireStaleBuildSelector(t *testing.T, capsule bool) {
	t.Helper()
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if selector == (powerlossoracle.ReplaySelector{}) {
		return
	}
	if err := validateStaleBuildSelector(selector, capsule); err != nil {
		t.Fatal(err)
	}
}

func validateStaleBuildSelector(selector powerlossoracle.ReplaySelector, capsule bool) error {
	cutID, variantID, seed := staleBuildBaseCutID, staleBuildBaseVariantID, staleBuildBaseSeed
	if capsule {
		cutID, variantID, seed = staleBuildCapsuleCutID, staleBuildCapsuleVariantID, staleBuildCapsuleSeed
	}
	if selector.CutID != cutID || selector.VariantID != variantID || selector.Seed != seed {
		return fmt.Errorf("replay selector=(%q,%q,%d) want=(%q,%q,%d)", selector.CutID, selector.VariantID, selector.Seed, cutID, variantID, seed)
	}
	return nil
}

func TestStaleBuildCurrentSelectorsRejectHistoricalAndCrossFormat(t *testing.T) {
	v5 := powerlossoracle.ReplaySelector{CutID: staleBuildBaseCutID, VariantID: staleBuildBaseVariantID, Seed: staleBuildBaseSeed}
	v6 := powerlossoracle.ReplaySelector{CutID: staleBuildCapsuleCutID, VariantID: staleBuildCapsuleVariantID, Seed: staleBuildCapsuleSeed}
	historical := powerlossoracle.ReplaySelector{CutID: "cut/public-stale-build-base-retry-stable-image/after-meta-sync/000", VariantID: "cut/public-stale-build-base-retry-stable-image/after-meta-sync/000/variant/full-writeback", Seed: 12505447533306515078}
	for _, capsule := range []bool{false, true} {
		correct, wrong := v5, v6
		if capsule {
			correct, wrong = v6, v5
		}
		if err := validateStaleBuildSelector(correct, capsule); err != nil {
			t.Fatal(err)
		}
		seedMismatch := correct
		seedMismatch.Seed++
		variantMismatch := correct
		variantMismatch.VariantID = wrong.VariantID
		for _, refused := range []powerlossoracle.ReplaySelector{historical, wrong, seedMismatch, variantMismatch} {
			if err := validateStaleBuildSelector(refused, capsule); err == nil {
				t.Fatalf("format capsule=%v accepted foreign selector %+v", capsule, refused)
			}
		}
	}
}

func assertStaleBuildCapsuleRecoveryStats(t *testing.T, result powerlossreopen.Result, sealed rootpublication.PrimaryCapsuleViewV6) {
	t.Helper()
	if result.CommitSeq != 4 || result.AppliedLSN != 0 {
		t.Fatalf("capsule recovered horizon=(%d,%d), want (4,0)", result.CommitSeq, result.AppliedLSN)
	}
	current, parent := sealed.Current().Record, sealed.Parent().Record
	if current.CommitSeq != 4 || parent.CommitSeq != 3 {
		t.Fatal("unbound capsule current/parent")
	}
	want := map[string]string{
		"treedb.profile.resolved":                 "no_wal_fast",
		"treedb.applied_command_lsn":              "0",
		"treedb.command_wal.durable_wal_lsn":      "0",
		"treedb.durable_root.selected_slot":       "1",
		"treedb.durable_root.commit_seq":          "4",
		"treedb.durable_root.slot0.commit_seq":    "3",
		"treedb.durable_root.slot1.commit_seq":    "4",
		"treedb.durable_root.freelist.generation": "4",
		"treedb.durable_root.manifest.entries":    "1",
		"treedb.durable_root.durable_seq":         "4",
	}
	if sealed.Slot() != 1 || current.Freelist.GenerationID != 4 || parent.Freelist.GenerationID != 3 || current.Manifest.EntryCount != 1 || current.DurableSeq != 4 || parent.DurableSeq != 3 {
		t.Fatal("sealed capsule selection/generation/parent differs from versioned horizon")
	}
	for key, value := range want {
		if got, ok := result.Stats[key]; !ok || got != value {
			t.Fatalf("capsule recovery stats[%q]=%q present=%v want=%q", key, got, ok, value)
		}
	}
}
