package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// A committed inline typed member belongs to the complete logical cut before
// its captured-prefix physical build. A post-close closure failure must keep
// that whole cut and the original failed build alive without installing a disk
// basis. Only a fresh ordinary owner may recover its canonical aggregate.
func TestNativeExistingColumnAcceptedBuildFailureCustody(t *testing.T) {
	runNativeColumnAcceptedBuildFailure(t, false)
}

func TestNativeTwoColumnAcceptedBuildFailureCustody(t *testing.T) {
	runNativeColumnAcceptedBuildFailure(t, true)
}

func runNativeColumnAcceptedBuildFailure(t *testing.T, two bool) {
	t.Helper()
	childEnv := "GOMAP_NATIVE_COLUMN_BUILD_FAILURE_DIR"
	names := []string{"events"}
	if two {
		childEnv += "_TWO"
		names = append(names, "events_two")
	}
	basis, accepted := uint64(2*len(names)), uint64(2*len(names)+1)
	marker := fmt.Sprintf("COLUMN BUILD FAILURE basis=%d accepted=%d original-build-retained", basis, accepted)
	const exitCode = 97
	options := func(dir string) treedb.Options {
		opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
		opts.MemtableMode = "cow_btree"
		opts.FlushThreshold = 1 << 30
		opts.BackgroundCheckpointInterval = -1
		opts.BackgroundCheckpointIdleDuration = -1
		opts.MaxWALBytes = -1
		return opts
	}
	seed := []byte(`{"time_us":7,"payload":"seed"}`)
	pending := []byte(`{"time_us":11,"payload":"accepted"}`)
	rawKey, rawValue := []byte("column/build/raw"), []byte("complete accepted aggregate")
	checkJSON := func(got, want []byte) {
		t.Helper()
		var left, right any
		if json.Unmarshal(got, &left) != nil || json.Unmarshal(want, &right) != nil {
			t.Fatalf("invalid JSON: got=%q want=%q", got, want)
		}
		a, _ := json.Marshal(left)
		b, _ := json.Marshal(right)
		if !bytes.Equal(a, b) {
			t.Fatalf("JSON got=%s want=%s", a, b)
		}
	}

	if dir := os.Getenv(childEnv); dir == "" {
		dir = t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
		cmd.Env = append(os.Environ(), childEnv+"="+dir)
		output, err := cmd.CombinedOutput()
		t.Logf("accepted build failure child:\n%s", output)
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != exitCode || !bytes.Contains(output, []byte(marker)) || bytes.Contains(output, []byte("DATA RACE")) {
			t.Fatalf("accepted build failure child: %v\n%s", err, output)
		}
		before := workstats.Read().Replay
		database, err := treedb.Open(options(dir))
		if err != nil {
			t.Fatal(err)
		}
		defer database.Close()
		after := workstats.Read().Replay
		if after.FramesApplied-before.FramesApplied != 1 || after.FrameErrors != before.FrameErrors {
			t.Fatalf("accepted canonical suffix replay: %+v -> %+v", before, after)
		}
		handle, err := database.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager := NewCollectionManager(handle)
		defer manager.Close()
		for _, name := range names {
			collection, err := manager.OpenCollection(name)
			if err != nil {
				t.Fatal(err)
			}
			for id, want := range map[string][]byte{"seed": seed, "accepted": pending} {
				got, err := collection.Get([]byte(id))
				if err != nil {
					t.Fatal(err)
				}
				checkJSON(got, want)
			}
		}
		if got, err := database.Get(rawKey); err != nil || !bytes.Equal(got, rawValue) {
			t.Fatalf("replayed raw member=%q: %v", got, err)
		}
		if handle.CommandWALNextLSN() != accepted+1 {
			t.Fatalf("replay changed original identity: next=%d", handle.CommandWALNextLSN())
		}
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		if stats := handle.ValueLogIdentityPinRegistry().Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
			t.Fatalf("reopened owner final resources=%+v", stats)
		}
		return
	}

	database, err := treedb.Open(options(os.Getenv(childEnv)))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(handle)
	defer manager.Close()
	collections := make([]*Collection, len(names))
	for i, name := range names {
		if _, err := manager.CreateCollection(&CollectionMeta{Name: name, Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true, Columns: []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart}},
			RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		}}}); err != nil {
			t.Fatalf("ordinary native typed Create %s: %v", name, err)
		}
		collections[i], err = manager.OpenCollection(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := collections[i].InsertBatch([][]byte{[]byte("seed")}, [][]byte{seed}); err != nil {
			t.Fatal(err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	physicalLSN := func() uint64 {
		// A failed accepted build poisons new snapshot admission. Scalar state
		// observation remains available to verify that it installed no disk cut.
		token, ok := handle.StateToken()
		if !ok {
			t.Fatal("missing physical state token")
		}
		return token.AppliedCommandLSN
	}
	if physicalLSN() != basis || handle.CommandWALNextLSN() != accepted {
		t.Fatalf("typed seed basis=%d next=%d want=%d/%d", physicalLSN(), handle.CommandWALNextLSN(), basis, accepted)
	}
	held := make([]*CollectionReadView, len(names))
	for i, collection := range collections {
		held[i], err = collection.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		defer held[i].Close()
	}
	checkHeld := func() {
		t.Helper()
		for i, view := range held {
			got, err := view.FetchDocumentsByID([][]byte{[]byte("seed"), []byte("accepted")}, DocumentFetchOptions{})
			if err != nil || len(got.Results) != 2 || !got.Results[0].Found || got.Results[1].Found {
				t.Fatalf("held typed cut %s: %+v %v", names[i], got.Results, err)
			}
			checkJSON(got.Results[0].Document, seed)
		}
	}
	checkHeld() // Load the actual captured typed assets before the newer cut.
	native, err := NewNativeJSONManager(handle)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := native.Begin(names[0], names[1:]...)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	for _, name := range names {
		if err := tx.InsertBatchInto(name, [][]byte{[]byte("accepted")}, [][]byte{pending}); err != nil {
			t.Fatalf("stage %s: %v", name, err)
		}
		if got, err := tx.GetFrom(name, []byte("accepted")); err != nil {
			t.Fatal(err)
		} else {
			checkJSON(got, pending)
		}
	}
	if err := tx.SetRaw(rawKey, rawValue); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("canonical inline typed Commit: %v", err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	sequence := database.Stats()["treedb.cache.cow.txn.logical_seq"]
	if physicalLSN() != basis || handle.CommandWALNextLSN() != accepted+1 {
		t.Fatal("accepted aggregate changed the physical basis or lost canonical identity")
	}
	current := make([]*NativeJSONRead, len(names))
	var engine *nativeEngineVersion
	for i, name := range names {
		current[i], err = native.Capture(name)
		if err != nil {
			t.Fatal(err)
		}
		defer current[i].Close()
		original, ok := current[i].capture.State().(*nativeEngineVersion)
		if !ok || original == nil || original.build != nil || (engine != nil && engine != original) {
			t.Fatal("typed collections did not retain the same original accepted engine")
		}
		engine = original
	}
	rawCut, err := database.NewConditionalTxnWithSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer rawCut.Close()
	checkCompleteCut := func() {
		t.Helper()
		for i, view := range current {
			for id, want := range map[string][]byte{"seed": seed, "accepted": pending} {
				got, err := view.Get([]byte(id))
				if err != nil {
					t.Fatalf("complete %s/%s: %v", names[i], id, err)
				}
				checkJSON(got, want)
			}
		}
		if got, err := rawCut.Get(rawKey); err != nil || !bytes.Equal(got, rawValue) {
			t.Fatalf("retained complete-cut raw member=%q: %v", got, err)
		}
	}
	checkCompleteCut()
	physicalBefore, ok := handle.StateToken()
	if !ok {
		t.Fatal("missing pre-build physical state token")
	}

	var selected atomic.Bool
	// Match original namespace paths and descriptor namespace/FileID together;
	// segment basenames alone can coincide in the two independent namespaces.
	segmentDirs := make(map[string]string, len(names))
	for i, name := range names {
		namespace := collections[i].Meta().Options.ColumnStore.AssetManager.Namespace
		path, err := columnAssetManagerNamespaceForRoot(handle.ColumnAssetRootDir(), namespace)
		if err != nil {
			t.Fatal(err)
		}
		segmentDirs[namespace] = filepath.Clean(path.SegmentDir)
		if namespace == "" {
			t.Fatalf("missing original asset namespace for %s", name)
		}
	}
	targetNamespace := collections[len(collections)-1].Meta().Options.ColumnStore.AssetManager.Namespace
	observed := make(map[string]rootpublication.StableIdentity)
	outputs := make(map[string]rootpublication.StableIdentity)
	restoreObserve := setColumnAssetStableBeforeObserveTestHook(func(parent, file *os.File, name string) {
		if handle.CommandWALNextLSN() == accepted+1 {
			for namespace, directory := range segmentDirs {
				if filepath.Clean(parent.Name()) != directory {
					continue
				}
				identity, err := rootpublication.StableIdentityFromFile(file)
				if err != nil {
					t.Fatal(err)
				}
				observed[filepath.Join(namespace, name)] = identity
				outputs[namespace] = identity
			}
		}
	})
	defer restoreObserve()
	var failedIdentity rootpublication.StableIdentity
	restore := setColumnAssetStableObligationTestHook(func(ref ColumnAssetRef, _ rootpublication.StableLogicalObligation, _ *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision {
		if ref.Kind == ColumnAssetKindTCS1TypedColumnPart && ref.Namespace == targetNamespace && handle.CommandWALNextLSN() == accepted+1 && selected.CompareAndSwap(false, true) {
			if two {
				first := outputs[collections[0].Meta().Options.ColumnStore.AssetManager.Namespace]
				if first == (rootpublication.StableIdentity{}) || handle.ValueLogIdentityPinRegistry().PinCount(first) == 0 {
					t.Fatal("second typed construction began without the first original output pin")
				}
			}
			failedIdentity = observed[filepath.Join(ref.Namespace, columnAssetSegmentFileName(ref.FileID))]
			return columnAssetStableCaptureOmitToken
		}
		return columnAssetStableCaptureKeep
	})
	defer restore()
	err = database.Checkpoint()
	if !selected.Load() || !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("real accepted build closure failure: selected=%v err=%v", selected.Load(), err)
	}
	restore()
	restoreObserve()
	physicalAfter, ok := handle.StateToken()
	if !ok || physicalAfter != physicalBefore || handle.CommandWALNextLSN() != accepted+1 || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence {
		t.Fatal("failed build changed the accepted logical cut or installed a partial disk basis")
	}
	build := engine.build
	if build == nil || build.refund == nil || build.allocation.parent == nil || !errors.Is(build.failure, rootpublication.ErrUnresolvedResource) || build.complete {
		t.Fatal("accepted failure lost its original incomplete physical build/refund")
	}
	failure := build.failure
	refund, allocation := build.refund, build.allocation.parent
	// The construction session has closed normally. The original native build
	// must retain the captured partial set, including the exact new output's pin;
	// the missing logical obligation prevents installing that physical basis.
	if failedIdentity == (rootpublication.StableIdentity{}) || failedIdentity != outputs[targetNamespace] {
		t.Fatal("missing exact failed second output identity")
	}
	checkOutputPins := func(wantDeleteError error) {
		t.Helper()
		var previous rootpublication.StableIdentity
		for _, collection := range collections {
			namespace := collection.Meta().Options.ColumnStore.AssetManager.Namespace
			identity := outputs[namespace]
			if identity == (rootpublication.StableIdentity{}) || identity == previous || handle.ValueLogIdentityPinRegistry().PinCount(identity) == 0 {
				t.Fatalf("original partial output lost exact independent pin: namespace=%s identity=%+v", namespace, identity)
			}
			previous = identity
			if lease, err := handle.ValueLogIdentityPinRegistry().BeginDelete(identity); lease != nil || !errors.Is(err, wantDeleteError) {
				if lease != nil {
					lease.Abort()
				}
				t.Fatalf("partial output deletion fence %s: %v", namespace, err)
			}
		}
	}
	checkOutputPins(rootpublication.ErrResourcePinned)
	if stats := handle.ValueLogIdentityPinRegistry().Stats(); stats.ActivePins == 0 || stats.ActiveIdentities == 0 || stats.ActiveStableNamespaceLinks == 0 {
		t.Fatalf("partial output authority lost: %+v", stats)
	}
	checkHeld()
	checkCompleteCut()
	if err := database.Set([]byte("after-build-failure"), []byte("blocked")); !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("poisoned owner accepted subsequent mutation: %v", err)
	}
	for i := range held {
		if err := held[i].Close(); err != nil {
			t.Fatal(err)
		}
		if err := current[i].Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawCut.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); !errors.Is(err, failure) {
		t.Fatalf("Close lost original accepted build failure: %v", err)
	}
	if engine.build != build || build.refund != refund || build.allocation.parent != allocation || !errors.Is(build.failure, failure) {
		t.Fatal("Close replaced or refunded the original failed accepted build")
	}
	// Failed accepted work is terminal for this owner. Do not reset its ledger,
	// retry output construction, or manufacture cleanup after the failed Close.
	// Registry Close makes new deletion leases terminal while retaining each
	// original output pin until its real release.
	checkOutputPins(retainedalloc.ErrClosed)
	fmt.Println(marker)
	os.Exit(exitCode)
}
