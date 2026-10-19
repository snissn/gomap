package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// A committed inline typed member belongs to the complete logical cut before
// its captured-prefix physical build. A real output rollback failure must keep
// that whole cut and the original failed build alive without installing a disk
// basis. Only a fresh ordinary owner may recover its canonical aggregate.
func TestNativeExistingColumnAcceptedBuildFailureCustody(t *testing.T) {
	const childEnv = "GOMAP_NATIVE_COLUMN_BUILD_FAILURE_DIR"
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
		cmd := exec.Command(os.Args[0], "-test.run=^TestNativeExistingColumnAcceptedBuildFailureCustody$", "-test.v")
		cmd.Env = append(os.Environ(), childEnv+"="+dir)
		output, err := cmd.CombinedOutput()
		t.Logf("accepted build failure child:\n%s", output)
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != exitCode || !bytes.Contains(output, []byte("COLUMN BUILD FAILURE basis=2 accepted=3 original-build-retained")) || bytes.Contains(output, []byte("DATA RACE")) {
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
		collection, err := manager.OpenCollection("events")
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
		if got, err := database.Get(rawKey); err != nil || !bytes.Equal(got, rawValue) {
			t.Fatalf("replayed raw member=%q: %v", got, err)
		}
		if handle.CommandWALNextLSN() != 4 {
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
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
		Enabled: true, Columns: []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart}},
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
	}}}); err != nil {
		t.Fatalf("ordinary native typed Create: %v", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.InsertBatch([][]byte{[]byte("seed")}, [][]byte{seed}); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	physicalLSN := func() uint64 {
		snapshot := handle.AcquireSnapshot()
		if snapshot == nil {
			t.Fatal("missing physical snapshot")
		}
		token, ok := snapshot.StateToken()
		snapshot.Close()
		if !ok {
			t.Fatal("missing physical state token")
		}
		return token.AppliedCommandLSN
	}
	if physicalLSN() != 2 || handle.CommandWALNextLSN() != 3 {
		t.Fatal("typed seed did not establish original LSN2 checkpoint")
	}
	held, err := collection.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	checkHeld := func() {
		t.Helper()
		got, err := held.FetchDocumentsByID([][]byte{[]byte("seed"), []byte("accepted")}, DocumentFetchOptions{})
		if err != nil || len(got.Results) != 2 || !got.Results[0].Found || got.Results[1].Found {
			t.Fatalf("held typed cut: %+v %v", got.Results, err)
		}
		checkJSON(got.Results[0].Document, seed)
	}
	checkHeld() // Load the actual captured typed assets before the newer cut.
	native, err := NewNativeJSONManager(handle)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := native.Begin("events")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if err := tx.InsertBatch([][]byte{[]byte("accepted")}, [][]byte{pending}); err != nil {
		t.Fatal(err)
	}
	if got, err := tx.Get([]byte("accepted")); err != nil {
		t.Fatal(err)
	} else {
		checkJSON(got, pending)
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
	if physicalLSN() != 2 || handle.CommandWALNextLSN() != 4 {
		t.Fatal("accepted aggregate changed the physical basis or lost LSN3")
	}
	current, err := native.Capture("events")
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	engine, ok := current.capture.State().(*nativeEngineVersion)
	if !ok || engine == nil || engine.build != nil {
		t.Fatal("missing original accepted engine before physical construction")
	}
	rawCut, err := database.NewConditionalTxnWithSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer rawCut.Close()
	checkCompleteCut := func() {
		t.Helper()
		for id, want := range map[string][]byte{"seed": seed, "accepted": pending} {
			got, err := current.Get([]byte(id))
			if err != nil {
				t.Fatal(err)
			}
			checkJSON(got, want)
		}
		if got, err := rawCut.Get(rawKey); err != nil || !bytes.Equal(got, rawValue) {
			t.Fatalf("retained complete-cut raw member=%q: %v", got, err)
		}
	}
	checkCompleteCut()

	failure := errors.New("native accepted typed build parent rollback sync failure")
	var selected atomic.Bool
	var rollbackCalls atomic.Int32
	originalSync := syncStableColumnAssetParentRollback
	syncStableColumnAssetParentRollback = func(parent *os.File) error {
		if selected.Load() {
			rollbackCalls.Add(1)
			return failure
		}
		return originalSync(parent)
	}
	defer func() { syncStableColumnAssetParentRollback = originalSync }()
	restore := setColumnAssetStableObligationTestHook(func(ref ColumnAssetRef, _ rootpublication.StableLogicalObligation, _ *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision {
		if ref.Kind == ColumnAssetKindTCS1TypedColumnPart && handle.CommandWALNextLSN() == 4 && selected.CompareAndSwap(false, true) {
			return columnAssetStableCaptureOmitToken
		}
		return columnAssetStableCaptureKeep
	})
	defer restore()
	var originalFile, originalParent *os.File
	restoreRollback := setColumnAssetStableBeforeRollbackTestHook(func(parent, file *os.File, _ string) {
		if selected.Load() {
			originalParent, originalFile = parent, file
		}
	})
	defer restoreRollback()
	err = database.Checkpoint()
	if !selected.Load() || rollbackCalls.Load() != 1 || !errors.Is(err, failure) || !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("real accepted build failure: selected=%v rollback=%d err=%v", selected.Load(), rollbackCalls.Load(), err)
	}
	restore()
	restoreRollback()
	syncStableColumnAssetParentRollback = originalSync
	if handle.CommandWALNextLSN() != 4 || physicalLSN() != 2 || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence {
		t.Fatal("failed build changed the accepted logical cut or installed a partial disk basis")
	}
	build := engine.build
	if build == nil || build.refund == nil || build.allocation.parent == nil || !errors.Is(build.failure, failure) || build.complete {
		t.Fatal("accepted failure lost its original incomplete physical build/refund")
	}
	refund, allocation := build.refund, build.allocation.parent
	if originalFile == nil || originalParent == nil {
		t.Fatal("failed rollback lost original file/parent authority")
	}
	if _, err := originalFile.Stat(); err != nil {
		t.Fatalf("original failed output closed before owner shutdown: %v", err)
	}
	if _, err := originalParent.Stat(); err != nil {
		t.Fatalf("original rollback parent closed before owner shutdown: %v", err)
	}
	failedIdentity, err := rootpublication.StableIdentityFromFile(originalFile)
	if err != nil || handle.ValueLogIdentityPinRegistry().PinCount(failedIdentity) == 0 {
		t.Fatalf("original failed output lost its exact identity pin: %v", err)
	}
	if stats := handle.ValueLogIdentityPinRegistry().Stats(); stats.ActivePins == 0 || stats.ActiveIdentities == 0 || stats.ActiveStableNamespaceLinks == 0 {
		t.Fatalf("partial output authority lost: %+v", stats)
	}
	checkHeld()
	checkCompleteCut()
	if err := database.Set([]byte("after-build-failure"), []byte("blocked")); !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("poisoned owner accepted subsequent mutation: %v", err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
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
	fmt.Println("COLUMN BUILD FAILURE basis=2 accepted=3 original-build-retained")
	os.Exit(exitCode)
}
