package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Diagnostic only: the side budget is independent of the producer's budget.
// This neither admits graph work nor proves immutable bytes or producer progress.
func TestNativePreparedReadRangeAtExistingHook(t *testing.T) {
	for _, deny := range []bool{false, true} {
		name := "capture"
		if deny {
			name = "side_admission_refused"
		}
		t.Run(name, func(t *testing.T) { runNativePreparedReadRangeDiagnostic(t, deny, false, false) })
	}
}

type testNativePreparedFragmentOwner struct {
	observe func(ColumnPublishPreparedAssets)
}

func (owner *testNativePreparedFragmentOwner) observeNativePreparedFragment(actual ColumnPublishPreparedAssets) {
	owner.observe(actual)
}

func TestNativePreparedReadRangeThroughInvocationSeam(t *testing.T) {
	for _, deny := range []bool{false, true} {
		name := "capture"
		if deny {
			name = "side_admission_refused"
		}
		t.Run(name, func(t *testing.T) { runNativePreparedReadRangeDiagnostic(t, deny, true, false) })
	}
	t.Run("original_hook_failure", func(t *testing.T) { runNativePreparedReadRangeDiagnostic(t, false, true, true) })
}

func TestNativePreparedFragmentObserverEmpty(t *testing.T) {
	calls := 0
	owner := &testNativePreparedFragmentOwner{observe: func(ColumnPublishPreparedAssets) { calls++ }}
	prepared, err := prepareNativeTypedAssets(columnPhysicalAssetBuildContext{}, columnWritePublishInput{operation: ColumnPublishOperationInsert}, ColumnPublishAssetPrepareInput{}, owner)
	if err != nil || calls != 0 || prepared.stableResources != nil || len(prepared.Assets) != 0 {
		t.Fatalf("empty preparation err=%v calls=%d assets=%d", err, calls, len(prepared.Assets))
	}
}

func runNativePreparedReadRangeDiagnostic(t *testing.T, deny, nativeSeam, hookFailure bool) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable namespace authority unavailable")
	}
	cfg, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{{Name: "n", Path: "n", ValueType: ColumnStoreValueInt64}}})
	if err != nil {
		t.Fatal(err)
	}
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close db: %v", err)
		}
	})
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &selectedCaptureBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	baseline := owner.Bytes()
	if deny {
		budget.cap = budget.used
	}
	var cleanupErr error
	var captureErr error
	t.Cleanup(func() {
		// Preserve a construction failure with retained metadata as debt;
		// this conservative test condition is not a typed cleanup receipt.
		if cleanupErr == nil && captureErr != nil && owner.Bytes() != baseline {
			cleanupErr = captureErr
		}
		if cleanupErr != nil {
			enrollment.CloseAfterCleanup(cleanupErr)
			return
		}
		enrollment.Close()
		if err := owner.Close(); err != nil {
			t.Errorf("close owner: %v", err)
		}
	})
	var held *rootpublication.OwnedPreparedReadRange
	t.Cleanup(func() {
		if held != nil {
			cleanupErr = held.CloseWithError()
			if cleanupErr != nil {
				t.Errorf("close read range: %v", cleanupErr)
			}
		}
	})
	var captured ColumnAssetRef
	var expected []byte
	calls := 0
	capture := func(actual ColumnPublishPreparedAssets) {
		calls++
		if calls != 1 || actual.stableResources == nil || len(actual.Assets) == 0 {
			captureErr = fmt.Errorf("missing single prepared invocation")
			return
		}
		captured = actual.Assets[0].Ref
		kind, field, classification, e := stableColumnAssetResourceClassification(captured.Kind)
		if e != nil || classification != "authoritative" || captured.Length <= 0 || captured.Length > 1<<20 {
			captureErr = fmt.Errorf("unsupported sampled asset: %v", e)
			return
		}
		selector := rootpublication.StableResourceSelector{Kind: kind, LogicalLane: captured.Namespace, ResourceID: fmt.Sprint(captured.FileID), PhysicalGeneration: uint64(captured.FileID), Obligation: stableColumnLogicalObligation(captured, field)}
		held, captureErr = rootpublication.CaptureOwnedPreparedReadRange(&owner, enrollment, db.StableResourceIdentityPinRegistry(), actual.stableResources, selector)
		if captureErr != nil {
			return
		}
		expected, captureErr = readColumnPhysicalAssetFromManager(db.ColumnAssetRootDir(), captured)
	}
	hookCalls := 0
	injectedHookError := errors.New("injected original hook failure")
	restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(actual ColumnPublishPreparedAssets) error {
		hookCalls++
		if !nativeSeam {
			capture(actual)
		} else if calls != 1 {
			captureErr = fmt.Errorf("original hook ran before invocation observer")
		}
		if hookFailure {
			return injectedHookError
		}
		return nil // Optional diagnostic refusal must not alter the producer.
	})
	defer restore()
	col := &Collection{db: db}
	rows := []columnDeclaredRow{{ID: []byte("a"), Values: []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 3}}}}
	input := columnWritePublishInput{meta: CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}, operation: ColumnPublishOperationInsert, rows: len(rows), declaredRows: rows, declaredRowsReady: true}
	hookInput := ColumnPublishAssetPrepareInput{Collection: "events", ColumnStore: *cfg, Operation: ColumnPublishOperationInsert, AppliedCommandLSN: 77}
	var prepared ColumnPublishPreparedAssets
	var prepareErr error
	if nativeSeam {
		context := columnPhysicalAssetBuildContext{rootDir: db.ColumnAssetRootDir(), registry: db.StableResourceIdentityPinRegistry(), singleOutput: true}
		invocationOwner := &testNativePreparedFragmentOwner{observe: capture}
		prepared, prepareErr = prepareNativeTypedAssets(context, input, hookInput, invocationOwner)
	} else {
		prepared, prepareErr = col.prepareColumnPhysicalAssetRowsAtIdentity(ColumnPublishPreparedAssets{}, input, hookInput, rows, 7, columnPhysicalRowAssetPartID, typedColumnPartAssetPartID)
	}
	if prepared.stableResources != nil {
		if err := prepared.stableResources.ReleaseWithError(); err != nil {
			t.Fatalf("release producer resources: %v", err)
		}
	}
	if hookFailure && !errors.Is(prepareErr, injectedHookError) {
		t.Fatalf("original hook error lost: %v", prepareErr)
	}
	if !hookFailure && prepareErr != nil {
		t.Fatalf("producer failed: %v", prepareErr)
	}
	if deny {
		if calls != 1 || hookCalls != 1 || held != nil || !errors.Is(captureErr, retainedalloc.ErrCapacity) || owner.Bytes() != baseline {
			t.Fatalf("side refusal calls=%d range=%v error=%v", calls, held != nil, captureErr)
		}
		return
	}
	if calls != 1 || hookCalls != 1 || captureErr != nil || held == nil {
		t.Fatalf("side capture calls=%d range=%v error=%v", calls, held != nil, captureErr)
	}
	actual := make([]byte, int(captured.Length))
	if n, err := held.ReadAt(actual, 0); err != nil || n != len(actual) || !bytes.Equal(actual, expected) {
		t.Fatalf("post-release exact-handle read n=%d err=%v", n, err)
	}
	closed := held
	held = nil
	cleanupErr = closed.CloseWithError()
	if cleanupErr != nil || owner.Bytes() != baseline || budget.used != baseline {
		t.Fatalf("side cleanup=%v owner=%d budget=%d baseline=%d", cleanupErr, owner.Bytes(), budget.used, baseline)
	}
}
