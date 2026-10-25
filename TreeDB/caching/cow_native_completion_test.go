package caching

import (
	"errors"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// The real public coordinator is covered by TreeDB's indexed BSON tests. This
// adapter isolates the lower request receipt, admitted wrapper lifetime and
// direct-constructor fallback at deterministic assigned-failure boundaries.
type nativeCompletionTestBridge struct {
	failure                  error
	appendCalls, completions int
	assigned                 uint64
	appendSync               bool
}

func (*nativeCompletionTestBridge) NativeCommandWALCompletionBytes() uint64 {
	return uint64(unsafe.Sizeof(nativeCompletionTestRequest{}))
}
func (b *nativeCompletionTestBridge) PrepareNativeCommandWAL(_ int, durable bool) (backenddb.NativeCommandWALCompletion, error) {
	if !durable {
		panic("durable obligation lost")
	}
	return &nativeCompletionTestRequest{bridge: b}, nil
}

type nativeCompletionTestRequest struct{ bridge *nativeCompletionTestBridge }

func (r *nativeCompletionTestRequest) RegisterNativeCommandWAL(appendFrame func(bool) (uint64, error)) error {
	b := r.bridge
	b.appendCalls++
	lsn, err := appendFrame(b.appendSync)
	b.assigned = lsn
	return err
}
func (*nativeCompletionTestRequest) ReleaseNativeCommandWAL() {}

func (r *nativeCompletionTestRequest) CompleteNativeCommandWAL(err error) error {
	b := r.bridge
	r.bridge = nil
	b.completions++
	if err != nil {
		return err
	}
	return b.failure
}

func TestCOWNativeDeferredCompletionPreservesInstalledReceipt(t *testing.T) {
	registerNativeBridgeTestCommand()
	cache, backend := nativeBridgeFixture(t, true)
	failure := errors.New("deferred original prefix sync failure")
	bridge := &nativeCompletionTestBridge{failure: failure}
	if err := backend.BindNativeCommandWALCompletion(cache.cow.txnOwner, bridge); err != nil {
		t.Fatal(err)
	}
	tx, err := backend.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	publication, plan := nativeTxnPublication(t, tx, "receipt")
	result, err := tx.CommitNative(publication, nativeTxnIntent(t, backend, true), true)
	if !errors.Is(err, failure) || !result.Consumed || !result.Assigned || !result.LogicalInstalled || !result.Uncertain || result.WALStable || result.AssignedLSN == 0 || result.AssignedLSN != bridge.assigned {
		t.Fatalf("installed deferred failure=%+v/%v bridge=%+v", result, err, bridge)
	}
	if plan.calls != 1 || bridge.appendCalls != 1 || bridge.completions != 1 {
		t.Fatalf("reexecuted original plan=%d bridge=%+v", plan.calls, bridge)
	}
}

func TestCOWNativeParticipantAssignedFailureKeepsIdentity(t *testing.T) {
	testCOWNativeCompletionAssignedFailureKeepsIdentity(t, false)
}

func TestCOWNativeSoloAssignedSyncFailureKeepsIdentity(t *testing.T) {
	testCOWNativeCompletionAssignedFailureKeepsIdentity(t, true)
}

func testCOWNativeCompletionAssignedFailureKeepsIdentity(t *testing.T, sync bool) {
	registerNativeBridgeTestCommand()
	cache, backend := nativeBridgeFixture(t, true)
	bridge := &nativeCompletionTestBridge{appendSync: sync}
	if err := backend.BindNativeCommandWALCompletion(cache.cow.txnOwner, bridge); err != nil {
		t.Fatal(err)
	}
	tx, err := backend.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	publication, plan := nativeTxnPublication(t, tx, "assigned")
	intent := nativeTxnIntent(t, backend, true)
	failure := errors.New("original participant assigned before flush failure")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		point := durabilitycut.AfterDependencyAppend
		if sync {
			point = durabilitycut.BeforeDependencyFileSync
		}
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == point {
			return failure
		}
		return nil
	})
	result, err := tx.CommitNative(publication, intent, true)
	restore()
	if !errors.Is(err, failure) || !result.Assigned || !result.Uncertain || result.LogicalInstalled || result.AssignedLSN == 0 || result.AssignedLSN != intent.AssignedLSN() || result.AssignedLSN != bridge.assigned {
		t.Fatalf("assigned failure=%+v/%v intent=%d bridge=%+v", result, err, intent.AssignedLSN(), bridge)
	}
	if plan.calls != 1 || bridge.appendCalls != 1 || bridge.completions != 1 {
		t.Fatalf("reexecuted original plan=%d bridge=%+v", plan.calls, bridge)
	}
}
