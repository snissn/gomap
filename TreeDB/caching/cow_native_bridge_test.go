package caching

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
)

func nativeBridgeFixture(t *testing.T, wal bool) (*DB, *backenddb.DB) {
	t.Helper()
	dir := t.TempDir()
	durability := backenddb.DurabilityWALOffRelaxed
	if wal {
		durability = backenddb.DurabilityWALOnRelaxed
	}
	backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend"), CommandWAL: wal, Durability: durability, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	cache, err := Open(dir, backend, Options{TransactionOwner: owner, MemtableMode: "cow_btree", DisableWAL: !wal, AllowUnsafe: true, ExternalCommandWAL: wal, MemtableShards: 2, FlushThreshold: 1 << 30, ValueLogPointerThreshold: 1, ValueLogCompression: uint8(vlogCompressionOff)})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	if err := backend.BindNativeSemanticBridge(owner, cache); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !cache.closing.Load() {
			_ = cache.Close()
		}
	})
	return cache, backend
}

func nativeBridgeState(t *testing.T, capture backenddb.NativeSemanticCapture) *testSemanticState {
	t.Helper()
	lease, err := capture.AdmitNativeBytes(uint64(unsafe.Sizeof(testSemanticState{})))
	if err != nil {
		t.Fatal(err)
	}
	state := newTestSemanticState(nil)
	state.refund = lease
	return state
}

func TestCOWNativeNoWALBridgeMinimalJournalAndExactSync(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, false)
	// Exercise the minimum finite ring with the actual enlarged namespace records
	// and the SAME governing budget, including separately priced backing arrays.
	journal, err := txnhistory.New(8, 16<<10, func(a, b, c, d, e uint64) (txnhistory.Lease, error) {
		var charge uint64
		for _, size := range [5]uint64{a, b, c, d, e} {
			charge += memtable.COWAllocationCharge(size)
		}
		return cache.cow.budget.AcquireExternal(charge)
	})
	if err != nil {
		t.Fatal(err)
	}
	if cache.cow.journal != nil {
		t.Fatal("journal unexpectedly initialized before first write")
	}
	cache.cow.journal = journal
	old := cache.AcquireSnapshot()
	if old == nil {
		t.Fatal("missing original raw view")
	}
	defer old.Close()
	namespace := txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: 7, Domain: 9, Interpretation: 11}
	key := []byte("same arbitrary raw and native key")
	value := bytes.Repeat([]byte("pointer"), 600)
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	native := &backenddb.NativePreparedPublication{Basis: capture.State(), State: nativeBridgeState(t, capture), StartSequence: capture.LogicalSequence(), Entries: []batch.Entry{{Type: batch.OpPut, Key: key, Value: value}}, Effects: []txnhistory.Effect{{Namespace: namespace, Kind: txnhistory.PointEffect, Key: key}}}
	result, err := backend.WriteNativePublication(native, nil, false)
	if err != nil || !result.Consumed || !result.LogicalInstalled || result.Assigned || result.AssignedLSN != 0 || result.Seal.Valid() {
		t.Fatalf("volatile publication=%+v err=%v", result, err)
	}
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	frontiers := cache.cow.prefix.LogicalFrontiers()
	if frontiers.V != result.LogicalSequence || frontiers.C != 0 || frontiers.W != 0 || frontiers.S.LogicalSequence != 0 {
		t.Fatalf("volatile frontiers=%+v", frontiers)
	}
	// Identical arbitrary bytes remain separate in the raw and semantic domains.
	if !journal.PointChangedIn(0, namespace, key) || !journal.PointChanged(0, key) || journal.PointChangedIn(0, txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: 8, Domain: 9, Interpretation: 11}, key) {
		t.Fatal("namespace collision or missing complete raw/native history")
	}
	capture, err = backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	rejected := &backenddb.NativePreparedPublication{Basis: capture.State(), State: nativeBridgeState(t, capture), StartSequence: 0, Dependencies: []txnhistory.Dependency{{Namespace: namespace, Kind: txnhistory.PointEffect, Key: key}}}
	conflict, err := backend.WriteNativePublication(rejected, nil, false)
	if !errors.Is(err, backenddb.ErrConcurrentModification) || !conflict.Consumed || conflict.LogicalInstalled || conflict.Assigned {
		t.Fatalf("dependency result=%+v err=%v", conflict, err)
	}
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 9; i++ {
		capture, err = backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		publication := &backenddb.NativePreparedPublication{Basis: capture.State(), State: nativeBridgeState(t, capture), StartSequence: capture.LogicalSequence(), Effects: []txnhistory.Effect{{Namespace: namespace, Kind: txnhistory.PointEffect, Key: []byte{byte(i)}}}}
		result, err = backend.WriteNativePublication(publication, nil, i == 8)
		if err != nil || !result.LogicalInstalled || result.Assigned || result.AssignedLSN != 0 {
			t.Fatalf("semantic-only publication %d=%+v err=%v", i, result, err)
		}
		if err := capture.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !errors.Is(journal.Check(0), txnhistory.ErrTooOld) {
		t.Fatal("minimum ring did not expire old history")
	}
	if !result.Seal.Valid() || !result.Seal.Sealed() {
		t.Fatal("explicit NoWAL sync did not return its exact sealed receipt")
	}
	frontiers = cache.cow.prefix.LogicalFrontiers()
	if frontiers.S.LogicalSequence != result.LogicalSequence || frontiers.S.CanonicalLSN != 0 {
		t.Fatalf("NoWAL explicit seal frontiers=%+v result=%+v", frontiers, result)
	}
	disk := backend.AcquireSnapshot()
	if disk == nil {
		t.Fatal("missing exact disk snapshot")
	}
	got, readErr := disk.Get(key)
	if readErr != nil || !bytes.Equal(got, value) {
		t.Fatalf("checkpoint pointer read len=%d err=%v", len(got), readErr)
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	if exists, err := old.Has(key); err != nil || exists {
		t.Fatalf("opening view changed: exists=%v %v", exists, err)
	}

}

// These fixtures provide their own already-prepared native publications. Use the
// production command registration for eligibility; registering a dummy handler
// for the same command would collide with the public collection graph fixture.
func registerNativeBridgeTestCommand() {
	collections.RegisterCommandWALReplayHandlers()
}

// This is the actual original admitted file/lease ledger used by the finalizer,
// rather than an error-only substitute custodian. Assignment failure parks it.
type nativeBridgeFailedAsset struct {
	resources                          backenddb.NativePublicationResources
	file                               *os.File
	path                               string
	lease                              backenddb.NativeAllocationReceipt
	assigned                           uint64
	failure                            error
	calls, notifications, destructions int
	cleanup                            rootpublication.SemanticCleanup
}

func (p *nativeBridgeFailedAsset) FinalizeNativePublication(ctx backenddb.CommandWALPublishContext) (backenddb.NativeFinalizedPublication, error) {
	p.calls++
	p.assigned = ctx.AppliedCommandLSN
	var err error
	p.file, err = os.OpenFile(p.path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0600)
	if err != nil {
		return backenddb.NativeFinalizedPublication{}, err
	}
	if _, err = p.file.Write([]byte("original partial native asset")); err != nil {
		return backenddb.NativeFinalizedPublication{}, err
	}
	return backenddb.NativeFinalizedPublication{}, p.failure
}
func (p *nativeBridgeFailedAsset) RetainAssignedNativeFailure(result backenddb.NativeFinalizedPublication, err error) {
	p.notifications++
	if result.State != nil || err != p.failure {
		panic("changed original failed asset identity")
	}
}
func (p *nativeBridgeFailedAsset) ReleaseNativeCustody() *rootpublication.SemanticCleanup {
	return &p.cleanup
}
func (p *nativeBridgeFailedAsset) FinishSemanticCleanup() (rootpublication.SemanticCleanupCompletion, error) {
	p.destructions++
	return rootpublication.SemanticCleanupCompletion{}, p.failure
}

func TestCOWNativeAssignedFailureRetainsOriginalAssetWithoutRawNeighbors(t *testing.T) {
	registerNativeBridgeTestCommand()
	cache, backend := nativeBridgeFixture(t, true)
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Close()
	opening, basis := cache.cow.cut, cache.cow.cut.basis
	beforeRefs, beforeBasis := opening.refs, basis.refs.Load()
	beforePins := cache.valueLogIdentityPins.ActivePins()
	lease, err := capture.AdmitNativeBytes(uint64(unsafe.Sizeof(nativeBridgeFailedAsset{})) + 16<<10)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("native asset failed after original file creation")
	plan := &nativeBridgeFailedAsset{path: filepath.Join(t.TempDir(), "partial.asset"), lease: lease, failure: failure}
	plan.cleanup.Work = plan
	if err := plan.resources.Initialize(nil); err != nil {
		t.Fatal(err)
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("original", []commitlog.CollectionDocument{{ID: []byte("id"), Document: []byte(`{"v":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := backend.NewCommandWALIntent(commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := intent.PrepareSealedReceiptWithAdmission(capture.NativeAllocator()); err != nil {
		t.Fatal(err)
	}
	defer intent.ReleaseNativeSealedReceipt()
	publication := &backenddb.NativePreparedPublication{Basis: capture.State(), StartSequence: capture.LogicalSequence(), Entries: []batch.Entry{{Type: batch.OpPut, Key: []byte("never-visible"), Value: bytes.Repeat([]byte("x"), 4096)}}, Custody: plan, Finalizer: plan, Resources: &plan.resources}
	result, err := backend.WriteNativePublication(publication, intent, false)
	if !errors.Is(err, failure) || !result.Consumed || !result.Assigned || !result.Uncertain || result.LogicalInstalled || result.AssignedLSN == 0 || plan.assigned != result.AssignedLSN {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if plan.calls != 1 || plan.notifications != 1 || plan.file == nil || plan.destructions != 0 {
		t.Fatal("lost original assigned partial file/ledger")
	}
	if opening.refs != beforeRefs || basis.refs.Load() != beforeBasis || cache.cow.cut != opening || cache.valueLogIdentityPins.ActivePins() != beforePins {
		t.Fatal("assigned failure retained private raw neighbors, basis or live pins")
	}
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	originalFile, originalLease := plan.file, plan.lease
	beforeRelease := cache.cow.budget.Stats().ExternalBytes
	intent.ReleaseNativeSealedReceipt()
	if cache.cow.budget.Stats().ExternalBytes != beforeRelease {
		t.Fatal("caller refunded original failed accepted request cell")
	}
	closeErr := cache.Close()
	if !errors.Is(closeErr, failure) || !cache.cow.prefix.AcceptedCleanupFailed() || cache.cow.prefixLease == nil {
		t.Fatalf("lost original accepted failure/control charge: %v", closeErr)
	}
	if plan.file != originalFile || plan.lease != originalLease || plan.calls != 1 || plan.notifications != 1 || plan.destructions != 0 {
		t.Fatal("Close replaced/destroyed/replayed original assigned asset")
	}
	got := make([]byte, len("original partial native asset"))
	if _, err := originalFile.ReadAt(got, 0); err != nil || string(got) != "original partial native asset" {
		t.Fatalf("original failed file lost: %q %v", got, err)
	}
	// Test-only terminal custody teardown after all observations; production never
	// reruns original failed Work or refunds its retained resources/charge.
	_ = originalFile.Close()
}

func TestCOWNativeBridgeKeepsUnconvertedProducerFence(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, false)
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Close()
	state := nativeBridgeState(t, capture)
	publication := &backenddb.NativePreparedPublication{Basis: capture.State(), State: state, StartSequence: capture.LogicalSequence()}
	calls := 0
	unregister := backend.RegisterCommandWALRawPublishBarrier(func() error { calls++; return nil })
	defer unregister()
	result, err := backend.WriteNativePublication(publication, nil, false)
	if !errors.Is(err, backenddb.ErrConditionalTxnUnsupported) || result.Consumed || result.LogicalInstalled || state.refs.Load() != 1 || calls != 0 {
		t.Fatalf("fenced publication=%+v err=%v refs=%d barrier calls=%d", result, err, state.refs.Load(), calls)
	}
	if err := cache.Set([]byte("raw"), []byte("blocked")); !errors.Is(err, backenddb.ErrConditionalTxnUnsupported) {
		t.Fatalf("raw bypassed unconverted producer: %v", err)
	}
	unregister()
	result, err = backend.WriteNativePublication(publication, nil, false)
	if err != nil || !result.LogicalInstalled {
		t.Fatalf("same original publication after fence removal=%+v %v", result, err)
	}
}

func TestCOWNativeWALExactRequestRequiresOriginalAdmission(t *testing.T) {
	registerNativeBridgeTestCommand()
	cache, backend := nativeBridgeFixture(t, true)
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Close()
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("native", []commitlog.CollectionDocument{{ID: []byte("id"), Document: []byte(`{"v":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	newIntent := func() *backenddb.CommandWALIntent {
		intent, err := backend.NewCommandWALIntent(commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)
		if err != nil {
			t.Fatal(err)
		}
		return intent
	}
	publication := &backenddb.NativePreparedPublication{Basis: capture.State(), State: nativeBridgeState(t, capture), StartSequence: capture.LogicalSequence(), Entries: []batch.Entry{{Type: batch.OpPut, Key: []byte("native-real-raw"), Value: []byte("value")}}}
	unadmitted := newIntent()
	if err := unadmitted.PrepareSealedReceipt(); err != nil {
		t.Fatal(err)
	}
	result, err := backend.WriteNativePublication(publication, unadmitted, false)
	if !errors.Is(err, ErrCOWUnsupported) || result.Consumed || result.Assigned || unadmitted.AssignedLSN() != 0 || cache.cow.prefix.LogicalFrontiers().C != 0 {
		t.Fatalf("unadmitted original request appended: %+v %v", result, err)
	}
	intent := newIntent()
	if err := intent.PrepareSealedReceiptWithAdmission(capture.NativeAllocator()); err != nil {
		t.Fatal(err)
	}
	defer intent.ReleaseNativeSealedReceipt()
	result, err = backend.WriteNativePublication(publication, intent, false)
	if err != nil || !result.Assigned || !result.LogicalInstalled || !result.Seal.Valid() || !result.Seal.Sealed() || result.AssignedLSN != intent.AssignedLSN() {
		t.Fatalf("admitted exact publication=%+v err=%v", result, err)
	}
	independent := result.Seal
	intent.ReleaseNativeSealedReceipt()
	if intent.NativeSealTarget() != nil || intent.RequestsExactSeal() || !independent.Sealed() {
		t.Fatal("caller release lost independent exact receipt or retained request alias")
	}
}
