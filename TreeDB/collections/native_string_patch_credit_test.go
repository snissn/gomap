package collections

import (
	"errors"
	"sync"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestNativeStringPatchInstalledResidentRolesPrebirthAndRequestTerminal(t *testing.T) {
	_, db, _ := r1MutationOpen5059(t, true)
	defer db.Close()
	request, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{1 << 20, 4096, 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !request.snapshot().Retired {
			request.retire()
		}
	}()
	if err := ensureNativePublicationResidentOwner(db, &request.facets[nativeRequestSourceCredit]); err != nil {
		t.Fatal(err)
	}
	control, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(residentcredit.Scope{})), true)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := db.NewNativePublicationResidentScopeV1()
	if err != nil {
		t.Fatal(err)
	}
	master := probe.(*residentcredit.Scope)
	probe.ReleaseStableMetadata()
	before := master.OwnerStats()
	denied, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{4096, control, 0})
	if err != nil {
		t.Fatal(err)
	}
	retained, scratch, err := db.NewNativePublicationResidentRolesV1(&denied.facets[nativeRequestMetadataCredit])
	if !errors.Is(err, ErrPreparedInsertResourceLimit) || retained != nil || scratch != nil ||
		master.OwnerStats() != before ||
		denied.snapshot().Debited[nativeRequestMetadataCredit] != control {
		t.Fatal("second scope-control refusal allocated a hidden scope or refunded request")
	}
	denied.retire()
	retained, scratch, err = db.NewNativePublicationResidentRolesV1(&request.facets[nativeRequestMetadataCredit])
	if err != nil {
		t.Fatal(err)
	}
	a := retained.(*residentcredit.Scope)
	b := scratch.(*residentcredit.Scope)
	if a == b || !a.SameOwner(master) || !b.SameOwner(master) {
		t.Fatal("distinct roles did not come from installed owner")
	}
	if err := a.ReserveAllocation(1024); err != nil {
		t.Fatal(err)
	}
	if err := a.RetainAllocationCredit(); err != nil {
		t.Fatal(err)
	}
	request.retire()
	if !request.snapshot().Closed || request.snapshot().Retained != 0 {
		t.Fatal("resident scopes retained current request")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if master.OwnerStats().Refs != 2 || master.OwnerStats().Control == 0 || a.Stats().Closed || b.Stats().Closed {
		t.Fatal("DB retired actual role backing prematurely")
	}
	b.ReleaseStableMetadata()
	a.ReleaseStableMetadata() // preparation edge; intrinsic allocator still owns one
	if a.Stats().Closed || master.OwnerStats().Refs != 1 {
		t.Fatal("scratch/preparation released intrinsic destination")
	}
	a.ReleaseAllocationCredit()
	if master.OwnerStats().Refs != 0 || master.OwnerStats().Live != 0 || master.OwnerStats().Control != 0 {
		t.Fatal("last real role edge did not discharge resident backing")
	}
}

func TestNativeStringPatchCreatorCreditSurvivesRequestTerminal(t *testing.T) {
	limits := [nativeRequestCreditKinds]uint64{4096, 4096, 4096}
	owner, err := newNativeRequestCredit(limits)
	if err != nil {
		t.Fatal(err)
	}
	stable := &owner.facets[nativeRequestMetadataCredit]
	allocator := &owner.facets[nativeRequestAllocatorCredit]
	if err = stable.RetainStableMetadata(); err != nil {
		t.Fatal(err)
	}
	if err = allocator.RetainAllocationCredit(); err != nil {
		t.Fatal(err)
	}
	if err = stable.ReserveStableMetadata(3000); err != nil {
		t.Fatal(err)
	}
	if err = allocator.ReserveAllocation(2000); err != nil {
		t.Fatal(err)
	}
	before := owner.snapshot()
	owner.retire()
	if got := owner.snapshot(); got.Closed || got.Reserved != before.Reserved || got.Debited != before.Debited || got.Retained != 2 {
		t.Fatalf("terminal erased live creator backing: %+v", got)
	}
	// Legitimate retained seal/recovery work may finish with the same owner.
	if err = stable.ReserveStableMetadata(1096); err != nil {
		t.Fatal(err)
	}
	if err = stable.ReserveStableMetadata(1); !errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("overflow debit: %v", err)
	}
	stable.ReleaseStableMetadata()
	if owner.snapshot().Closed {
		t.Fatal("allocator descendant lost credit with stable terminal")
	}
	allocator.ReleaseAllocationCredit()
	got := owner.snapshot()
	if !got.Closed || got.Retained != 0 || got.Reserved != limits || got.Debited[nativeRequestMetadataCredit] != 4096 {
		t.Fatalf("last consumer accounting %+v", got)
	}
	if err = stable.RetainStableMetadata(); !errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("revived terminal facet: %v", err)
	}
}

func TestNativeStringPatchStableAdapterPreservesRefusalAndCredit(t *testing.T) {
	owner, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{4096, 4096, 4096})
	if err != nil {
		t.Fatal(err)
	}
	facet := &owner.facets[nativeRequestMetadataCredit]
	bridge, err := newNativeStableMetadataCredit(facet, 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	metadata := bridge.metadata
	if err != nil {
		t.Fatal(err)
	}
	first := owner.snapshot().Debited[nativeRequestMetadataCredit]
	if first == 0 {
		t.Fatal("finite facade was uncharged")
	}
	if err = metadata.RequireRootPublicationHooks(); !errors.Is(err, valuelog.ErrFiniteStableMetadataHooksUnavailable) {
		t.Fatalf("adapter enabled uncertified route: %v", err)
	}
	if err = metadata.RetainStableMetadata(); err != nil {
		t.Fatal(err)
	}
	owner.retire()
	if owner.snapshot().Closed {
		t.Fatal("Reserve closure lost actual creator retention")
	}
	if err = bridge.close(); err == nil {
		t.Fatal("closed retained stable facade")
	}
	metadata.ReleaseStableMetadata()
	if err = bridge.close(); err != nil {
		t.Fatal(err)
	}
	if owner.snapshot().Debited[nativeRequestMetadataCredit] != first {
		t.Fatal("constructor debit refunded")
	}
	if !owner.snapshot().Closed {
		t.Fatal("closed stable facade stranded creator retention")
	}
}

func TestNativeStringPatchCreatorFacetConcurrentTerminal(t *testing.T) {
	owner, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{4096, 4096, 4096})
	if err != nil {
		t.Fatal(err)
	}
	facet := &owner.facets[nativeRequestAllocatorCredit]
	const consumers = 32
	for range consumers {
		if err = facet.RetainAllocationCredit(); err != nil {
			t.Fatal(err)
		}
	}
	owner.retire()
	var wg sync.WaitGroup
	wg.Add(consumers)
	for range consumers {
		go func() {
			defer wg.Done()
			if e := facet.ReserveAllocation(16); e != nil {
				t.Error(e)
			}
			facet.ReleaseAllocationCredit()
		}()
	}
	wg.Wait()
	got := owner.snapshot()
	if !got.Closed || got.Retained != 0 || got.Debited[nativeRequestAllocatorCredit] != 16*consumers {
		t.Fatalf("creator terminal race: %+v", got)
	}
}

func TestNativeStringPatchStableCallbackBackingDeniedBeforeRetention(t *testing.T) {
	wrapper, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(nativeStableMetadataCredit{})), true)
	if err != nil {
		t.Fatal(err)
	}
	callback, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(struct {
		code     uintptr
		receiver *nativeRequestCreditFacet
	}{})), true)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{4096, wrapper + callback - 1, 0})
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := newNativeStableMetadataCredit(&owner.facets[nativeRequestMetadataCredit], 4, 2)
	if !errors.Is(err, ErrPreparedInsertResourceLimit) || bridge != nil {
		t.Fatalf("undercharged retained callback accepted: %v %v", bridge, err)
	}
	got := owner.snapshot()
	if got.Debited[nativeRequestMetadataCredit] != 0 || got.Retained != 0 {
		t.Fatalf("denied callback created a retained owner: %+v", got)
	}
	owner.retire()
	if !owner.snapshot().Closed {
		t.Fatal("denied creator did not terminate")
	}
}

func TestNativeStringPatchResidentIsAttachedToActualDBAndRetainedCut(t *testing.T) {
	_, db, c := r1MutationOpen5059(t, true)
	defer db.Close()
	source, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{1 << 20, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	defer source.retire()
	if err = ensureNativePublicationResidentOwner(db, &source.facets[nativeRequestSourceCredit]); err != nil {
		t.Fatal(err)
	}
	dbScope, err := db.NewNativePublicationResidentScopeV1()
	if err != nil {
		t.Fatal(err)
	}
	cut := dbScope.(*residentcredit.Scope)
	master := cut
	if err = cut.ReserveStableMetadata(4096); err != nil {
		t.Fatal(err)
	}
	if err = cut.RetainStableMetadata(); err != nil {
		t.Fatal(err)
	}
	cut.ReleaseStableMetadata() // destination preparation drops; actual cut remains
	debit := source.snapshot().Debited[nativeRequestSourceCredit]
	if err = ensureNativePublicationResidentOwner(db, &source.facets[nativeRequestSourceCredit]); err != nil {
		t.Fatal(err)
	}
	if source.snapshot().Debited[nativeRequestSourceCredit] != debit {
		t.Fatal("reused installed owner was reborn")
	}
	if c.writeDomain == nil {
		t.Fatal("fixture has no actual collection close-hook owner")
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if db.HasNativePublicationResidentOwnerV1() {
		t.Fatal("DB kept retired resident installation")
	}
	stats := master.OwnerStats()
	closed, refs, live, control := stats.Closed, stats.Refs, stats.Live, stats.Control
	if !closed || refs != 1 || live != control+cut.Stats().Bytes || control == 0 {
		t.Fatalf("Close released retained cut backing: closed=%v refs=%d live=%d control=%d", closed, refs, live, control)
	}
	if _, err = db.NewNativePublicationResidentScopeV1(); err == nil {
		t.Fatal("closed DB constructed a new resident")
	}
	cut.ReleaseStableMetadata()
	stats = master.OwnerStats()
	refs, live, control = stats.Refs, stats.Live, stats.Control
	if refs != 0 || live != 0 || control != 0 {
		t.Fatalf("last physical cut retained master: refs=%d live=%d control=%d", refs, live, control)
	}
	if source.snapshot().Debited[nativeRequestSourceCredit] != debit {
		t.Fatal("DB Close refunded cumulative constructor debit")
	}
}

// Scratch and intrinsic creators share the installed resident ceiling, while
// scratch's exact last edge must release its live charge before creator transfer.
