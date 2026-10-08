package collections

import (
	"bytes"
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestNativeStringPatchOwnedPrepareBackingAndSingleUse(t *testing.T) {
	expected := DocumentRowRef{DocumentID: []byte("row"), Generation: 3, PartID: 4, RowIndex: 5}
	requests := []TypedStringPatch{{ID: []byte("row"), Expected: &expected, ResidualMode: TypedStringResidualReplace, Residual: []byte("{}"), Edits: []TypedStringEdit{{Column: "bio", Value: "owned text"}}}}
	backing, err := nativeStringPatchInputBacking(requests)
	if err != nil {
		t.Fatal(err)
	}
	control, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(nativeRequestCredit{})), true)
	if err != nil {
		t.Fatal(err)
	}
	exact := [nativeRequestCreditKinds]uint64{backing + control, 4096, 4096}
	if _, err = prepareNativeStringPatchInput(requests, [nativeRequestCreditKinds]uint64{exact[0] - 1, 4096, 4096}); !errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("admitted below actual backing: %v", err)
	}
	prepared, err := prepareNativeStringPatchInput(requests, exact)
	if err != nil {
		t.Fatal(err)
	}
	credit := prepared.credit
	if got := credit.snapshot(); got.Debited[nativeRequestSourceCredit] != exact[0] {
		t.Fatalf("wrong class debit %+v", got)
	}
	copy(requests[0].ID, []byte("bad"))
	copy(requests[0].Expected.DocumentID, []byte("bad"))
	requests[0].Edits[0] = TypedStringEdit{Column: "city", Value: "caller changed"}
	copy(requests[0].Residual, []byte("[]"))
	got := prepared.input[0]
	if !bytes.Equal(got.ID, []byte("row")) || !bytes.Equal(got.Expected.DocumentID, []byte("row")) ||
		got.Edits[0].Column != "bio" || got.Edits[0].Value != "owned text" || !bytes.Equal(got.Residual, []byte("{}")) {
		t.Fatalf("prepared input aliased caller backing: %+v", got)
	}
	if !prepared.consume() || prepared.consume() || prepared.abandon() {
		t.Fatal("prepared input was not single-use")
	}
	facet := &credit.facets[nativeRequestAllocatorCredit]
	if err = facet.RetainAllocationCredit(); err != nil {
		t.Fatal(err)
	}
	if !prepared.finish() || prepared.finish() || len(prepared.input) != 0 || prepared.credit != nil {
		t.Fatal("terminal did not clear owned input exactly once")
	}
	if credit.snapshot().Closed {
		t.Fatal("input finish dropped a retained allocator stage")
	}
	facet.ReleaseAllocationCredit()
	if !credit.snapshot().Closed {
		t.Fatal("final retained stage failed to close creator")
	}
}

func TestNativeStringPatchOwnedAbandonHasNoIdentity(t *testing.T) {
	limits := [nativeRequestCreditKinds]uint64{4096, 4096, 4096}
	prepared, err := prepareNativeStringPatchInput([]TypedStringPatch{{ID: []byte("row"), Edits: []TypedStringEdit{{Column: "bio", Value: "unused"}}}}, limits)
	if err != nil {
		t.Fatal(err)
	}
	credit := prepared.credit
	if !prepared.abandon() || prepared.abandon() || prepared.consume() || prepared.finish() {
		t.Fatal("abandoned input could be reused")
	}
	if prepared.input != nil || prepared.credit != nil || !credit.snapshot().Closed {
		t.Fatal("abandon retained request backing")
	}
}
