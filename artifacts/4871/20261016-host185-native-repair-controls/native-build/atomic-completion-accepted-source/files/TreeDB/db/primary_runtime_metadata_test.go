package db

import (
	"errors"
	"math"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestPrimaryRuntimeBackingGrowthScrubsOldAliasesAndRefusesOverflow(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	owner := a.MetadataOwner()
	before := owner.Bytes()
	headerCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(rootPublicationRuntimeV1{})))
	if err := owner.AddPending(headerCharge); err != nil {
		t.Fatal(err)
	}
	runtime := &rootPublicationRuntimeV1{idx: &indexGen{primary: a}, metadata: owner, metadataCharge: headerCharge}
	var values []*rootPublicationVisibleMemberV1
	if err := growPrimaryRuntimeSlice(runtime, &values, 1); err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() <= before+headerCharge {
		t.Fatal("actual runtime owner did not admit backing")
	}
	member := &rootPublicationVisibleMemberV1{}
	values = append(values, member)
	old := values[:cap(values)]
	if err := growPrimaryRuntimeSlice(runtime, &values, cap(values)+1); err != nil {
		t.Fatal(err)
	}
	if values[0] != member {
		t.Fatal("replacement lost exact member")
	}
	for _, stale := range old {
		if stale != nil {
			t.Fatal("refunded old backing retained member alias")
		}
	}
	admitted := a.MetadataOwner().Bytes()
	capacity := cap(values)
	if err := growPrimaryRuntimeSlice(runtime, &values, math.MaxInt); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatal("overflow not refused", err)
	}
	if a.MetadataOwner().Bytes() != admitted || cap(values) != capacity || values[0] != member {
		t.Fatal("refusal changed backing/custody")
	}
	charge := retainedalloc.AllocationCharge(uint64(cap(values)) * uint64(unsafe.Sizeof((*rootPublicationVisibleMemberV1)(nil))))
	clear(values[:cap(values)])
	values = nil
	if runtime.metadataCharge != headerCharge+charge {
		t.Fatal("runtime containing/backing charge mismatch")
	}
	owner.RemovePending(runtime.metadataCharge)
	runtime.metadata, runtime.metadataCharge = nil, 0
	if a.MetadataOwner().Bytes() != before {
		t.Fatal("runtime backing cleanup changed original baseline")
	}
}

// This is an allocator/control test only: no DB, activation or publication.
func TestPrimaryRuntimeNativeBackingTransfersDetachedAccountingV6(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	r := rootPublicationRuntimeV1{metadata: &owner}
	var values []*rootPublicationVisibleMemberV1
	if err := growPrimaryRuntimeSlice(&r, &values, 1); err != nil {
		t.Fatal(err)
	}
	member := &rootPublicationVisibleMemberV1{}
	values = append(values, member)
	old := values[:cap(values)]
	oldCharge, before := r.metadataCharge, owner.Bytes()
	var refund primaryBackingRefundV6
	zero := &iterator.OrdinalScanWork{}
	if ready, err := growPrimaryRuntimeSliceWithWorkV6(&r, &values, cap(values)+1, &refund, zero); ready || err != nil {
		t.Fatal(ready, err)
	}
	if owner.Bytes() != before || r.metadataCharge != oldCharge || &values[0] != &old[0] || refund.owner != nil {
		t.Fatal("refusal changed original custody")
	}
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := growPrimaryRuntimeSliceWithWorkV6(&r, &values, cap(values)+1, &refund, w); !ready || err != nil {
		t.Fatal(ready, err)
	}
	if values[0] != member || refund.owner != &owner || refund.amount != oldCharge || owner.Bytes() != r.metadataCharge+oldCharge {
		t.Fatal("replacement lost accounting delta")
	}
	for _, alias := range old {
		if alias != nil {
			t.Fatal("detached old backing still aliases member")
		}
	}
	if ready, err := refund.complete(&iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}); ready || err != nil {
		t.Fatal("expected partial original removal", ready, err)
	}
	// Ordinary growth touches only the replacement and its own accounting. It
	// must neither drain nor invalidate the caller's partial old refund.
	if err := growPrimaryRuntimeSlice(&r, &values, cap(values)+1); err != nil {
		t.Fatal(err)
	}
	if refund.owner != &owner || refund.amount != oldCharge {
		t.Fatal("ordinary growth stole native refund")
	}
	for i := 0; ; i++ {
		if i > 20 {
			t.Fatal("refund did not finish")
		}
		ready, err := refund.complete(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if owner.Bytes() != r.metadataCharge || refund.owner != nil || refund.amount != 0 {
		t.Fatal("refund duplicated/lost original delta")
	}
	clear(values[:cap(values)])
	values = nil
	owner.RemovePending(r.metadataCharge)
	if owner.Bytes() != 0 {
		t.Fatal("accounting leaked")
	}
}

func TestPrimaryRuntimeDebtFullBackingSurvivesPrefixAdvanceV6(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	r := rootPublicationRuntimeV1{metadata: &owner}
	if err := r.growPrimaryRuntimeDebtV6(4); err != nil {
		t.Fatal(err)
	}
	originalCapacity, originalCharge := cap(r.debt), r.metadataCharge
	r.debt = r.debt[:4]
	r.debtBackingCapacity = originalCapacity
	clear(r.debt[:2])
	r.debt = r.debt[2:]
	if err := r.growPrimaryRuntimeDebtV6(2); err != nil {
		t.Fatal(err)
	}
	if r.debtBackingCapacity != originalCapacity || r.metadataCharge != originalCharge {
		t.Fatal("no-growth lost original full capacity")
	}
	clear(r.debt)
	r.debt = r.debt[len(r.debt):]
	if err := r.growPrimaryRuntimeDebtV6(1); err != nil {
		t.Fatal(err)
	}
	if r.debtBackingCapacity != cap(r.debt) || owner.Bytes() != r.metadataCharge {
		t.Fatal("empty suffix growth refunded wrong capacity")
	}
	before, capacity := owner.Bytes(), cap(r.debt)
	r.debtBackingCapacity = capacity - 1
	if err := r.growPrimaryRuntimeDebtV6(capacity + 1); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatal("invalid capacity accepted", err)
	}
	if owner.Bytes() != before || cap(r.debt) != capacity {
		t.Fatal("invalid geometry changed storage")
	}
	clear(r.debt[:cap(r.debt)])
	r.debt = nil
	r.debtBackingCapacity = 0
	owner.RemovePending(r.metadataCharge)
	if owner.Bytes() != 0 {
		t.Fatal("full backing accounting leaked")
	}
}
