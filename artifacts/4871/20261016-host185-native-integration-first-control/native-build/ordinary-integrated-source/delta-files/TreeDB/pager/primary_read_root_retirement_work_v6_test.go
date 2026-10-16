package pager

import (
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// These controls exercise only pricing/refusal over literal table headers.
// They create no file, mapping, registration, publication or release effect.
func TestPrimaryReadRootRetirementLookupRefusesBeforeLinkV6(t *testing.T) {
	last := &primaryReadRootEntryV6{id: PrimaryReadRootLocalBaseV6 + 2, owner: "last"}
	first := &primaryReadRootEntryV6{id: PrimaryReadRootLocalBaseV6, owner: "first", next: last}
	p := &Pager{immutableReadRoots: primaryReadRootTableV6{heads: []*primaryReadRootEntryV6{first}, count: 2}}
	lookup := iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}
	owner, ready := p.PrimaryReadRootOwnerWithWorkV6(last.id, &lookup)
	if ready || owner != nil || lookup.Records != 2 || lookup.Bytes != uint64(unsafe.Sizeof(primaryReadRootTableV6{}))+uint64(unsafe.Sizeof(first))+uint64(unsafe.Sizeof(*first)) {
		t.Fatalf("lookup crossed an unadmitted link: owner=%v ready=%v work=%+v", owner, ready, lookup)
	}
	remove := iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}
	if ready, err := p.RemovePrimaryReadRootV6(last.id, &remove); ready || err != nil || remove.Records != 2 {
		t.Fatalf("removal crossed an unadmitted link: ready=%v err=%v work=%+v", ready, err, remove)
	}
	if p.immutableReadRoots.heads[0] != first || first.next != last || p.immutableReadRoots.count != 2 || last.owner != "last" {
		t.Fatal("refusal consumed original registration custody")
	}
}
