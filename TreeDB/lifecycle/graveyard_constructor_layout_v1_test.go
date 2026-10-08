package lifecycle

import (
	"testing"
	"unsafe"
)

func TestGraveyardInitialBatchBackingMatchesActualConstructor(t *testing.T) {
	g := NewGraveyard()
	if len(g.retiredPages) != 0 || cap(g.retiredPages) != 64 { t.Fatalf("initial slice len=%d cap=%d", len(g.retiredPages), cap(g.retiredPages)) }
	actual := uint64(cap(g.retiredPages)) * uint64(unsafe.Sizeof(batch{}))
	if got := GraveyardInitialBatchBackingBytes(); got != actual || got == 0 { t.Fatalf("raw census=%d actual backing=%d", got, actual) }
	if allocations := testing.AllocsPerRun(100, func() {
		if GraveyardInitialBatchBackingBytes() != actual { panic("unstable constructor layout") }
	}); allocations != 0 { t.Fatalf("raw layout helper allocates: %g", allocations) }
	// Initial slots are real backing, not a zero-length census: fill every
	// slot and verify the same backing remains the constructor's full capacity.
	first := unsafe.SliceData(g.retiredPages[:cap(g.retiredPages)])
	for seq := uint64(0); seq < 64; seq++ { g.Add(seq, []uint64{seq+2}) }
	if len(g.retiredPages) != 64 || cap(g.retiredPages) != 64 || unsafe.SliceData(g.retiredPages) != first { t.Fatal("constructor slots grew or changed before capacity filled") }
	batches := g.ExtractBatchesUpTo(100, 100, 0, 64)
	if len(batches) != 64 { t.Fatalf("retirement batches=%d", len(batches)) }
	for i, b := range batches { if b.Seq != uint64(i) || len(b.IDs) != 1 || b.IDs[0] != uint64(i)+2 { t.Fatalf("retirement order changed at %d: %+v", i, b) } }
}
