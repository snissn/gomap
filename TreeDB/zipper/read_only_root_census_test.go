package zipper

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestReadOnlyRootCensusBoundsUnknownPurePointKeys(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	const oldRows = 8192
	rootID := buildReadOnlyPrepareRootWithKeys(t, z, oldRows)
	limits := ReadOnlyRootCensusLimits{MaxPages: 2048, MaxEntries: oldRows + 2048, MaxDepth: 16}
	census, err := z.CensusRootReadOnly(rootID, limits)
	if err != nil {
		t.Fatal(err)
	}
	if census.LeafEntries != oldRows || census.LeafPages < 2 || census.InternalChildren == 0 || census.MaxDepth < 2 {
		t.Fatalf("unexpected whole-root census: %+v", census)
	}
	// These keys stand in for segment-marker IDs assigned after the admission
	// pass. Their positions are not needed by the whole-root bound.
	delta := batch.NewRetainingLargeEntries(panicValueReader{}, page.DefaultInlineThreshold)
	defer delta.Close()
	for _, key := range []string{"key--00001", "key-000010a", "key-008193"} {
		if err := delta.Set([]byte(key), []byte("new")); err != nil {
			t.Fatal(err)
		}
	}
	bound, err := census.PurePointOutputPageUpperBound(delta.Len())
	if err != nil {
		t.Fatal(err)
	}
	before := z.pager.PageCount()
	if _, _, _, err := z.Apply(rootID, delta); err != nil {
		t.Fatal(err)
	}
	if emitted := z.pager.PageCount() - before; emitted > bound {
		t.Fatalf("emitted %d pages, whole-root bound %d", emitted, bound)
	}
	for _, tight := range []ReadOnlyRootCensusLimits{
		{MaxPages: 1, MaxEntries: oldRows + 2048, MaxDepth: 16},
		{MaxPages: 2048, MaxEntries: 1, MaxDepth: 16},
		{MaxPages: 2048, MaxEntries: oldRows + 2048, MaxDepth: 1},
	} {
		if _, err := z.CensusRootReadOnly(rootID, tight); !errors.Is(err, ErrReadOnlyPrepareProfileLimit) {
			t.Errorf("limits %+v: got %v", tight, err)
		}
	}
}

func TestReadOnlyRootCensusColdBound(t *testing.T) {
	_, z := newReadOnlyPrepareZipper(t)
	census, err := z.CensusRootReadOnly(0, ReadOnlyRootCensusLimits{MaxPages: 1, MaxEntries: 1, MaxDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := census.PurePointOutputPageUpperBound(16 << 10)
	if err != nil || bound == 0 {
		t.Fatalf("cold bound=%d err=%v", bound, err)
	}
	if _, err := z.CensusRootReadOnly(0, ReadOnlyRootCensusLimits{}); !errors.Is(err, ErrReadOnlyPrepareProfileLimit) {
		t.Fatalf("zero limits: %v", err)
	}
}

func TestReadOnlyRootCensusCountsEmptyExistingLeaf(t *testing.T) {
	p, z := newReadOnlyPrepareZipper(t)
	if _, err := p.Alloc(1); err != nil { // Page zero is the absent-root sentinel.
		t.Fatal(err)
	}
	rootID, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	data, err := p.Get(rootID)
	if err != nil {
		t.Fatal(err)
	}
	n := node.NewNode(data)
	n.SetPageID(rootID)
	n.SetType(page.PageTypeLeaf)
	n.UpdateChecksum()
	census, err := z.CensusRootReadOnly(rootID, ReadOnlyRootCensusLimits{MaxPages: 1, MaxEntries: 1, MaxDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	if census.Pages != 1 || census.LeafPages != 1 || census.LeafEntries != 0 {
		t.Fatalf("empty root census=%+v", census)
	}
}
