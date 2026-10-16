package pager

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"testing"
)

type readRootConstructionRefusalOperandV6 struct{ calls int }

func (c *readRootConstructionRefusalOperandV6) ConstructPrimaryReadRootOwnerV6() any {
	c.calls++
	return c
}

// Scalar table controls only: no DB or page write/registration/publication.
func TestPrimaryReadRootConstructionRefusesBeforeAllocationV6(t *testing.T) {
	var p Pager
	var metadata, foreign retainedalloc.Owner
	metadata.Initialize(128)
	foreign.Initialize(128)
	p.SetPrimaryMetadataOwnerV6(&metadata)
	operand := &readRootConstructionRefusalOperandV6{}
	local := PrimaryReadRootLocalBaseV6 + 1
	check := func(label string, owner *retainedalloc.Owner, w *iterator.OrdinalScanWork) {
		t.Helper()
		before, count := metadata.Bytes(), p.immutableReadRoots.count
		ready, err := p.PrepareConstructedPrimaryReadRootOwnerWithWorkV6(local, owner, 64, operand, w)
		if ready || operand.calls != 0 || metadata.Bytes() != before || p.immutableReadRoots.count != count {
			t.Fatalf("%s changed original custody ready=%v err=%v", label, ready, err)
		}
	}
	check("zero work", &metadata, &iterator.OrdinalScanWork{})
	check("foreign retention", &foreign, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	p.immutableReadRoots.count = len(p.immutableReadRoots.heads) * 2
	check("unsupported rehash", &metadata, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	p.immutableReadRoots.count = 1
	p.immutableReadRoots.heads[local%uint64(len(p.immutableReadRoots.heads))] = &primaryReadRootEntryV6{id: local, owner: operand}
	check("duplicate", &metadata, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if p.immutableReadRoots.heads[local%uint64(len(p.immutableReadRoots.heads))].owner != operand {
		t.Fatal("duplicate consumed existing original owner")
	}
}

func TestPrimaryReadRootConstructionGrowthYieldV6(t *testing.T) {
	var p Pager
	var metadata retainedalloc.Owner
	metadata.Initialize(128)
	p.SetPrimaryMetadataOwnerV6(&metadata)
	operand := &readRootConstructionRefusalOperandV6{}
	local := PrimaryReadRootLocalBaseV6 + 1
	before := metadata.Bytes()
	// Original header2 + empty-bucket lookup1 leaves no work for the SAME
	// atomic owner growth header. No serial/node/count or operand may change.
	w := &iterator.OrdinalScanWork{RecordLimit: 3, ByteLimit: 1 << 20}
	if ready, err := p.PrepareConstructedPrimaryReadRootOwnerWithWorkV6(local, &metadata, 64, operand, w); ready || err != nil || w.Records != 3 || metadata.Bytes() != before || operand.calls != 0 || p.immutableReadRoots.count != 0 {
		t.Fatal("aggregate growth yield lost custody", ready, err, w, metadata.Bytes(), operand.calls)
	}
	// A fresh original vector reaches the original concrete constructor once.
	w = &iterator.OrdinalScanWork{RecordLimit: 4, ByteLimit: 1 << 20}
	if ready, err := p.PrepareConstructedPrimaryReadRootOwnerWithWorkV6(local, &metadata, 64, operand, w); !ready || err != nil || w.Records != 4 || operand.calls != 1 || p.immutableReadRoots.count != 1 {
		t.Fatal("aggregate constructor did not resume", ready, err, w, operand.calls)
	}
	before = metadata.Bytes()
	w = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := p.PrepareConstructedPrimaryReadRootOwnerWithWorkV6(local, &metadata, 64, operand, w); ready || err != retainedalloc.ErrWorkRequired || metadata.Bytes() != before || operand.calls != 1 || p.immutableReadRoots.count != 1 {
		t.Fatal("duplicate capability was relaxed", ready, err)
	}
}
