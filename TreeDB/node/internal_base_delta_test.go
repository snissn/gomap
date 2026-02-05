package node

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestInternalBaseDelta_RoundTripAndSearch(t *testing.T) {
	const nKeys = 64

	keys := make([][]byte, nKeys)
	childIDs := make([]uint64, nKeys)
	for i := 0; i < nKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("user:%08d", i))
		childIDs[i] = uint64(10_000 + i)
	}

	build := func(compress bool) *Node {
		buf := make([]byte, page.PageSize)
		opts := BuilderOptions{}
		if compress {
			opts.InternalBaseDelta = true
		}
		b := NewBuilderWithOptions(buf, page.PageTypeInternal, opts)
		b.SetPageID(1)
		for i := 0; i < nKeys; i++ {
			if err := b.AddInternalChild(keys[i], childIDs[i]); err != nil {
				t.Fatalf("AddInternalChild: %v", err)
			}
		}
		return b.Finish()
	}

	plain := build(false)
	compressed := build(true)

	if plain.Type() != page.PageTypeInternal || compressed.Type() != page.PageTypeInternal {
		t.Fatalf("expected internal pages")
	}
	if compressed.internalBaseDelta() != true {
		t.Fatalf("expected internalBaseDelta flag set on compressed page")
	}
	if plain.internalBaseDelta() {
		t.Fatalf("did not expect internalBaseDelta flag on plain page")
	}

	for i := 0; i < nKeys; i++ {
		gotID, err := compressed.GetInternalChildID(uint16(i))
		if err != nil {
			t.Fatalf("GetInternalChildID(%d): %v", i, err)
		}
		if gotID != childIDs[i] {
			t.Fatalf("childID mismatch idx=%d got=%d want=%d", i, gotID, childIDs[i])
		}

		k, id, err := compressed.GetInternalEntryView(uint16(i))
		if err != nil {
			t.Fatalf("GetInternalEntryView(%d): %v", i, err)
		}
		if id != childIDs[i] || !bytes.Equal(k, keys[i]) {
			t.Fatalf("entry mismatch idx=%d key=%q want=%q id=%d want=%d", i, k, keys[i], id, childIDs[i])
		}
	}

	queries := [][]byte{
		[]byte("user:00000000"), // exact
		[]byte("user:00000001"), // exact
		[]byte("user:00000010"), // exact
		[]byte("user:00000010a"),
		[]byte("user:00000063"), // last
		[]byte("user:00000099"), // beyond
		[]byte("user:000000-1"), // before
	}

	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 1_000; i++ {
		queries = append(queries, []byte(fmt.Sprintf("user:%08d", rng.Intn(nKeys*2))))
	}

	for _, q := range queries {
		pIdx, pFound := plain.SearchInternal(q)
		cIdx, cFound := compressed.SearchInternal(q)
		if pIdx != cIdx || pFound != cFound {
			t.Fatalf("SearchInternal mismatch q=%q plain idx=%d found=%v compressed idx=%d found=%v", q, pIdx, pFound, cIdx, cFound)
		}
	}
}

func TestInternalBaseDelta_SkipsWhenDeltaOverflowsU32(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(1)

	base := uint64(1_000)
	tooFar := base + uint64(^uint32(0)) + 1 // delta = MaxUint32+1

	if err := b.AddInternalChild([]byte("a"), base); err != nil {
		t.Fatalf("AddInternalChild: %v", err)
	}
	if err := b.AddInternalChild([]byte("b"), tooFar); err != nil {
		t.Fatalf("AddInternalChild: %v", err)
	}
	n := b.Finish()
	if n.internalBaseDelta() {
		t.Fatalf("expected internalBaseDelta compression to be skipped")
	}
}

func TestInternalBaseDelta_CompactPreservesFooter(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(1)
	for i := 0; i < 32; i++ {
		key := []byte(fmt.Sprintf("user:%08d", i))
		if err := b.AddInternalChild(key, uint64(1_000+i)); err != nil {
			t.Fatalf("AddInternalChild: %v", err)
		}
	}
	n := b.Finish()
	if !n.internalBaseDelta() {
		t.Fatalf("expected internalBaseDelta enabled")
	}

	if err := n.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if !n.internalBaseDelta() {
		t.Fatalf("expected internalBaseDelta preserved after compact")
	}

	// Spot-check entries still decode.
	for i := 0; i < 32; i++ {
		key := []byte(fmt.Sprintf("user:%08d", i))
		idx, _ := n.SearchInternal(key)
		k, id, err := n.GetInternalEntryView(idx)
		if err != nil {
			t.Fatalf("GetInternalEntryView(%d): %v", idx, err)
		}
		if !bytes.Equal(k, key) || id != uint64(1_000+i) {
			t.Fatalf("decode mismatch idx=%d key=%q want=%q id=%d want=%d", idx, k, key, id, 1_000+i)
		}
	}
}

func TestInternalBaseDelta_SplitRoundTrip(t *testing.T) {
	const nKeys = 32

	keys := make([][]byte, nKeys)
	childIDs := make([]uint64, nKeys)
	for i := 0; i < nKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("user:%08d", i))
		childIDs[i] = uint64(1_000 + i)
	}

	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(1)
	for i := 0; i < nKeys; i++ {
		if err := b.AddInternalChild(keys[i], childIDs[i]); err != nil {
			t.Fatalf("AddInternalChild: %v", err)
		}
	}
	n1 := b.Finish()
	if !n1.internalBaseDelta() {
		t.Fatalf("expected internalBaseDelta enabled on source node")
	}

	n2 := NewNode(make([]byte, page.PageSize))
	n2.SetPageID(2)

	pivot, err := n1.Split(n2)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	splitIndex := uint16(nKeys / 2)
	if !bytes.Equal(pivot, keys[splitIndex]) {
		t.Fatalf("pivot mismatch got=%q want=%q", pivot, keys[splitIndex])
	}
	if !n2.internalBaseDelta() {
		t.Fatalf("expected internalBaseDelta enabled on split node")
	}

	for i := uint16(0); i < splitIndex; i++ {
		k, id, err := n1.GetInternalEntryView(i)
		if err != nil {
			t.Fatalf("n1 GetInternalEntryView(%d): %v", i, err)
		}
		if !bytes.Equal(k, keys[i]) || id != childIDs[i] {
			t.Fatalf("n1 mismatch idx=%d key=%q want=%q id=%d want=%d", i, k, keys[i], id, childIDs[i])
		}
	}
	for i := uint16(0); i < splitIndex; i++ {
		wantIdx := splitIndex + i
		k, id, err := n2.GetInternalEntryView(i)
		if err != nil {
			t.Fatalf("n2 GetInternalEntryView(%d): %v", i, err)
		}
		if !bytes.Equal(k, keys[wantIdx]) || id != childIDs[wantIdx] {
			t.Fatalf("n2 mismatch idx=%d key=%q want=%q id=%d want=%d", i, k, keys[wantIdx], id, childIDs[wantIdx])
		}
	}
}

func TestInternalBaseDelta_IncreasesFanout(t *testing.T) {
	buildCount := func(compress bool) int {
		buf := make([]byte, page.PageSize)
		opts := BuilderOptions{}
		if compress {
			opts.InternalBaseDelta = true
		}
		b := NewBuilderWithOptions(buf, page.PageTypeInternal, opts)
		b.SetPageID(1)

		for i := 0; ; i++ {
			key := []byte(fmt.Sprintf("user:%012d", i))
			err := b.AddInternalChild(key, uint64(1_000_000+i))
			if err == ErrNodeFull {
				break
			}
			if err != nil {
				t.Fatalf("AddInternalChild: %v", err)
			}
		}
		return int(b.Count())
	}

	plain := buildCount(false)
	compressed := buildCount(true)
	if compressed <= plain {
		t.Fatalf("expected compressed fanout to increase: plain=%d compressed=%d", plain, compressed)
	}
	// Conservative guardrail: base+delta saves 4B/entry (minus a 10B footer),
	// which should yield a measurable increase for typical key sizes.
	if compressed < plain+(plain/20) {
		t.Fatalf("expected >=5%% fanout increase: plain=%d compressed=%d", plain, compressed)
	}
}
