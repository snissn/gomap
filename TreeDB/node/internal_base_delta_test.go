package node

import (
	"bytes"
	"encoding/binary"
	"errors"
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

		pChild, pChildFound, err := plain.SearchInternalChildID(q)
		if err != nil {
			t.Fatalf("plain SearchInternalChildID(%q): %v", q, err)
		}
		if pChildFound != pFound {
			t.Fatalf("plain SearchInternalChildID found mismatch q=%q got=%v want=%v", q, pChildFound, pFound)
		}
		if want := childIDs[pIdx]; pChild != want {
			t.Fatalf("plain SearchInternalChildID child mismatch q=%q got=%d want=%d", q, pChild, want)
		}

		cChild, cChildFound, err := compressed.SearchInternalChildID(q)
		if err != nil {
			t.Fatalf("compressed SearchInternalChildID(%q): %v", q, err)
		}
		if cChildFound != cFound {
			t.Fatalf("compressed SearchInternalChildID found mismatch q=%q got=%v want=%v", q, cChildFound, cFound)
		}
		if want := childIDs[cIdx]; cChild != want {
			t.Fatalf("compressed SearchInternalChildID child mismatch q=%q got=%d want=%d", q, cChild, want)
		}
		if pChild != cChild {
			t.Fatalf("SearchInternalChildID child mismatch q=%q plain=%d compressed=%d", q, pChild, cChild)
		}
	}
}

func TestInternalBaseDelta_AdaptiveWidth_U16AndU32(t *testing.T) {
	build := func(base uint64, step uint64, nKeys int) *Node {
		buf := make([]byte, page.PageSize)
		b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
		b.SetPageID(1)
		for i := 0; i < nKeys; i++ {
			key := []byte(fmt.Sprintf("k:%04d", i))
			child := base + uint64(i)*step
			if err := b.AddInternalChild(key, child); err != nil {
				t.Fatalf("AddInternalChild(%d): %v", i, err)
			}
		}
		return b.Finish()
	}

	u16Node := build(10_000, 1, 64)
	if !u16Node.internalBaseDelta() {
		t.Fatalf("expected internalBaseDelta")
	}
	if !u16Node.internalBaseDeltaU16() {
		t.Fatalf("expected u16 delta mode")
	}

	u32Node := build(10_000, 2_000, 64)
	if !u32Node.internalBaseDelta() {
		t.Fatalf("expected internalBaseDelta")
	}
	if u32Node.internalBaseDeltaU16() {
		t.Fatalf("expected u32 delta mode")
	}
}

func TestInternalBaseDelta_RejectsDeltaOverflowsU32(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(1)

	base := uint64(1_000)
	tooFar := base + uint64(^uint32(0)) + 1 // delta = MaxUint32+1

	if err := b.AddInternalChild([]byte("a"), base); err != nil {
		t.Fatalf("AddInternalChild: %v", err)
	}
	err := b.AddInternalChild([]byte("b"), tooFar)
	if !errors.Is(err, ErrInternalBaseDeltaOutOfRange) {
		t.Fatalf("expected ErrInternalBaseDeltaOutOfRange, got %v", err)
	}
	n := b.Finish()
	if !n.internalBaseDelta() {
		t.Fatalf("expected internalBaseDelta encoding to remain enabled for representable entries")
	}
}

func TestInternalBaseDelta_CompactPreservesFooter(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(1)
	b.SetInternalFenceBounds([]byte("user:00000000"), []byte("user:00000099"))
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
	low, high, ok, err := n.InternalFenceBounds()
	if err != nil {
		t.Fatalf("InternalFenceBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected persisted fence bounds")
	}
	if !bytes.Equal(low, []byte("user:00000000")) || !bytes.Equal(high, []byte("user:00000099")) {
		t.Fatalf("unexpected fence bounds low=%q high=%q", low, high)
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

func TestInternalBaseDelta_BorrowedFenceBoundsCopiedOnFinish(t *testing.T) {
	buf := make([]byte, page.PageSize)
	low := []byte("user:00000000")
	high := []byte("user:00000099")
	wantLow := append([]byte(nil), low...)
	wantHigh := append([]byte(nil), high...)

	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(1)
	b.SetInternalFenceBoundsBorrowed(low, high)
	for i := 0; i < 16; i++ {
		key := []byte(fmt.Sprintf("user:%08d", i))
		if err := b.AddInternalChild(key, uint64(1_000+i)); err != nil {
			t.Fatalf("AddInternalChild: %v", err)
		}
	}
	b.FinishNoNode()

	for i := range low {
		low[i] = 'x'
	}
	for i := range high {
		high[i] = 'y'
	}

	n := NewNode(buf)
	gotLow, gotHigh, ok, err := n.InternalFenceBounds()
	if err != nil {
		t.Fatalf("InternalFenceBounds: %v", err)
	}
	if !ok {
		t.Fatalf("expected persisted fence bounds")
	}
	if !bytes.Equal(gotLow, wantLow) || !bytes.Equal(gotHigh, wantHigh) {
		t.Fatalf("unexpected fence bounds low=%q high=%q want low=%q high=%q", gotLow, gotHigh, wantLow, wantHigh)
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
	b.SetInternalFenceBounds([]byte("user:00000000"), []byte("user:00000099"))
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

	low1, high1, ok1, err := n1.InternalFenceBounds()
	if err != nil {
		t.Fatalf("n1 InternalFenceBounds: %v", err)
	}
	if !ok1 {
		t.Fatalf("expected n1 fence bounds")
	}
	low2, high2, ok2, err := n2.InternalFenceBounds()
	if err != nil {
		t.Fatalf("n2 InternalFenceBounds: %v", err)
	}
	if !ok2 {
		t.Fatalf("expected n2 fence bounds")
	}
	if !bytes.Equal(low1, []byte("user:00000000")) || !bytes.Equal(high1, pivot) {
		t.Fatalf("unexpected n1 fences low=%q high=%q pivot=%q", low1, high1, pivot)
	}
	if !bytes.Equal(low2, pivot) || !bytes.Equal(high2, []byte("user:00000099")) {
		t.Fatalf("unexpected n2 fences low=%q high=%q pivot=%q", low2, high2, pivot)
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

func TestInternalBaseDelta_StrictLowCountStillEncodes(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(1)

	for i := 0; i < 15; i++ {
		key := []byte(fmt.Sprintf("user:%08d", i))
		if err := b.AddInternalChild(key, uint64(10_000+i)); err != nil {
			t.Fatalf("AddInternalChild(%d): %v", i, err)
		}
	}

	n := b.Finish()
	if !n.internalBaseDelta() {
		t.Fatalf("expected strict mode to keep internal base-delta when count<16")
	}
}

func TestInternalBaseDelta_StrictLowSharedPrefixStillEncodes(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(1)

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("%c_key_%04d", byte('a'+i), i))
		if err := b.AddInternalChild(key, uint64(20_000+i)); err != nil {
			t.Fatalf("AddInternalChild(%d): %v", i, err)
		}
	}

	n := b.Finish()
	if !n.internalBaseDelta() {
		t.Fatalf("expected strict mode to keep internal base-delta when shared prefix < 2 bytes")
	}
}

func TestInternalBaseDelta_StrictLowSavingsStillEncodes(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(1)

	for i := 0; i < 16; i++ {
		// Long keys with only a 2-byte shared prefix reduce compression ratio.
		key := append([]byte("aa"), bytes.Repeat([]byte{byte('a' + i)}, 120)...)
		if err := b.AddInternalChild(key, uint64(30_000+i)); err != nil {
			t.Fatalf("AddInternalChild(%d): %v", i, err)
		}
	}

	n := b.Finish()
	if !n.internalBaseDelta() {
		t.Fatalf("expected strict mode to keep internal base-delta when net savings ratio is low")
	}
}

func TestInternalBaseDelta_StrictShortAverageSeparatorStillEncodes(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(1)

	for i := 0; i < 64; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := b.AddInternalChild(key[:], uint64(40_000+i)); err != nil {
			t.Fatalf("AddInternalChild(%d): %v", i, err)
		}
	}

	n := b.Finish()
	if !n.internalBaseDelta() {
		t.Fatalf("expected strict mode to keep internal base-delta for short separator keys")
	}
}

func TestInternalBaseDelta_StrictShortFirstSeparatorNoEarlyDisable(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(1)

	var key [8]byte
	binary.BigEndian.PutUint64(key[:], uint64(1))
	if err := b.AddInternalChild(key[:], 42); err != nil {
		t.Fatalf("AddInternalChild: %v", err)
	}
	if !b.internalBaseDelta {
		t.Fatalf("expected internal base-delta to remain enabled")
	}
	if b.count != 1 {
		t.Fatalf("expected 1 staged entry, got %d", b.count)
	}
	if len(b.internalBaseEntries) != 1 {
		t.Fatalf("expected 1 staged base-delta entry, got %d", len(b.internalBaseEntries))
	}
}

func TestInternalBaseDelta_FinishNoPanicOnMixedSharedPrefixWidths(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetPageID(1)

	entries := []struct {
		key   []byte
		child uint64
	}{
		{key: []byte("tenant:region:alpha:0001"), child: 101},
		{key: []byte("t"), child: 102},
		{key: []byte("tenant:region:alpha:0002"), child: 103},
	}
	for i, e := range entries {
		if err := b.AddInternalChild(e.key, e.child); err != nil {
			t.Fatalf("AddInternalChild(%d): %v", i, err)
		}
	}

	var n *Node
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Finish panicked: %v", r)
			}
		}()
		n = b.Finish()
	}()

	if !n.internalBaseDelta() {
		t.Fatalf("expected internalBaseDelta enabled")
	}
	if got, want := n.Count(), uint16(len(entries)); got != want {
		t.Fatalf("count mismatch: got=%d want=%d", got, want)
	}
	for i, e := range entries {
		k, child, err := n.GetInternalEntryView(uint16(i))
		if err != nil {
			t.Fatalf("GetInternalEntryView(%d): %v", i, err)
		}
		if !bytes.Equal(k, e.key) || child != e.child {
			t.Fatalf("entry mismatch idx=%d key=%q want=%q child=%d want=%d", i, k, e.key, child, e.child)
		}
	}
}
