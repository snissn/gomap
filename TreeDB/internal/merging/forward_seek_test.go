package merging

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

type forwardProbe struct {
	*mockRevisionUnsafeIter
	keys                [][]byte
	seeks, heads, keeps int
	reads               int
	err, seekErr        error
}

func newForwardProbe(data []revisionEntry) *forwardProbe {
	p := &forwardProbe{mockRevisionUnsafeIter: &mockRevisionUnsafeIter{data: data}}
	for _, e := range data {
		p.keys = append(p.keys, []byte(e.k))
	}
	return p
}
func (p *forwardProbe) Valid() bool       { return p.err == nil && p.idx >= 0 && p.idx < len(p.data) }
func (p *forwardProbe) UnsafeKey() []byte { return p.keys[p.idx] }
func (p *forwardProbe) Key() []byte       { return p.UnsafeKey() }
func (p *forwardProbe) Error() error      { return p.err }
func (p *forwardProbe) Seek(key []byte) {
	p.seeks++
	p.err = p.seekErr
	if p.err == nil {
		p.mockRevisionUnsafeIter.Seek(key)
	}
}
func (p *forwardProbe) UnsafeValue() []byte { p.reads++; return p.mockRevisionUnsafeIter.UnsafeValue() }
func (p *forwardProbe) Value() []byte       { return p.UnsafeValue() }
func (p *forwardProbe) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	p.reads++
	return p.mockRevisionUnsafeIter.UnsafeEntryWithRevision()
}
func (p *forwardProbe) SeekForwardHead(key []byte) ([]byte, bool, error) {
	p.heads++
	if p.err != nil {
		return nil, false, p.err
	}
	if !p.Valid() || bytes.Compare(p.UnsafeKey(), key) < 0 {
		p.Seek(key)
	} else {
		p.keeps++
	}
	if !p.Valid() {
		return nil, false, p.err
	}
	return p.UnsafeKey(), true, nil
}

// Embedding only the original interface intentionally hides the capability.
type genericForwardProbe struct{ iterator.UnsafeIterator }

func forwardFixture(count int, capable bool) (Iterator, []*forwardProbe) {
	sources := make([]IteratorSource, count)
	probes := make([]*forwardProbe, count)
	for i := range sources {
		data := []revisionEntry{
			{k: "\x00", v: fmt.Sprintf("zero-%d", i), rev: page.EntryRevision(100 + i)},
			{k: "a", v: fmt.Sprintf("a-%d", i), del: i == 0, rev: page.EntryRevision(200 + i)},
			{k: "a\x00", v: "binary", del: i < 2, rev: page.EntryRevision(300 + i)},
			{k: "b", v: "b", rev: page.EntryRevision(400 + i)},
			{k: fmt.Sprintf("c%d", i), v: "unique", rev: page.EntryRevision(500 + i)},
			{k: "d", v: "d", del: i == 0, rev: page.EntryRevision(600 + i)},
			{k: "z", v: "end", rev: page.EntryRevision(700 + i)},
		}
		probes[i] = newForwardProbe(data)
		sources[i] = IteratorSource{Iter: probes[i], Priority: i}
		if !capable {
			sources[i].Iter = genericForwardProbe{UnsafeIterator: probes[i]}
		}
	}
	return NewMergingIteratorWithFixedHeap(sources, []byte{0}, []byte("z")), probes
}

func compareForwardIterators(t *testing.T, fast, full Iterator) {
	t.Helper()
	if fast.Valid() != full.Valid() || fast.Error() != full.Error() {
		t.Fatalf("state mismatch: %v/%v vs %v/%v", fast.Valid(), fast.Error(), full.Valid(), full.Error())
	}
	if !fast.Valid() {
		return
	}
	if !bytes.Equal(fast.Key(), full.Key()) || !bytes.Equal(fast.Value(), full.Value()) {
		t.Fatalf("record mismatch: %x/%q vs %x/%q", fast.Key(), fast.Value(), full.Key(), full.Value())
	}
	fv, fp, ff, fr := fast.(iterator.RevisionUnsafeIterator).UnsafeEntryWithRevision()
	bv, bp, bf, br := full.(iterator.RevisionUnsafeIterator).UnsafeEntryWithRevision()
	if !bytes.Equal(fv, bv) || fp != bp || ff != bf || fr != br {
		t.Fatal("native metadata mismatch")
	}
}

func TestForwardCompositeSeekMatchesFullSeek(t *testing.T) {
	for _, count := range []int{0, 1, 2, 4, 9} {
		for _, capable := range []bool{false, true} {
			t.Run(fmt.Sprintf("sources%d/capable%t", count, capable), func(t *testing.T) {
				fast, _ := forwardFixture(count, capable)
				full, _ := forwardFixture(count, capable)
				forward := fast.(ForwardSeeker)
				// Fresh, equal, backward, exhausted, binary and both endpoints;
				// interleave normal Next so current can be outside the heap.
				for _, key := range [][]byte{nil, {0}, []byte("a"), []byte("a\x00"), []byte("b"), []byte("c0"), []byte("c3"), []byte("z"), {255}, []byte("b"), []byte("b"), {}, []byte("d")} {
					forward.SeekForward(key)
					full.Seek(key)
					compareForwardIterators(t, fast, full)
					if fast.Valid() {
						fast.Next()
						full.Next()
						compareForwardIterators(t, fast, full)
					}
				}
				fast.Close()
				full.Close()
			})
		}
	}
}

func TestForwardCompositeSeekAlignmentAndSourceCalls(t *testing.T) {
	for _, count := range []int{2, 4} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			p := []*forwardProbe{
				newForwardProbe([]revisionEntry{{k: "005"}, {k: "050"}}),
				newForwardProbe([]revisionEntry{{k: "020"}, {k: "100"}}),
			}
			p[1].idx = 1 // arbitrary constructor position hides its earlier 020
			for len(p) < count {
				p = append(p, newForwardProbe(nil)) // invalid children must be sought
			}
			sources := make([]IteratorSource, count)
			for i := range sources {
				sources[i] = IteratorSource{Iter: p[i], Priority: i}
			}
			merged := NewMergingIteratorWithFixedHeap(sources, nil, nil)
			forward := merged.(ForwardSeeker)
			forward.SeekForward([]byte("010"))
			if !merged.Valid() || string(merged.Key()) != "020" || p[1].seeks != 1 || p[1].heads != 0 {
				t.Fatal("constructor positions were mistaken for aligned sources")
			}
			merged.Seek([]byte("000"))
			before := make([]int, count)
			for i := range p {
				before[i] = p[i].seeks
			}
			forward.SeekForward([]byte("010"))
			if !merged.Valid() || string(merged.Key()) != "020" || p[0].seeks != before[0]+1 || p[1].seeks != before[1] || p[1].keeps != 1 {
				t.Fatal("ahead child was not retained or lagging child was not sought")
			}
			for i := 2; i < count; i++ {
				if p[i].seeks != before[i]+1 {
					t.Fatal("invalid child was retained")
				}
			}
			for _, source := range p {
				if source.reads != 0 {
					t.Fatal("source selection materialized a value")
				}
			}
		})
	}
}

func TestForwardCompositeSeekErrorRecoveryDoesNotRetryMovement(t *testing.T) {
	for _, count := range []int{2, 4} {
		merged, probes := forwardFixture(count, true)
		merged.Seek([]byte{0})
		failure := errors.New("forward source failure")
		probes[1].err = failure
		before := probes[1].seeks
		merged.(ForwardSeeker).SeekForward([]byte("b"))
		if merged.Error() != nil || probes[1].seeks != before+1 {
			t.Fatal("preexisting source failure did not use ordinary Seek recovery")
		}
		merged.Seek([]byte{0})
		probes[0].seekErr = failure
		before = probes[0].seeks
		merged.(ForwardSeeker).SeekForward([]byte("b"))
		if !errors.Is(merged.Error(), failure) || merged.Valid() || probes[0].seeks != before+1 {
			t.Fatal("new movement failure was hidden or retried")
		}
		merged.Close()
	}
}

func TestForwardCompositeSeekFixedFootprintAndWarmAllocation(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) == 8 && (unsafe.Sizeof(MergingIterator{}) != 168 || unsafe.Sizeof(TwoWayMerger{}) != 120) {
		t.Fatal("alignment flags grew original admitted merger wrappers")
	}
	for _, count := range []int{2, 4} {
		merged, _ := forwardFixture(count, true)
		low, high := []byte{0}, []byte("b")
		operation := func() { merged.Seek(low); merged.(ForwardSeeker).SeekForward(high) }
		operation()
		if allocations := testing.AllocsPerRun(5, operation); allocations != 0 {
			t.Fatalf("fixed-heap forward seek allocated: %g", allocations)
		}
		merged.Close()
	}
}
