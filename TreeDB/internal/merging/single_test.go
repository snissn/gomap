package merging

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

var singleConstructors = []struct {
	name string
	new  func([]IteratorSource, []byte, []byte) Iterator
}{
	{"ordinary", NewMergingIterator},
	{"fixed", NewMergingIteratorWithFixedHeap},
}

func TestSingleSourceDispatchAndAllocation(t *testing.T) {
	for _, constructor := range singleConstructors {
		t.Run(constructor.name, func(t *testing.T) {
			sources := []IteratorSource{{Iter: newForwardProbe(nil), Priority: 123}}
			merged := constructor.new(sources, nil, nil)
			single, ok := merged.(*SingleSourceMerger)
			if !ok || &single.sources[0] != &sources[0] {
				t.Fatalf("one original source did not retain its admitted backing: %T", merged)
			}
			wrapper, heap := ForwardAllocationSizes(1)
			if wrapper != uint64(unsafe.Sizeof(*single)) || heap != 0 {
				t.Fatalf("allocation description=%d/%d, actual wrapper=%d and no heap", wrapper, heap, unsafe.Sizeof(*single))
			}
			merged.Close()
			if single.sources != nil || single.key != nil || single.Valid() {
				t.Fatal("closed adapter retained source storage")
			}
			// One currently valid child among several is not one original source.
			for _, count := range []int{0, 2, 3} {
				children := make([]IteratorSource, count)
				for i := range children {
					children[i].Iter = newForwardProbe(nil)
				}
				if count != 0 {
					children[0].Iter = newForwardProbe([]revisionEntry{{k: "a"}})
				}
				other := constructor.new(children, nil, nil)
				if _, ok := other.(*SingleSourceMerger); ok {
					t.Fatalf("specialized %d original children", count)
				}
				other.Close()
			}
		})
	}
}

// Deliberately bypass constructor dispatch to keep the original heap algorithm
// as an independent oracle rather than comparing two instances of the adapter.
func singleHeapOracle(source iterator.UnsafeIterator, start, end []byte) *MergingIterator {
	m := &MergingIterator{sources: []IteratorSource{{Iter: source, Priority: 91}}, start: start, end: end}
	m.rebuildHeap()
	m.advance()
	return m
}

func TestSingleSourceMatchesHeapOracle(t *testing.T) {
	data := []revisionEntry{
		{k: "\x00", v: "zero", rev: 1},
		{k: "a", v: "deleted", del: true, rev: 2},
		{k: "a\x00", v: "binary", rev: 3},
		{k: "b", v: "first", rev: 4},
		{k: "b", v: "second", rev: 5}, // Preserve same-child duplicates.
		{k: "c", del: true, rev: 6},
		{k: "z", v: "upper", rev: 7},
	}
	for _, constructor := range singleConstructors {
		for _, domain := range []struct{ start, end []byte }{
			{}, {[]byte{0}, []byte("z")}, {[]byte("b"), []byte("b")}, {[]byte("zz"), nil},
		} {
			t.Run(fmt.Sprintf("%s/%x/%x", constructor.name, domain.start, domain.end), func(t *testing.T) {
				a, b := newForwardProbe(data), newForwardProbe(data)
				// The original child, not the merger, establishes constructor position.
				a.Seek(domain.start)
				b.Seek(domain.start)
				fast := constructor.new([]IteratorSource{{Iter: a}}, domain.start, domain.end)
				oracle := singleHeapOracle(b, domain.start, domain.end)
				defer fast.Close()
				defer oracle.Close()
				start, end := fast.Domain()
				if !bytes.Equal(start, domain.start) || !bytes.Equal(end, domain.end) || a.seeks != 1 {
					t.Fatal("constructor changed domain or sought the original child")
				}
				for fast.Valid() || oracle.Valid() {
					compareForwardIterators(t, fast, oracle)
					fast.Next()
					oracle.Next()
				}
				for _, key := range [][]byte{nil, {}, {0}, []byte("b"), []byte("z"), {255}, []byte("a"), []byte("b")} {
					fast.Seek(key)
					oracle.Seek(key)
					compareForwardIterators(t, fast, oracle)
					if fast.Valid() {
						fast.Next()
						oracle.Next()
						compareForwardIterators(t, fast, oracle)
					}
				}
			})
		}
	}
}

type singleFaultProbe struct {
	*forwardProbe
	nextErr, valueErr, entryErr, closeErr error
	nexts, closes                         int
	failNextAt                            int
	keyBuffer, valueBuffer                []byte
	ptr                                   page.ValuePtr
}

func newSingleFaultProbe(data []revisionEntry) *singleFaultProbe {
	return &singleFaultProbe{forwardProbe: newForwardProbe(data), ptr: page.ValuePtr{Offset: 64, Length: 128, FileID: 7}}
}

func (p *singleFaultProbe) Next() {
	p.nexts++
	for i := range p.keyBuffer {
		p.keyBuffer[i] = '!'
	}
	for i := range p.valueBuffer {
		p.valueBuffer[i] = '!'
	}
	p.idx++
	p.err = nil
	if p.failNextAt == 0 || p.nexts == p.failNextAt {
		p.err = p.nextErr
	}
}

func (p *singleFaultProbe) UnsafeKey() []byte {
	p.keyBuffer = append(p.keyBuffer[:0], p.keys[p.idx]...)
	return p.keyBuffer
}

func (p *singleFaultProbe) UnsafeValue() []byte {
	p.reads++
	p.err = p.valueErr
	if p.err != nil {
		return nil
	}
	p.valueBuffer = append(p.valueBuffer[:0], p.data[p.idx].v...)
	return p.valueBuffer
}

func (p *singleFaultProbe) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	p.reads++
	p.err = p.entryErr
	return nil, p.ptr, node.FlagPointer
}

func (p *singleFaultProbe) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	value, ptr, flags := p.UnsafeEntry()
	return value, ptr, flags, p.data[p.idx].rev
}

func (p *singleFaultProbe) Close() error { p.closes++; return p.closeErr }

func TestSingleSourceLazyMetadataCopiesAndClose(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			p := newSingleFaultProbe([]revisionEntry{{k: "a", v: "first", rev: 99}, {k: "b", v: "other", rev: 100}})
			var child iterator.UnsafeIterator = p
			if legacy {
				child = genericForwardProbe{UnsafeIterator: child} // Hide native revision only.
			}
			merged := NewMergingIterator([]IteratorSource{{Iter: child}}, nil, nil)
			if !merged.Valid() || p.reads != 0 {
				t.Fatal("constructor eagerly loaded value/entry")
			}
			key := merged.Key()
			keyCopy := merged.KeyCopy([]byte("reuse"))
			value := merged.Value()
			valueCopy := merged.ValueCopy([]byte("reuse"))
			_, ptr, flags, revision := merged.(iterator.RevisionUnsafeIterator).UnsafeEntryWithRevision()
			wantRevision := page.EntryRevision(99)
			if legacy {
				wantRevision = page.LegacyEntryRevision
			}
			if ptr != p.ptr || flags != node.FlagPointer || revision != wantRevision {
				t.Fatalf("entry metadata=%+v/%d/%d", ptr, flags, revision)
			}
			reads := p.reads
			merged.Next()
			if p.reads != reads || string(key) != "b" || string(value) != "!!!!!" || string(keyCopy) != "a" || string(valueCopy) != "first" {
				t.Fatal("movement decoded a value or changed copy/borrow lifetimes")
			}
			p.closeErr = errors.New("child close")
			if !errors.Is(merged.Close(), p.closeErr) || merged.Close() != nil || p.closes != 1 || merged.Valid() {
				t.Fatal("child close ownership/error was not preserved")
			}
			if string(keyCopy) != "a" || string(valueCopy) != "first" {
				t.Fatal("close changed owned copies")
			}
		})
	}
}

func TestSingleSourceErrorsAndRecovery(t *testing.T) {
	failure := errors.New("single source failure")
	for _, phase := range []string{"initial-empty", "initial-tombstone", "seek", "next", "tombstone", "value", "entry"} {
		t.Run(phase, func(t *testing.T) {
			p := newSingleFaultProbe([]revisionEntry{{k: "a"}, {k: "b", del: phase == "tombstone"}, {k: "c"}})
			if phase == "initial-empty" {
				p.idx = len(p.data)
				p.err = failure
			}
			if phase == "initial-tombstone" {
				p.data[0].del = true
				p.nextErr = failure
			}
			merged := NewMergingIteratorWithFixedHeap([]IteratorSource{{Iter: p}}, nil, nil)
			defer merged.Close()
			switch phase {
			case "seek":
				p.seekErr = failure
				merged.Seek([]byte("b"))
			case "next", "tombstone":
				p.nextErr = failure
				if phase == "tombstone" {
					p.failNextAt = 2 // Fail while skipping the tombstone, after movement to it succeeds.
				}
				merged.Next()
			case "value":
				p.valueErr = failure
				merged.Value()
			case "entry":
				p.entryErr = failure
				merged.(iterator.RevisionUnsafeIterator).UnsafeEntryWithRevision()
			}
			if !errors.Is(merged.Error(), failure) {
				t.Fatal("child error was hidden")
			}
			if merged.Valid() { // Deferred-access errors stop at the next movement.
				before := p.nexts
				merged.Next()
				if merged.Valid() || p.nexts != before {
					t.Fatal("deferred failure moved the child or exposed another entry")
				}
			}
			p.seekErr, p.nextErr, p.valueErr, p.entryErr = nil, nil, nil, nil
			merged.Seek([]byte("c"))
			if merged.Error() != nil || !merged.Valid() || string(merged.Key()) != "c" {
				t.Fatal("ordinary Seek did not recover the original child")
			}
		})
	}
}
