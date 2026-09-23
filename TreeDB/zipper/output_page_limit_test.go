package zipper

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/page"
)

type outputLimitTestAllocator struct{ calls int }

func (a *outputLimitTestAllocator) Alloc(uint64) (uint64, error) {
	a.calls++
	return uint64(a.calls), nil
}

type outputLimitTestLeafLog struct{ calls int }

func (l *outputLimitTestLeafLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	l.calls++
	return page.LeafLogPtr{FileID: 1, Offset: uint64(l.calls)}, nil
}

func TestOutputPageLimitRejectsBeforePagerAllocation(t *testing.T) {
	allocator := &outputLimitTestAllocator{}
	z := New(nil, allocator)
	z.SetOutputPageLimit(1)
	if _, err := z.allocator.Alloc(0); err != nil {
		t.Fatal(err)
	}
	if _, err := z.allocator.Alloc(0); !errors.Is(err, ErrOutputPageLimit) {
		t.Fatalf("second allocation error=%v, want output-page limit", err)
	}
	if allocator.calls != 1 {
		t.Fatalf("underlying allocator calls=%d, want 1", allocator.calls)
	}
}

func TestOutputPageLimitRejectsBeforeLeafLogAppend(t *testing.T) {
	log := &outputLimitTestLeafLog{}
	z := New(nil, &outputLimitTestAllocator{})
	z.SetLeafPageLog(log)
	z.SetOutputPageLimit(1)
	var metrics adaptive.Metrics
	if _, err := z.persistLeafPageData([]byte("one"), &metrics); err != nil {
		t.Fatal(err)
	}
	if _, err := z.persistLeafPageData([]byte("two"), &metrics); !errors.Is(err, ErrOutputPageLimit) {
		t.Fatalf("second leaf append error=%v, want output-page limit", err)
	}
	if log.calls != 1 {
		t.Fatalf("underlying leaf-log calls=%d, want 1", log.calls)
	}
}
