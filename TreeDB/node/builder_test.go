package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestBuilderReusesInternalFenceScratch(t *testing.T) {
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})

	low := bytes.Repeat([]byte("l"), 32)
	high := bytes.Repeat([]byte("h"), 32)
	b.SetInternalFenceBounds(low, high)
	lowCap := cap(b.internalFenceLow)
	highCap := cap(b.internalFenceHigh)
	if lowCap < len(low) || highCap < len(high) {
		t.Fatalf("initial fence caps low/high=%d/%d want at least %d/%d", lowCap, highCap, len(low), len(high))
	}

	b.ReleaseScratch()
	if len(b.internalFenceLow) != 0 || len(b.internalFenceHigh) != 0 {
		t.Fatalf("released fence lengths low/high=%d/%d want 0/0", len(b.internalFenceLow), len(b.internalFenceHigh))
	}
	if cap(b.internalFenceLow) != lowCap || cap(b.internalFenceHigh) != highCap {
		t.Fatalf("released fence caps low/high=%d/%d want %d/%d", cap(b.internalFenceLow), cap(b.internalFenceHigh), lowCap, highCap)
	}

	b.ResetWithOptions(data, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	b.SetInternalFenceBounds(low[:8], high[:8])
	if cap(b.internalFenceLow) != lowCap || cap(b.internalFenceHigh) != highCap {
		t.Fatalf("reused fence caps low/high=%d/%d want %d/%d", cap(b.internalFenceLow), cap(b.internalFenceHigh), lowCap, highCap)
	}
	if !bytes.Equal(b.internalFenceLow, low[:8]) || !bytes.Equal(b.internalFenceHigh, high[:8]) {
		t.Fatalf("reused fence values low/high=%q/%q", b.internalFenceLow, b.internalFenceHigh)
	}
}

func TestBuilderDropsOversizedInternalFenceScratch(t *testing.T) {
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})

	oversized := bytes.Repeat([]byte("x"), internalFenceScratchMaxCap+1)
	b.SetInternalFenceBounds(oversized, oversized)
	b.ReleaseScratch()
	if cap(b.internalFenceLow) != 0 || cap(b.internalFenceHigh) != 0 {
		t.Fatalf("oversized fence caps low/high=%d/%d want 0/0", cap(b.internalFenceLow), cap(b.internalFenceHigh))
	}
}
