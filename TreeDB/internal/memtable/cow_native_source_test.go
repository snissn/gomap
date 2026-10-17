package memtable

import (
	"errors"
	"sync"
	"testing"
	"unsafe"
)

func nativeSourceTestRoot(t *testing.T, b *COWBudget, bytes uint64) *COWRoot {
	t.Helper()
	w, err := b.NewNativeCOWWriter(bytes)
	if err != nil {
		t.Fatal(err)
	}
	p, err := w.Prepare([]COWMutation{{Key: []byte("key"), Value: []byte("value")}}, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r := p.Publish()
	p = nil
	retirement := w.Close()
	w = nil
	retirement.Drain()
	return r
}

func TestCOWNativeSourceCaptureRequiresNoProducerHeadroom(t *testing.T) {
	limits := DefaultCOWLimits()
	limits.MaxTotalBytes, limits.MaxRetiredBytes, limits.MaxInFlightBytes = 1<<20, 1<<20, 1<<20
	limits.MaxGenerationBytes = 1 << 19
	b, err := NewCOWBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	r := nativeSourceTestRoot(t, b, 128)
	before := b.Stats()
	// Saturate total admission exactly, including the filler lease's header.
	leaseHeader := COWAllocationCharge(uint64(unsafe.Sizeof(COWExternalLease{})))
	filler, err := b.AcquireExternal(limits.MaxTotalBytes - before.TotalBytes - leaseHeader)
	if err != nil {
		t.Fatal(err)
	}
	if b.Stats().TotalBytes != limits.MaxTotalBytes {
		t.Fatal(b.Stats())
	}
	if _, err = b.AcquireExternal(1); !errors.Is(err, ErrCOWCapacity) {
		t.Fatal(err)
	}
	full := b.Stats()
	claim, err := r.AcquireCapturedSource(128)
	if err != nil {
		t.Fatal("capture depended on producer headroom", err)
	}
	if got := b.Stats(); got.TotalBytes != full.TotalBytes || got.ReservedBytes != full.ReservedBytes || got.Sources != full.Sources+1 {
		t.Fatal("capture admitted new storage", full, got)
	}
	if _, err = r.AcquireCapturedSource(128); !errors.Is(err, ErrCOWPending) {
		t.Fatal("duplicate claim", err)
	}
	// Release only the acquired exact root edge before canceling its receipt.
	retirement := r.Release()
	retirement.Drain()
	claim.Close()
	claim = nil
	if b.Stats().Sources != 0 {
		t.Fatal(b.Stats())
	}
	// Original generation metadata remains available to the next attempt.
	claim, err = r.AcquireCapturedSource(128)
	if err != nil {
		t.Fatal("canceled rotation lost preadmission", err)
	}
	retirement = r.Release()
	retirement.Drain()
	filler.Close()
	filler = nil
	b.Close()
	retirement = r.Release()
	r = nil
	retirement.Drain()
	if b.Stats().Sources != 1 || b.Stats().Generations != 0 || b.Stats().TotalBytes == 0 {
		t.Fatal(b.Stats())
	}
	claim.Close()
	claim = nil
	if b.Stats().TotalBytes != 0 {
		t.Fatal(b.Stats())
	}
}

func TestCOWNativeSourceLastGenerationAndClaimReleaseMayOverlap(t *testing.T) {
	for i := 0; i < 20; i++ {
		b, err := NewCOWBudget(DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		r := nativeSourceTestRoot(t, b, 128)
		claim, err := r.AcquireCapturedSource(128)
		if err != nil {
			t.Fatal(err)
		}
		// Source root edge is already detached. Only the opening root remains;
		// its final generation release can race the independent source receipt.
		retirement := r.Release()
		retirement.Drain()
		b.Close()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); retirement := r.Release(); r = nil; retirement.Drain() }()
		go func() { defer wg.Done(); claim.Close() }()
		wg.Wait()
		r, claim = nil, nil
		if b.Stats().TotalBytes != 0 || b.Stats().Sources != 0 {
			t.Fatal(b.Stats())
		}
	}
}

func TestCOWNativeSourceReservationRefusesBeforeWriterAndChecksOriginalCapacity(t *testing.T) {
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	before := b.Stats()
	if _, err = b.NewNativeCOWWriter(^uint64(0)); !errors.Is(err, ErrCOWCapacity) {
		t.Fatal(err)
	}
	if b.Stats() != before {
		t.Fatal("overflow allocated native writer", before, b.Stats())
	}
	r := nativeSourceTestRoot(t, b, 64)
	before = b.Stats()
	if _, err = r.AcquireCapturedSource(65); !errors.Is(err, ErrCOWCapacity) {
		t.Fatal(err)
	}
	if b.Stats() != before {
		t.Fatal("oversized source claim changed accounting")
	}
	b.Close()
	retirement := r.Release()
	r = nil
	retirement.Drain()
	if b.Stats().TotalBytes != 0 {
		t.Fatal(b.Stats())
	}
}
