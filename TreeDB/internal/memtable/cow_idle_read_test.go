package memtable

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"sync"
	"testing"
	"unsafe"
)

func idleReadTestAllocation(t *testing.T, b *COWBudget) (any, [2]*COWExternalLease) {
	t.Helper()
	var leases [2]*COWExternalLease
	for i := range leases {
		var err error
		leases[i], err = b.AcquireExternal(4096)
		if err != nil {
			t.Fatal(err)
		}
	}
	return new([8192]byte), leases
}

func TestCOWIdleReadScratchTransferInvalidatesOldCapabilities(t *testing.T) {
	b, e := NewCOWBudget(DefaultCOWLimits())
	if e != nil {
		t.Fatal(e)
	}
	baseline := b.Stats()
	value, old := idleReadTestAllocation(t, b)
	if !b.PutIdleReadScratch(value, old) {
		t.Fatal("put refused")
	}
	v, fresh := b.TakeIdleReadScratch()
	if v != value || fresh[0] == old[0] || fresh[1] == old[1] {
		t.Fatal("transfer reused capability")
	}
	active := b.Stats()
	for _, l := range old {
		l.Close()
		if !errors.Is(l.Resize(1), ErrCOWClosed) {
			t.Fatal("stale resize allowed")
		}
		if !errors.Is(l.ShrinkNativeBytes(1, 0), ErrCOWClosed) {
			t.Fatal("stale shrink allowed")
		}
		if _, e = l.SplitNativeBytes(1, 0); !errors.Is(e, ErrCOWClosed) {
			t.Fatal("stale split allowed")
		}
	}
	if got := b.Stats(); got != active {
		t.Fatalf("stale capability changed stats: before%+v after%+v", active, got)
	}
	for _, l := range fresh {
		l.Close()
	}
	if got := b.Stats(); got.TotalBytes != baseline.TotalBytes || got.ReservedBytes != baseline.ReservedBytes || got.ExternalLeases != 0 {
		t.Fatalf("refund: %+v", got)
	}
	b.Close()
	if b.Stats().TotalBytes != 0 {
		t.Fatal("close retained budget")
	}
}

func TestCOWIdleReadScratchEveryByteAdmissionReclaims(t *testing.T) {
	for _, dimension := range []string{"inflight", "retired", "total"} {
		t.Run(dimension, func(t *testing.T) {
			limits := DefaultCOWLimits()
			limits.MaxGenerationBytes = 1024
			switch dimension {
			case "inflight":
				limits.MaxInFlightBytes = 32 << 10
			case "retired":
				limits.MaxRetiredBytes = 32 << 10
			case "total":
				limits.MaxTotalBytes = 32 << 10
			}
			b, e := NewCOWBudget(limits)
			if e != nil {
				t.Fatal(e)
			}
			baseline := b.Stats()
			value, leases := idleReadTestAllocation(t, b)
			if !b.PutIdleReadScratch(value, leases) {
				t.Fatal("put refused")
			}
			available := uint64(32 << 10)
			if dimension == "total" {
				available -= baseline.TotalBytes
			}
			header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
			pressure, e := b.AcquireExternal(available - header)
			if e != nil {
				t.Fatalf("idle scratch denied otherwise-admissible allocation: %v", e)
			}
			if v, _ := b.TakeIdleReadScratch(); v != nil {
				t.Fatal("pressure did not evict")
			}
			pressure.Close()
			b.Close()
			if got := b.Stats(); got.TotalBytes != 0 || got.ReservedBytes != 0 || got.ExternalLeases != 0 {
				t.Fatalf("drain: %+v", got)
			}
		})
	}
}

func TestCOWIdleReadScratchBoundAndConcurrentClose(t *testing.T) {
	b, e := NewCOWBudget(DefaultCOWLimits())
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 5; i++ {
		v, l := idleReadTestAllocation(t, b)
		ok := b.PutIdleReadScratch(v, l)
		if ok != (i < 4) {
			t.Fatalf("slot %d admitted=%t", i, ok)
		}
		if !ok {
			for _, x := range l {
				x.Close()
			}
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				v, l := b.TakeIdleReadScratch()
				if v == nil {
					continue
				}
				if !b.PutIdleReadScratch(v, l) {
					for _, x := range l {
						x.Close()
					}
				}
			}
		}()
	}
	b.Close()
	wg.Wait()
	if got := b.Stats(); got.TotalBytes != 0 || got.ReservedBytes != 0 || got.ExternalLeases != 0 {
		t.Fatalf("concurrent drain: %+v", got)
	}
}

func TestCOWIdleReadScratchResizeAndBoundedWorkPressure(t *testing.T) {
	for _, route := range []string{"resize", "acquire-work", "resize-work"} {
		t.Run(route, func(t *testing.T) {
			limits := DefaultCOWLimits()
			limits.MaxInFlightBytes = 64 << 10
			b, e := NewCOWBudget(limits)
			if e != nil {
				t.Fatal(e)
			}
			target, e := b.AcquireExternal(1)
			if e != nil {
				t.Fatal(e)
			}
			header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
			idleEnvelope := uint64(8192) + 2*header
			pressure, e := b.AcquireExternal(limits.MaxInFlightBytes - b.Stats().ReservedBytes - idleEnvelope - header)
			if e != nil {
				t.Fatal(e)
			}
			value, leases := idleReadTestAllocation(t, b)
			if !b.PutIdleReadScratch(value, leases) {
				t.Fatal("put")
			}
			before := b.Stats()
			insufficient := &iterator.OrdinalScanWork{}
			if route == "acquire-work" {
				if l, ok, e := b.AcquireRetentionWork(4096, insufficient); l != nil || ok || e != nil {
					t.Fatal("work refusal")
				}
			} else if route == "resize-work" {
				if ok, e := target.ResizeWork(4096, insufficient); ok || e != nil {
					t.Fatal("work refusal")
				}
			}
			if got := b.Stats(); got != before {
				t.Fatal("work refusal mutated retention")
			}
			work := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
			switch route {
			case "resize":
				e = target.Resize(4096)
			case "resize-work":
				var ok bool
				ok, e = target.ResizeWork(4096, work)
				if !ok {
					t.Fatalf("resize work refused: %v", e)
				}
			case "acquire-work":
				l, ok, err := b.AcquireRetentionWork(4096, work)
				e = err
				if !ok {
					t.Fatalf("acquire work refused: %v", err)
				}
				l.Close()
			}
			if e != nil {
				t.Fatal(e)
			}
			if v, _ := b.TakeIdleReadScratch(); v != nil {
				t.Fatal("admission failed to reclaim idle scratch")
			}
			target.Close()
			pressure.Close()
			b.Close()
			if b.Stats().TotalBytes != 0 {
				t.Fatal("drain")
			}
		})
	}
}

func TestCOWIdleReadScratchWriterPreparationPressure(t *testing.T) {
	limits := DefaultCOWLimits()
	limits.MaxInFlightBytes = 64 << 10
	b, w := cowTestWriter(t, limits)
	header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
	pressure, e := b.AcquireExternal(limits.MaxInFlightBytes - b.Stats().ReservedBytes - 8192 - 3*header)
	if e != nil {
		t.Fatal(e)
	}
	v, l := idleReadTestAllocation(t, b)
	if !b.PutIdleReadScratch(v, l) {
		t.Fatal("put")
	}
	prepared, e := w.Prepare([]COWMutation{{Key: []byte("key"), Value: []byte("value")}}, COWPrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if v, _ := b.TakeIdleReadScratch(); v != nil {
		t.Fatal("writer did not reclaim idle scratch")
	}
	root := prepared.Publish()
	cowTestRelease(root)
	cowTestClose(w)
	pressure.Close()
	b.Close()
	if b.Stats().TotalBytes != 0 {
		t.Fatal("drain")
	}
}
