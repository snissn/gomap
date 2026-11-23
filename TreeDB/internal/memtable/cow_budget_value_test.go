package memtable

import (
	"errors"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWAcquireExternalValueEmbeddedGuardsAndZeroAlloc(t *testing.T) {
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		b.Close()
		if got := b.Stats(); got.TotalBytes != 0 || got.ExternalBytes != 0 || got.ExternalLeases != 0 {
			t.Fatalf("leaked budget stats: %+v", got)
		}
	}()

	// Zero-value lease Close is a safe no-op.
	var zero COWExternalLease
	zero.Close()
	if err := zero.Resize(64); !errors.Is(err, ErrCOWClosed) {
		t.Fatalf("zero Resize err=%v, want ErrCOWClosed", err)
	}
	if err := zero.ShrinkNativeBytes(0, 0); !errors.Is(err, ErrCOWClosed) {
		t.Fatalf("zero ShrinkNativeBytes err=%v, want ErrCOWClosed", err)
	}
	if _, err := zero.SplitNativeBytes(0, 0); !errors.Is(err, ErrCOWClosed) {
		t.Fatalf("zero SplitNativeBytes err=%v, want ErrCOWClosed", err)
	}

	before := b.Stats()
	lease, err := b.AcquireExternalValue(256)
	if err != nil {
		t.Fatalf("AcquireExternalValue: %v", err)
	}
	after := b.Stats()
	if after.ExternalBytes-before.ExternalBytes != 256 || after.ReservedBytes-before.ReservedBytes != 256 || after.ExternalLeases-before.ExternalLeases != 1 {
		t.Fatalf("unexpected embedded lease accounting: before=%+v after=%+v", before, after)
	}
	if err := lease.Resize(512); !errors.Is(err, ErrCOWCapacity) {
		t.Fatalf("embedded Resize err=%v, want ErrCOWCapacity", err)
	}
	if err := lease.ShrinkNativeBytes(64, 64); !errors.Is(err, ErrCOWCapacity) {
		t.Fatalf("embedded ShrinkNativeBytes err=%v, want ErrCOWCapacity", err)
	}
	if _, err := lease.SplitNativeBytes(64, 64); !errors.Is(err, ErrCOWCapacity) {
		t.Fatalf("embedded SplitNativeBytes err=%v, want ErrCOWCapacity", err)
	}
	if b.PutIdleReadScratch(struct{}{}, [2]*COWExternalLease{&lease, nil}) {
		t.Fatal("PutIdleReadScratch accepted embedded lease")
	}
	lease.Close()
	lease.Close() // idempotent
	if got := b.Stats(); got.ExternalBytes != before.ExternalBytes || got.ExternalLeases != before.ExternalLeases {
		t.Fatalf("Close did not refund embedded lease: got=%+v before=%+v", got, before)
	}

	// Verify 0 heap allocations per AcquireExternalValue + Close.
	allocs := testing.AllocsPerRun(200, func() {
		l, err := b.AcquireExternalValue(128)
		if err != nil {
			t.Fatal(err)
		}
		l.Close()
	})
	if allocs != 0 {
		t.Fatalf("AcquireExternalValue+Close allocated %.0f objects, want 0", allocs)
	}
}

func TestCOWRootAtomicRefcountConcurrentLifecycle(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())
	root := cowTestPublish(t, w, []COWMutation{{Key: []byte("k"), Value: []byte("v"), Flags: node.FlagInline}})
	cowTestClose(w)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 200 {
				if root.RetainOwned() {
					_, _ = root.Get([]byte("k"))
					ret := root.Release()
					ret.Drain()
				}
				if v, err := root.Acquire(0); err == nil {
					_, _, _ = v.Get([]byte("k"))
					v.Close()
				}
				_, _ = root.GenerationUsage()
			}
		}()
	}
	close(start)
	// Concurrently release the original root while workers race RetainOwned/Acquire/GenerationUsage.
	cowTestRelease(root)
	wg.Wait()

	if root.RetainOwned() {
		t.Fatal("RetainOwned succeeded on released root")
	}
	if root.Retain() {
		t.Fatal("Retain succeeded on released root")
	}
	if _, err := root.Acquire(0); !errors.Is(err, ErrCOWClosed) {
		t.Fatalf("Acquire on released root err=%v, want ErrCOWClosed", err)
	}
	b.Close()
	if got := b.Stats(); got.TotalBytes != 0 || got.Views != 0 || got.Generations != 0 {
		t.Fatalf("leaked budget stats after concurrent root close: %+v", got)
	}
}
