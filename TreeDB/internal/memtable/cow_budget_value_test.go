package memtable

import (
	"errors"
	"fmt"
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

func TestCOWSortedNonDeleteReplacementArenaBudgetGuard(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())

	// Seed 21 sorted keys with 7-byte values (vSize=8 -> per-entry cowAllocation=8,
	// admittedPayload for pure replacements = 21*8 = 168, while cowAllocation(168) = 176 > 168).
	const n = 21
	initial := make([]COWMutation, n)
	replace := make([]COWMutation, n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("replace-key-%04d", i))
		initial[i] = COWMutation{Key: key, Value: []byte("initval"), Flags: node.FlagInline}
		replace[i] = COWMutation{Key: key, Value: []byte(fmt.Sprintf("val%04d", i)), Flags: node.FlagInline}
	}

	r1 := cowTestPublish(t, w, initial)
	est, err := w.Estimate(replace, COWPrepareOptions{})
	if err != nil {
		t.Fatalf("Estimate(replace): %v", err)
	}
	wantPayload := uint64(n) * cowAllocation(cowValueMetadataBytes+7)
	if est.Payload != wantPayload {
		t.Fatalf("Estimate(replace).Payload=%d, want %d", est.Payload, wantPayload)
	}
	if cowAllocation(uint64(n)*(cowValueMetadataBytes+7)) <= est.Payload {
		t.Fatalf("test invariant violated: expected contiguous valCharge > admittedPayload (%d)", est.Payload)
	}
	r2 := cowTestPublish(t, w, replace)

	for i := 0; i < n; i++ {
		got, ok := r2.Get(replace[i].Key)
		if !ok {
			t.Fatalf("missing key %q after sorted replacement", replace[i].Key)
		}
		if got.Value != string(replace[i].Value) {
			t.Fatalf("key %q value=%q, want %q", replace[i].Key, got.Value, replace[i].Value)
		}
	}

	// Verify budget accounting stays bounded and refunds completely on close.
	cowTestClose(w)
	cowTestRelease(r1)
	cowTestRelease(r2)
	b.Close()
	if got := b.Stats(); got.TotalBytes != 0 || got.Views != 0 || got.Generations != 0 {
		t.Fatalf("leaked budget stats after sorted replacement: %+v", got)
	}
}
