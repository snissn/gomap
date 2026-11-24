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
	for i := 0; i < cowIdleReadMaxSlots+1; i++ {
		v, l := idleReadTestAllocation(t, b)
		ok := b.PutIdleReadScratch(v, l)
		if ok != (i < cowIdleReadMaxSlots) {
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

func TestCOWIdleReadScratchByteCeiling(t *testing.T) {
	b, e := NewCOWBudget(DefaultCOWLimits())
	if e != nil {
		t.Fatal(e)
	}
	const entryBytes = 64 << 10
	maxEntries := cowIdleReadMaxBytes / entryBytes
	for i := 0; i < maxEntries+1; i++ {
		var leases [2]*COWExternalLease
		for j := range leases {
			leases[j], e = b.AcquireExternal(entryBytes / 2)
			if e != nil {
				t.Fatal(e)
			}
		}
		admitted := b.PutIdleReadScratch(new([entryBytes]byte), leases)
		if admitted != (i < maxEntries) {
			t.Fatalf("64KiB payload%d admitted=%t", i, admitted)
		}
		if !admitted {
			for _, l := range leases {
				l.Close()
			}
		}
	}
	if got := b.Stats(); got.ReservedBytes != cowIdleReadMaxBytes || got.ExternalLeases != 0 {
		t.Fatalf("byte ceiling retention: %+v", got)
	}
	b.Close()
	if b.Stats().TotalBytes != 0 {
		t.Fatal("byte-ceiling close did not drain")
	}
}

func TestCOWIdleReadScratchEightWorkerReuse(t *testing.T) {
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	baseline := b.Stats()
	const workers = 8
	if workers > cowIdleReadMaxSlots {
		t.Fatalf("workers=%d exceeds cowIdleReadMaxSlots=%d", workers, cowIdleReadMaxSlots)
	}

	// Pre-populate 8 distinct scratch entries via PutIdleReadScratch.
	seed := make([]any, workers)
	for i := range workers {
		v, leases := idleReadTestAllocation(t, b)
		seed[i] = v
		if !b.PutIdleReadScratch(v, leases) {
			for _, l := range leases {
				l.Close()
			}
			t.Fatalf("seed slot %d refused", i)
		}
	}

	// 8 concurrent workers simultaneously hold and return entries without ever missing the pool.
	start := make(chan struct{})
	barrier := make(chan struct{}, workers)
	release := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, leases := b.TakeIdleReadScratch()
			if v == nil {
				barrier <- struct{}{}
				errs <- errors.New("expected idle scratch hit for 8-worker pool")
				return
			}
			barrier <- struct{}{}
			<-release
			if !b.PutIdleReadScratch(v, leases) {
				for _, l := range leases {
					l.Close()
				}
				errs <- errors.New("put back into 8-worker pool refused")
				return
			}
		}()
	}
	close(start)
	for range workers {
		<-barrier
	}
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// All 8 entries remain parked and can be drained cleanly.
	seen := make(map[any]bool, workers)
	for range workers {
		v, leases := b.TakeIdleReadScratch()
		if v == nil {
			t.Fatal("missing parked scratch after 8-worker roundtrip")
		}
		seen[v] = true
		for _, l := range leases {
			l.Close()
		}
	}
	if len(seen) != workers {
		t.Fatalf("distinct reused scratch=%d want=%d", len(seen), workers)
	}
	if v, _ := b.TakeIdleReadScratch(); v != nil {
		t.Fatal("unexpected extra idle scratch")
	}
	if got := b.Stats(); got.TotalBytes != baseline.TotalBytes || got.ReservedBytes != baseline.ReservedBytes || got.ExternalLeases != 0 {
		t.Fatalf("8-worker reuse accounting: before=%+v after=%+v", baseline, got)
	}
	b.Close()
	if b.Stats().TotalBytes != 0 {
		t.Fatalf("close retained budget: %+v", b.Stats())
	}
}

func TestCOWIdleReadScratchSingleReceiptOwnershipPressureAndClose(t *testing.T) {
	limits := DefaultCOWLimits()
	limits.MaxInFlightBytes = 32 << 10
	b, e := NewCOWBudget(limits)
	if e != nil {
		t.Fatal(e)
	}
	baseline := b.Stats()
	old, e := b.AcquireExternal(256)
	if e != nil {
		t.Fatal(e)
	}
	value := new([128]byte)
	if !b.PutIdleReadScratch(value, [2]*COWExternalLease{old, nil}) {
		t.Fatal("single receipt park refused")
	}
	v, fresh := b.TakeIdleReadScratch()
	if v != value || fresh[0] == nil || fresh[0] == old || fresh[1] != nil {
		t.Fatal("single receipt ownership transfer")
	}
	active := b.Stats()
	old.Close()
	if !errors.Is(old.Resize(1), ErrCOWClosed) || !errors.Is(old.ShrinkNativeBytes(1, 0), ErrCOWClosed) {
		t.Fatal("stale single receipt mutation")
	}
	if _, e = old.SplitNativeBytes(1, 0); !errors.Is(e, ErrCOWClosed) {
		t.Fatal("stale single receipt split")
	}
	if b.Stats() != active {
		t.Fatal("stale handle changed active borrow")
	}
	if !b.PutIdleReadScratch(v, fresh) {
		t.Fatal("return single receipt")
	}
	header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
	pressure, e := b.AcquireExternal(limits.MaxInFlightBytes - header)
	if e != nil {
		t.Fatal(e)
	}
	if v, _ := b.TakeIdleReadScratch(); v != nil {
		t.Fatal("single receipt survived pressure")
	}
	pressure.Close()
	if b.Stats().TotalBytes != baseline.TotalBytes {
		t.Fatal("single receipt pressure leaked")
	}
	l, e := b.AcquireExternal(256)
	if e != nil {
		t.Fatal(e)
	}
	if !b.PutIdleReadScratch(value, [2]*COWExternalLease{l, nil}) {
		t.Fatal("repark")
	}
	b.Close()
	if got := b.Stats(); got.TotalBytes != 0 || got.ExternalLeases != 0 || got.ReservedBytes != 0 {
		t.Fatalf("single receipt close: %+v", got)
	}
}

func TestCOWIdleReadScratchThreeLeasesAndTwoLeaseIsolation(t *testing.T) {
	b, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	baseline := b.Stats()

	l0, err := b.AcquireExternal(512)
	if err != nil {
		t.Fatal(err)
	}
	l1, err := b.AcquireExternal(1024)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := b.AcquireExternal(2048)
	if err != nil {
		t.Fatal(err)
	}
	v3 := new([64]byte)
	if !b.PutIdleReadScratch3(v3, [3]*COWExternalLease{l0, l1, l2}) {
		t.Fatal("PutIdleReadScratch3 refused")
	}
	// Duplicate third lease must be rejected before consuming anything.
	if b.PutIdleReadScratch3(v3, [3]*COWExternalLease{l0, nil, l0}) {
		t.Fatal("duplicate lease accepted")
	}
	// 2-lease TakeIdleReadScratch must not drop a 3-lease entry.
	if got2, _ := b.TakeIdleReadScratch(); got2 != nil {
		t.Fatalf("2-lease TakeIdleReadScratch returned 3-lease entry: %v", got2)
	}
	// 2-lease entry pushed afterward is visible to TakeIdleReadScratch while 3-lease stays parked.
	v2, twoLeases := idleReadTestAllocation(t, b)
	if !b.PutIdleReadScratch(v2, twoLeases) {
		t.Fatal("2-lease PutIdleReadScratch failed")
	}
	got2, got2Leases := b.TakeIdleReadScratch()
	if got2 != v2 || got2Leases[0] == nil || got2Leases[1] == nil {
		t.Fatalf("unexpected 2-lease take: %v %+v", got2, got2Leases)
	}
	for _, l := range got2Leases {
		l.Close()
	}

	got3, got3Leases := b.TakeIdleReadScratch3()
	if got3 != v3 || got3Leases[0] == nil || got3Leases[1] == nil || got3Leases[2] == nil {
		t.Fatalf("TakeIdleReadScratch3 mismatch: %v %+v", got3, got3Leases)
	}
	if !b.PutIdleReadScratch3(got3, got3Leases) {
		t.Fatal("repark 3-lease failed")
	}
	if got := b.Stats(); got.ReservedBytes != baseline.ReservedBytes+512+1024+2048 || got.ExternalLeases != 0 {
		t.Fatalf("3-lease parked stats=%+v", got)
	}
	got3, got3Leases = b.TakeIdleReadScratch3()
	for _, l := range got3Leases {
		l.Close()
	}
	if got := b.Stats(); got.TotalBytes != baseline.TotalBytes || got.ReservedBytes != baseline.ReservedBytes || got.ExternalLeases != 0 {
		t.Fatalf("3-lease refund mismatch: before=%+v after=%+v", baseline, got)
	}
	b.Close()
	if b.Stats().TotalBytes != 0 {
		t.Fatalf("close leaked: %+v", b.Stats())
	}
}

func TestCOWIdleReadScratchCloseAndPutFused(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())
	prepared, err := w.Prepare([]COWMutation{{Key: []byte("k"), Value: []byte("v")}}, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root := prepared.Publish()
	baseline := b.Stats()

	// 1. Immediate refund path (root still retained by caller): view closes and parks 3 leases in one lock.
	view1, err := root.Acquire(128)
	if err != nil {
		t.Fatal(err)
	}
	l0, err := b.AcquireExternal(256)
	if err != nil {
		t.Fatal(err)
	}
	l1, err := b.AcquireExternal(512)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := b.AcquireExternal(1024)
	if err != nil {
		t.Fatal(err)
	}
	scratch1 := new([64]byte)
	if !view1.CloseAndPutIdleReadScratch3(scratch1, [3]*COWExternalLease{l0, l1, l2}) {
		t.Fatal("CloseAndPutIdleReadScratch3 refused immediate-refund park")
	}
	if got := b.Stats(); got.Views != baseline.Views || got.ExternalLeases != 0 || got.ReservedBytes != baseline.ReservedBytes+256+512+1024 {
		t.Fatalf("immediate-refund fused stats=%+v baseline=%+v", got, baseline)
	}

	// 2. TakeIdleReadScratch3 returns distinct fresh handles with exact stats and zero leak.
	got1, fresh1 := b.TakeIdleReadScratch3()
	if got1 != scratch1 || fresh1[0] == nil || fresh1[1] == nil || fresh1[2] == nil || fresh1[0] == l0 || fresh1[1] == l1 || fresh1[2] == l2 {
		t.Fatalf("TakeIdleReadScratch3 mismatch: got=%v fresh=%+v", got1, fresh1)
	}

	// 3. Failed park rollback (duplicate lease): view still closes cleanly, caller keeps valid leases.
	view2, err := root.Acquire(128)
	if err != nil {
		t.Fatal(err)
	}
	if view2.CloseAndPutIdleReadScratch3(got1, [3]*COWExternalLease{fresh1[0], nil, fresh1[0]}) {
		t.Fatal("CloseAndPutIdleReadScratch3 accepted duplicate lease")
	}
	if got := b.Stats(); got.Views != baseline.Views {
		t.Fatalf("failed park did not close view: views=%d want=%d", got.Views, baseline.Views)
	}
	for _, l := range fresh1 {
		l.Close()
	}
	if got := b.Stats(); got.TotalBytes != baseline.TotalBytes || got.ReservedBytes != baseline.ReservedBytes || got.ExternalLeases != 0 {
		t.Fatalf("failed-park rollback leaked: before=%+v after=%+v", baseline, got)
	}

	// 4. Deferred retirement path (view holds the last reference to a frozen root).
	view3, err := root.Acquire(128)
	if err != nil {
		t.Fatal(err)
	}
	cowTestRelease(root)
	cowTestClose(w)
	d0, err := b.AcquireExternal(256)
	if err != nil {
		t.Fatal(err)
	}
	d1, err := b.AcquireExternal(512)
	if err != nil {
		t.Fatal(err)
	}
	scratch2 := new([64]byte)
	if !view3.CloseAndPutIdleReadScratch3(scratch2, [3]*COWExternalLease{d0, d1, nil}) {
		t.Fatal("CloseAndPutIdleReadScratch3 refused deferred-retirement park")
	}
	if got := b.Stats(); got.Views != 0 || got.DeferredBytes != 0 || got.ExternalLeases != 0 || got.ReservedBytes != 256+512 {
		t.Fatalf("deferred-retirement fused stats=%+v", got)
	}
	got2, fresh2 := b.TakeIdleReadScratch3()
	if got2 != scratch2 || fresh2[0] == nil || fresh2[1] == nil || fresh2[2] != nil {
		t.Fatalf("deferred-retirement take mismatch: got=%v fresh=%+v", got2, fresh2)
	}
	fresh2[0].Close()
	fresh2[1].Close()
	b.Close()
	if got := b.Stats(); got.TotalBytes != 0 || got.ReservedBytes != 0 || got.ExternalLeases != 0 || got.Views != 0 {
		t.Fatalf("final drain leaked: %+v", got)
	}
}
