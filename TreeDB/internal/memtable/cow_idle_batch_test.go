package memtable

import (
	"errors"
	"sync"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestCOWIdleBatchScratchPutTakePressureAndClose(t *testing.T) {
	// Verify exact COWBudget footprint and COWView.ReleaseWithWork <= 2048B invariant.
	if got := unsafe.Sizeof(COWBudget{}); got > 776 {
		t.Fatalf("sizeof(COWBudget)=%d > 776B", got)
	}
	releaseWorkBytes := 2*uint64(unsafe.Sizeof(COWView{})) +
		2*uint64(unsafe.Sizeof(COWRoot{})) +
		2*uint64(unsafe.Sizeof(cowGeneration{})) +
		2*uint64(unsafe.Sizeof(COWBudget{}))
	if releaseWorkBytes > 2048 {
		t.Fatalf("COWView.ReleaseWithWork bytes=%d > 2048B", releaseWorkBytes)
	}

	t.Run("park_from_lease_and_lifo_take", func(t *testing.T) {
		b, err := NewCOWBudget(DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		baseline := b.Stats()
		header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
		const baseBytes = uint64(512)
		const payload1 = uint64(4096)
		const transient = uint64(1024)

		l1, err := b.AcquireExternal(baseBytes + payload1 + transient)
		if err != nil {
			t.Fatal(err)
		}
		v1 := new([64]byte)
		// Reject invalid inputs without mutating lease or stats.
		beforeInvalid := b.Stats()
		if b.ParkIdleBatchScratchFromLease(nil, l1, baseBytes, payload1) {
			t.Fatal("nil value accepted")
		}
		if b.ParkIdleBatchScratchFromLease(v1, nil, baseBytes, payload1) {
			t.Fatal("nil lease accepted")
		}
		if b.ParkIdleBatchScratchFromLease(v1, l1, baseBytes, 0) {
			t.Fatal("zero payload accepted")
		}
		if b.ParkIdleBatchScratchFromLease(v1, l1, baseBytes, payload1+transient+1) {
			t.Fatal("over-charged payload accepted")
		}
		other, err := NewCOWBudget(DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		if other.ParkIdleBatchScratchFromLease(v1, l1, baseBytes, payload1) {
			t.Fatal("foreign lease accepted")
		}
		other.Close()
		if got := b.Stats(); got != beforeInvalid {
			t.Fatalf("invalid park mutated stats: before=%+v after=%+v", beforeInvalid, got)
		}
		embedded, err := b.AcquireExternalValue(baseBytes + payload1)
		if err != nil {
			t.Fatal(err)
		}
		beforeEmbedded := b.Stats()
		if b.ParkIdleBatchScratchFromLease(v1, &embedded, baseBytes, payload1) {
			t.Fatal("embedded lease accepted")
		}
		if got := b.Stats(); got != beforeEmbedded {
			t.Fatalf("embedded park mutated stats: before=%+v after=%+v", beforeEmbedded, got)
		}
		embedded.Close()

		// Park from active lease: keeps l1 open at header+baseBytes, refunds transient, keeps payload1 charged.
		if !b.ParkIdleBatchScratchFromLease(v1, l1, baseBytes, payload1) {
			t.Fatal("ParkIdleBatchScratchFromLease refused")
		}
		wantParked := baseline.ReservedBytes + header + baseBytes + payload1
		if got := b.Stats(); got.ReservedBytes != wantParked || got.ExternalBytes != wantParked || got.ExternalLeases != 1 {
			t.Fatalf("after park stats=%+v want reserved=%d leases=1", got, wantParked)
		}
		// Close l1: only header+baseBytes is refunded; payload1 remains parked.
		l1.Close()
		if got := b.Stats(); got.ReservedBytes != payload1 || got.ExternalBytes != payload1 || got.ExternalLeases != 0 {
			t.Fatalf("after lease close stats=%+v want reserved=%d leases=0", got, payload1)
		}

		// Park a second entry via PutIdleBatchScratch (consumes lease, LIFO order).
		const payload2 = uint64(8192)
		l2, err := b.AcquireExternal(baseBytes + payload2)
		if err != nil {
			t.Fatal(err)
		}
		v2 := new([64]byte)
		if !b.PutIdleBatchScratch(v2, l2, payload2) {
			t.Fatal("PutIdleBatchScratch refused")
		}
		if got := b.Stats(); got.ReservedBytes != payload1+payload2 || got.ExternalBytes != payload1+payload2 || got.ExternalLeases != 0 {
			t.Fatalf("after put2 stats=%+v want reserved=%d leases=0", got, payload1+payload2)
		}

		// Third entry exceeds cowIdleBatchMaxSlots (2) and must be refused without consuming l3.
		l3, err := b.AcquireExternal(baseBytes + 4096)
		if err != nil {
			t.Fatal(err)
		}
		if b.PutIdleBatchScratch(new([64]byte), l3, 4096) {
			t.Fatal("3rd slot accepted past cowIdleBatchMaxSlots")
		}
		if b.ParkIdleBatchScratchFromLease(new([64]byte), l3, baseBytes, 4096) {
			t.Fatal("3rd slot parked past cowIdleBatchMaxSlots")
		}
		l3.Close()

		// Stale l2 cannot mutate stats or active borrows.
		parkedStats := b.Stats()
		l2.Close()
		if !errors.Is(l2.Resize(1), ErrCOWClosed) || !errors.Is(l2.ShrinkNativeBytes(1, 0), ErrCOWClosed) {
			t.Fatal("stale l2 mutation allowed")
		}
		if _, err := l2.SplitNativeBytes(1, 0); !errors.Is(err, ErrCOWClosed) {
			t.Fatal("stale l2 split allowed")
		}
		if got := b.Stats(); got != parkedStats {
			t.Fatalf("stale l2 changed stats: before=%+v after=%+v", parkedStats, got)
		}

		// Take LIFO: v2 first, then v1.
		got2, fresh2, p2 := b.TakeIdleBatchScratch(baseBytes)
		if got2 != v2 || fresh2 == nil || fresh2 == l2 || p2 != payload2 {
			t.Fatalf("first LIFO take mismatch: got=%v lease=%p payload=%d", got2, fresh2, p2)
		}
		got1, fresh1, p1 := b.TakeIdleBatchScratch(baseBytes)
		if got1 != v1 || fresh1 == nil || fresh1 == l1 || p1 != payload1 {
			t.Fatalf("second LIFO take mismatch: got=%v lease=%p payload=%d", got1, fresh1, p1)
		}
		if v, l, p := b.TakeIdleBatchScratch(baseBytes); v != nil || l != nil || p != 0 {
			t.Fatalf("empty take returned non-nil: %v %v %d", v, l, p)
		}
		fresh2.Close()
		fresh1.Close()
		if got := b.Stats(); got.TotalBytes != baseline.TotalBytes || got.ReservedBytes != 0 || got.ExternalBytes != 0 || got.ExternalLeases != 0 {
			t.Fatalf("refund mismatch: baseline=%+v got=%+v", baseline, got)
		}
		b.Close()
		if got := b.Stats(); got.TotalBytes != 0 || got.ControlBytes != 0 {
			t.Fatalf("close did not drain: %+v", got)
		}
	})

	t.Run("byte_cap_quarter_inflight_and_8mib", func(t *testing.T) {
		limits := DefaultCOWLimits()
		limits.MaxInFlightBytes = 128 << 10 // quarter = 32 KiB
		b, err := NewCOWBudget(limits)
		if err != nil {
			t.Fatal(err)
		}
		const quarter = uint64(32 << 10)
		lBig, err := b.AcquireExternal(quarter + 1024)
		if err != nil {
			t.Fatal(err)
		}
		if b.PutIdleBatchScratch(new([32]byte), lBig, quarter+1) {
			t.Fatal("payload above MaxInFlightBytes/4 accepted")
		}
		lBig.Close()

		lFit1, err := b.AcquireExternal(quarter/2 + 512)
		if err != nil {
			t.Fatal(err)
		}
		if !b.PutIdleBatchScratch(new([32]byte), lFit1, quarter/2) {
			t.Fatal("first half of quarter-inflight refused")
		}
		lOver, err := b.AcquireExternal(quarter/2 + 512)
		if err != nil {
			t.Fatal(err)
		}
		if b.PutIdleBatchScratch(new([32]byte), lOver, quarter/2+1) {
			t.Fatal("aggregate above MaxInFlightBytes/4 accepted")
		}
		if !b.PutIdleBatchScratch(new([32]byte), lOver, quarter/2) {
			t.Fatal("exact MaxInFlightBytes/4 aggregate refused")
		}
		if got := b.Stats(); got.ReservedBytes != quarter || got.ExternalLeases != 0 {
			t.Fatalf("quarter cap retention mismatch: %+v", got)
		}
		b.Close()
		if b.Stats().TotalBytes != 0 {
			t.Fatalf("close did not drain byte-cap budget: %+v", b.Stats())
		}
	})

	t.Run("pressure_eviction_all_dimensions_and_take_failure_refund", func(t *testing.T) {
		for _, dimension := range []string{"inflight", "retired", "total"} {
			t.Run(dimension, func(t *testing.T) {
				limits := DefaultCOWLimits()
				limits.MaxGenerationBytes = 1024
				switch dimension {
				case "inflight":
					limits.MaxInFlightBytes = 64 << 10
				case "retired":
					limits.MaxRetiredBytes = 64 << 10
				case "total":
					limits.MaxTotalBytes = 64 << 10
				}
				b, err := NewCOWBudget(limits)
				if err != nil {
					t.Fatal(err)
				}
				baseline := b.Stats()
				const payload = uint64(8192)
				l, err := b.AcquireExternal(payload + 512)
				if err != nil {
					t.Fatal(err)
				}
				if !b.PutIdleBatchScratch(new([32]byte), l, payload) {
					t.Fatal("put refused")
				}
				available := uint64(64 << 10)
				if dimension == "total" {
					available -= baseline.TotalBytes
				}
				header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
				pressure, err := b.AcquireExternal(available - header)
				if err != nil {
					t.Fatalf("idle batch scratch denied %s admission: %v", dimension, err)
				}
				if v, _, _ := b.TakeIdleBatchScratch(512); v != nil {
					t.Fatalf("%s pressure did not evict idle batch scratch", dimension)
				}
				pressure.Close()
				b.Close()
				if got := b.Stats(); got.TotalBytes != 0 || got.ReservedBytes != 0 || got.ExternalLeases != 0 {
					t.Fatalf("%s drain leaked: %+v", dimension, got)
				}
			})
		}

		// Take failure refunds popped payload cleanly when extra header+baseBytes does not fit.
		limits := DefaultCOWLimits()
		limits.MaxInFlightBytes = 64 << 10
		b, err := NewCOWBudget(limits)
		if err != nil {
			t.Fatal(err)
		}
		baseline := b.Stats()
		const payload = uint64(8192)
		l, err := b.AcquireExternal(payload + 512)
		if err != nil {
			t.Fatal(err)
		}
		if !b.PutIdleBatchScratch(new([32]byte), l, payload) {
			t.Fatal("put refused")
		}
		header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
		filler, err := b.AcquireExternal(limits.MaxInFlightBytes - payload - header - 16)
		if err != nil {
			t.Fatal(err)
		}
		// Extra = header + 512 > 16 remaining bytes -> Take fails and refunds payload.
		if v, fl, p := b.TakeIdleBatchScratch(512); v != nil || fl != nil || p != 0 {
			t.Fatalf("expected take refusal under tight budget: %v %v %d", v, fl, p)
		}
		filler.Close()
		if got := b.Stats(); got.TotalBytes != baseline.TotalBytes || got.ReservedBytes != 0 || got.ExternalBytes != 0 {
			t.Fatalf("take failure leaked parked payload: baseline=%+v got=%+v", baseline, got)
		}
		b.Close()
		if b.Stats().TotalBytes != 0 {
			t.Fatalf("close leaked after take failure: %+v", b.Stats())
		}
	})

	t.Run("resize_work_and_writer_prepare_pressure", func(t *testing.T) {
		for _, route := range []string{"resize", "acquire-work", "resize-work"} {
			t.Run(route, func(t *testing.T) {
				limits := DefaultCOWLimits()
				limits.MaxInFlightBytes = 64 << 10
				b, err := NewCOWBudget(limits)
				if err != nil {
					t.Fatal(err)
				}
				target, err := b.AcquireExternal(1)
				if err != nil {
					t.Fatal(err)
				}
				header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
				const payload = uint64(8192)
				pressure, err := b.AcquireExternal(limits.MaxInFlightBytes - b.Stats().ReservedBytes - payload - 2*header)
				if err != nil {
					t.Fatal(err)
				}
				l, err := b.AcquireExternal(payload)
				if err != nil {
					t.Fatal(err)
				}
				if !b.PutIdleBatchScratch(new([32]byte), l, payload) {
					t.Fatal("put refused")
				}
				before := b.Stats()
				insufficient := &iterator.OrdinalScanWork{}
				if route == "acquire-work" {
					if rl, ok, err := b.AcquireRetentionWork(4096, insufficient); rl != nil || ok || err != nil {
						t.Fatal("work refusal failed")
					}
				} else if route == "resize-work" {
					if ok, err := target.ResizeWork(4096, insufficient); ok || err != nil {
						t.Fatal("work refusal failed")
					}
				}
				if got := b.Stats(); got != before {
					t.Fatal("work refusal mutated retention")
				}
				work := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
				switch route {
				case "resize":
					err = target.Resize(4096)
				case "resize-work":
					var ok bool
					ok, err = target.ResizeWork(4096, work)
					if !ok {
						t.Fatalf("resize work refused: %v", err)
					}
				case "acquire-work":
					var rl interface{ Close() }
					var ok bool
					rl, ok, err = b.AcquireRetentionWork(4096, work)
					if !ok {
						t.Fatalf("acquire work refused: %v", err)
					}
					rl.Close()
				}
				if err != nil {
					t.Fatal(err)
				}
				if v, _, _ := b.TakeIdleBatchScratch(64); v != nil {
					t.Fatalf("%s did not evict idle batch scratch", route)
				}
				target.Close()
				pressure.Close()
				b.Close()
				if b.Stats().TotalBytes != 0 {
					t.Fatalf("drain leaked: %+v", b.Stats())
				}
			})
		}

		// Writer Prepare pressure reclaims idle batch scratch.
		limits := DefaultCOWLimits()
		limits.MaxInFlightBytes = 64 << 10
		b, w := cowTestWriter(t, limits)
		header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
		const payload = uint64(8192)
		pressure, err := b.AcquireExternal(limits.MaxInFlightBytes - b.Stats().ReservedBytes - payload - 2*header)
		if err != nil {
			t.Fatal(err)
		}
		l, err := b.AcquireExternal(payload)
		if err != nil {
			t.Fatal(err)
		}
		if !b.PutIdleBatchScratch(new([32]byte), l, payload) {
			t.Fatal("put refused")
		}
		prepared, err := w.Prepare([]COWMutation{{Key: []byte("key"), Value: []byte("value")}}, COWPrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if v, _, _ := b.TakeIdleBatchScratch(64); v != nil {
			t.Fatal("writer did not evict idle batch scratch")
		}
		root := prepared.Publish()
		cowTestRelease(root)
		cowTestClose(w)
		pressure.Close()
		b.Close()
		if b.Stats().TotalBytes != 0 {
			t.Fatalf("writer drain leaked: %+v", b.Stats())
		}
	})

	t.Run("concurrent_park_put_take_and_close", func(t *testing.T) {
		b, err := NewCOWBudget(DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		const baseBytes = uint64(256)
		const payload = uint64(4096)
		for i := 0; i < cowIdleBatchMaxSlots; i++ {
			l, err := b.AcquireExternal(baseBytes + payload)
			if err != nil {
				t.Fatal(err)
			}
			if !b.PutIdleBatchScratch(new([32]byte), l, payload) {
				l.Close()
				t.Fatalf("seed slot %d refused", i)
			}
		}
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 200; j++ {
					v, lease, p := b.TakeIdleBatchScratch(baseBytes)
					if lease == nil {
						continue
					}
					if (id+j)%2 == 0 {
						if b.ParkIdleBatchScratchFromLease(v, lease, baseBytes, p) {
							lease.Close()
						} else {
							lease.Close()
						}
					} else {
						if !b.PutIdleBatchScratch(v, lease, p) {
							lease.Close()
						}
					}
				}
			}(i)
		}
		b.Close()
		wg.Wait()
		if got := b.Stats(); got.TotalBytes != 0 || got.ReservedBytes != 0 || got.ExternalBytes != 0 || got.ExternalLeases != 0 || got.ControlBytes != 0 {
			t.Fatalf("concurrent close leaked: %+v", got)
		}
	})

	t.Run("take_into_existing_lease", func(t *testing.T) {
		b, err := NewCOWBudget(DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		const baseBytes = uint64(512)
		const payload = uint64(4096)
		l, err := b.AcquireExternal(baseBytes + payload)
		if err != nil {
			t.Fatal(err)
		}
		v := new([32]byte)
		if !b.ParkIdleBatchScratchFromLease(v, l, baseBytes, payload) {
			t.Fatal("ParkIdleBatchScratchFromLease refused")
		}
		before := b.Stats()
		gotV, gotP := b.TakeIdleBatchScratchIntoLease(l)
		if gotV != v || gotP != payload {
			t.Fatalf("TakeIdleBatchScratchIntoLease=(%v, %d), want (%v, %d)", gotV, gotP, v, payload)
		}
		// Total/Reserved/External/Leases must remain unchanged because payload was
		// already continuously charged in this budget.
		if after := b.Stats(); after != before {
			t.Fatalf("TakeIdleBatchScratchIntoLease changed stats: before=%+v after=%+v", before, after)
		}
		// Second take on empty stack returns nil, 0.
		if v2, p2 := b.TakeIdleBatchScratchIntoLease(l); v2 != nil || p2 != 0 {
			t.Fatalf("empty TakeIdleBatchScratchIntoLease=(%v, %d)", v2, p2)
		}
		l.Close()
		b.Close()
		if got := b.Stats(); got.TotalBytes != 0 || got.ReservedBytes != 0 || got.ExternalLeases != 0 {
			t.Fatalf("take_into_existing_lease leaked: %+v", got)
		}
	})
}
