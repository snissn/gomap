package caching

import (
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCOWBatchStorageAdmissionAndReset(t *testing.T) {
	db := cowPointerFixture(t)
	baseline := db.cow.budget.Stats()
	b := db.NewBatchWithSize(1)
	defer b.Close()
	lease := b.cowState.lease
	for i := 0; i < 65; i++ {
		if err := b.Set([]byte{byte(i)}, make([]byte, 1025)); err != nil {
			t.Fatal(err)
		}
	}
	if cap(b.entries) < 65 {
		t.Fatal("missing admitted growth")
	}
	opened := db.cow.budget.Stats()
	if opened.ExternalBytes <= baseline.ExternalBytes || opened.ExternalLeases != baseline.ExternalLeases+1 {
		t.Fatalf("backing growth did not share the batch admission: before=%+v after=%+v", baseline, opened)
	}
	b.Reset()
	if len(b.entries) != 0 || b.copyArena != nil {
		t.Fatal("reset retained batch storage")
	}
	leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	ownerBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchOwner{}))) + leaseBytes
	reset := db.cow.budget.Stats()
	if reset.ExternalBytes != baseline.ExternalBytes+ownerBytes || reset.ExternalLeases != baseline.ExternalLeases+1 || b.cowState.lease != lease {
		t.Fatalf("reset lost the original owner admission: before=%+v after=%+v", baseline, reset)
	}
	if err := b.Set([]byte("again"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b.Reset()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(b.Set([]byte("closed"), nil), ErrBatchClosed) {
		t.Fatal("closed batch accepted growth")
	}
	closed := db.cow.budget.Stats()
	if closed.ExternalBytes != baseline.ExternalBytes || closed.ExternalLeases != baseline.ExternalLeases {
		t.Fatalf("external charge leaked: before=%+v after=%+v", baseline, closed)
	}
}

func TestCOWBatchGuardReservationSurvivesReset(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		externalWAL, owner   bool
		wantGuardReservation bool
	}{
		{name: "no external WAL", owner: true},
		{name: "no transaction owner", externalWAL: true},
		{name: "reserved guard", externalWAL: true, owner: true, wantGuardReservation: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := nativeBridgeFixture(t, false)
			owner := db.cow.txnOwner
			db.externalCommandWAL = tc.externalWAL
			if !tc.owner {
				db.cow.txnOwner = nil
			}
			defer func() {
				db.externalCommandWAL = false
				db.cow.txnOwner = owner
			}()
			before := db.cow.budget.Stats()
			b := db.NewBatchWithSize(1)
			defer b.Close()
			if b == &cowDeniedBatch || b.cowState.guardReserved != tc.wantGuardReservation || b.cowState.guard != nil {
				t.Fatal("constructor did not capture the guard reservation before guard acquisition")
			}
			lease := b.cowState.lease
			ownerBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchOwner{})))
			ownerBytes += memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			if tc.wantGuardReservation {
				ownerBytes += memtable.COWAllocationCharge(backenddb.CommandWALStagingGuardAllocationBytesV1())
			}
			// Change both constructor inputs before growth and reuse. Neither the
			// mutable DB configuration nor the absent guard owns this reservation.
			db.externalCommandWAL = !tc.wantGuardReservation
			if tc.wantGuardReservation {
				db.cow.txnOwner = nil
			} else {
				db.cow.txnOwner = owner
			}
			for i := 0; i < 2; i++ {
				if err := b.Set([]byte("key"), make([]byte, 1025)); err != nil {
					t.Fatal(err)
				}
				grown := db.cow.budget.Stats()
				if grown.ExternalBytes != before.ExternalBytes+ownerBytes+b.cowState.storageBytes || grown.ExternalLeases != before.ExternalLeases+1 {
					t.Fatalf("growth changed the constructor reservation: before=%+v after=%+v", before, grown)
				}
				b.Reset()
				reset := db.cow.budget.Stats()
				if reset.ExternalBytes != before.ExternalBytes+ownerBytes || reset.ExternalLeases != before.ExternalLeases+1 || b.cowState.lease != lease || b.cowState.storageBytes != 0 {
					t.Fatalf("reset changed the constructor reservation: before=%+v after=%+v", before, reset)
				}
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			closed := db.cow.budget.Stats()
			if closed.ExternalBytes != before.ExternalBytes || closed.ExternalLeases != before.ExternalLeases {
				t.Fatalf("close leaked the constructor reservation: before=%+v after=%+v", before, closed)
			}
		})
	}
}

func TestCOWBatchGrowthRefusalKeepsExistingStorage(t *testing.T) {
	db := cowPointerFixture(t)
	b := db.newCOWBatch(1)
	defer b.Close()
	if err := b.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	before := db.cow.budget.Stats()
	leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	filler, err := db.cow.budget.AcquireExternal(db.cow.budget.Limits().MaxInFlightBytes - before.ReservedBytes - leaseBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer filler.Close()
	full := db.cow.budget.Stats()
	capacity := cap(b.entries)
	b.Reserve(capacity + 1)
	if !errors.Is(b.cowState.err, memtable.ErrCOWCapacity) || cap(b.entries) != capacity || len(b.entries) != 1 || string(b.entries[0].Value) != "value" {
		t.Fatal("refused growth changed existing storage")
	}
	if after := db.cow.budget.Stats(); after.TotalBytes != full.TotalBytes || after.ReservedBytes != full.ReservedBytes || after.ExternalBytes != full.ExternalBytes || after.ExternalLeases != full.ExternalLeases {
		t.Fatalf("refused growth changed admission: before=%+v after=%+v", full, after)
	}
	b.Reset()
	if after := db.cow.budget.Stats(); after.ExternalBytes >= full.ExternalBytes || after.ExternalLeases != full.ExternalLeases {
		t.Fatalf("reset failed to refund admitted backing: before=%+v after=%+v", full, after)
	}
}

func TestCOWBatchResetAfterBudgetCloseReleasesStorage(t *testing.T) {
	db := cowPointerFixture(t)
	b := db.NewBatchWithSize(1)
	defer b.Close()
	if err := b.Set([]byte("key"), make([]byte, 1025)); err != nil {
		t.Fatal(err)
	}
	db.cow.budget.Close()
	before := db.cow.budget.Stats()
	backingBytes := b.cowState.storageBytes
	b.Reset()
	leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	ownerBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowBatchOwner{}))) + leaseBytes
	if after := db.cow.budget.Stats(); after.ExternalBytes != before.ExternalBytes-backingBytes || after.ExternalLeases != before.ExternalLeases {
		t.Fatalf("closed budget prevented backing refund: before=%+v after=%+v", before, after)
	}
	if !errors.Is(b.Set([]byte("refused"), nil), memtable.ErrCOWClosed) {
		t.Fatal("closed budget admitted new backing")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if after := db.cow.budget.Stats(); after.ExternalBytes != before.ExternalBytes-backingBytes-ownerBytes || after.ExternalLeases != before.ExternalLeases-1 {
		t.Fatalf("close leaked admission: before=%+v after=%+v", before, after)
	}
}

func TestCOWBatchRefusalBeforeStagingAndCallback(t *testing.T) {
	db := cowPointerFixture(t)
	b := db.NewBatch()
	defer b.Close()
	if err := b.SetOps([]batch.Entry{{Type: batch.OpPut, Key: []byte("good"), Value: []byte("v")}, {Type: batch.OpDeleteRange, Key: []byte("a"), Value: []byte("z")}}); !errors.Is(err, ErrCOWUnsupported) {
		t.Fatal(err)
	}
	if len(b.entries) != 0 {
		t.Fatal("unsupported group partially staged")
	}
	b.Reserve(1 << 30)
	if !errors.Is(b.Set([]byte("key"), []byte("v")), memtable.ErrCOWCapacity) {
		t.Fatal("reserve refusal lost")
	}
	called := false
	if err := b.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error { called = true; return nil }); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatal(err)
	}
	if called {
		t.Fatal("refused batch invoked append")
	}
	db.cow.budget.Close()
	denied := db.NewBatch()
	if denied != &cowDeniedBatch {
		t.Fatal("refused constructor allocated a wrapper")
	}
	denied.Reserve(8)
	denied.Reset()
	_ = denied.Close()
	if !errors.Is(denied.Set([]byte("x"), []byte("v")), memtable.ErrCOWCapacity) {
		t.Fatal("denied carrier changed")
	}
}

func TestCOWHugeFiniteInputsRefuseBeforeAllocation(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	if _, _, err := validateCOWOpenOptions(Options{DisableWAL: true, MemtableShards: maxInt}); err == nil {
		t.Fatal("overflowing shard input accepted")
	}
	db := cowPointerFixture(t)
	baseline := db.cow.budget.Stats().ExternalBytes
	for _, n := range []int{maxInt, maxInt / int(unsafe.Sizeof(batch.Entry{})), 1 << 30} {
		b := db.NewBatchWithSize(n)
		// The public hint normalizer may cap a constructor hint. An explicit
		// Reserve must still refuse the original finite input before make.
		b.Reserve(n)
		if !errors.Is(b.Set([]byte("x"), []byte("v")), memtable.ErrCOWCapacity) {
			t.Fatalf("Reserve(%d) accepted", n)
		}
		_ = b.Close()
	}
	if got := db.cow.budget.Stats().ExternalBytes; got != baseline {
		t.Fatalf("refusal retained charge: %d want %d", got, baseline)
	}
	b := db.NewBatch()
	defer b.Close()
	before := db.cow.budget.Stats()
	for _, bytes := range []uint64{^uint64(0), ^uint64(0) - b.cowState.storageBytes} {
		if !errors.Is(b.cowAdmitStorage(bytes), memtable.ErrCOWCapacity) {
			t.Fatal("cumulative backing charge wrapped")
		}
	}
	if after := db.cow.budget.Stats(); after.TotalBytes != before.TotalBytes || after.ReservedBytes != before.ReservedBytes || after.ExternalBytes != before.ExternalBytes || after.ExternalLeases != before.ExternalLeases {
		t.Fatalf("overflow changed admission: before=%+v after=%+v", before, after)
	}
}

func TestCOWBatchResetLastHintPreSizingAndTightBudgetFallback(t *testing.T) {
	db := cowPointerFixture(t)
	baseline := db.cow.budget.Stats()
	b := db.NewBatchWithSize(100)
	defer b.Close()

	if err := b.Set([]byte("k0"), []byte("v0")); err != nil {
		t.Fatal(err)
	}
	b.Reset()
	if cap(b.entries) != 0 || b.cowState.storageBytes != 0 {
		t.Fatalf("Reset retained backing: cap=%d storageBytes=%d", cap(b.entries), b.cowState.storageBytes)
	}
	if err := b.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if got := cap(b.entries); got != 100 {
		t.Fatalf("post-Reset cap(entries)=%d, want 100 from constructor hint", got)
	}

	// Grow beyond initial hint and verify Reset remembers the larger count.
	for i := 1; i < 200; i++ {
		if err := b.Set([]byte{byte(i >> 8), byte(i)}, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	b.Reset()
	if err := b.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if got := cap(b.entries); got != 200 {
		t.Fatalf("post-Reset cap(entries)=%d, want 200 from prior batch len", got)
	}
	b.Reset()

	// Tight-budget fallback: leave enough headroom for 16 entries + 256B copyArena,
	// but NOT enough for 200 entries. First Set must fall back to 16 without
	// latching b.cowState.err.
	beforeTight := db.cow.budget.Stats()
	leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	entry16Bytes := memtable.COWAllocationCharge(16 * uint64(unsafe.Sizeof(batch.Entry{})))
	arena256Bytes := memtable.COWAllocationCharge(256)
	entry200Bytes := memtable.COWAllocationCharge(200 * uint64(unsafe.Sizeof(batch.Entry{})))
	headroom := entry16Bytes + arena256Bytes
	if headroom >= entry200Bytes {
		t.Fatalf("test headroom %d >= 200-entry charge %d", headroom, entry200Bytes)
	}
	avail := db.cow.budget.Limits().MaxInFlightBytes - beforeTight.ReservedBytes - leaseBytes
	filler, err := db.cow.budget.AcquireExternal(avail - headroom)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("tight"), []byte("ok")); err != nil {
		filler.Close()
		t.Fatalf("tight-budget Set failed instead of falling back to 16: %v", err)
	}
	if b.cowState.err != nil {
		filler.Close()
		t.Fatalf("tight-budget fallback latched cowState.err=%v", b.cowState.err)
	}
	if got := cap(b.entries); got != 16 {
		filler.Close()
		t.Fatalf("tight-budget fallback cap(entries)=%d, want 16", got)
	}
	filler.Close()
	b.Reset()

	// Tight-budget payload headroom regression: leave enough headroom for the
	// 200-entry hint alone, but NOT enough for 200 entries + the 256B copyArena
	// (while 16 entries + 256B copyArena fits). Best-effort admission must fall
	// back to 16 so the entry + payload succeed together without ErrCOWCapacity.
	beforePayloadTight := db.cow.budget.Stats()
	availPayload := db.cow.budget.Limits().MaxInFlightBytes - beforePayloadTight.ReservedBytes - leaseBytes
	payloadTightHeadroom := entry200Bytes
	if payloadTightHeadroom < entry16Bytes+arena256Bytes || payloadTightHeadroom >= entry200Bytes+arena256Bytes {
		t.Fatalf("invalid payload-tight headroom=%d entry16+arena=%d entry200+arena=%d", payloadTightHeadroom, entry16Bytes+arena256Bytes, entry200Bytes+arena256Bytes)
	}
	payloadFiller, err := db.cow.budget.AcquireExternal(availPayload - payloadTightHeadroom)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("tight-payload"), []byte("ok")); err != nil {
		payloadFiller.Close()
		t.Fatalf("payload-tight Set failed instead of falling back to 16: %v", err)
	}
	if b.cowState.err != nil {
		payloadFiller.Close()
		t.Fatalf("payload-tight fallback latched cowState.err=%v", b.cowState.err)
	}
	if got := cap(b.entries); got != 16 {
		payloadFiller.Close()
		t.Fatalf("payload-tight fallback cap(entries)=%d, want 16", got)
	}
	payloadFiller.Close()
	b.Reset()

	// Verify cowSetOps pre-reserves exact len(ops) on an empty batch when > 16,
	// and preserves geometric growth across repeated small SetOps calls.
	ops := make([]batch.Entry, 50)
	for i := range ops {
		ops[i] = batch.Entry{Type: batch.OpPut, Key: []byte{byte(i)}, Value: []byte("v")}
	}
	if err := b.SetOps(ops); err != nil {
		t.Fatal(err)
	}
	if got := cap(b.entries); got != 50 {
		t.Fatalf("SetOps(50) cap(entries)=%d, want 50", got)
	}
	small := make([]batch.Entry, 5)
	for i := range small {
		small[i] = batch.Entry{Type: batch.OpPut, Key: []byte{byte(100 + i)}, Value: []byte("v")}
	}
	if err := b.SetOps(small); err != nil {
		t.Fatal(err)
	}
	if got := cap(b.entries); got != 100 {
		t.Fatalf("repeated SetOps cap(entries)=%d, want geometric 100", got)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if after := db.cow.budget.Stats(); after.ExternalBytes != baseline.ExternalBytes || after.ExternalLeases != baseline.ExternalLeases {
		t.Fatalf("external charge leaked: before=%+v after=%+v", baseline, after)
	}
}

func TestCOWNewBatchAdaptiveHintAndTightBudgetFallback(t *testing.T) {
	db := cowPointerFixture(t)
	baseline := db.cow.budget.Stats()

	// 1. Cold DB keeps eager 16-entry constructor contract.
	cold := db.NewBatch()
	if cold.cowState.err != nil || cap(cold.entries) != batchDefaultEntriesCap {
		_ = cold.Close()
		t.Fatalf("cold NewBatch(): cap(entries)=%d want=%d err=%v", cap(cold.entries), batchDefaultEntriesCap, cold.cowState.err)
	}
	if err := cold.Close(); err != nil {
		t.Fatal(err)
	}

	// 2. Commit a large batch to warm batchEntryHint and batchCopyBytesHint.
	warm := db.NewBatch()
	val := make([]byte, 320)
	for i := 0; i < 256; i++ {
		if err := warm.Set([]byte(fmt.Sprintf("warm-%04d", i)), val); err != nil {
			_ = warm.Close()
			t.Fatal(err)
		}
	}
	if err := warm.Write(); err != nil {
		_ = warm.Close()
		t.Fatal(err)
	}
	if err := warm.Close(); err != nil {
		t.Fatal(err)
	}
	wantEntryHint := int(db.batchEntryHint.Load())
	wantCopyHint := int(db.batchCopyBytesHint.Load())
	if wantEntryHint <= batchDefaultEntriesCap || wantCopyHint <= 4096 {
		t.Fatalf("expected warm hints: entry=%d copy=%d", wantEntryHint, wantCopyHint)
	}

	// 3. Warm NewBatch skips eager backing in constructor, then admits both
	// entry and copyArena hints together on first owned write.
	adaptive := db.NewBatch()
	if adaptive.cowState.err != nil || cap(adaptive.entries) != 0 || cap(adaptive.copyArena) != 0 {
		_ = adaptive.Close()
		t.Fatalf("warm NewBatch allocated eagerly: entries=%d arena=%d err=%v", cap(adaptive.entries), cap(adaptive.copyArena), adaptive.cowState.err)
	}
	wantArenaCap := db.batchCopyArenaInitCap(adaptive.cowState.lastHint)
	if err := adaptive.Set([]byte("owned"), []byte("payload")); err != nil {
		_ = adaptive.Close()
		t.Fatal(err)
	}
	if cap(adaptive.entries) != adaptive.cowState.lastHint || cap(adaptive.copyArena) < wantArenaCap {
		_ = adaptive.Close()
		t.Fatalf("warm Set admission mismatch: entries=%d (want %d) arena=%d (want >= %d)", cap(adaptive.entries), adaptive.cowState.lastHint, cap(adaptive.copyArena), wantArenaCap)
	}
	if err := adaptive.Close(); err != nil {
		t.Fatal(err)
	}

	// 4. SetView skips copyArena while still using lastHint for entries.
	viewBatch := db.NewBatch()
	if err := viewBatch.SetView([]byte("view-k"), []byte("view-v")); err != nil {
		_ = viewBatch.Close()
		t.Fatal(err)
	}
	if cap(viewBatch.entries) != viewBatch.cowState.lastHint || cap(viewBatch.copyArena) != 0 {
		_ = viewBatch.Close()
		t.Fatalf("SetView unexpected copyArena: entries=%d arena=%d", cap(viewBatch.entries), cap(viewBatch.copyArena))
	}
	if err := viewBatch.Close(); err != nil {
		t.Fatal(err)
	}

	// 5. Tight budget: NewBatch succeeds (zero eager backing), then first Set
	// falls back cleanly to 16 entries and 256B copyArena without latching err.
	beforeTight := db.cow.budget.Stats()
	leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	ownerBytes := cowBatchBaseBytes(false)
	entry16Bytes := memtable.COWAllocationCharge(uint64(batchDefaultEntriesCap) * uint64(unsafe.Sizeof(batch.Entry{})))
	arena256Bytes := memtable.COWAllocationCharge(256)
	headroom := leaseBytes + ownerBytes + entry16Bytes + arena256Bytes
	avail := db.cow.budget.Limits().MaxInFlightBytes - beforeTight.ReservedBytes - leaseBytes
	filler, err := db.cow.budget.AcquireExternal(avail - headroom)
	if err != nil {
		t.Fatal(err)
	}
	tight := db.NewBatch()
	if tight.cowState.err != nil {
		filler.Close()
		_ = tight.Close()
		t.Fatalf("tight warm NewBatch latched err=%v", tight.cowState.err)
	}
	if err := tight.Set([]byte("tight-k"), []byte("ok")); err != nil {
		filler.Close()
		_ = tight.Close()
		t.Fatalf("tight Set failed instead of falling back: %v", err)
	}
	filler.Close()
	if tight.cowState.err != nil || cap(tight.entries) != batchDefaultEntriesCap || cap(tight.copyArena) != 256 {
		_ = tight.Close()
		t.Fatalf("tight fallback mismatch: err=%v entries=%d arena=%d", tight.cowState.err, cap(tight.entries), cap(tight.copyArena))
	}
	if err := tight.Close(); err != nil {
		t.Fatal(err)
	}
	after := db.cow.budget.Stats()
	if after.ReservedBytes != beforeTight.ReservedBytes || after.ExternalBytes != beforeTight.ExternalBytes || after.ExternalLeases != beforeTight.ExternalLeases {
		t.Fatalf("external charge leaked: baseline=%+v beforeTight=%+v after=%+v", baseline, beforeTight, after)
	}
}

func BenchmarkCOWNewBatchAdaptive(b *testing.B) {
	dir := b.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		DisableWAL:               true,
		AllowUnsafe:              true,
		MemtableMode:             "cow_btree",
		FlushThreshold:           1 << 30,
		MemtableShards:           4,
		ValueLogPointerThreshold: 512,
		ValueLogCompression:      uint8(vlogCompressionOff),
	})
	if err != nil {
		_ = backend.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	const batchEntries = 8000
	keys := make([][]byte, batchEntries)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%07d", i))
	}
	val := make([]byte, 128)
	// Warm DB hints with one committed batch.
	warm := db.NewBatch()
	for i := range keys {
		if err := warm.Set(keys[i], val); err != nil {
			_ = warm.Close()
			b.Fatal(err)
		}
	}
	if err := warm.Write(); err != nil {
		_ = warm.Close()
		b.Fatal(err)
	}
	if err := warm.Close(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch := db.NewBatch()
		for i := range keys {
			if err := batch.Set(keys[i], val); err != nil {
				_ = batch.Close()
				b.Fatal(err)
			}
		}
		if err := batch.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
