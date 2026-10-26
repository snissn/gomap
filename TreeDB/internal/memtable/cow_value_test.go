package memtable

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWPackedValueRoundTrip(t *testing.T) {
	for _, e := range []COWMutation{
		{},
		{Value: []byte{}, Flags: node.FlagInline},
		{Value: []byte{0, 1, 0xff, 0, 'v'}, Revision: 0x8877665544332211, Flags: node.FlagInline},
		{Ptr: page.ValuePtr{Offset: 0xfedcba9876543210, Length: 0x87654321, FileID: 0xfedcba98}, Revision: ^page.EntryRevision(0), Flags: node.FlagPointer},
		{Revision: 0x8877665544332211, Flags: node.FlagTombstone},
		{Value: []byte("payload"), Ptr: page.ValuePtr{Offset: 8, Length: 9, FileID: 10}, Revision: 11, Flags: node.FlagPointer},
	} {
		want := COWRecord{Key: "key", Value: string(e.Value), Ptr: e.Ptr, Revision: e.Revision, Flags: e.Flags}
		encoded := cowEncodeValue(e)
		if len(encoded) != cowValueMetadataBytes+len(e.Value) {
			t.Fatalf("encoded length=%d payload=%d", len(encoded), len(e.Value))
		}
		if len(e.Value) != 0 {
			e.Value[0] ^= 0xff
		}
		if got := encoded.record("key"); got != want {
			t.Fatalf("decoded=%+v want=%+v", got, want)
		}
		if allocations := testing.AllocsPerRun(100, func() {
			if encoded.record("key") != want {
				panic("packed value changed")
			}
		}); allocations != 0 {
			t.Fatalf("metadata decoding allocated %g", allocations)
		}
	}
	encoded := cowEncodeValue(COWMutation{
		Ptr:      page.ValuePtr{Offset: 0x0807060504030201, Length: 0x0c0b0a09, FileID: 0x100f0e0d},
		Revision: 0x1817161514131211, Flags: 0x19,
	})
	for i := range encoded {
		if encoded[i] != byte(i+1) {
			t.Fatalf("metadata byte %d=%x", i, encoded[i])
		}
	}
	if got := unsafe.Sizeof(cowPairLayout{}); got != 2*unsafe.Sizeof("") {
		t.Fatalf("packed pair=%d want two string headers", got)
	}
	for _, n := range []int{-1, int(^uint(0) >> 1), int(^uint(0)>>1) - cowValueMetadataBytes + 1} {
		if _, ok := cowValueSize(n); ok {
			t.Fatalf("invalid payload size accepted: %d", n)
		}
	}
	maxPayload := int(^uint(0)>>1) - cowValueMetadataBytes
	if size, ok := cowValueSize(maxPayload); !ok || size != int(^uint(0)>>1) {
		t.Fatalf("largest supported payload size=%d ok=%v", size, ok)
	}
}

func TestCOWPackedValueAllocationCharge(t *testing.T) {
	cowTestIsolateAllocationWitness(t)
	// Prefix-only rows now allocate too. Cross both small size classes and the
	// large-allocation threshold with the metadata included before rounding.
	for _, size := range []int{0, 1, 7, 8, 31, 64, 487, 488, 512, 1024, 32743, 32744, 65536} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			e := COWMutation{Value: bytes.Repeat([]byte{'v'}, size), Flags: node.FlagPointer}
			var encoded cowValue
			if allocations := testing.AllocsPerRun(20, func() { encoded = cowEncodeValue(e) }); allocations != 1 {
				t.Fatalf("encoding allocations=%g want one backing", allocations)
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			encoded = cowEncodeValue(e)
			runtime.ReadMemStats(&after)
			allocated := after.TotalAlloc - before.TotalAlloc
			charge := cowAllocation(uint64(cowValueMetadataBytes + size))
			if allocated > charge {
				t.Fatalf("encoding allocated=%d > charge=%d", allocated, charge)
			}
			t.Logf("payload=%d encoding=%d allocated=%d charge=%d", size, len(encoded), allocated, charge)
			runtime.KeepAlive(encoded)
		})
	}
}

func TestCOWPackedPointerAdmissionCancelAndRetention(t *testing.T) {
	cowTestIsolateAllocationWitness(t)
	b, w := cowTestWriter(t, DefaultCOWLimits())
	oldMutation := COWMutation{Key: []byte("key"), Ptr: page.ValuePtr{Offset: 1, Length: 2, FileID: 3}, Revision: 4, Flags: node.FlagPointer}
	old := cowTestPublish(t, w, []COWMutation{oldMutation})
	nextMutation := COWMutation{Key: []byte("key"), Ptr: page.ValuePtr{Offset: ^uint64(0), Length: ^uint32(0), FileID: ^uint32(0)}, Revision: ^page.EntryRevision(0), Flags: node.FlagPointer}
	entries := []COWMutation{nextMutation}
	charge, err := w.Estimate(entries, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if charge.Payload != cowAllocation(cowValueMetadataBytes) {
		t.Fatalf("prefix-only replacement payload charge=%d", charge.Payload)
	}
	before := b.Stats()
	originalLimit := b.limits.MaxInFlightBytes
	// Refuse the irreducible header/payload envelope, independent of the
	// conservative node bound which raw preparation can now refine.
	b.limits.MaxInFlightBytes = charge.Total() - charge.Nodes - charge.Payload
	if p, err := w.Prepare(entries, COWPrepareOptions{}); p != nil || !errors.Is(err, ErrCOWCapacity) {
		t.Fatal("prefix escaped finite admission", p, err)
	}
	if b.Stats() != before {
		t.Fatal("refused prefix admission changed accounting")
	}
	b.limits.MaxInFlightBytes = originalLimit
	runtime.GC()
	var a, z runtime.MemStats
	runtime.ReadMemStats(&a)
	p, err := w.Prepare(entries, COWPrepareOptions{})
	runtime.ReadMemStats(&z)
	if err != nil {
		t.Fatal(err)
	}
	if actual := z.TotalAlloc - a.TotalAlloc; actual > charge.Total() {
		t.Fatalf("prefix-only Prepare allocated=%d reserve=%d", actual, charge.Total())
	}
	retirement := p.Cancel()
	p = nil
	retirement.Drain()
	if stats := b.Stats(); stats.TotalBytes != before.TotalBytes || stats.ReservedBytes != 0 {
		t.Fatal("cancel retained packed value charge", stats)
	}
	next := cowTestPublish(t, w, entries)
	if got, ok := old.Get(oldMutation.Key); !ok || got.Ptr != oldMutation.Ptr || got.Revision != oldMutation.Revision || got.Value != "" || got.Flags != node.FlagPointer {
		t.Fatal("opening pointer changed", got, ok)
	}
	if got, ok := next.Get(nextMutation.Key); !ok || got.Ptr != nextMutation.Ptr || got.Revision != nextMutation.Revision || got.Value != "" || got.Flags != node.FlagPointer {
		t.Fatal("successor pointer mismatch", got, ok)
	}
	cowTestClose(w)
	b.Close()
	cowTestRelease(next)
	if b.Stats().HistoryBytes == 0 {
		t.Fatal("opening root lost retained encoding history")
	}
	cowTestRelease(old)
	if b.Stats().TotalBytes != 0 {
		t.Fatal("packed values leaked history", b.Stats())
	}
}

func TestCOWPackedCohortHeldCutsAndCancelledReplacement(t *testing.T) {
	b, w := cowTestWriter(t, DefaultCOWLimits())
	entries := cowTestEntries(96)
	wantOld := make(map[string]COWRecord, len(entries))
	for i := range entries {
		e := &entries[i]
		switch i % 3 {
		case 0:
			e.Value, e.Flags = nil, node.FlagPointer
			e.Ptr = page.ValuePtr{Offset: uint64(i)<<40 | 1, Length: uint32(i + 2), FileID: uint32(i + 3)}
		case 1:
			e.Value, e.Flags = nil, node.FlagTombstone
		case 2:
			e.Value = []byte{0, byte(i), 0xff}
		}
		wantOld[string(e.Key)] = COWRecord{Key: string(e.Key), Value: string(e.Value), Ptr: e.Ptr, Revision: e.Revision, Flags: e.Flags}
	}
	old := cowTestPublish(t, w, entries)
	oldCursor, err := old.Cursor(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []COWMutation{
		{Key: []byte("00000000"), Value: []byte("replacement"), Revision: 100, Flags: node.FlagInline},
		{Key: []byte("00000000a"), Ptr: page.ValuePtr{Offset: 200, Length: 201, FileID: 202}, Revision: 203, Flags: node.FlagPointer},
		{Key: []byte("00000001"), Remove: true},
		{Key: []byte("00000002"), Revision: 300, Flags: node.FlagTombstone},
		{Key: []byte("00000000"), Value: []byte{0xff, 0, 0xfe}, Revision: 400, Flags: node.FlagInline},
	}
	before := b.Stats()
	cancelled, err := w.Prepare(mutations, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	retirement := cancelled.Cancel()
	cancelled = nil
	retirement.Drain()
	if stats := b.Stats(); stats.TotalBytes != before.TotalBytes || stats.ReservedBytes != 0 {
		t.Fatal("cancelled cohort retained private encodings", stats)
	}
	wantNext := make(map[string]COWRecord, len(wantOld))
	for k, r := range wantOld {
		wantNext[k] = r
	}
	for _, e := range mutations {
		key := string(e.Key)
		if e.Remove {
			delete(wantNext, key)
		} else {
			wantNext[key] = COWRecord{Key: key, Value: string(e.Value), Ptr: e.Ptr, Revision: e.Revision, Flags: e.Flags}
		}
	}
	prepared, err := w.Prepare(mutations, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range mutations {
		for i := range e.Key {
			e.Key[i] = '!'
		}
		for i := range e.Value {
			e.Value[i] = '!'
		}
	}
	next := prepared.Publish()
	for _, rootCase := range []struct {
		root *COWRoot
		want map[string]COWRecord
	}{{old, wantOld}, {next, wantNext}} {
		count := 0
		rootCase.root.ScanOwned(func(got COWRecord) bool {
			if got != rootCase.want[got.Key] {
				t.Fatalf("scan record=%+v want=%+v", got, rootCase.want[got.Key])
			}
			point, ok := rootCase.root.Get([]byte(got.Key))
			if !ok || point != got {
				t.Fatal("point/scan mismatch", point, got)
			}
			count++
			return true
		})
		if count != len(rootCase.want) {
			t.Fatalf("scan count=%d want=%d", count, len(rootCase.want))
		}
	}
	if _, ok := next.Get([]byte("00000001")); ok {
		t.Fatal("deleted record survived cohort")
	}
	work := COWTraversalWork{RecordLimit: 32, ByteLimit: 1 << 20}
	traversal, err := next.NewTraversal(&work, 0)
	if err != nil {
		t.Fatal(err)
	}
	cowTestRelease(old)
	cowTestClose(w)
	b.Close()
	count := 0
	for {
		got, valid, err := oldCursor.Record()
		if err != nil {
			t.Fatal(err)
		}
		if !valid {
			break
		}
		if got != wantOld[got.Key] {
			t.Fatal("held opening cursor changed", got)
		}
		count++
		if err := oldCursor.Next(); err != nil {
			t.Fatal(err)
		}
	}
	if count != len(wantOld) {
		t.Fatalf("held cursor count=%d want=%d", count, len(wantOld))
	}
	for key, want := range wantNext {
		if got, valid := traversalRequest(t, traversal, COWSeekGE, key); !valid || got != want {
			t.Fatal("held successor traversal changed", got, want)
		}
	}
	retirement = traversalClose(t, traversal)
	retirement.Drain()
	oldCursor.Close()
	cowTestRelease(next)
	if b.Stats().TotalBytes != 0 {
		t.Fatal("held packed cuts leaked", b.Stats())
	}
}
