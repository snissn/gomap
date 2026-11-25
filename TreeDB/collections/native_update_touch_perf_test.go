package collections

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func openNativeUpdateTouchDB(tb testing.TB, dir string, pointerThresh int) (*treedb.DB, *CollectionManager, *Collection) {
	tb.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.VerifyOnRead = false
	opts.IndexOuterLeavesInValueLog = true
	opts.LeafPageReadCacheEntries = 256
	opts.LeafPrefixCompression = true
	opts.IndexColumnarLeaves = true
	opts.ValueLog.Compression = treedb.ValueLogCompressionOff
	opts.ValueLog.PointerThreshold = pointerThresh
	opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()

	owner, err := treedb.Open(opts)
	if err != nil {
		tb.Fatal(err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		_ = owner.Close()
		tb.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	if !manager.UsesNativeCommandWALApply() {
		_ = manager.Close()
		_ = owner.Close()
		tb.Fatal("expected native COW collection manager")
	}
	col, err := manager.OpenCollection("items")
	if err != nil {
		meta := &CollectionMeta{
			Name: "items",
			Options: CollectionOptions{
				DocumentFormat: DocumentFormatBSON,
			},
			Indexes: []IndexDefinition{
				{Name: "group", Field: "group", ValueType: IndexValueBSONOrderedV2},
				{Name: "tag", Field: "tag", ValueType: IndexValueBSONOrderedV2},
			},
		}
		if _, createErr := manager.CreateCollection(meta); createErr != nil {
			_ = manager.Close()
			_ = owner.Close()
			tb.Fatal(createErr)
		}
		col, err = manager.OpenCollection("items")
		if err != nil {
			_ = manager.Close()
			_ = owner.Close()
			tb.Fatal(err)
		}
	}
	if col.nativeJSON == nil {
		_ = manager.Close()
		_ = owner.Close()
		tb.Fatal("expected native JSON collection handle")
	}
	return owner, manager, col
}

func nativeUpdateTouchStatUint64(tb testing.TB, owner *treedb.DB, key string) uint64 {
	tb.Helper()
	raw := owner.Stats()[key]
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		tb.Fatalf("parse stat %q=%q: %v", key, raw, err)
	}
	return v
}

func makeNativeUpdateTouchBSONDoc(tb testing.TB, group, tag string, visits int64, payload string) []byte {
	tb.Helper()
	doc, err := bson.Marshal(bson.D{
		{Key: "group", Value: group},
		{Key: "tag", Value: tag},
		{Key: "visits", Value: visits},
		{Key: "payload", Value: payload},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return doc
}

func makeNativeUpdateTouchEmptyBSONDoc(tb testing.TB) []byte {
	tb.Helper()
	doc, err := bson.Marshal(bson.D{})
	if err != nil {
		tb.Fatal(err)
	}
	return doc
}

func TestTouchNativeRowUpdateUsesWarmLaneAndVerifiedProof(t *testing.T) {
	dir := t.TempDir()
	const pointerThresh = 4096
	owner, manager, col := openNativeUpdateTouchDB(t, dir, pointerThresh)
	closed := false
	closeAll := func() {
		if closed {
			return
		}
		closed = true
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(closeAll)

	const (
		inlineCount = 48
		emptyCount  = 8
		largeCount  = 8
	)
	inlinePad := string(bytes.Repeat([]byte("i"), 64))
	largePad := string(bytes.Repeat([]byte("L"), 6144))

	var ids, docs [][]byte
	for i := 0; i < inlineCount; i++ {
		ids = append(ids, []byte(fmt.Sprintf("inline-%03d", i)))
		docs = append(docs, makeNativeUpdateTouchBSONDoc(t, "g1", fmt.Sprintf("t%02d", i%8), int64(i), inlinePad))
	}
	for i := 0; i < emptyCount; i++ {
		ids = append(ids, []byte(fmt.Sprintf("empty-%03d", i)))
		docs = append(docs, makeNativeUpdateTouchEmptyBSONDoc(t))
	}
	for i := 0; i < largeCount; i++ {
		ids = append(ids, []byte(fmt.Sprintf("large-%03d", i)))
		docs = append(docs, makeNativeUpdateTouchBSONDoc(t, "g2", fmt.Sprintf("p%02d", i), int64(i), largePad))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	// Also seed a 0-byte raw inline key to verify NativeEntryAtRootAdmitted
	// preserves non-nil []byte{} and pre-admits COWAllocationCharge(0).
	if err := owner.Set([]byte("zero-byte-inline-key"), []byte{}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Verify 0-byte inline value and ErrKeyNotFound contracts on NativeEntryAtRootAdmitted.
	type admittedReader interface {
		NativeEntryAtRootAdmitted(root uint64, key []byte, admitOutput func(uint64) error) (node.LeafEntry, error)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	capRaw, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	ar, ok := capRaw.(admittedReader)
	if !ok {
		_ = capRaw.Close()
		t.Fatal("NativeSemanticCapture does not implement NativeEntryAtRootAdmitted")
	}
	tok, ok := capRaw.Snapshot().StateToken()
	if !ok {
		_ = capRaw.Close()
		t.Fatal("missing basis state token")
	}
	zeroAdmitCalls := 0
	var zeroAdmitCharge uint64
	zeroEntry, err := ar.NativeEntryAtRootAdmitted(tok.RootPageID, []byte("zero-byte-inline-key"), func(n uint64) error {
		zeroAdmitCalls++
		zeroAdmitCharge = n
		return nil
	})
	if err != nil {
		_ = capRaw.Close()
		t.Fatalf("NativeEntryAtRootAdmitted zero-byte key: %v", err)
	}
	if zeroEntry.Value == nil || len(zeroEntry.Value) != 0 {
		_ = capRaw.Close()
		t.Fatalf("zero-byte inline value must be non-nil empty slice: got nil=%v len=%d", zeroEntry.Value == nil, len(zeroEntry.Value))
	}
	if zeroAdmitCalls != 1 || zeroAdmitCharge != memtable.COWAllocationCharge(0) {
		_ = capRaw.Close()
		t.Fatalf("zero-byte inline admit calls=%d charge=%d want 1, %d", zeroAdmitCalls, zeroAdmitCharge, memtable.COWAllocationCharge(0))
	}
	missAdmitCalls := 0
	missEntry, missErr := ar.NativeEntryAtRootAdmitted(tok.RootPageID, []byte("definitely-absent-key"), func(uint64) error {
		missAdmitCalls++
		return nil
	})
	if !errors.Is(missErr, tree.ErrKeyNotFound) || missEntry.Value != nil || missAdmitCalls != 0 {
		_ = capRaw.Close()
		t.Fatalf("absent key: err=%v val=%v admitCalls=%d", missErr, missEntry.Value, missAdmitCalls)
	}
	if err := capRaw.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify explicit NativeJSONTxn touch on both inline and pointer docs uses warm lane (scratch == nil).
	for _, sampleID := range [][]byte{[]byte("inline-000"), []byte("empty-000"), []byte("large-000")} {
		tx, err := col.nativeJSON.Begin(col.name)
		if err != nil {
			t.Fatal(err)
		}
		g := &tx.bindings[0]
		row, err := tx.touchNativeRow(g, sampleID)
		if err != nil {
			_ = tx.Close()
			t.Fatalf("touchNativeRow(%q): %v", sampleID, err)
		}
		if row == nil || row.opening == nil {
			_ = tx.Close()
			t.Fatalf("touchNativeRow(%q): nil row/opening", sampleID)
		}
		if g.probe.scratch != nil {
			_ = tx.Close()
			t.Fatalf("touchNativeRow(%q) allocated physicalScratch on warm lane", sampleID)
		}
		if err := tx.Close(); err != nil {
			t.Fatal(err)
		}
	}

	verifiedHitsBefore := nativeUpdateTouchStatUint64(t, owner, "treedb.process.read_path.outer_leaf.cache.page_checksum_verified_hits")
	skipsBefore := nativeUpdateTouchStatUint64(t, owner, "treedb.process.read_path.outer_leaf.checksum.skips_total")

	wantVisits := make(map[string]int64, len(ids))
	for i, id := range ids {
		newVisits := int64(1000 + i)
		matched, modified, err := col.UpdateBSONSet(id, []BSONSetField{
			{Key: "visits", Value: bsonRawInt64(t, newVisits)},
		})
		if err != nil || !matched || !modified {
			t.Fatalf("UpdateBSONSet(%q): matched=%v modified=%v err=%v", id, matched, modified, err)
		}
		wantVisits[string(id)] = newVisits
	}

	verifiedHitsAfter := nativeUpdateTouchStatUint64(t, owner, "treedb.process.read_path.outer_leaf.cache.page_checksum_verified_hits")
	skipsAfter := nativeUpdateTouchStatUint64(t, owner, "treedb.process.read_path.outer_leaf.checksum.skips_total")
	if verifiedHitsAfter <= verifiedHitsBefore {
		t.Fatalf("expected page_checksum_verified_hits > %d after warm-lane updates, got %d", verifiedHitsBefore, verifiedHitsAfter)
	}
	if skipsAfter <= skipsBefore {
		t.Fatalf("expected outer_leaf.checksum.skips_total > %d after warm-lane updates, got %d", skipsBefore, skipsAfter)
	}

	verifyAll := func(c *Collection, phase string) {
		t.Helper()
		for _, id := range ids {
			raw, err := c.Get(id)
			if err != nil {
				t.Fatalf("%s Get(%q): %v", phase, id, err)
			}
			gotVisits := bson.Raw(raw).Lookup("visits").Int64()
			if gotVisits != wantVisits[string(id)] {
				t.Fatalf("%s doc %q visits=%d want %d", phase, id, gotVisits, wantVisits[string(id)])
			}
		}
	}
	verifyAll(col, "before-reopen")

	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	closeAll()

	owner2, manager2, col2 := openNativeUpdateTouchDB(t, dir, pointerThresh)
	t.Cleanup(func() {
		if err := manager2.Close(); err != nil {
			t.Error(err)
		}
		if err := owner2.Close(); err != nil {
			t.Error(err)
		}
	})
	verifyAll(col2, "after-reopen")
}

func TestTouchNativeRowUpdateCOWCapacityNoLeak(t *testing.T) {
	dir := t.TempDir()
	const pointerThresh = 4096
	owner, manager, col := openNativeUpdateTouchDB(t, dir, pointerThresh)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})

	inlineDoc := makeNativeUpdateTouchBSONDoc(t, "g1", "t1", 1, string(bytes.Repeat([]byte("a"), 1536)))
	pointerDoc := makeNativeUpdateTouchBSONDoc(t, "g2", "t2", 2, string(bytes.Repeat([]byte("p"), 6144)))
	if _, err := col.InsertBatch(
		[][]byte{[]byte("inline-doc"), []byte("pointer-doc")},
		[][]byte{inlineDoc, pointerDoc},
	); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Warm the basis read lanes first so lane workspace admission is settled.
	warmTx, err := col.nativeJSON.Begin(col.name)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range [][]byte{[]byte("inline-doc"), []byte("pointer-doc")} {
		if _, err := warmTx.touchNativeRow(&warmTx.bindings[0], id); err != nil {
			_ = warmTx.Close()
			t.Fatal(err)
		}
	}
	if err := warmTx.Close(); err != nil {
		t.Fatal(err)
	}

	// 1. Direct NativeEntryAtRootAdmitted refusal on admitOutput returns
	// ErrCOWCapacity with zero leaked bytes/leases and no output bytes.
	type admittedReader interface {
		NativeEntryAtRootAdmitted(root uint64, key []byte, admitOutput func(uint64) error) (node.LeafEntry, error)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	capRaw, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer capRaw.Close()
	budget := capRaw.NativeAllocator().(nativeHNSWAdmissionBudget)
	ar, ok := capRaw.(admittedReader)
	if !ok {
		t.Fatal("capture lacks NativeEntryAtRootAdmitted")
	}

	tx0, err := col.nativeJSON.Begin(col.name)
	if err != nil {
		t.Fatal(err)
	}
	primaryRoot := tx0.bindings[0].probe.domain.domain.baseRoot
	if err := tx0.Close(); err != nil {
		t.Fatal(err)
	}

	beforeDirect := budget.Stats()
	var refusedCharge uint64
	gotEntry, directErr := ar.NativeEntryAtRootAdmitted(primaryRoot, []byte("inline-doc"), func(n uint64) error {
		refusedCharge = n
		return memtable.ErrCOWCapacity
	})
	afterDirect := budget.Stats()
	if !errors.Is(directErr, memtable.ErrCOWCapacity) || gotEntry.Value != nil {
		t.Fatalf("direct admit refusal: entry.Value=%v err=%v want ErrCOWCapacity", gotEntry.Value, directErr)
	}
	if refusedCharge != memtable.COWAllocationCharge(uint64(len(inlineDoc))) {
		t.Fatalf("direct admit charge=%d want %d", refusedCharge, memtable.COWAllocationCharge(uint64(len(inlineDoc))))
	}
	if afterDirect.ReservedBytes != beforeDirect.ReservedBytes || afterDirect.ExternalLeases != beforeDirect.ExternalLeases {
		t.Fatalf("direct admit refusal leaked budget: before=%+v after=%+v", beforeDirect, afterDirect)
	}

	// Drain any parked idle read/batch scratch from setup so tight-budget
	// headroom reflects true remaining in-flight capacity deterministically
	// across normal, -race, and Windows builds.
	if lease, err := capRaw.NativeAllocator().AdmitNativeBytes(budget.Limits().MaxInFlightBytes); err == nil {
		lease.Close()
		t.Fatal("expected MaxInFlightBytes probe to fail with ErrCOWCapacity")
	}

	// 2. Tight COW budget during touchNativeRow for both inline and pointer docs
	// must refuse with ErrCOWCapacity and leave zero leaked leases/bytes after tx.Close.
	for _, tc := range []struct {
		name     string
		id       []byte
		docLen   int
		headroom uint64
	}{
		{name: "inline_tight", id: []byte("inline-doc"), docLen: len(inlineDoc), headroom: 512},
		{name: "pointer_tight", id: []byte("pointer-doc"), docLen: len(pointerDoc), headroom: 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := budget.Stats()
			tx, err := col.nativeJSON.Begin(col.name)
			if err != nil {
				t.Fatal(err)
			}
			// Normalize tx.lease from Begin's rounded AdmitNativeBytes charge to
			// exact t.charge so touchNativeRow's trackRead adds bytes rather than
			// shrinking tx.lease (which differs under -race/windows inspection sizes).
			if err := tx.trackRead(&tx.bindings[0], []byte("dep-normalize")); err != nil {
				_ = tx.Close()
				t.Fatal(err)
			}
			// Drain any idle read scratch parked during setup so blocker leaves
			// exact reclaim-free headroom.
			if lease, err := capRaw.NativeAllocator().AdmitNativeBytes(budget.Limits().MaxInFlightBytes); err == nil {
				lease.Close()
				_ = tx.Close()
				t.Fatal("expected MaxInFlightBytes probe to fail with ErrCOWCapacity")
			}
			blocker, err := capRaw.NativeAllocator().AdmitNativeBytes(0)
			if err != nil {
				_ = tx.Close()
				t.Fatal(err)
			}
			avail := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
			if avail <= tc.headroom {
				blocker.Close()
				_ = tx.Close()
				t.Fatalf("insufficient available budget %d for headroom %d", avail, tc.headroom)
			}
			if err := blocker.Resize(avail - tc.headroom); err != nil {
				blocker.Close()
				_ = tx.Close()
				t.Fatal(err)
			}
			_, touchErr := tx.touchNativeRow(&tx.bindings[0], tc.id)
			blocker.Close()
			if closeErr := tx.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if !errors.Is(touchErr, memtable.ErrCOWCapacity) {
				t.Fatalf("touchNativeRow(%q) under tight budget err=%v want ErrCOWCapacity", tc.id, touchErr)
			}
			if gotErr := tx.Commit(); !errors.Is(gotErr, backenddb.ErrClosed) {
				t.Fatalf("closed tx commit err=%v want ErrClosed", gotErr)
			}
			after := budget.Stats()
			if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases {
				t.Fatalf("tight budget touch leaked: before=%+v after=%+v", before, after)
			}
		})
	}
}

func BenchmarkUpdateOneStandaloneCOWTouchPath(b *testing.B) {
	dir := b.TempDir()
	owner, manager, col := openNativeUpdateTouchDB(b, dir, 4096)
	b.Cleanup(func() {
		if err := manager.Close(); err != nil {
			b.Error(err)
		}
		if err := owner.Close(); err != nil {
			b.Error(err)
		}
	})

	const totalDocs = 4096
	pad := string(bytes.Repeat([]byte("u"), 96))
	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	for i := 0; i < totalDocs; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		docs[i] = makeNativeUpdateTouchBSONDoc(b, fmt.Sprintf("g%02d", i%16), fmt.Sprintf("t%02d", i%32), int64(i), pad)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		b.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		b.Fatal(err)
	}

	b.Run("TouchStageOnly", func(b *testing.B) {
		// Warm read lanes and outer leaf cache before timing.
		for i := 0; i < totalDocs; i++ {
			tx, err := col.nativeJSON.Begin(col.name)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := tx.touchNativeRow(&tx.bindings[0], ids[i]); err != nil {
				_ = tx.Close()
				b.Fatal(err)
			}
			if err := tx.Close(); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx, err := col.nativeJSON.Begin(col.name)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := tx.touchNativeRow(&tx.bindings[0], ids[i%totalDocs]); err != nil {
				_ = tx.Close()
				b.Fatal(err)
			}
			if err := tx.Close(); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})

	var docParity [totalDocs]uint8
	b.Run("UpdateBSONSet", func(b *testing.B) {
		if err := owner.Checkpoint(); err != nil {
			b.Fatal(err)
		}
		fields := [2][]BSONSetField{
			{{Key: "visits", Value: bsonRawInt64(b, 100000)}},
			{{Key: "visits", Value: bsonRawInt64(b, 100001)}},
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx := i % totalDocs
			if i > 0 && idx == 0 {
				// Re-settle mutated rows into leaf_vlog so every UpdateBSONSet
				// continues to exercise the settled physical touch path.
				b.StopTimer()
				if err := owner.Checkpoint(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			docParity[idx] ^= 1
			matched, modified, err := col.UpdateBSONSet(ids[idx], fields[docParity[idx]])
			if err != nil || !matched || !modified {
				b.Fatalf("UpdateBSONSet(%d): matched=%v modified=%v err=%v", idx, matched, modified, err)
			}
		}
		b.StopTimer()
	})
}
