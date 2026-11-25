package collections

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type noopUniqueScanOwnedMapLease struct{}

func (noopUniqueScanOwnedMapLease) Close() {}

type noopUniqueScanDomainLifetime struct{}

func (noopUniqueScanDomainLifetime) Retain(*nativeDomainState) {}
func (noopUniqueScanDomainLifetime) Release(*nativeDomainState) (cowbtree.OwnedMapCompletion, error) {
	return cowbtree.OwnedMapCompletion{}, nil
}

func openUniqueScanTestDB(t testing.TB, limits treedb.COWMemtableLimits, indexes []IndexDefinition) (*treedb.DB, *backenddb.DB, *CollectionManager, *Collection, string) {
	t.Helper()
	dir := t.TempDir()
	owner, backend, manager, col := openUniqueScanTestDBAtDir(t, dir, limits, indexes, true)
	return owner, backend, manager, col, dir
}

func openUniqueScanTestDBAtDir(t testing.TB, dir string, limits treedb.COWMemtableLimits, indexes []IndexDefinition, create bool) (*treedb.DB, *backenddb.DB, *CollectionManager, *Collection) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = limits
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open treedb: %v", err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		_ = owner.Close()
		t.Fatalf("native semantic backend: %v", err)
	}
	manager := NewCollectionManager(backend)
	if create {
		meta := &CollectionMeta{
			Name: "unique_scan_docs",
			Options: CollectionOptions{
				DocumentFormat: DocumentFormatBSON,
			},
			Indexes: indexes,
		}
		if _, err := manager.CreateCollection(meta); err != nil {
			_ = manager.Close()
			_ = owner.Close()
			t.Fatalf("create collection: %v", err)
		}
	}
	col, err := manager.OpenCollection("unique_scan_docs")
	if err != nil {
		_ = manager.Close()
		_ = owner.Close()
		t.Fatalf("open collection: %v", err)
	}
	if col.nativeJSON == nil {
		_ = manager.Close()
		_ = owner.Close()
		t.Fatalf("native JSON owner inactive")
	}
	return owner, backend, manager, col
}

func marshalUniqueScanBSONDoc(t testing.TB, id, email, code string) []byte {
	t.Helper()
	doc, err := bson.Marshal(bson.D{
		{Key: "_id", Value: id},
		{Key: "email", Value: email},
		{Key: "code", Value: code},
	})
	if err != nil {
		t.Fatalf("marshal bson doc %q: %v", id, err)
	}
	return doc
}

func buildUniqueIndexBSONBatch(t testing.TB, prefix string, startIdx, count int) ([][]byte, [][]byte) {
	t.Helper()
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		n := startIdx + i
		idStr := fmt.Sprintf("%s-doc-%06d", prefix, n)
		ids[i] = []byte(idStr)
		doc, err := bson.Marshal(bson.D{
			{Key: "_id", Value: idStr},
			{Key: "email", Value: fmt.Sprintf("%s-user-%06d@example.com", prefix, n)},
			{Key: "age", Value: int64(20 + (n % 50))},
		})
		if err != nil {
			t.Fatalf("marshal bson doc %d: %v", i, err)
		}
		docs[i] = doc
	}
	return ids, docs
}

func TestScanNativeUpdateUniqueParityAndConflicts(t *testing.T) {
	t.Run("empty_pending_settled_cross_batch_and_multi_unique", func(t *testing.T) {
		indexes := []IndexDefinition{
			{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "code_1", Field: "code", ValueType: IndexValueString, Unique: true},
		}
		owner, _, manager, col, dir := openUniqueScanTestDB(t, treedb.StandaloneCOWMemtableLimits(), indexes)
		closed := false
		defer func() {
			if !closed {
				if err := manager.Close(); err != nil {
					t.Errorf("close manager: %v", err)
				}
				if err := owner.Close(); err != nil {
					t.Errorf("close owner: %v", err)
				}
			}
		}()

		// 1. Empty collection bulk insert succeeds.
		ids1 := [][]byte{[]byte("doc-1"), []byte("doc-2")}
		docs1 := [][]byte{
			marshalUniqueScanBSONDoc(t, "doc-1", "a@example.com", "C001"),
			marshalUniqueScanBSONDoc(t, "doc-2", "b@example.com", "C002"),
		}
		if _, err := col.InsertBatchValidatedBSON(ids1, docs1); err != nil {
			t.Fatalf("empty collection bulk insert: %v", err)
		}

		// 2. Pending-only collection: conflict on first unique index (email_1) and second (code_1).
		if _, err := col.InsertBatchValidatedBSON(
			[][]byte{[]byte("doc-3")},
			[][]byte{marshalUniqueScanBSONDoc(t, "doc-3", "a@example.com", "C003")},
		); !errors.Is(err, ErrUniqueIndexConflict) {
			t.Fatalf("pending-only email conflict: got %v, want ErrUniqueIndexConflict", err)
		}
		if _, err := col.InsertBatchValidatedBSON(
			[][]byte{[]byte("doc-3")},
			[][]byte{marshalUniqueScanBSONDoc(t, "doc-3", "c@example.com", "C002")},
		); !errors.Is(err, ErrUniqueIndexConflict) {
			t.Fatalf("pending-only code conflict: got %v, want ErrUniqueIndexConflict", err)
		}

		// Non-conflicting second pending batch succeeds.
		if _, err := col.InsertBatchValidatedBSON(
			[][]byte{[]byte("doc-3")},
			[][]byte{marshalUniqueScanBSONDoc(t, "doc-3", "c@example.com", "C003")},
		); err != nil {
			t.Fatalf("pending-only non-conflicting insert: %v", err)
		}

		// 3. Settled base collection (checkpoint + close + reopen).
		if err := owner.Checkpoint(); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		if err := manager.Close(); err != nil {
			t.Fatalf("close manager before reopen: %v", err)
		}
		if err := owner.Close(); err != nil {
			t.Fatalf("close owner before reopen: %v", err)
		}
		closed = true

		owner, _, manager, col = openUniqueScanTestDBAtDir(t, dir, treedb.StandaloneCOWMemtableLimits(), indexes, false)
		closed = false

		// Cross-batch conflicts against settled base root on both unique indexes.
		if _, err := col.InsertBatchValidatedBSON(
			[][]byte{[]byte("doc-4")},
			[][]byte{marshalUniqueScanBSONDoc(t, "doc-4", "b@example.com", "C004")},
		); !errors.Is(err, ErrUniqueIndexConflict) {
			t.Fatalf("settled base email conflict: got %v, want ErrUniqueIndexConflict", err)
		}
		if _, err := col.InsertBatchValidatedBSON(
			[][]byte{[]byte("doc-4")},
			[][]byte{marshalUniqueScanBSONDoc(t, "doc-4", "d@example.com", "C001")},
		); !errors.Is(err, ErrUniqueIndexConflict) {
			t.Fatalf("settled base code conflict: got %v, want ErrUniqueIndexConflict", err)
		}

		// 4. Replacement-set self-update on same document and cross-document swap handoff.
		selfDoc := marshalUniqueScanBSONDoc(t, "doc-1", "a@example.com", "C101")
		matched, modified, err := col.Update([]byte("doc-1"), func([]byte) ([]byte, bool, error) {
			return selfDoc, true, nil
		})
		if err != nil || !matched || !modified {
			t.Fatalf("self-update on same document: matched=%v modified=%v err=%v", matched, modified, err)
		}

		// Swap email and code between doc-1 and doc-2 in one UpdateBatch.
		swap1 := marshalUniqueScanBSONDoc(t, "doc-1", "b@example.com", "C002")
		swap2 := marshalUniqueScanBSONDoc(t, "doc-2", "a@example.com", "C101")
		results, err := col.UpdateBatch([]UpdateBatchItem{
			{DocumentID: []byte("doc-1"), Update: func([]byte) ([]byte, bool, error) { return swap1, true, nil }},
			{DocumentID: []byte("doc-2"), Update: func([]byte) ([]byte, bool, error) { return swap2, true, nil }},
		})
		if err != nil || len(results) != 2 || !results[0].Modified || !results[1].Modified {
			t.Fatalf("replacement-set handoff: results=%+v err=%v", results, err)
		}

		// Verify post-swap unique index lookups.
		got, err := col.FindByIndexValue("email_1", "b@example.com")
		if err != nil || len(got) != 1 || !bytes.Equal(got[0], []byte("doc-1")) {
			t.Fatalf("post-swap email_1 lookup: got=%q err=%v", got, err)
		}
		got, err = col.FindByIndexValue("code_1", "C101")
		if err != nil || len(got) != 1 || !bytes.Equal(got[0], []byte("doc-2")) {
			t.Fatalf("post-swap code_1 lookup: got=%q err=%v", got, err)
		}
	})

	t.Run("prefix_bounds_oversized_key_and_empty_domain_read_error", func(t *testing.T) {
		indexes := []IndexDefinition{
			{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true},
		}
		owner, _, manager, col, _ := openUniqueScanTestDB(t, treedb.StandaloneCOWMemtableLimits(), indexes)
		defer func() {
			if err := manager.Close(); err != nil {
				t.Errorf("close manager: %v", err)
			}
			if err := owner.Close(); err != nil {
				t.Errorf("close owner: %v", err)
			}
		}()

		tx, err := col.nativeJSON.Begin(col.name)
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		defer func() {
			if err := tx.Close(); err != nil {
				t.Errorf("close tx: %v", err)
			}
		}()
		g := &tx.bindings[0]
		runtimes := []indexRuntime{
			{def: indexDefinition{name: "email_1", valueType: IndexValueString, unique: true}},
		}

		// Empty prefix, all-0xff prefix, trailing-0xff prefix, and normal prefix.
		changed := []preparedBatchUpdate{
			{
				documentID:     []byte("d1"),
				changedIndexes: 1,
				newState: orderedDocumentIndexState{
					{
						[]byte{},
						[]byte{0xff, 0xff},
						[]byte{0x01, 0xff},
						[]byte{0x61, 0x00, 0x00},
					},
				},
			},
		}
		depsBefore := len(tx.dependencies)
		if err := tx.scanNativeUpdateUnique(g, runtimes, changed, nil); err != nil {
			t.Fatalf("scanNativeUpdateUnique prefix bounds: %v", err)
		}
		gotDeps := tx.dependencies[depsBefore:]
		if len(gotDeps) != 4 {
			t.Fatalf("dependency count=%d want 4", len(gotDeps))
		}

		// Entry 0: empty prefix -> non-nil []byte{}, nil End.
		if gotDeps[0].Key == nil || len(gotDeps[0].Key) != 0 {
			t.Fatalf("empty prefix Key=%#v want non-nil empty slice", gotDeps[0].Key)
		}
		if gotDeps[0].End != nil {
			t.Fatalf("empty prefix End=%#v want nil", gotDeps[0].End)
		}
		if gotDeps[0].Kind != txnhistory.RangeEffect {
			t.Fatalf("dep kind=%v want RangeEffect", gotDeps[0].Kind)
		}

		// Entry 1: all-0xff prefix -> exact Key, nil End.
		if !bytes.Equal(gotDeps[1].Key, []byte{0xff, 0xff}) || gotDeps[1].End != nil {
			t.Fatalf("all-0xff dep=(%x, %x) want (ffff, nil)", gotDeps[1].Key, gotDeps[1].End)
		}

		// Entry 2: trailing-0xff prefix -> Key={0x01, 0xff}, End={0x02}.
		if !bytes.Equal(gotDeps[2].Key, []byte{0x01, 0xff}) || !bytes.Equal(gotDeps[2].End, []byte{0x02}) {
			t.Fatalf("trailing-0xff dep=(%x, %x) want (01ff, 02)", gotDeps[2].Key, gotDeps[2].End)
		}

		// Entry 3: normal prefix -> Key={0x61, 0x00, 0x00}, End={0x61, 0x00, 0x01}.
		if !bytes.Equal(gotDeps[3].Key, []byte{0x61, 0x00, 0x00}) || !bytes.Equal(gotDeps[3].End, []byte{0x61, 0x00, 0x01}) {
			t.Fatalf("normal dep=(%x, %x) want (610000, 610001)", gotDeps[3].Key, gotDeps[3].End)
		}

		// Oversized > 65535 key must fail BEFORE COW admission or dependency mutation.
		chargeBefore := tx.charge
		depsBefore = len(tx.dependencies)
		oversized := []preparedBatchUpdate{
			{
				documentID:     []byte("d-big"),
				changedIndexes: 1,
				newState: orderedDocumentIndexState{
					{
						[]byte("ok"),
						bytes.Repeat([]byte("a"), 65536),
					},
				},
			},
		}
		err = tx.scanNativeUpdateUnique(g, runtimes, oversized, nil)
		if err == nil || !strings.Contains(err.Error(), "collections: index key too large") {
			t.Fatalf("oversized key err=%v want 'collections: index key too large'", err)
		}
		if tx.charge != chargeBefore || len(tx.dependencies) != depsBefore {
			t.Fatalf("oversized key mutated state before validation: charge %d->%d deps %d->%d",
				chargeBefore, tx.charge, depsBefore, len(tx.dependencies))
		}

		// Empty domain with zero refs must still surface newNativeDomainRead error
		// (must NOT skip newNativeDomainRead before checking empty domain).
		badDomain := &nativeDomainState{} // refs == 0, baseRoot == 0, captured == nil, pending == nil
		builder, err := cowbtree.NewOwnedMapBuilder[string, *nativeDomainState](
			nil,
			1,
			noopUniqueScanDomainLifetime{},
			func(uint64) (cowbtree.OwnedMapLease, error) { return noopUniqueScanOwnedMapLease{}, nil },
		)
		if err != nil {
			t.Fatalf("new owned map builder: %v", err)
		}
		if err := builder.Set("index/email_1", badDomain); err != nil {
			t.Fatalf("set bad domain: %v", err)
		}
		badDomains := builder.Seal()
		origCol := g.probe.collection
		badCol := *origCol
		badCol.domains = badDomains
		g.probe.collection = &badCol
		defer func() {
			g.probe.collection = origCol
		}()
		validOne := []preparedBatchUpdate{
			{
				documentID:     []byte("d-err"),
				changedIndexes: 1,
				newState: orderedDocumentIndexState{
					{[]byte("x\x00\x00")},
				},
			},
		}
		if err := tx.scanNativeUpdateUnique(g, runtimes, validOne, nil); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
			t.Fatalf("zero-ref empty domain err=%v want ErrNativeSemanticUnsupported", err)
		}
	})

	t.Run("tight_cow_capacity_refusal_zero_lease_leak", func(t *testing.T) {
		tight := treedb.DefaultCOWMemtableLimits()
		tight.MaxInFlightBytes = 2 << 20
		tight.MaxTotalBytes = 4 << 20
		tight.MaxRetiredBytes = 4 << 20
		tight.MaxGenerationBytes = 1 << 20

		indexes := []IndexDefinition{
			{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true},
		}
		owner, backend, manager, col, _ := openUniqueScanTestDB(t, tight, indexes)
		defer func() {
			if err := manager.Close(); err != nil {
				t.Errorf("close manager: %v", err)
			}
			if err := owner.Close(); err != nil {
				t.Errorf("close owner: %v", err)
			}
		}()

		if err := backend.JoinNativeSemanticCleanupForTest(); err != nil {
			t.Fatalf("join cleanup before: %v", err)
		}
		beforeObs, err := backend.ObserveNativeProducer()
		if err != nil {
			t.Fatalf("observe before: %v", err)
		}
		beforeLSN := backend.CommandWALNextLSN()

		tx, err := col.nativeJSON.Begin(col.name)
		if err != nil {
			t.Fatalf("begin tx: %v", err)
		}
		g := &tx.bindings[0]
		runtimes := []indexRuntime{
			{def: indexDefinition{name: "email_1", valueType: IndexValueString, unique: true}},
		}
		// 256 entries × 8 KiB prefix+end (~4 MiB arena) exceeds 2 MiB in-flight cap.
		vals := make([][]byte, 256)
		for i := range vals {
			vals[i] = bytes.Repeat([]byte{byte('a' + (i % 26))}, 8192)
		}
		changed := []preparedBatchUpdate{
			{
				documentID:     []byte("doc-tight"),
				changedIndexes: 1,
				newState:       orderedDocumentIndexState{vals},
			},
		}
		chargeBefore := tx.charge
		depsBefore := len(tx.dependencies)
		err = tx.scanNativeUpdateUnique(g, runtimes, changed, nil)
		if !errors.Is(err, memtable.ErrCOWCapacity) {
			_ = tx.Close()
			t.Fatalf("tight COW capacity err=%v want ErrCOWCapacity", err)
		}
		if tx.charge != chargeBefore || len(tx.dependencies) != depsBefore {
			_ = tx.Close()
			t.Fatalf("capacity refusal mutated tx state: charge %d->%d deps %d->%d",
				chargeBefore, tx.charge, depsBefore, len(tx.dependencies))
		}
		if err := tx.Close(); err != nil {
			t.Fatalf("close tx: %v", err)
		}

		if err := backend.JoinNativeSemanticCleanupForTest(); err != nil {
			t.Fatalf("join cleanup after: %v", err)
		}
		afterObs, err := backend.ObserveNativeProducer()
		if err != nil {
			t.Fatalf("observe after: %v", err)
		}
		if backend.CommandWALNextLSN() != beforeLSN {
			t.Fatalf("LSN advanced on refused unique scan: before=%d after=%d", beforeLSN, backend.CommandWALNextLSN())
		}
		if afterObs.ReservedBytes != beforeObs.ReservedBytes {
			t.Fatalf("leaked COW reservation on capacity refusal: before=%d after=%d", beforeObs.ReservedBytes, afterObs.ReservedBytes)
		}
	})
}

func BenchmarkBulkInsertUniqueIndex1000StandaloneCOW(b *testing.B) {
	const batchSize = 1000
	indexes := []IndexDefinition{
		{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true},
	}
	owner, _, manager, col, _ := openUniqueScanTestDB(b, treedb.StandaloneCOWMemtableLimits(), indexes)
	defer func() {
		if err := manager.Close(); err != nil {
			b.Errorf("close manager: %v", err)
		}
		if err := owner.Close(); err != nil {
			b.Errorf("close owner: %v", err)
		}
	}()
	// Seed 1 document so the unique secondary index domain exists and is active,
	// matching steady-state bulk inserts that execute the live unique scan.
	seedIDs, seedDocs := buildUniqueIndexBSONBatch(b, "seed", 0, 1)
	if _, err := col.InsertBatchValidatedBSON(seedIDs, seedDocs); err != nil {
		b.Fatalf("seed unique index: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		b.StopTimer()
		batchIDs, batchDocs := buildUniqueIndexBSONBatch(b, fmt.Sprintf("b%06d", iter), 0, batchSize)
		b.StartTimer()

		if _, err := col.InsertBatchValidatedBSON(batchIDs, batchDocs); err != nil {
			b.Fatalf("bulk insert unique index batch %d: %v", iter, err)
		}
	}
	b.StopTimer()
}
