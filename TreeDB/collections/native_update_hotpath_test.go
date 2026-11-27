package collections

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeNoPendingTypedFlagTransitions(t *testing.T) {
	// 1. Ordinary non-typed collection starts and stays noPendingTyped == true
	// across inserts and updates.
	_, manager, col := openNativePrepareChangesFixture(t)
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: "k1"}, {Key: "n", Value: int64(1)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("k1"), doc); err != nil {
		t.Fatal(err)
	}
	if _, _, err := col.UpdateBSONSet([]byte("k1"), []BSONSetField{
		{Key: "n", Value: bson.RawValue{Type: bson.TypeInt64, Value: []byte{2, 0, 0, 0, 0, 0, 0, 0}}},
	}); err != nil {
		t.Fatal(err)
	}
	read, err := manager.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	if !read.engine.noPendingTyped {
		_ = read.Close()
		t.Fatal("expected ordinary non-typed engine version to keep noPendingTyped=true")
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}

	// 2. Typed delta flips noPendingTyped to false on the resulting engine version
	// and never skips typed constructibility checks.
	database, handle, typedMgr, typedCol := openNativeTypedGroup(t, t.TempDir(), true)
	defer finishNativeTypedGroup(t, database, handle, typedMgr, typedCol)

	if _, _, err := typedCol.InsertTypedBatchWithStats(nativeTypedGroupBatch("k-typed")); err != nil {
		t.Fatal(err)
	}
	typedRead, err := typedMgr.nativeJSON.Capture("events")
	if err != nil {
		t.Fatal(err)
	}
	if typedRead.engine.noPendingTyped {
		_ = typedRead.Close()
		t.Fatal("expected typed delta to clear noPendingTyped on engine version")
	}
	inv, err := nativeTypedBuildInventoryForCompaction(typedRead.engine, typedRead.capture.Snapshot(), 0)
	if err != nil {
		_ = typedRead.Close()
		t.Fatalf("nativeTypedBuildInventoryForCompaction: %v", err)
	}
	if inv.count == 0 {
		_ = typedRead.Close()
		t.Fatal("expected non-zero pending typed sources after typed insert")
	}
	if err := typedRead.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativePrimaryOnlyBSONSetUpdateBorrowSafety(t *testing.T) {
	_, manager, col := openNativePrepareChangesFixture(t)
	initDoc, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "doc-1"},
		{Key: "n", Value: int64(10)},
		{Key: "note", Value: "initial-payload"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("doc-1"), initDoc); err != nil {
		t.Fatal(err)
	}

	// Hold an opening snapshot across same-shape, grow, shrink, no-op, and missing-key
	// primary-only BSON $set updates to prove borrowed inline opening bytes are never mutated.
	snapRead, err := manager.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := snapRead.Close(); err != nil {
			t.Errorf("snapRead.Close: %v", err)
		}
	}()
	beforeSnap, err := snapRead.Get([]byte("doc-1"))
	if err != nil {
		t.Fatalf("opening snap Get: err=%v", err)
	}

	marshalVal := func(v any) bson.RawValue {
		t.Helper()
		raw, err := bson.Marshal(bson.D{{Key: "v", Value: v}})
		if err != nil {
			t.Fatal(err)
		}
		elems, err := bson.Raw(raw).Elements()
		if err != nil || len(elems) != 1 {
			t.Fatalf("elements: %v", err)
		}
		return elems[0].Value()
	}

	// Same-shape update (int64 -> int64).
	matched, modified, err := col.UpdateBSONSet([]byte("doc-1"), []BSONSetField{
		{Key: "n", Value: marshalVal(int64(20))},
	})
	if err != nil || !matched || !modified {
		t.Fatalf("same-shape: matched=%v modified=%v err=%v", matched, modified, err)
	}
	// No-op update (same value).
	matched, modified, err = col.UpdateBSONSet([]byte("doc-1"), []BSONSetField{
		{Key: "n", Value: marshalVal(int64(20))},
	})
	if err != nil || !matched || modified {
		t.Fatalf("no-op: matched=%v modified=%v err=%v", matched, modified, err)
	}
	// Grow update (longer string + new field).
	longStr := string(bytes.Repeat([]byte("A"), 512))
	matched, modified, err = col.UpdateBSONSet([]byte("doc-1"), []BSONSetField{
		{Key: "note", Value: marshalVal(longStr)},
		{Key: "extra", Value: marshalVal("added")},
	})
	if err != nil || !matched || !modified {
		t.Fatalf("grow: matched=%v modified=%v err=%v", matched, modified, err)
	}
	// Shrink update (shorter string).
	matched, modified, err = col.UpdateBSONSet([]byte("doc-1"), []BSONSetField{
		{Key: "note", Value: marshalVal("s")},
	})
	if err != nil || !matched || !modified {
		t.Fatalf("shrink: matched=%v modified=%v err=%v", matched, modified, err)
	}
	// Missing key.
	matched, modified, err = col.UpdateBSONSet([]byte("missing"), []BSONSetField{
		{Key: "n", Value: marshalVal(int64(99))},
	})
	if err != nil || matched || modified {
		t.Fatalf("missing: matched=%v modified=%v err=%v", matched, modified, err)
	}

	// Concurrent snapshot readers + writers exercising borrowed opening bytes.
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			v := marshalVal(fmt.Sprintf("step-%03d-%s", i, string(bytes.Repeat([]byte("z"), (i%4)*32))))
			if _, _, err := col.UpdateBSONSet([]byte("doc-1"), []BSONSetField{{Key: "note", Value: v}}); err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			r, err := manager.nativeJSON.Capture("docs")
			if err != nil {
				errCh <- err
				return
			}
			got, err := r.Get([]byte("doc-1"))
			if err != nil {
				_ = r.Close()
				errCh <- fmt.Errorf("concurrent read err=%v", err)
				return
			}
			if err := bson.Raw(got).Validate(); err != nil {
				_ = r.Close()
				errCh <- fmt.Errorf("corrupt BSON: %w", err)
				return
			}
			if err := r.Close(); err != nil {
				errCh <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Original snapshot MUST still observe the exact unmodified opening bytes.
	afterSnap, err := snapRead.Get([]byte("doc-1"))
	if err != nil {
		t.Fatalf("post-update snap Get: err=%v", err)
	}
	if !bytes.Equal(beforeSnap, initDoc) || !bytes.Equal(afterSnap, initDoc) {
		t.Fatalf("opening snapshot bytes mutated!\nbefore=%x\n after=%x\n  want=%x", beforeSnap, afterSnap, initDoc)
	}
}

func TestNativeSchemaPlannerCacheParity(t *testing.T) {
	_, manager, _ := openNativePrepareChangesFixture(t)
	read, err := manager.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) {
		return read.engine.allocator.AdmitNativeBytes(n)
	}
	defer func() {
		if err := read.Close(); err != nil {
			t.Errorf("read.Close: %v", err)
		}
	}()

	metas := []CollectionMeta{
		{
			Name:    "zero_idx",
			Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
		},
		{
			Name:    "single_idx",
			Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
			Indexes: []IndexDefinition{
				{Name: "n_1", Field: "n", ValueType: IndexValueInt64},
			},
		},
		{
			Name:    "compound_unique_idx",
			Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
			Indexes: []IndexDefinition{
				{
					Name:      "tenant_email_u",
					Field:     "tenant.id",
					ValueType: IndexValueString,
					Unique:    true,
					Components: []IndexComponent{
						{Field: "tenant.id", Direction: IndexDirectionAscending},
						{Field: "user.email", Direction: IndexDirectionDescending},
					},
				},
				{
					Name:      "tags_mk",
					Field:     "tags",
					ValueType: IndexValueString,
					MultiKey:  true,
				},
			},
		},
	}

	for _, meta := range metas {
		t.Run(meta.Name, func(t *testing.T) {
			schema, err := newNativeSchemaState(&meta, admit)
			if err != nil {
				t.Fatalf("newNativeSchemaState: %v", err)
			}
			defer func() {
				schema.release().Complete()
			}()
			if !schema.plannerOK {
				t.Fatalf("expected schema.plannerOK=true for %s", meta.Name)
			}
			cached, err := nativeJSONInsertPlannerForSchema(schema, meta)
			if err != nil {
				t.Fatalf("nativeJSONInsertPlannerForSchema: %v", err)
			}
			fresh, err := nativeJSONInsertPlanner(meta)
			if err != nil {
				t.Fatalf("nativeJSONInsertPlanner: %v", err)
			}
			if cached.collection != fresh.collection ||
				cached.primaryRoot != fresh.primaryRoot ||
				cached.templateRoot != fresh.templateRoot ||
				cached.indexStateRoot != fresh.indexStateRoot ||
				cached.options.documentFormat != fresh.options.documentFormat ||
				cached.options.dataStoragePolicy != fresh.options.dataStoragePolicy ||
				cached.options.indexStateStoragePolicy != fresh.options.indexStateStoragePolicy ||
				cached.options.allowArrayValuesInIndex != fresh.options.allowArrayValuesInIndex ||
				cached.directBufferedRuns != fresh.directBufferedRuns {
				t.Fatalf("planner header mismatch:\ncached=%+v\n fresh=%+v", cached, fresh)
			}
			if !reflect.DeepEqual(cached.indexes, fresh.indexes) {
				t.Fatalf("indexes mismatch:\ncached=%+v\n fresh=%+v", cached.indexes, fresh.indexes)
			}
			cachedRT, err := cached.indexRuntimes()
			if err != nil {
				t.Fatalf("cached.indexRuntimes: %v", err)
			}
			freshRT, err := fresh.indexRuntimes()
			if err != nil {
				t.Fatalf("fresh.indexRuntimes: %v", err)
			}
			if !reflect.DeepEqual(cachedRT, freshRT) {
				t.Fatalf("indexRuntimes mismatch:\ncached=%+v\n fresh=%+v", cachedRT, freshRT)
			}
			pRoot, sRoot := nativeSchemaRootNames(schema, meta.Name)
			if pRoot != collectionPrimaryRootName(meta.Name) || sRoot != collectionIndexStateRootName(meta.Name) {
				t.Fatalf("root names mismatch: got (%q, %q)", pRoot, sRoot)
			}
		})
	}
}

func TestCollectionWithMetaViewBorrowedCutPanicSafetyAndZeroAlloc(t *testing.T) {
	_, _, col := openNativePrepareChangesFixture(t)
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: "k1"}, {Key: "n", Value: int64(1)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("k1"), doc); err != nil {
		t.Fatal(err)
	}

	// Verify panic inside fn still releases the borrowed cut pin cleanly.
	recovered := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				recovered = true
			}
		}()
		col.WithMetaView(func(*CollectionMeta) {
			panic("synthetic WithMetaView callback panic")
		})
	}()
	if !recovered {
		t.Fatal("expected callback panic to propagate")
	}

	// Verify steady-state borrowed-cut WithMetaView performs 0 heap allocations.
	var seenFormat DocumentFormat
	metaFn := func(meta *CollectionMeta) {
		seenFormat = meta.Options.DocumentFormat
	}
	col.WithMetaView(metaFn)
	if seenFormat != DocumentFormatBSON {
		t.Fatalf("seenFormat=%v want BSON", seenFormat)
	}
	allocs := testing.AllocsPerRun(200, func() {
		col.WithMetaView(metaFn)
	})
	if allocs != 0 {
		t.Fatalf("WithMetaView allocated %.2f objects/op, want 0", allocs)
	}
}

func TestBSONSetAppendSameShapeNoSpeculativeAllocOnRejectAndOverflow(t *testing.T) {
	marshalVal := func(v any) bson.RawValue {
		t.Helper()
		raw, err := bson.Marshal(bson.D{{Key: "v", Value: v}})
		if err != nil {
			t.Fatal(err)
		}
		elems, err := bson.Raw(raw).Elements()
		if err != nil || len(elems) != 1 {
			t.Fatalf("elements: %v", err)
		}
		return elems[0].Value()
	}

	current, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "doc-1"},
		{Key: "a", Value: int64(1)},
		{Key: "b", Value: "short"},
	})
	if err != nil {
		t.Fatal(err)
	}

	rejectCases := []struct {
		name   string
		doc    []byte
		fields []BSONSetField
	}{
		{
			name: "equal_then_grow",
			doc:  current,
			fields: []BSONSetField{
				{Key: "a", Value: marshalVal(int64(2))},
				{Key: "b", Value: marshalVal("much-longer-value")},
			},
		},
		{
			name: "equal_then_missing",
			doc:  current,
			fields: []BSONSetField{
				{Key: "a", Value: marshalVal(int64(2))},
				{Key: "absent", Value: marshalVal(int64(9))},
			},
		},
		{
			name: "equal_then_type_change",
			doc:  current,
			fields: []BSONSetField{
				{Key: "a", Value: marshalVal(int64(2))},
				{Key: "b", Value: marshalVal(int64(5))},
			},
		},
	}

	for _, tc := range rejectCases {
		t.Run(tc.name, func(t *testing.T) {
			upd, err := newBSONSetUpdate(tc.fields)
			if err != nil {
				t.Fatal(err)
			}
			_, repl, changed, ok, err := upd.appendSameShapeReplacement(nil, tc.doc)
			if err != nil || ok || changed || repl != nil {
				t.Fatalf("expected reject (ok=false), got repl=%v changed=%v ok=%v err=%v", repl, changed, ok, err)
			}
			allocs := testing.AllocsPerRun(100, func() {
				_, _, _, ok, _ := upd.appendSameShapeReplacement(nil, tc.doc)
				if ok {
					t.Fatal("unexpected ok=true")
				}
			})
			if allocs != 0 {
				t.Fatalf("rejected appendSameShapeReplacement(%s) allocated %.2f allocs/op, want 0", tc.name, allocs)
			}
		})
	}

	// Verify unchanged document performs 0 allocations.
	t.Run("unchanged_zero_alloc", func(t *testing.T) {
		upd, err := newBSONSetUpdate([]BSONSetField{
			{Key: "a", Value: marshalVal(int64(1))},
			{Key: "b", Value: marshalVal("short")},
		})
		if err != nil {
			t.Fatal(err)
		}
		allocs := testing.AllocsPerRun(100, func() {
			_, repl, changed, ok, err := upd.appendSameShapeReplacement(nil, current)
			if err != nil || !ok || changed || !bytes.Equal(repl, current) {
				t.Fatalf("unchanged: ok=%v changed=%v err=%v", ok, changed, err)
			}
		})
		if allocs != 0 {
			t.Fatalf("unchanged appendSameShapeReplacement allocated %.2f allocs/op, want 0", allocs)
		}
	})

	// Verify >8 changed same-shape fields (patchesOverflow fallback) matches appendReplacement byte-for-byte.
	t.Run("overflow_10_fields", func(t *testing.T) {
		var initDoc bson.D
		fields := make([]BSONSetField, 0, 10)
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("f%02d", i)
			initDoc = append(initDoc, bson.E{Key: k, Value: int64(i)})
			fields = append(fields, BSONSetField{Key: k, Value: marshalVal(int64(100 + i))})
		}
		raw, err := bson.Marshal(initDoc)
		if err != nil {
			t.Fatal(err)
		}
		upd, err := newBSONSetUpdate(fields)
		if err != nil {
			t.Fatal(err)
		}
		_, got, changed, ok, err := upd.appendSameShapeReplacement(nil, raw)
		if err != nil || !ok || !changed {
			t.Fatalf("overflow same-shape: ok=%v changed=%v err=%v", ok, changed, err)
		}
		_, want, wantChanged, err := upd.appendReplacement(nil, raw)
		if err != nil || !wantChanged {
			t.Fatalf("appendReplacement: changed=%v err=%v", wantChanged, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("overflow mismatch:\n got=%x\nwant=%x", got, want)
		}
	})
}
