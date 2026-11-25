package collections

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func parseUintStat(t testing.TB, stats map[string]string, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %q", key)
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse stat %q=%q: %v", key, raw, err)
	}
	return v
}

func bsonSetCustodyTestMeta(name string, idxCount int) *CollectionMeta {
	meta := &CollectionMeta{
		Name:    name,
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}
	switch idxCount {
	case 0:
	case 1:
		meta.Indexes = []IndexDefinition{
			{Name: "idx_email", Field: "email", ValueType: IndexValueBSONOrderedV2, Unique: true},
		}
	case 2:
		meta.Indexes = []IndexDefinition{
			{Name: "idx_email", Field: "email", ValueType: IndexValueBSONOrderedV2, Unique: true},
			{Name: "idx_age", Field: "age", ValueType: IndexValueBSONOrderedV2},
			{Name: "idx_city", Field: "city", ValueType: IndexValueBSONOrderedV2},
		}
	default:
		panic(fmt.Sprintf("unsupported idxCount %d", idxCount))
	}
	return meta
}

func bsonSetCustodyTestDoc(tb testing.TB, id string, age, score int32, email, city string) []byte {
	tb.Helper()
	doc, err := bson.Marshal(bson.D{
		{Key: "_id", Value: id},
		{Key: "email", Value: email},
		{Key: "age", Value: age},
		{Key: "city", Value: city},
		{Key: "score", Value: score},
	})
	if err != nil {
		tb.Fatalf("marshal bson doc: %v", err)
	}
	return doc
}

func bsonSetInt32Field(tb testing.TB, key string, val int32) BSONSetField {
	tb.Helper()
	typ, raw, err := bson.MarshalValue(val)
	if err != nil {
		tb.Fatalf("marshal int32 value: %v", err)
	}
	return BSONSetField{Key: key, Value: bson.RawValue{Type: typ, Value: raw}}
}

func bsonSetStringField(tb testing.TB, key string, val string) BSONSetField {
	tb.Helper()
	typ, raw, err := bson.MarshalValue(val)
	if err != nil {
		tb.Fatalf("marshal string value: %v", err)
	}
	return BSONSetField{Key: key, Value: bson.RawValue{Type: typ, Value: raw}}
}

func openCustodyTestDB(tb testing.TB, dir string) (*treedb.DB, *backenddb.DB, *CollectionManager) {
	tb.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		tb.Fatal(err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		_ = owner.Close()
		tb.Fatal(err)
	}
	return owner, backend, NewCollectionManager(backend)
}

func waitExternalBytesSettle(t *testing.T, owner *treedb.DB, before uint64, maxDelta uint64) uint64 {
	t.Helper()
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		cur := parseUintStat(t, owner.Stats(), "treedb.cache.cow.external_bytes")
		if cur <= before || cur-before <= maxDelta {
			return cur
		}
		select {
		case <-deadline:
			return cur
		case <-ticker.C:
		}
	}
}

func TestNativeBSONSetIndexedCustodyNoLeak(t *testing.T) {
	for _, tc := range []struct {
		name     string
		idxCount int
	}{
		{name: "idx0", idxCount: 0},
		{name: "idx1_unique_email", idxCount: 1},
		{name: "idx2_age_email_city", idxCount: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner, backend, manager := openCustodyTestDB(t, t.TempDir())
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := manager.CreateCollection(bsonSetCustodyTestMeta("users", tc.idxCount)); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			const seedCount = 20
			ids := make([][]byte, seedCount)
			docs := make([][]byte, seedCount)
			for i := 0; i < seedCount; i++ {
				id := fmt.Sprintf("user-%04d", i)
				ids[i] = []byte(id)
				docs[i] = bsonSetCustodyTestDoc(t, id, int32(20+i), 0, fmt.Sprintf("u%d@example.com", i), "hnl")
			}
			if _, err := col.InsertBatchValidatedBSON(ids, docs); err != nil {
				t.Fatal(err)
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			// Measure external_bytes growth, rollovers, and handoff scans over 1,200 updates.
			const updates = 1200
			beforeStats := owner.Stats()
			beforeExt := parseUintStat(t, beforeStats, "treedb.cache.cow.external_bytes")
			beforeRollovers := parseUintStat(t, beforeStats, "treedb.cache.cow.rollovers_total")

			for i := 0; i < updates; i++ {
				id := ids[i%seedCount]
				field := bsonSetInt32Field(t, "score", int32(100+i))
				var (
					matched, modified bool
					err               error
				)
				if i%2 == 0 {
					matched, modified, _, err = col.UpdateBSONSetWithJournalReceipt(id, []BSONSetField{field})
				} else {
					matched, modified, err = col.UpdateBSONSet(id, []BSONSetField{field})
				}
				if err != nil || !matched || !modified {
					t.Fatalf("update %d: matched=%t modified=%t err=%v", i, matched, modified, err)
				}
			}

			maxAllowedDelta := uint64(updates) * 256
			afterExt := waitExternalBytesSettle(t, owner, beforeExt, maxAllowedDelta)
			afterStats := owner.Stats()
			afterRollovers := parseUintStat(t, afterStats, "treedb.cache.cow.rollovers_total")

			var extDelta uint64
			if afterExt > beforeExt {
				extDelta = afterExt - beforeExt
			}
			perOp := float64(extDelta) / float64(updates)
			rolloverDelta := afterRollovers - beforeRollovers
			if extDelta > maxAllowedDelta || rolloverDelta != 0 {
				t.Errorf("%s: custody leak/rollover storm over %d updates: extDelta=%d B (%.1f B/op, limit <=256 B/op), rollovers=%d (want 0)",
					tc.name, updates, extDelta, perOp, rolloverDelta)
			}

			// Steady-state checkpoint/handoff must execute 0 candidate reachability scans.
			var scans atomic.Int64
			restore := backend.SetScanCandidateExternalReferencesHookForTesting(func() {
				scans.Add(1)
			})
			err = owner.Checkpoint()
			restore()
			if err != nil {
				t.Fatal(err)
			}
			if got := scans.Load(); got != 0 {
				t.Errorf("%s: steady-state native COW checkpoint triggered %d candidate reachability scans, want 0", tc.name, got)
			}

			// Verify ReturnCustodyOnInstall is active for indexed non-indexed-field updates
			// by comparing retained ref counts against a generic Update before seal.
			captureState := func() (backenddb.NativeSemanticCapture, *nativeCollectionState) {
				t.Helper()
				capture, err := backend.CaptureNativeSemanticState()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := capture.Close(); err != nil {
						t.Error(err)
					}
				})
				engine, ok := capture.State().(*nativeEngineVersion)
				if !ok {
					t.Fatal("missing nativeEngineVersion")
				}
				state, ok := engine.names.Get("users")
				if !ok {
					t.Fatal("missing users collection state")
				}
				return capture, state
			}

			if matched, modified, err := col.UpdateBSONSet(ids[0], []BSONSetField{bsonSetInt32Field(t, "score", 9001)}); err != nil || !matched || !modified {
				t.Fatalf("covered UpdateBSONSet: matched=%t modified=%t err=%v", matched, modified, err)
			}
			_, covered := captureState()
			if matched, modified, err := col.Update(ids[0], func([]byte) ([]byte, bool, error) {
				return bsonSetCustodyTestDoc(t, "user-0000", 20, 9002, "u0@example.com", "hnl"), true, nil
			}); err != nil || !matched || !modified {
				t.Fatalf("generic Update: matched=%t modified=%t err=%v", matched, modified, err)
			}
			_, fallback := captureState()
			deadline := time.After(2 * time.Second)
			ticker := time.NewTicker(time.Millisecond)
			for covered.refs.Load()+1 != fallback.refs.Load() {
				select {
				case <-deadline:
					ticker.Stop()
					t.Fatalf("ReturnCustodyOnInstall inactive for %s: covered.refs=%d fallback.refs=%d", tc.name, covered.refs.Load(), fallback.refs.Load())
				case <-ticker.C:
				}
			}
			ticker.Stop()
		})
	}
}

func TestNativeBSONSetIndexedFieldMutationKeepsFallbackCustodyAndUniqueConflict(t *testing.T) {
	dir := t.TempDir()
	owner, backend, manager := openCustodyTestDB(t, dir)
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := manager.CreateCollection(bsonSetCustodyTestMeta("users", 1)); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	id0, id1 := []byte("u0"), []byte("u1")
	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{id0, id1},
		[][]byte{
			bsonSetCustodyTestDoc(t, "u0", 30, 1, "a@example.com", "hnl"),
			bsonSetCustodyTestDoc(t, "u1", 31, 1, "b@example.com", "sea"),
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	captureState := func() *nativeCollectionState {
		t.Helper()
		capture, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := capture.Close(); err != nil {
				t.Error(err)
			}
		})
		engine := capture.State().(*nativeEngineVersion)
		state, ok := engine.names.Get("users")
		if !ok {
			t.Fatal("missing users collection")
		}
		return state
	}

	// Mutating an indexed field (email) produces secondary-index domain changes
	// (len(g.plan.additional) > 0) and MUST keep ReturnCustodyOnInstall == false.
	if matched, modified, _, err := col.UpdateBSONSetWithJournalReceipt(id0, []BSONSetField{bsonSetStringField(t, "email", "a2@example.com")}); err != nil || !matched || !modified {
		t.Fatalf("indexed field UpdateBSONSetWithJournalReceipt: matched=%t modified=%t err=%v", matched, modified, err)
	}
	indexedMutationState := captureState()
	if matched, modified, err := col.Update(id0, func([]byte) ([]byte, bool, error) {
		return bsonSetCustodyTestDoc(t, "u0", 30, 2, "a3@example.com", "hnl"), true, nil
	}); err != nil || !matched || !modified {
		t.Fatalf("generic Update: matched=%t modified=%t err=%v", matched, modified, err)
	}
	genericMutationState := captureState()

	// Both must retain the fallback custody edge before physical seal.
	time.Sleep(50 * time.Millisecond)
	if indexedMutationState.refs.Load() != genericMutationState.refs.Load() {
		t.Fatalf("indexed-field mutation unexpectedly returned custody early: indexed.refs=%d generic.refs=%d",
			indexedMutationState.refs.Load(), genericMutationState.refs.Load())
	}

	// Unique conflict on indexed field must still fail with ErrUniqueIndexConflict.
	if _, _, err := col.UpdateBSONSet(id0, []BSONSetField{bsonSetStringField(t, "email", "b@example.com")}); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("expected ErrUniqueIndexConflict, got %v", err)
	}

	// Non-indexed score update after indexed email change, then close without explicit
	// checkpoint and reopen to verify WAL replay + secondary index consistency.
	if matched, modified, err := col.UpdateBSONSet(id0, []BSONSetField{bsonSetInt32Field(t, "score", 77)}); err != nil || !matched || !modified {
		t.Fatalf("post-email score update: matched=%t modified=%t err=%v", matched, modified, err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	reopened, _, reopenedMgr := openCustodyTestDB(t, dir)
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	reopenedCol, err := reopenedMgr.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopenedCol.Get(id0)
	if err != nil {
		t.Fatal(err)
	}
	want := bsonSetCustodyTestDoc(t, "u0", 30, 77, "a3@example.com", "hnl")
	if !bytes.Equal(got, want) {
		t.Fatalf("reopened u0 mismatch:\n got=%x\nwant=%x", got, want)
	}
	// Secondary unique index on email must still reject duplicate "a3@example.com".
	if _, _, err := reopenedCol.UpdateBSONSet(id1, []BSONSetField{bsonSetStringField(t, "email", "a3@example.com")}); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("reopened unique conflict check: got %v, want ErrUniqueIndexConflict", err)
	}
}

func TestNativeBSONSetNoSecondaryUniqueIndexChangesEdges(t *testing.T) {
	owner, _, manager := openCustodyTestDB(t, t.TempDir())
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	meta := &CollectionMeta{
		Name:    "nested",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
		Indexes: []IndexDefinition{
			{Name: "idx_email", Field: "email", ValueType: IndexValueBSONOrderedV2, Unique: true},
			{Name: "idx_addr_code", Field: "addr.code", ValueType: IndexValueBSONOrderedV2, Unique: true},
		},
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("nested")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "n1"},
		{Key: "email", Value: "n1@example.com"},
		{Key: "addr", Value: bson.D{{Key: "code", Value: "96815"}}},
		{Key: "score", Value: int32(1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("n1")}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}

	// 1. Non-unique field batch succeeds.
	res, batched, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{
		{DocumentID: []byte("n1"), Fields: []BSONSetField{bsonSetInt32Field(t, "score", 2)}},
	})
	if err != nil || !batched || len(res) != 1 || !res[0].Matched || !res[0].Modified {
		t.Fatalf("non-unique batch: res=%+v batched=%t err=%v", res, batched, err)
	}

	// 2. Setting unique field to its identical value + changing score succeeds (no actual unique change).
	res, batched, err = col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{
		{DocumentID: []byte("n1"), Fields: []BSONSetField{
			bsonSetStringField(t, "email", "n1@example.com"),
			bsonSetInt32Field(t, "score", 3),
		}},
	})
	if err != nil || !batched || len(res) != 1 || !res[0].Matched || !res[0].Modified {
		t.Fatalf("same-value unique batch: res=%+v batched=%t err=%v", res, batched, err)
	}

	// 3. Changing top-level unique field declines with batched=false.
	_, batched, err = col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{
		{DocumentID: []byte("n1"), Fields: []BSONSetField{bsonSetStringField(t, "email", "other@example.com")}},
	})
	if err != nil || batched {
		t.Fatalf("changed unique email batch: batched=%t err=%v, want batched=false err=nil", batched, err)
	}

	// 4. Changing top-level parent ("addr") of nested unique index ("addr.code") MUST decline.
	addrTyp, addrRaw, err := bson.MarshalValue(bson.D{{Key: "code", Value: "98101"}})
	if err != nil {
		t.Fatal(err)
	}
	_, batched, err = col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{
		{DocumentID: []byte("n1"), Fields: []BSONSetField{{Key: "addr", Value: bson.RawValue{Type: addrTyp, Value: addrRaw}}}},
	})
	if err != nil || batched {
		t.Fatalf("changed nested unique addr.code batch: batched=%t err=%v, want batched=false err=nil", batched, err)
	}

	// 5. Generic callback UpdateBatchIfNoSecondaryUniqueIndexChanges when unique unchanged succeeds
	//    via single-resolution cached path, and declines when unique changes.
	res, batched, err = col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("n1"),
		Update: func([]byte) ([]byte, bool, error) {
			next, err := bson.Marshal(bson.D{
				{Key: "_id", Value: "n1"},
				{Key: "email", Value: "n1@example.com"},
				{Key: "addr", Value: bson.D{{Key: "code", Value: "96815"}}},
				{Key: "score", Value: int32(4)},
			})
			return next, true, err
		},
	}})
	if err != nil || !batched || len(res) != 1 || !res[0].Modified {
		t.Fatalf("callback unchanged-unique batch: res=%+v batched=%t err=%v", res, batched, err)
	}
}

func BenchmarkUpdateBSONSetIndexed(b *testing.B) {
	for _, tc := range []struct {
		name     string
		idxCount int
	}{
		{name: "idx0", idxCount: 0},
		{name: "idx1", idxCount: 1},
		{name: "idx2", idxCount: 2},
	} {
		b.Run(tc.name, func(b *testing.B) {
			owner, backend, manager := openCustodyTestDB(b, b.TempDir())
			defer func() {
				if err := owner.Close(); err != nil {
					b.Error(err)
				}
			}()
			if _, err := manager.CreateCollection(bsonSetCustodyTestMeta("users", tc.idxCount)); err != nil {
				b.Fatal(err)
			}
			col, err := manager.OpenCollection("users")
			if err != nil {
				b.Fatal(err)
			}
			const seedCount = 64
			ids := make([][]byte, seedCount)
			docs := make([][]byte, seedCount)
			for i := 0; i < seedCount; i++ {
				id := fmt.Sprintf("user-%04d", i)
				ids[i] = []byte(id)
				docs[i] = bsonSetCustodyTestDoc(b, id, int32(20+i), 0, fmt.Sprintf("u%d@example.com", i), "hnl")
			}
			if _, err := col.InsertBatchValidatedBSON(ids, docs); err != nil {
				b.Fatal(err)
			}
			if err := owner.Checkpoint(); err != nil {
				b.Fatal(err)
			}
			var scans atomic.Int64
			restore := backend.SetScanCandidateExternalReferencesHookForTesting(func() {
				scans.Add(1)
			})
			defer restore()

			beforeStats := owner.Stats()
			beforeExt := parseUintStat(b, beforeStats, "treedb.cache.cow.external_bytes")
			beforeRollovers := parseUintStat(b, beforeStats, "treedb.cache.cow.rollovers_total")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := ids[i%seedCount]
				field := bsonSetInt32Field(b, "score", int32(i+1))
				matched, modified, _, err := col.UpdateBSONSetWithJournalReceipt(id, []BSONSetField{field})
				if err != nil || !matched || !modified {
					b.Fatalf("update %d: matched=%t modified=%t err=%v", i, matched, modified, err)
				}
			}
			b.StopTimer()
			afterStats := owner.Stats()
			afterExt := parseUintStat(b, afterStats, "treedb.cache.cow.external_bytes")
			afterRollovers := parseUintStat(b, afterStats, "treedb.cache.cow.rollovers_total")
			var extDelta uint64
			if afterExt > beforeExt {
				extDelta = afterExt - beforeExt
			}
			if b.N > 0 {
				b.ReportMetric(float64(extDelta)/float64(b.N), "external_B/op")
				b.ReportMetric(float64(afterRollovers-beforeRollovers), "rollovers")
				b.ReportMetric(float64(scans.Load()), "scans")
			}
		})
	}
}
