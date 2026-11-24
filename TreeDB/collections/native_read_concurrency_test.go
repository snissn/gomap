package collections

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func openNativeReadConcurrencyFixture(t *testing.T, pointerThreshold int) (*treedb.DB, *CollectionManager, *Collection) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.ValueLog.PointerThreshold = pointerThreshold
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	meta := &CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:          DocumentFormatBSON,
			AllowArrayValuesInIndex: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "score", Field: "score", ValueType: IndexValueInt64},
			{Name: "tags", Field: "tags", ValueType: IndexValueString, MultiKey: true},
		},
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	return owner, manager, col
}

func makeNativeBSONUser(t testing.TB, email string, score int64, tags []string, pad string) []byte {
	t.Helper()
	doc, err := bson.Marshal(bson.D{
		{Key: "email", Value: email},
		{Key: "score", Value: score},
		{Key: "tags", Value: tags},
		{Key: "pad", Value: pad},
	})
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestNativeIndexCursorCorrectnessAndOverlays(t *testing.T) {
	for _, threshold := range []int{64, 1 << 20} {
		t.Run(fmt.Sprintf("pointer_threshold=%d", threshold), func(t *testing.T) {
			owner, manager, col := openNativeReadConcurrencyFixture(t, threshold)
			pad := string(bytes.Repeat([]byte("p"), 160))

			// Insert settled batch and checkpoint into physical B-tree.
			const baseCount = 60
			ids := make([][]byte, baseCount)
			docs := make([][]byte, baseCount)
			for i := 0; i < baseCount; i++ {
				ids[i] = []byte(fmt.Sprintf("u%04d", i))
				// Include duplicate tags within the same doc to exercise dedupe.
				docs[i] = makeNativeBSONUser(t, fmt.Sprintf("user%04d@example.com", i), int64(i*10), []string{"common", "dup", fmt.Sprintf("bucket-%d", i%5)}, pad)
			}
			if _, err := col.InsertBatch(ids, docs); err != nil {
				t.Fatal(err)
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			// Capture an older cut BEFORE overlays.
			oldRead, err := col.nativeJSON.Capture(col.name)
			if err != nil {
				t.Fatal(err)
			}
			defer oldRead.Close()

			// Apply overlays on top of settled physical roots:
			// - Delete u0005 (tombstones primary + secondary email/score/tags)
			// - Re-insert email user0005@example.com under new id u9005 (tests tombstone skipping on unique index!)
			// - Update u0010 score from 100 -> 105 (tombstones old score, inserts new score)
			// - Insert brand-new overlay doc u9000 with score 15
			if err := col.Delete([]byte("u0005")); err != nil {
				t.Fatal(err)
			}
			reusedDoc := makeNativeBSONUser(t, "user0005@example.com", 55, []string{"reused", "common"}, pad)
			if _, err := col.Insert([]byte("u9005"), reusedDoc); err != nil {
				t.Fatal(err)
			}
			updatedDoc := makeNativeBSONUser(t, "user0010@example.com", 105, []string{"updated", "common"}, pad)
			if _, _, err := col.Update([]byte("u0010"), func([]byte) ([]byte, bool, error) { return updatedDoc, true, nil }); err != nil {
				t.Fatal(err)
			}
			overlayDoc := makeNativeBSONUser(t, "overlay@example.com", 15, []string{"overlay", "common"}, pad)
			if _, err := col.Insert([]byte("u9000"), overlayDoc); err != nil {
				t.Fatal(err)
			}

			// 1. One-shot Get & GetInto (both dst==nil and reused dst) across settled, overlay, updated, deleted.
			var buf []byte
			got, found, err := col.GetInto([]byte("u0000"), buf)
			if err != nil || !found || !bytes.Equal(got, docs[0]) {
				t.Fatalf("settled GetInto nil dst: found=%v err=%v", found, err)
			}
			buf = make([]byte, 0, 512)
			got, found, err = col.GetInto([]byte("u0001"), buf)
			if err != nil || !found || !bytes.Equal(got, docs[1]) {
				t.Fatalf("settled GetInto reused dst: found=%v err=%v", found, err)
			}
			if cap(got) != cap(buf) || &got[0] != &buf[:1][0] {
				t.Fatal("GetInto did not reuse caller buffer")
			}
			got, found, err = col.GetInto([]byte("u0005"), buf)
			if err != nil || found || len(got) != 0 {
				t.Fatalf("deleted GetInto: found=%v len=%d err=%v", found, len(got), err)
			}
			got, found, err = col.GetInto([]byte("u9005"), buf)
			if err != nil || !found || !bytes.Equal(got, reusedDoc) {
				t.Fatalf("overlay GetInto: found=%v err=%v", found, err)
			}
			got, found, err = col.GetInto([]byte("u0010"), buf)
			if err != nil || !found || !bytes.Equal(got, updatedDoc) {
				t.Fatalf("updated GetInto: found=%v err=%v", found, err)
			}

			// 2. Unique index FindByIndex / FindByIndexValue / FindByIndexValueLimit:
			// - Settled key with maxResults=1 must NOT report truncated (unique short-circuit)
			res, truncated, err := col.FindByIndexValueLimit("email", "user0020@example.com", 1)
			if err != nil || truncated || len(res) != 1 || string(res[0]) != "u0020" {
				t.Fatalf("unique settled FindByIndexValueLimit: res=%q truncated=%v err=%v", res, truncated, err)
			}
			resSimple, err := col.FindByIndex("email", "user0020@example.com")
			if err != nil || len(resSimple) != 1 || string(resSimple[0]) != "u0020" {
				t.Fatalf("unique settled FindByIndex: res=%q err=%v", resSimple, err)
			}
			resVal, err := col.FindByIndexValue("email", "user0020@example.com")
			if err != nil || len(resVal) != 1 || string(resVal[0]) != "u0020" {
				t.Fatalf("unique settled FindByIndexValue: res=%q err=%v", resVal, err)
			}
			// - Tombstone shadowed by new doc with same unique email
			res, truncated, err = col.FindByIndexValueLimit("email", "user0005@example.com", 1)
			if err != nil || truncated || len(res) != 1 || string(res[0]) != "u9005" {
				t.Fatalf("unique tombstone+reinsert FindByIndexValueLimit: res=%q truncated=%v err=%v", res, truncated, err)
			}
			// - Deleted unique key returns empty
			if err := col.Delete([]byte("u0002")); err != nil {
				t.Fatal(err)
			}
			res, truncated, err = col.FindByIndexValueLimit("email", "user0002@example.com", 1)
			if err != nil || truncated || len(res) != 0 {
				t.Fatalf("unique deleted FindByIndexValueLimit: res=%q truncated=%v err=%v", res, truncated, err)
			}
			// - Unknown index returns empty without error
			res, truncated, err = col.FindByIndexValueLimit("does_not_exist", "x", 10)
			if err != nil || truncated || len(res) != 0 {
				t.Fatalf("unknown index FindByIndexValueLimit: res=%q truncated=%v err=%v", res, truncated, err)
			}

			// 3. Non-unique index FindByIndexValueLimit with truncation & exact limit.
			res, truncated, err = col.FindByIndexValueLimit("tags", "common", 5)
			if err != nil || !truncated || len(res) != 5 {
				t.Fatalf("non-unique truncated FindByIndexValueLimit: len=%d truncated=%v err=%v", len(res), truncated, err)
			}
			resAll, err := col.FindByIndexValue("tags", "common")
			if err != nil || len(resAll) != baseCount {
				// baseCount=60, -2 deleted (u0002, u0005), +2 inserted (u9000, u9005) = 60
				t.Fatalf("non-unique full FindByIndexValue: len=%d want=%d err=%v", len(resAll), baseCount, err)
			}
			resExact, truncated, err := col.FindByIndexValueLimit("tags", "common", len(resAll))
			if err != nil || truncated || len(resExact) != len(resAll) {
				t.Fatalf("non-unique exact limit FindByIndexValueLimit: len=%d truncated=%v err=%v", len(resExact), truncated, err)
			}

			// 4. Range scan (IDs and documents) across settled + overlay + tombstones + limit truncation.
			// Scores in [0, 110]:
			// u0000(0), u0001(10), u9000(15), [u0002 deleted], u0003(30), u0004(40),
			// [u0005 deleted], u9005(55), u0006(60), u0007(70), u0008(80), u0009(90),
			// [u0010 old 100 tombstoned], u0010(105), u0011(110)
			wantRangeIDs := []string{
				"u0000", "u0001", "u9000", "u0003", "u0004",
				"u9005", "u0006", "u0007", "u0008", "u0009",
				"u0010", "u0011",
			}
			rangeOpts := IndexRangeOptions{
				Lower: IndexRangeBound{Value: int64(0), Inclusive: true},
				Upper: IndexRangeBound{Value: int64(110), Inclusive: true},
				Limit: 10,
			}
			gotRange, truncated, err := col.FindByIndexRange("score", rangeOpts)
			if err != nil || !truncated {
				t.Fatalf("FindByIndexRange limit=10: truncated=%v err=%v", truncated, err)
			}
			var gotRangeStr []string
			for _, id := range gotRange {
				gotRangeStr = append(gotRangeStr, string(id))
			}
			if !reflect.DeepEqual(gotRangeStr, wantRangeIDs[:10]) {
				t.Fatalf("FindByIndexRange IDs=%v want=%v", gotRangeStr, wantRangeIDs[:10])
			}

			// Exact limit = len(wantRangeIDs) -> truncated must be false.
			rangeOpts.Limit = len(wantRangeIDs)
			gotRange, truncated, err = col.FindByIndexRange("score", rangeOpts)
			if err != nil || truncated || len(gotRange) != len(wantRangeIDs) {
				t.Fatalf("FindByIndexRange exact limit: len=%d truncated=%v err=%v", len(gotRange), truncated, err)
			}

			// ScanBorrowedDocumentsByIndexRange verifies same-cut primary materialization.
			rangeOpts.Limit = 10
			var scannedIDs []string
			truncated, err = col.ScanBorrowedDocumentsByIndexRange("score", rangeOpts, func(rec BorrowedDocumentRecord) (bool, error) {
				scannedIDs = append(scannedIDs, string(rec.ID))
				if len(rec.Document) == 0 {
					return false, errors.New("empty document")
				}
				return true, nil
			})
			if err != nil || !truncated || !reflect.DeepEqual(scannedIDs, wantRangeIDs[:10]) {
				t.Fatalf("ScanBorrowedDocumentsByIndexRange: ids=%v truncated=%v err=%v", scannedIDs, truncated, err)
			}

			// MultiKey range dedupe: scanning tags ["common", "dup"] should emit each doc once!
			dedupeOpts := IndexRangeOptions{
				Lower: IndexRangeBound{Value: "common", Inclusive: true},
				Upper: IndexRangeBound{Value: "dup", Inclusive: true},
				Limit: 10,
			}
			dedupeIDs, truncated, err := col.FindByIndexRange("tags", dedupeOpts)
			if err != nil || !truncated || len(dedupeIDs) != 10 {
				t.Fatalf("dedupe range scan: len=%d truncated=%v err=%v", len(dedupeIDs), truncated, err)
			}
			seen := make(map[string]struct{}, len(dedupeIDs))
			for _, id := range dedupeIDs {
				if _, dup := seen[string(id)]; dup {
					t.Fatalf("duplicate ID %q in deduped range scan", id)
				}
				seen[string(id)] = struct{}{}
			}

			// 5. Cut isolation: oldRead must still see original u0005 and u0010=100!
			var oldIDs []string
			if err := oldRead.visitIndexValueIDs("email", "user0005@example.com", func(id []byte) error {
				oldIDs = append(oldIDs, string(id))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(oldIDs, []string{"u0005"}) {
				t.Fatalf("old cut email lookup=%v want [u0005]", oldIDs)
			}

			// 6. Reentrant Close during callback on captured NativeJSONRead.
			reentrantRead, err := col.nativeJSON.Capture(col.name)
			if err != nil {
				t.Fatal(err)
			}
			rangeCalls := 0
			_, _, err = reentrantRead.scanIndexRange("score", IndexRangeOptions{
				Lower: IndexRangeBound{Value: int64(0), Inclusive: true},
				Upper: IndexRangeBound{Value: int64(110), Inclusive: true},
				Limit: 10,
			}, true, func(id, doc []byte) (bool, error) {
				rangeCalls++
				if err := reentrantRead.Close(); err != nil {
					return false, err
				}
				if _, err := reentrantRead.Get(id); !errors.Is(err, backenddb.ErrClosed) {
					return false, fmt.Errorf("expected ErrClosed after reentrant Close, got %v", err)
				}
				return true, nil
			})
			if !errors.Is(err, backenddb.ErrClosed) || rangeCalls != 1 || reentrantRead.capture != nil {
				t.Fatalf("reentrant Close scanIndexRange: calls=%d capture=%v err=%v", rangeCalls, reentrantRead.capture != nil, err)
			}

			// Reentrant Close during non-unique visitIndexValueIDs.
			reentrantVisit, err := col.nativeJSON.Capture(col.name)
			if err != nil {
				t.Fatal(err)
			}
			visitCalls := 0
			err = reentrantVisit.visitIndexValueIDs("tags", "common", func(id []byte) error {
				visitCalls++
				if err := reentrantVisit.Close(); err != nil {
					return err
				}
				return nil
			})
			if !errors.Is(err, backenddb.ErrClosed) || visitCalls != 1 || reentrantVisit.capture != nil {
				t.Fatalf("reentrant Close visitIndexValueIDs: calls=%d capture=%v err=%v", visitCalls, reentrantVisit.capture != nil, err)
			}

			// 7. Capacity exhaustion returns ErrCOWCapacity without leaking leases.
			budget := oldRead.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
			blocker, err := oldRead.capture.NativeAllocator().AdmitNativeBytes(0)
			if err != nil {
				t.Fatal(err)
			}
			if err := blocker.Resize(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes); err != nil {
				blocker.Close()
				t.Fatal(err)
			}
			beforeFull := budget.Stats()
			if _, _, err := col.GetInto([]byte("u0000"), nil); !errors.Is(err, memtable.ErrCOWCapacity) {
				blocker.Close()
				t.Fatalf("full budget GetInto: %v", err)
			}
			if _, _, err := col.FindByIndexValueLimit("email", "user0000@example.com", 1); !errors.Is(err, memtable.ErrCOWCapacity) {
				blocker.Close()
				t.Fatalf("full budget FindByIndexValueLimit: %v", err)
			}
			if _, _, err := col.FindByIndexRange("score", rangeOpts); !errors.Is(err, memtable.ErrCOWCapacity) {
				blocker.Close()
				t.Fatalf("full budget FindByIndexRange: %v", err)
			}
			afterFull := budget.Stats()
			blocker.Close()
			if afterFull.ReservedBytes != beforeFull.ReservedBytes || afterFull.ExternalLeases != beforeFull.ExternalLeases {
				t.Fatalf("capacity refusal leaked: before=%+v after=%+v", beforeFull, afterFull)
			}

			if err := oldRead.Close(); err != nil {
				t.Fatal(err)
			}

			// 8. 16 concurrent readers across GetInto, FindByIndex, FindByIndexRange, ScanBorrowedDocumentsByIndexRange.
			const readers = 16
			var wg sync.WaitGroup
			errCh := make(chan error, readers)
			for r := 0; r < readers; r++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					localBuf := make([]byte, 0, 512)
					for iter := 0; iter < 50; iter++ {
						idx := (worker*13 + iter) % baseCount
						if idx == 2 || idx == 5 || idx == 10 {
							idx = 0
						}
						id := ids[idx]
						got, found, err := col.GetInto(id, localBuf)
						if err != nil || !found || !bytes.Equal(got, docs[idx]) {
							errCh <- fmt.Errorf("worker=%d GetInto(%s): found=%v err=%v", worker, id, found, err)
							return
						}
						localBuf = got[:0]

						email := fmt.Sprintf("user%04d@example.com", idx)
						foundIDs, trunc, err := col.FindByIndexValueLimit("email", email, 1)
						if err != nil || trunc || len(foundIDs) != 1 || !bytes.Equal(foundIDs[0], id) {
							errCh <- fmt.Errorf("worker=%d FindByIndexValueLimit(%s): ids=%q trunc=%v err=%v", worker, email, foundIDs, trunc, err)
							return
						}

						lo := int64((idx % 40) * 10)
						hi := lo + 100
						rIDs, _, err := col.FindByIndexRange("score", IndexRangeOptions{
							Lower: IndexRangeBound{Value: lo, Inclusive: true},
							Upper: IndexRangeBound{Value: hi, Inclusive: true},
							Limit: 10,
						})
						if err != nil || len(rIDs) == 0 {
							errCh <- fmt.Errorf("worker=%d FindByIndexRange(%d..%d): len=%d err=%v", worker, lo, hi, len(rIDs), err)
							return
						}

						docHits := 0
						_, err = col.ScanBorrowedDocumentsByIndexRange("score", IndexRangeOptions{
							Lower: IndexRangeBound{Value: lo, Inclusive: true},
							Upper: IndexRangeBound{Value: hi, Inclusive: true},
							Limit: 5,
						}, func(rec BorrowedDocumentRecord) (bool, error) {
							if len(rec.ID) == 0 || len(rec.Document) == 0 {
								return false, errors.New("empty borrowed record")
							}
							docHits++
							return true, nil
						})
						if err != nil || docHits == 0 {
							errCh <- fmt.Errorf("worker=%d ScanBorrowedDocumentsByIndexRange: hits=%d err=%v", worker, docHits, err)
							return
						}
					}
				}(r)
			}
			wg.Wait()
			close(errCh)
			for err := range errCh {
				t.Fatal(err)
			}

			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
				t.Fatalf("close leaked budget: %+v", stats)
			}
		})
	}
}

func TestNativeCompoundUniqueIndexScalarPrefixVisitsAllMatches(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatalf("NativeSemanticBackend: %v", err)
	}
	mgr := NewCollectionManager(backend)
	defer func() {
		if err := mgr.Close(); err != nil {
			t.Fatalf("mgr.Close: %v", err)
		}
	}()
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "compound_unique",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{
			{
				Name:      "xy",
				ValueType: IndexValueBSONOrderedV2,
				Unique:    true,
				Components: []IndexComponent{
					{Field: "x", Direction: IndexDirectionAscending},
					{Field: "y", Direction: IndexDirectionAscending},
				},
			},
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("compound_unique")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	docs := []struct {
		id string
		x  string
		y  int64
	}{
		{"doc-1", "a", 1},
		{"doc-2", "a", 2},
		{"doc-3", "a", 3},
		{"doc-4", "b", 1},
	}
	ids := make([][]byte, len(docs))
	raws := make([][]byte, len(docs))
	for i, d := range docs {
		ids[i] = []byte(d.id)
		raw, err := bson.Marshal(bson.D{{Key: "x", Value: d.x}, {Key: "y", Value: d.y}})
		if err != nil {
			t.Fatalf("bson.Marshal: %v", err)
		}
		raws[i] = raw
	}
	if _, err := col.InsertBatch(ids, raws); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	rawA, err := bson.Marshal(bson.D{{Key: "v", Value: "a"}})
	if err != nil {
		t.Fatalf("bson.Marshal lookup: %v", err)
	}
	got, err := col.FindByIndexValue("xy", bson.Raw(rawA).Lookup("v"))
	if err != nil {
		t.Fatalf("FindByIndexValue: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("FindByIndexValue(xy, a) returned %d IDs (%q), want 3", len(got), got)
	}
}
