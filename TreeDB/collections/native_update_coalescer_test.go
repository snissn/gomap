package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeBSONSetCoalescerConcurrencyDuplicateKeysErrorIsolationAndReopen(t *testing.T) {
	dir := t.TempDir()
	openDB := func() (*treedb.DB, *CollectionManager, *Collection) {
		opts := treedb.OptionsForBenchmark(treedb.ProfileCommandWALRelaxed, dir)
		opts.MemtableMode = "cow_btree"
		opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
		owner, err := treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err := owner.NativeSemanticBackend()
		if err != nil {
			_ = owner.Close()
			t.Fatal(err)
		}
		mgr := NewCollectionManager(backend)
		col, err := mgr.OpenCollection("coalesce_docs")
		if err != nil {
			if _, err = mgr.CreateCollection(&CollectionMeta{
				Name:    "coalesce_docs",
				Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
				Indexes: []IndexDefinition{
					{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true},
				},
			}); err != nil {
				_ = owner.Close()
				t.Fatal(err)
			}
			col, err = mgr.OpenCollection("coalesce_docs")
			if err != nil {
				_ = owner.Close()
				t.Fatal(err)
			}
		}
		return owner, mgr, col
	}

	mustRawInt32 := func(v int32) bson.RawValue {
		t.Helper()
		tp, val, err := bson.MarshalValue(v)
		if err != nil {
			t.Fatal(err)
		}
		return bson.RawValue{Type: tp, Value: val}
	}
	mustRawString := func(s string) bson.RawValue {
		t.Helper()
		tp, val, err := bson.MarshalValue(s)
		if err != nil {
			t.Fatal(err)
		}
		return bson.RawValue{Type: tp, Value: val}
	}

	owner, _, col := openDB()
	const numDocs = 64
	ids := make([][]byte, numDocs)
	docs := make([][]byte, numDocs)
	for i := 0; i < numDocs; i++ {
		id := fmt.Sprintf("doc-%04d", i)
		raw, err := bson.Marshal(bson.D{
			{Key: "_id", Value: id},
			{Key: "email", Value: fmt.Sprintf("u%04d@example.com", i)},
			{Key: "seq", Value: int32(0)},
		})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = []byte(id)
		docs[i] = raw
	}
	if err := col.InsertBatchValidatedBSONNoResultIDs(ids, docs); err != nil {
		t.Fatal(err)
	}

	// Concurrent UpdateBSONSet on:
	// - distinct keys owned by workers 0..15 (ids[4..59]), verifying final per-doc seq
	// - hot shared duplicate keys (ids[0..3]) updated concurrently by workers 16..23
	// - error-isolation worker attempting invalid duplicate-email / invalid-field writes on ids[63]
	const distinctWorkers = 16
	const hotWorkers = 8
	const itersPerWorker = 25
	var expectedDistinct [numDocs]atomic.Int32
	var errWorkerErrors atomic.Int64
	var wg sync.WaitGroup
	errCh := make(chan error, distinctWorkers+hotWorkers+1)

	for w := 0; w < distinctWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			docIdx := 4 + workerID
			id := ids[docIdx]
			for iter := 1; iter <= itersPerWorker; iter++ {
				seqVal := int32(workerID*1000 + iter)
				matched, modified, err := col.UpdateBSONSet(id, []BSONSetField{
					{Key: "seq", Value: mustRawInt32(seqVal)},
				})
				if err != nil || !matched || !modified {
					select {
					case errCh <- fmt.Errorf("distinct worker=%d iter=%d id=%s matched=%t modified=%t err=%v", workerID, iter, id, matched, modified, err):
					default:
					}
					return
				}
				expectedDistinct[docIdx].Store(seqVal)
			}
		}(w)
	}

	for w := 0; w < hotWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			id := ids[workerID%4]
			for iter := 1; iter <= itersPerWorker; iter++ {
				seqVal := int32(100000 + workerID*1000 + iter)
				matched, _, err := col.UpdateBSONSet(id, []BSONSetField{
					{Key: "seq", Value: mustRawInt32(seqVal)},
				})
				if err != nil || !matched {
					select {
					case errCh <- fmt.Errorf("hot worker=%d iter=%d id=%s matched=%t err=%v", workerID, iter, id, matched, err):
					default:
					}
					return
				}
			}
		}(w)
	}

	// Error isolation worker: attempts unique-index collision on doc-0063 -> email of doc-0062.
	// Must fail on its own without aborting any coalesced peer update.
	wg.Add(1)
	go func() {
		defer wg.Done()
		dupEmail := mustRawString("u0062@example.com")
		for i := 0; i < 15; i++ {
			_, _, err := col.UpdateBSONSet(ids[63], []BSONSetField{
				{Key: "email", Value: dupEmail},
			})
			if err == nil {
				select {
				case errCh <- fmt.Errorf("expected unique index violation on doc-0063, got nil"):
				default:
				}
				return
			}
			errWorkerErrors.Add(1)
		}
	}()

	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	if errWorkerErrors.Load() != 15 {
		t.Fatalf("errWorkerErrors=%d want 15", errWorkerErrors.Load())
	}

	verifyDocs := func(c *Collection, phase string) {
		for i := 0; i < numDocs; i++ {
			raw, err := c.Get(ids[i])
			if err != nil {
				t.Fatalf("%s Get(%s): %v", phase, ids[i], err)
			}
			var doc bson.D
			if err := bson.Unmarshal(raw, &doc); err != nil {
				t.Fatalf("%s Unmarshal(%s): %v", phase, ids[i], err)
			}
			var gotSeq int32
			var gotEmail string
			for _, elem := range doc {
				switch elem.Key {
				case "seq":
					gotSeq = elem.Value.(int32)
				case "email":
					gotEmail = elem.Value.(string)
				}
			}
			if i >= 4 && i < 4+distinctWorkers {
				want := expectedDistinct[i].Load()
				if gotSeq != want {
					t.Fatalf("%s doc-%04d seq=%d want %d", phase, i, gotSeq, want)
				}
			}
			if i < 4 && gotSeq < 100000 {
				t.Fatalf("%s hot doc-%04d seq=%d want >= 100000", phase, i, gotSeq)
			}
			if i == 63 && gotEmail != "u0063@example.com" {
				t.Fatalf("%s doc-0063 email=%q want u0063@example.com after rejected unique collisions", phase, gotEmail)
			}
		}
	}

	verifyDocs(col, "before-reopen")
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}

	owner2, _, col2 := openDB()
	defer func() { _ = owner2.Close() }()
	verifyDocs(col2, "after-reopen")
}

func TestNativeUpdateCommitCoalescerConcurrencyDuplicateKeysErrorIsolationAndReopen(t *testing.T) {
	dir := t.TempDir()
	openDB := func() (*treedb.DB, *CollectionManager, *Collection) {
		opts := treedb.OptionsForBenchmark(treedb.ProfileCommandWALRelaxed, dir)
		opts.MemtableMode = "cow_btree"
		opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
		owner, err := treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err := owner.NativeSemanticBackend()
		if err != nil {
			_ = owner.Close()
			t.Fatal(err)
		}
		mgr := NewCollectionManager(backend)
		col, err := mgr.OpenCollection("commit_coalesce_docs")
		if err != nil {
			if _, err = mgr.CreateCollection(&CollectionMeta{
				Name:    "commit_coalesce_docs",
				Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
				Indexes: []IndexDefinition{
					{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true},
				},
			}); err != nil {
				_ = owner.Close()
				t.Fatal(err)
			}
			col, err = mgr.OpenCollection("commit_coalesce_docs")
			if err != nil {
				_ = owner.Close()
				t.Fatal(err)
			}
		}
		return owner, mgr, col
	}

	owner, _, col := openDB()
	const numDocs = 64
	ids := make([][]byte, numDocs)
	docs := make([][]byte, numDocs)
	for i := 0; i < numDocs; i++ {
		id := fmt.Sprintf("doc-%04d", i)
		raw, err := bson.Marshal(bson.D{
			{Key: "_id", Value: id},
			{Key: "email", Value: fmt.Sprintf("u%04d@example.com", i)},
			{Key: "count", Value: int32(0)},
		})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = []byte(id)
		docs[i] = raw
	}
	if err := col.InsertBatchValidatedBSONNoResultIDs(ids, docs); err != nil {
		t.Fatal(err)
	}

	injectedErr := errors.New("injected per-item callback error")
	const workers = 16
	const itersPerWorker = 20
	var injectedSeen atomic.Int64
	var wg sync.WaitGroup
	errCh := make(chan error, workers+1)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for iter := 0; iter < itersPerWorker; iter++ {
				var targetIdx int
				if workerID < 8 {
					targetIdx = 4 + (workerID*itersPerWorker+iter)%56
				} else {
					targetIdx = workerID % 4 // hot shared duplicate keys requiring precondition retry
				}
				id := ids[targetIdx]
				matched, modified, err := col.Update(id, func(stored []byte) ([]byte, bool, error) {
					var doc bson.D
					if uErr := bson.Unmarshal(stored, &doc); uErr != nil {
						return nil, false, uErr
					}
					for idx := range doc {
						if doc[idx].Key == "count" {
							doc[idx].Value = doc[idx].Value.(int32) + 1
						}
					}
					out, mErr := bson.Marshal(doc)
					return out, true, mErr
				})
				if err != nil || !matched || !modified {
					select {
					case errCh <- fmt.Errorf("worker=%d iter=%d id=%s matched=%t modified=%t err=%v", workerID, iter, id, matched, modified, err):
					default:
					}
					return
				}
			}
		}(w)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			_, _, err := col.Update(ids[63], func(stored []byte) ([]byte, bool, error) {
				return nil, false, injectedErr
			})
			if !errors.Is(err, injectedErr) {
				select {
				case errCh <- fmt.Errorf("expected injectedErr, got %v", err):
				default:
				}
				return
			}
			injectedSeen.Add(1)
		}
	}()

	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	if injectedSeen.Load() != 10 {
		t.Fatalf("injectedSeen=%d want 10", injectedSeen.Load())
	}

	verifyCounts := func(c *Collection, phase string) {
		var total int32
		for i := 0; i < numDocs; i++ {
			raw, err := c.Get(ids[i])
			if err != nil {
				t.Fatalf("%s Get(%s): %v", phase, ids[i], err)
			}
			var doc bson.D
			if err := bson.Unmarshal(raw, &doc); err != nil {
				t.Fatalf("%s Unmarshal(%s): %v", phase, ids[i], err)
			}
			for _, elem := range doc {
				if elem.Key == "count" {
					v := elem.Value.(int32)
					if i == 63 && v != 0 {
						t.Fatalf("%s doc-0063 count=%d want 0", phase, v)
					}
					total += v
				}
			}
		}
		wantTotal := int32(workers * itersPerWorker)
		if total != wantTotal {
			t.Fatalf("%s total increments=%d want %d (lost update!)", phase, total, wantTotal)
		}
	}

	verifyCounts(col, "before-reopen")
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}

	owner2, _, col2 := openDB()
	defer func() { _ = owner2.Close() }()
	verifyCounts(col2, "after-reopen")
}

func TestNativeUpdateCoalescerTemplateV1ConcurrentUpdates(t *testing.T) {
	dir := t.TempDir()
	openDB := func() (*treedb.DB, *CollectionManager, *Collection) {
		opts := treedb.OptionsForBenchmark(treedb.ProfileCommandWALRelaxed, dir)
		opts.MemtableMode = "cow_btree"
		opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
		owner, err := treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err := owner.NativeSemanticBackend()
		if err != nil {
			_ = owner.Close()
			t.Fatal(err)
		}
		mgr := NewCollectionManager(backend)
		col, err := mgr.OpenCollection("tpl_docs")
		if err != nil {
			if _, err = mgr.CreateCollection(&CollectionMeta{
				Name:    "tpl_docs",
				Options: CollectionOptions{DocumentFormat: DocumentFormatTemplateV1},
				Indexes: []IndexDefinition{
					{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true},
				},
			}); err != nil {
				_ = owner.Close()
				t.Fatal(err)
			}
			col, err = mgr.OpenCollection("tpl_docs")
			if err != nil {
				_ = owner.Close()
				t.Fatal(err)
			}
		}
		return owner, mgr, col
	}

	owner, _, col := openDB()
	const numDocs = 32
	ids := make([][]byte, numDocs)
	for i := 0; i < numDocs; i++ {
		id := []byte(fmt.Sprintf("tpl-%04d", i))
		ids[i] = id
		rawJSON := []byte(fmt.Sprintf(`{"_id":"tpl-%04d","email":"t%04d@example.com","version":0}`, i, i))
		encoded, err := EncodeTemplateV1DocumentJSON(rawJSON)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := col.Insert(id, encoded); err != nil {
			t.Fatal(err)
		}
	}

	const workers = 16
	const iters = 15
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			id := ids[workerID]
			for iter := 1; iter <= iters; iter++ {
				wantJSON := []byte(fmt.Sprintf(`{"_id":"tpl-%04d","email":"t%04d@example.com","version":%d,"extra":"v%d"}`, workerID, workerID, iter, iter))
				encoded, err := EncodeTemplateV1DocumentJSON(wantJSON)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				matched, modified, err := col.Update(id, func(_ []byte) ([]byte, bool, error) {
					return encoded, true, nil
				})
				if err != nil || !matched || !modified {
					select {
					case errCh <- fmt.Errorf("worker=%d iter=%d matched=%t modified=%t err=%v", workerID, iter, matched, modified, err):
					default:
					}
					return
				}
			}
		}(w)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	verifyDocs := func(c *Collection, phase string) {
		mat, err := c.NewStoredDocumentJSONMaterializer()
		if err != nil {
			t.Fatalf("%s NewStoredDocumentJSONMaterializer: %v", phase, err)
		}
		defer func() { _ = mat.Close() }()
		for w := 0; w < workers; w++ {
			got, err := c.Get(ids[w])
			if err != nil {
				t.Fatalf("%s Get(%s): %v", phase, ids[w], err)
			}
			jsonDoc, err := mat.StoredDocumentJSON(got)
			if err != nil {
				t.Fatalf("%s StoredDocumentJSON(%s): %v", phase, ids[w], err)
			}
			needle := []byte(fmt.Sprintf(`"version":%d`, iters))
			if !bytes.Contains(jsonDoc, needle) {
				t.Fatalf("%s doc %s = %s, missing %s", phase, ids[w], jsonDoc, needle)
			}
		}
	}
	verifyDocs(col, "before-reopen")
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	owner2, _, col2 := openDB()
	defer func() { _ = owner2.Close() }()
	verifyDocs(col2, "after-reopen")
}

func TestNativeUpdateCoalescerRejectsConcurrentUniqueIndexSwap(t *testing.T) {
	openCol := func(t *testing.T, name string) (*treedb.DB, *Collection) {
		t.Helper()
		opts := treedb.OptionsForBenchmark(treedb.ProfileCommandWALRelaxed, t.TempDir())
		opts.MemtableMode = "cow_btree"
		opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
		owner, err := treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = owner.Close() })
		backend, err := owner.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		mgr := NewCollectionManager(backend)
		t.Cleanup(func() { _ = mgr.Close() })
		if _, err := mgr.CreateCollection(&CollectionMeta{
			Name:    name,
			Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
			Indexes: []IndexDefinition{
				{Name: "email_1", Field: "email", ValueType: IndexValueString, Unique: true},
			},
		}); err != nil {
			t.Fatal(err)
		}
		col, err := mgr.OpenCollection(name)
		if err != nil {
			t.Fatal(err)
		}
		docA, err := bson.Marshal(bson.D{{Key: "_id", Value: "A"}, {Key: "email", Value: "a@x"}})
		if err != nil {
			t.Fatal(err)
		}
		docB, err := bson.Marshal(bson.D{{Key: "_id", Value: "B"}, {Key: "email", Value: "b@x"}})
		if err != nil {
			t.Fatal(err)
		}
		if err := col.InsertBatchValidatedBSONNoResultIDs([][]byte{[]byte("A"), []byte("B")}, [][]byte{docA, docB}); err != nil {
			t.Fatal(err)
		}
		return owner, col
	}

	mustRawString := func(t *testing.T, s string) bson.RawValue {
		t.Helper()
		tp, val, err := bson.MarshalValue(s)
		if err != nil {
			t.Fatal(err)
		}
		return bson.RawValue{Type: tp, Value: val}
	}

	t.Run("UpdateBSONSet", func(t *testing.T) {
		_, col := openCol(t, "swap_bson_set")
		coord := collectionSchemaCoordinatorForDBCollection(col.nativeJSON.db, col.name)

		// Queue both A->b@x and B->a@x in the same coalescer wave deterministically.
		itemA := &nativeBSONSetCoalescerItem{
			col:    col,
			id:     []byte("A"),
			fields: []BSONSetField{{Key: "email", Value: mustRawString(t, "b@x")}},
			done:   make(chan struct{}, 1),
		}
		itemB := &nativeBSONSetCoalescerItem{
			col:    col,
			id:     []byte("B"),
			fields: []BSONSetField{{Key: "email", Value: mustRawString(t, "a@x")}},
			done:   make(chan struct{}, 1),
		}
		coord.nativeBSONSetMu.Lock()
		coord.nativeBSONSetActive = true
		coord.nativeBSONSetPending = append(coord.nativeBSONSetPending, itemA, itemB)
		coord.nativeBSONSetMu.Unlock()

		coord.drainNativeBSONSetPending()
		<-itemA.done
		<-itemB.done

		if itemA.err == nil && itemB.err == nil {
			t.Fatalf("illegal cross-document unique email swap succeeded in UpdateBSONSet coalescer (A.err=%v, B.err=%v)", itemA.err, itemB.err)
		}
		if itemA.err == nil || itemB.err == nil {
			// Either individual ordering must fail BOTH (first fails because target exists,
			// so original email remains occupied, and second ALSO fails!).
			t.Fatalf("expected both individual swap attempts to encounter unique conflict, got A.err=%v B.err=%v", itemA.err, itemB.err)
		}
	})

	t.Run("UpdateCommitCoalescer", func(t *testing.T) {
		_, col := openCol(t, "swap_update_commit")
		coord := collectionSchemaCoordinatorForDBCollection(col.nativeJSON.db, col.name)

		oldA, err := col.Get([]byte("A"))
		if err != nil {
			t.Fatal(err)
		}
		oldB, err := col.Get([]byte("B"))
		if err != nil {
			t.Fatal(err)
		}
		newA, err := bson.Marshal(bson.D{{Key: "_id", Value: "A"}, {Key: "email", Value: "b@x"}})
		if err != nil {
			t.Fatal(err)
		}
		newB, err := bson.Marshal(bson.D{{Key: "_id", Value: "B"}, {Key: "email", Value: "a@x"}})
		if err != nil {
			t.Fatal(err)
		}

		unlockVector, _, err := col.prepareNativeProducerMutation()
		if err != nil {
			t.Fatal(err)
		}
		defer unlockVector()

		probeTx, err := col.nativeJSON.beginPrepared(col.name)
		if err != nil {
			t.Fatal(err)
		}
		inc := probeTx.binding(col.name).incarnation
		if err := probeTx.Close(); err != nil {
			t.Fatal(err)
		}

		itemA := &nativeUpdateCommitItem{
			col:         col,
			id:          []byte("A"),
			expectedOld: oldA,
			stagedNew:   newA,
			incarnation: inc,
			done:        make(chan struct{}, 1),
		}
		itemB := &nativeUpdateCommitItem{
			col:         col,
			id:          []byte("B"),
			expectedOld: oldB,
			stagedNew:   newB,
			incarnation: inc,
			done:        make(chan struct{}, 1),
		}
		coord.nativeUpdateCommitMu.Lock()
		coord.nativeUpdateCommitActive = true
		coord.nativeUpdateCommitPending = append(coord.nativeUpdateCommitPending, itemA, itemB)
		coord.nativeUpdateCommitMu.Unlock()

		coord.drainNativeUpdateCommitPending()
		<-itemA.done
		<-itemB.done

		if itemA.err == nil && itemB.err == nil {
			t.Fatalf("illegal cross-document unique email swap succeeded in Update commit coalescer (A.err=%v, B.err=%v)", itemA.err, itemB.err)
		}
		if itemA.err == nil || itemB.err == nil {
			t.Fatalf("expected both individual swap attempts to encounter unique conflict, got A.err=%v B.err=%v", itemA.err, itemB.err)
		}
	})
}
