package collections

import (
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
