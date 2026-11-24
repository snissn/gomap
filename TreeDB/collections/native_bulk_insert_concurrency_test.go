package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func openNativeBulkInsertFixture(t *testing.T, dir string, profile treedb.Profile, limits treedb.COWMemtableLimits, meta *CollectionMeta) (*treedb.DB, *backenddb.DB, *CollectionManager, *Collection) {
	t.Helper()
	opts := treedb.OptionsFor(profile, dir)
	opts.MemtableMode = "cow_btree"
	opts.COWMemtableLimits = limits
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("treedb.Open(%s): %v", profile, err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		_ = owner.Close()
		t.Fatalf("NativeSemanticBackend: %v", err)
	}
	manager := NewCollectionManager(backend)
	if meta != nil {
		if _, err := manager.CreateCollection(meta); err != nil {
			_ = manager.Close()
			_ = owner.Close()
			t.Fatalf("CreateCollection(%s): %v", meta.Name, err)
		}
		col, err := manager.OpenCollection(meta.Name)
		if err != nil {
			_ = manager.Close()
			_ = owner.Close()
			t.Fatalf("OpenCollection(%s): %v", meta.Name, err)
		}
		return owner, backend, manager, col
	}
	return owner, backend, manager, nil
}

func makeBulkBSONBatch(t *testing.T, producer, batchIdx, batchSize int) ([][]byte, [][]byte) {
	t.Helper()
	ids := make([][]byte, batchSize)
	docs := make([][]byte, batchSize)
	base := producer*1_000_000 + batchIdx*batchSize
	for i := 0; i < batchSize; i++ {
		seq := base + i
		id := fmt.Sprintf("p%02d-b%02d-r%05d", producer, batchIdx, i)
		ids[i] = []byte(id)
		raw, err := bson.Marshal(bson.D{
			{Key: "_id", Value: id},
			{Key: "category", Value: fmt.Sprintf("cat-%02d", seq%32)},
			{Key: "score", Value: int64(seq)},
			{Key: "payload", Value: fmt.Sprintf("bulk-document-payload-%08d", seq)},
		})
		if err != nil {
			t.Fatalf("bson.Marshal: %v", err)
		}
		docs[i] = raw
	}
	return ids, docs
}

func bulkIndexedBSONMeta(name string, uniqueCategory bool) *CollectionMeta {
	return &CollectionMeta{
		Name: name,
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{
			{Name: "category", Field: "category", ValueType: IndexValueString, Unique: uniqueCategory},
			{Name: "score", Field: "score", ValueType: IndexValueInt64},
		},
	}
}

func TestNativeBulkInsertConcurrencyProd8Batch1000(t *testing.T) {
	profiles := []struct {
		name    string
		profile treedb.Profile
	}{
		{name: "command_wal_relaxed", profile: treedb.ProfileCommandWALRelaxed},
		{name: "no_wal_fast", profile: treedb.ProfileNoWALFast},
	}
	for _, tc := range profiles {
		t.Run(tc.name, func(t *testing.T) {
			for _, route := range []string{"collection_bson", "manager"} {
				t.Run(route, func(t *testing.T) {
					owner, _, manager, col := openNativeBulkInsertFixture(
						t,
						t.TempDir(),
						tc.profile,
						treedb.StandaloneCOWMemtableLimits(),
						bulkIndexedBSONMeta("items", false),
					)
					defer func() {
						if err := manager.Close(); err != nil {
							t.Errorf("manager.Close: %v", err)
						}
						if err := owner.Close(); err != nil {
							t.Errorf("owner.Close: %v", err)
						}
					}()

					const (
						producers = 8
						batches   = 2
						batchSize = 1000
					)
					type batchInput struct {
						ids  [][]byte
						docs [][]byte
					}
					inputs := make([][]batchInput, producers)
					for p := 0; p < producers; p++ {
						inputs[p] = make([]batchInput, batches)
						for b := 0; b < batches; b++ {
							ids, docs := makeBulkBSONBatch(t, p, b, batchSize)
							inputs[p][b] = batchInput{ids: ids, docs: docs}
						}
					}

					start := make(chan struct{})
					errs := make(chan error, producers*batches)
					var wg sync.WaitGroup
					for p := 0; p < producers; p++ {
						wg.Add(1)
						go func(producer int) {
							defer wg.Done()
							<-start
							for b := 0; b < batches; b++ {
								in := inputs[producer][b]
								var err error
								if route == "manager" {
									err = col.nativeJSON.InsertBatch("items", in.ids, in.docs)
								} else {
									_, err = col.InsertBatchValidatedBSON(in.ids, in.docs)
								}
								if err != nil {
									errs <- fmt.Errorf("producer=%d batch=%d: %w", producer, b, err)
									return
								}
							}
						}(p)
					}
					close(start)
					wg.Wait()
					close(errs)

					for err := range errs {
						t.Fatal(err)
					}

					// Spot-check sample documents from every producer/batch.
					for p := 0; p < producers; p++ {
						for b := 0; b < batches; b++ {
							in := inputs[p][b]
							for _, idx := range []int{0, batchSize / 2, batchSize - 1} {
								got, err := col.Get(in.ids[idx])
								if err != nil {
									t.Fatalf("Get(%q): %v", in.ids[idx], err)
								}
								if !bytes.Equal(got, in.docs[idx]) {
									t.Fatalf("Get(%q) mismatch", in.ids[idx])
								}
							}
						}
					}
				})
			}
		})
	}
}

func TestNativeBulkInsertConcurrencyProd8Batch10000(t *testing.T) {
	owner, _, manager, col := openNativeBulkInsertFixture(
		t,
		t.TempDir(),
		treedb.ProfileCommandWALRelaxed,
		treedb.StandaloneCOWMemtableLimits(),
		bulkIndexedBSONMeta("large_items", false),
	)
	defer func() {
		if err := manager.Close(); err != nil {
			t.Errorf("manager.Close: %v", err)
		}
		if err := owner.Close(); err != nil {
			t.Errorf("owner.Close: %v", err)
		}
	}()

	const (
		producers = 8
		batchSize = 10000
	)
	idsByProducer := make([][][]byte, producers)
	docsByProducer := make([][][]byte, producers)
	for p := 0; p < producers; p++ {
		idsByProducer[p], docsByProducer[p] = makeBulkBSONBatch(t, p, 0, batchSize)
	}

	start := make(chan struct{})
	errs := make(chan error, producers)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(producer int) {
			defer wg.Done()
			<-start
			if _, err := col.InsertBatchValidatedBSON(idsByProducer[producer], docsByProducer[producer]); err != nil {
				errs <- fmt.Errorf("producer=%d: %w", producer, err)
			}
		}(p)
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}

	for p := 0; p < producers; p++ {
		for _, idx := range []int{0, batchSize - 1} {
			got, err := col.Get(idsByProducer[p][idx])
			if err != nil {
				t.Fatalf("Get(%q): %v", idsByProducer[p][idx], err)
			}
			if !bytes.Equal(got, docsByProducer[p][idx]) {
				t.Fatalf("Get(%q) mismatch", idsByProducer[p][idx])
			}
		}
	}
}

func TestNativeBulkInsertConcurrencyConflicts(t *testing.T) {
	t.Run("duplicate_primary_id", func(t *testing.T) {
		owner, _, manager, col := openNativeBulkInsertFixture(
			t,
			t.TempDir(),
			treedb.ProfileCommandWALRelaxed,
			treedb.StandaloneCOWMemtableLimits(),
			bulkIndexedBSONMeta("dup_id", false),
		)
		defer func() {
			if err := manager.Close(); err != nil {
				t.Errorf("manager.Close: %v", err)
			}
			if err := owner.Close(); err != nil {
				t.Errorf("owner.Close: %v", err)
			}
		}()

		const (
			producers = 4
			batchSize = 128
		)
		sharedID := []byte("shared-conflict-id")
		idsByProducer := make([][][]byte, producers)
		docsByProducer := make([][][]byte, producers)
		for p := 0; p < producers; p++ {
			ids, docs := makeBulkBSONBatch(t, p, 0, batchSize)
			ids[batchSize/2] = sharedID
			raw, err := bson.Marshal(bson.D{
				{Key: "_id", Value: string(sharedID)},
				{Key: "category", Value: fmt.Sprintf("winner-%d", p)},
				{Key: "score", Value: int64(p)},
			})
			if err != nil {
				t.Fatal(err)
			}
			docs[batchSize/2] = raw
			idsByProducer[p] = ids
			docsByProducer[p] = docs
		}

		start := make(chan struct{})
		var succeeded atomic.Int32
		var dupErrors atomic.Int32
		var otherErrs []error
		var otherMu sync.Mutex
		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(producer int) {
				defer wg.Done()
				<-start
				_, err := col.InsertBatchValidatedBSON(idsByProducer[producer], docsByProducer[producer])
				if err == nil {
					succeeded.Add(1)
					return
				}
				if errors.Is(err, ErrDocumentExists) || errors.Is(err, backenddb.ErrConcurrentModification) {
					dupErrors.Add(1)
					return
				}
				otherMu.Lock()
				otherErrs = append(otherErrs, fmt.Errorf("producer=%d unexpected err: %w", producer, err))
				otherMu.Unlock()
			}(p)
		}
		close(start)
		wg.Wait()

		if len(otherErrs) != 0 {
			t.Fatalf("unexpected conflict errors: %v", otherErrs)
		}
		if succeeded.Load() != 1 || dupErrors.Load() != producers-1 {
			t.Fatalf("duplicate primary ID conflict: succeeded=%d dupErrors=%d want 1/%d", succeeded.Load(), dupErrors.Load(), producers-1)
		}
		got, err := col.Get(sharedID)
		if err != nil || len(got) == 0 {
			t.Fatalf("winning document missing: len=%d err=%v", len(got), err)
		}
	})

	t.Run("unique_secondary_index_conflict", func(t *testing.T) {
		owner, _, manager, col := openNativeBulkInsertFixture(
			t,
			t.TempDir(),
			treedb.ProfileCommandWALRelaxed,
			treedb.StandaloneCOWMemtableLimits(),
			bulkIndexedBSONMeta("uniq_idx", true),
		)
		defer func() {
			if err := manager.Close(); err != nil {
				t.Errorf("manager.Close: %v", err)
			}
			if err := owner.Close(); err != nil {
				t.Errorf("owner.Close: %v", err)
			}
		}()

		const (
			producers = 4
			batchSize = 64
		)
		idsByProducer := make([][][]byte, producers)
		docsByProducer := make([][][]byte, producers)
		for p := 0; p < producers; p++ {
			ids := make([][]byte, batchSize)
			docs := make([][]byte, batchSize)
			for i := 0; i < batchSize; i++ {
				id := fmt.Sprintf("u-p%d-r%03d", p, i)
				ids[i] = []byte(id)
				cat := fmt.Sprintf("unique-p%d-r%03d", p, i)
				if i == batchSize/2 {
					cat = "shared-unique-category"
				}
				raw, err := bson.Marshal(bson.D{
					{Key: "_id", Value: id},
					{Key: "category", Value: cat},
					{Key: "score", Value: int64(p*1000 + i)},
				})
				if err != nil {
					t.Fatal(err)
				}
				docs[i] = raw
			}
			idsByProducer[p] = ids
			docsByProducer[p] = docs
		}

		start := make(chan struct{})
		var succeeded atomic.Int32
		var conflictErrors atomic.Int32
		var otherErrs []error
		var otherMu sync.Mutex
		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(producer int) {
				defer wg.Done()
				<-start
				_, err := col.InsertBatchValidatedBSON(idsByProducer[producer], docsByProducer[producer])
				if err == nil {
					succeeded.Add(1)
					return
				}
				if errors.Is(err, ErrUniqueIndexConflict) || errors.Is(err, backenddb.ErrConcurrentModification) {
					conflictErrors.Add(1)
					return
				}
				otherMu.Lock()
				otherErrs = append(otherErrs, fmt.Errorf("producer=%d unexpected err: %w", producer, err))
				otherMu.Unlock()
			}(p)
		}
		close(start)
		wg.Wait()

		if len(otherErrs) != 0 {
			t.Fatalf("unexpected unique index errors: %v", otherErrs)
		}
		if succeeded.Load() != 1 || conflictErrors.Load() != producers-1 {
			t.Fatalf("unique index conflict: succeeded=%d conflictErrors=%d want 1/%d", succeeded.Load(), conflictErrors.Load(), producers-1)
		}
		matched, err := col.FindByIndex("category", "shared-unique-category")
		if err != nil {
			t.Fatalf("FindByIndex: %v", err)
		}
		if len(matched) != 1 {
			t.Fatalf("expected exactly 1 winner for unique index, got %d", len(matched))
		}
	})
}

func TestNativeBulkInsertExplicitTxnStrictOCC(t *testing.T) {
	owner, backend, manager, col := openNativeBulkInsertFixture(
		t,
		t.TempDir(),
		treedb.ProfileCommandWALRelaxed,
		treedb.StandaloneCOWMemtableLimits(),
		bulkIndexedBSONMeta("strict_occ", false),
	)
	defer func() {
		if err := manager.Close(); err != nil {
			t.Errorf("manager.Close: %v", err)
		}
		if err := owner.Close(); err != nil {
			t.Errorf("owner.Close: %v", err)
		}
	}()

	tx, err := col.nativeJSON.Begin("strict_occ")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() {
		if err := tx.Close(); err != nil {
			t.Errorf("tx.Close: %v", err)
		}
	}()

	conflictID := []byte("occ-conflict-id")
	doc1, err := bson.Marshal(bson.D{
		{Key: "_id", Value: string(conflictID)},
		{Key: "category", Value: "tx-cat"},
		{Key: "score", Value: int64(1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.InsertBatch([][]byte{conflictID}, [][]byte{doc1}); err != nil {
		t.Fatalf("tx.InsertBatch: %v", err)
	}

	// Commit a competing write to the same ID before explicit tx.Commit().
	doc2, err := bson.Marshal(bson.D{
		{Key: "_id", Value: string(conflictID)},
		{Key: "category", Value: "winner-cat"},
		{Key: "score", Value: int64(2)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatchValidatedBSON([][]byte{conflictID}, [][]byte{doc2}); err != nil {
		t.Fatalf("competing InsertBatchValidatedBSON: %v", err)
	}

	lsnBefore := backend.CommandWALNextLSN()
	err = tx.Commit()
	if !errors.Is(err, backenddb.ErrConcurrentModification) {
		t.Fatalf("explicit tx.Commit err=%v want ErrConcurrentModification", err)
	}
	if backend.CommandWALNextLSN() != lsnBefore {
		t.Fatalf("conflicted explicit tx assigned WAL LSN: before=%d after=%d", lsnBefore, backend.CommandWALNextLSN())
	}
	got, err := col.Get(conflictID)
	if err != nil || !bytes.Equal(got, doc2) {
		t.Fatalf("winner doc overwritten or missing: err=%v", err)
	}
}

func TestNativeBulkInsertDurabilityReopenMatrix(t *testing.T) {
	profiles := []struct {
		name    string
		profile treedb.Profile
	}{
		{name: "command_wal_relaxed", profile: treedb.ProfileCommandWALRelaxed},
		{name: "command_wal_durable", profile: treedb.ProfileCommandWALDurable},
		{name: "no_wal_fast", profile: treedb.ProfileNoWALFast},
	}
	for _, tc := range profiles {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			owner, _, manager, col := openNativeBulkInsertFixture(
				t,
				dir,
				tc.profile,
				treedb.StandaloneCOWMemtableLimits(),
				bulkIndexedBSONMeta("durable_items", false),
			)
			ownerOpen := true
			managerOpen := true
			defer func() {
				if managerOpen {
					if err := manager.Close(); err != nil {
						t.Errorf("manager.Close: %v", err)
					}
				}
				if ownerOpen {
					if err := owner.Close(); err != nil {
						t.Errorf("owner.Close: %v", err)
					}
				}
			}()

			const (
				producers = 8
				batchSize = 1000
			)
			idsByProducer := make([][][]byte, producers)
			docsByProducer := make([][][]byte, producers)
			for p := 0; p < producers; p++ {
				idsByProducer[p], docsByProducer[p] = makeBulkBSONBatch(t, p, 0, batchSize)
			}

			start := make(chan struct{})
			errs := make(chan error, producers)
			var wg sync.WaitGroup
			for p := 0; p < producers; p++ {
				wg.Add(1)
				go func(producer int) {
					defer wg.Done()
					<-start
					if _, err := col.InsertBatchValidatedBSON(idsByProducer[producer], docsByProducer[producer]); err != nil {
						errs <- fmt.Errorf("producer=%d: %w", producer, err)
					}
				}(p)
			}
			close(start)
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}

			verifyReadBack := func(phase string, c *Collection) {
				t.Helper()
				// 1. Primary Get check across all producers.
				for p := 0; p < producers; p++ {
					for _, idx := range []int{0, 17, batchSize / 2, batchSize - 1} {
						got, err := c.Get(idsByProducer[p][idx])
						if err != nil {
							t.Fatalf("%s Get(%q): %v", phase, idsByProducer[p][idx], err)
						}
						if !bytes.Equal(got, docsByProducer[p][idx]) {
							t.Fatalf("%s Get(%q) content mismatch", phase, idsByProducer[p][idx])
						}
					}
				}
				// 2. FindByIndex exact lookup across categories.
				totalCatHits := 0
				for catIdx := 0; catIdx < 32; catIdx++ {
					ids, err := c.FindByIndex("category", fmt.Sprintf("cat-%02d", catIdx))
					if err != nil {
						t.Fatalf("%s FindByIndex(cat-%02d): %v", phase, catIdx, err)
					}
					totalCatHits += len(ids)
				}
				if totalCatHits != producers*batchSize {
					t.Fatalf("%s FindByIndex total=%d want=%d", phase, totalCatHits, producers*batchSize)
				}
				// 3. FindByIndexRange on score for one full producer slice.
				rangeIDs, truncated, err := c.FindByIndexRange("score", IndexRangeOptions{
					Lower: IndexRangeBound{Value: int64(2_000_000), Inclusive: true},
					Upper: IndexRangeBound{Value: int64(2_000_000 + batchSize - 1), Inclusive: true},
				})
				if err != nil || truncated {
					t.Fatalf("%s FindByIndexRange: truncated=%v err=%v", phase, truncated, err)
				}
				if len(rangeIDs) != batchSize {
					t.Fatalf("%s FindByIndexRange len=%d want=%d", phase, len(rangeIDs), batchSize)
				}
			}

			verifyReadBack("post-ack", col)

			if err := owner.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			verifyReadBack("post-checkpoint", col)

			managerOpen = false
			if err := manager.Close(); err != nil {
				t.Fatalf("manager.Close: %v", err)
			}
			ownerOpen = false
			if err := owner.Close(); err != nil {
				t.Fatalf("owner.Close: %v", err)
			}

			// Reopen fresh and verify full read-back.
			reopened, _, reopenedMgr, _ := openNativeBulkInsertFixture(
				t,
				dir,
				tc.profile,
				treedb.StandaloneCOWMemtableLimits(),
				nil,
			)
			defer func() {
				if err := reopenedMgr.Close(); err != nil {
					t.Errorf("reopenedMgr.Close: %v", err)
				}
				if err := reopened.Close(); err != nil {
					t.Errorf("reopened.Close: %v", err)
				}
			}()
			reopenedCol, err := reopenedMgr.OpenCollection("durable_items")
			if err != nil {
				t.Fatalf("reopened OpenCollection: %v", err)
			}
			verifyReadBack("post-reopen", reopenedCol)
		})
	}
}

func makeBulkBSONDocsSlice(tb testing.TB, count int) ([][]byte, [][]byte) {
	tb.Helper()
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("doc-%08d", i)
		ids[i] = []byte(id)
		raw, err := bson.Marshal(bson.D{
			{Key: "_id", Value: id},
			{Key: "category", Value: fmt.Sprintf("cat-%03d", i%128)},
			{Key: "score", Value: int64(i)},
			{Key: "payload", Value: fmt.Sprintf("bulk-benchmark-payload-value-%08d", i)},
		})
		if err != nil {
			tb.Fatalf("bson.Marshal: %v", err)
		}
		docs[i] = raw
	}
	return ids, docs
}

func BenchmarkNativeBulkInsertBSON(b *testing.B) {
	cases := []struct {
		name      string
		profile   treedb.Profile
		producers int
		batchSize int
		totalDocs int
	}{
		{name: "relaxed_p1_b1000_idx2", profile: treedb.ProfileCommandWALRelaxed, producers: 1, batchSize: 1000, totalDocs: 40000},
		{name: "nowal_p1_b1000_idx2", profile: treedb.ProfileNoWALFast, producers: 1, batchSize: 1000, totalDocs: 40000},
		{name: "relaxed_p8_b1000_idx2", profile: treedb.ProfileCommandWALRelaxed, producers: 8, batchSize: 1000, totalDocs: 40000},
		{name: "nowal_p8_b1000_idx2", profile: treedb.ProfileNoWALFast, producers: 8, batchSize: 1000, totalDocs: 40000},
		{name: "relaxed_p8_b10000_idx2", profile: treedb.ProfileCommandWALRelaxed, producers: 8, batchSize: 10000, totalDocs: 80000},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			ids, docs := makeBulkBSONDocsSlice(b, tc.totalDocs)
			b.ResetTimer()
			var totalInserted int64
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				opts := treedb.OptionsFor(tc.profile, b.TempDir())
				opts.MemtableMode = "cow_btree"
				opts.COWMemtableLimits = treedb.StandaloneCOWMemtableLimits()
				owner, err := treedb.Open(opts)
				if err != nil {
					b.Fatalf("treedb.Open: %v", err)
				}
				backend, err := owner.NativeSemanticBackend()
				if err != nil {
					_ = owner.Close()
					b.Fatalf("NativeSemanticBackend: %v", err)
				}
				manager := NewCollectionManager(backend)
				if _, err := manager.CreateCollection(bulkIndexedBSONMeta("bench_items", false)); err != nil {
					_ = manager.Close()
					_ = owner.Close()
					b.Fatalf("CreateCollection: %v", err)
				}
				col, err := manager.OpenCollection("bench_items")
				if err != nil {
					_ = manager.Close()
					_ = owner.Close()
					b.Fatalf("OpenCollection: %v", err)
				}

				b.StartTimer()
				if tc.producers == 1 {
					for off := 0; off < tc.totalDocs; off += tc.batchSize {
						end := min(off+tc.batchSize, tc.totalDocs)
						if _, err := col.InsertBatchValidatedBSON(ids[off:end], docs[off:end]); err != nil {
							b.StopTimer()
							_ = manager.Close()
							_ = owner.Close()
							b.Fatalf("InsertBatchValidatedBSON: %v", err)
						}
					}
				} else {
					perProd := tc.totalDocs / tc.producers
					var wg sync.WaitGroup
					errs := make(chan error, tc.producers)
					for p := 0; p < tc.producers; p++ {
						wg.Add(1)
						go func(prod int) {
							defer wg.Done()
							startIdx := prod * perProd
							endIdx := startIdx + perProd
							for off := startIdx; off < endIdx; off += tc.batchSize {
								end := min(off+tc.batchSize, endIdx)
								if _, err := col.InsertBatchValidatedBSON(ids[off:end], docs[off:end]); err != nil {
									errs <- err
									return
								}
							}
						}(p)
					}
					wg.Wait()
					close(errs)
					for err := range errs {
						b.StopTimer()
						_ = manager.Close()
						_ = owner.Close()
						b.Fatalf("InsertBatchValidatedBSON: %v", err)
					}
				}
				b.StopTimer()
				totalInserted += int64(tc.totalDocs)
				if err := manager.Close(); err != nil {
					_ = owner.Close()
					b.Fatalf("manager.Close: %v", err)
				}
				if err := owner.Close(); err != nil {
					b.Fatalf("owner.Close: %v", err)
				}
			}
			elapsed := b.Elapsed().Seconds()
			if elapsed > 0 {
				b.ReportMetric(float64(totalInserted)/elapsed, "docs/s")
			}
		})
	}
}

func TestNativeBulkInsertIndexedJSONDottedPathFallback(t *testing.T) {
	meta := &CollectionMeta{
		Name: "json_dotted_items",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		Indexes: []IndexDefinition{
			{Name: "meta_tag", Field: "meta.tag", ValueType: IndexValueString},
		},
	}
	owner, backend, manager, col := openNativeBulkInsertFixture(
		t,
		t.TempDir(),
		treedb.ProfileCommandWALRelaxed,
		treedb.StandaloneCOWMemtableLimits(),
		meta,
	)
	defer func() {
		if err := manager.Close(); err != nil {
			t.Errorf("manager.Close: %v", err)
		}
		if err := owner.Close(); err != nil {
			t.Errorf("owner.Close: %v", err)
		}
	}()

	if nativeBulkInsertEligibleMeta(backend, *meta) {
		t.Fatal("indexed JSON collection unexpectedly eligible for preimage bulk fast path")
	}

	const (
		producers = 4
		batchSize = 64
	)
	idsByProd := make([][][]byte, producers)
	docsByProd := make([][][]byte, producers)
	for p := 0; p < producers; p++ {
		ids := make([][]byte, batchSize)
		docs := make([][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			id := fmt.Sprintf("jp%02d-r%04d", p, i)
			ids[i] = []byte(id)
			docs[i] = []byte(fmt.Sprintf(`{"_id":%q,"meta":{"tag":"tag-%02d"}}`, id, p))
		}
		idsByProd[p] = ids
		docsByProd[p] = docs
	}

	start := make(chan struct{})
	errs := make(chan error, producers)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(prod int) {
			defer wg.Done()
			<-start
			if err := col.nativeJSON.InsertBatch(meta.Name, idsByProd[prod], docsByProd[prod]); err != nil {
				errs <- fmt.Errorf("producer=%d: %w", prod, err)
			}
		}(p)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// Verify secondary dotted-path index entries were populated cleanly.
	read, err := col.nativeJSON.Capture(meta.Name)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	defer func() {
		if err := read.Close(); err != nil {
			t.Errorf("read.Close: %v", err)
		}
	}()
	for p := 0; p < producers; p++ {
		count := 0
		if err := read.visitIndexValueIDs("meta_tag", fmt.Sprintf("tag-%02d", p), func([]byte) error {
			count++
			return nil
		}); err != nil {
			t.Fatalf("visitIndexValueIDs(tag-%02d): %v", p, err)
		}
		if count != batchSize {
			t.Fatalf("visitIndexValueIDs(tag-%02d) count=%d, want %d", p, count, batchSize)
		}
	}
}
