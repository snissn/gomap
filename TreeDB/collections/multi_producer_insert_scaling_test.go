package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestMultiProducerInsertBatchContendedScalingAndUniqueConflicts(t *testing.T) {
	dir := t.TempDir()
	openOpts := func() backenddb.Options {
		return backenddb.Options{
			Dir:        dir,
			Durability: backenddb.DurabilityWALOnRelaxed,
			CommandWAL: true,
		}
	}

	db, err := backenddb.Open(openOpts())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "age_idx", Field: "age", ValueType: IndexValueInt64},
			{Name: "email_idx", Field: "email", ValueType: IndexValueString, Unique: true},
		},
	}); err != nil {
		_ = db.Close()
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = db.Close()
		t.Fatalf("open collection: %v", err)
	}

	const (
		producers        = 8
		batchesPerWorker = 3
		batchSize        = 400 // >= contendedBufferedInsertFrozenRunMinDocuments (256), <= 1024
	)

	var wg sync.WaitGroup
	errs := make(chan error, producers)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for b := 0; b < batchesPerWorker; b++ {
				ids := make([][]byte, batchSize)
				docs := make([][]byte, batchSize)
				base := (worker*batchesPerWorker + b) * batchSize
				for i := 0; i < batchSize; i++ {
					n := base + i
					ids[i] = []byte(fmt.Sprintf("doc-%06d", n))
					docs[i] = []byte(fmt.Sprintf(`{"age":%d,"email":"u-%06d@example.com"}`, n%50, n))
				}
				if _, err := col.InsertBatch(ids, docs); err != nil {
					errs <- fmt.Errorf("worker=%d batch=%d: %w", worker, b, err)
					return
				}
			}
		}(p)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		_ = db.Close()
		t.Fatalf("concurrent insert failed: %v", err)
	}

	// Concurrent unique-index conflict race: 8 workers race to insert the same
	// unique email with distinct primary IDs; exactly 1 must succeed and 7 must
	// fail with ErrUniqueIndexConflict.
	var uniqueSuccesses atomic.Int32
	var uniqueConflicts atomic.Int32
	raceErrs := make(chan error, producers)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			ids := make([][]byte, 300)
			docs := make([][]byte, 300)
			for i := 0; i < len(ids); i++ {
				ids[i] = []byte(fmt.Sprintf("race-w%d-%04d", worker, i))
				email := fmt.Sprintf("race-w%d-%04d@example.com", worker, i)
				if i == 150 {
					email = "shared-unique@example.com"
				}
				docs[i] = []byte(fmt.Sprintf(`{"age":42,"email":%q}`, email))
			}
			_, err := col.InsertBatch(ids, docs)
			if err == nil {
				uniqueSuccesses.Add(1)
				return
			}
			if errors.Is(err, ErrUniqueIndexConflict) {
				uniqueConflicts.Add(1)
				return
			}
			raceErrs <- fmt.Errorf("worker=%d unexpected err: %w", worker, err)
		}(p)
	}
	wg.Wait()
	close(raceErrs)
	for err := range raceErrs {
		_ = db.Close()
		t.Fatalf("unique race error: %v", err)
	}
	if uniqueSuccesses.Load() != 1 || uniqueConflicts.Load() != producers-1 {
		_ = db.Close()
		t.Fatalf("unique race successes=%d conflicts=%d want 1/%d", uniqueSuccesses.Load(), uniqueConflicts.Load(), producers-1)
	}

	// Duplicate primary ID against buffered/persisted state must also fail cleanly.
	if _, err := col.InsertBatch(
		[][]byte{[]byte("doc-000042"), []byte("doc-fresh-999999")},
		[][]byte{[]byte(`{"age":1,"email":"dup-id@example.com"}`), []byte(`{"age":1,"email":"fresh-id@example.com"}`)},
	); !errors.Is(err, ErrDocumentExists) {
		_ = db.Close()
		t.Fatalf("expected ErrDocumentExists, got %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// Reopen and verify durability + index integrity.
	db, err = backenddb.Open(openOpts())
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = db.Close() }()
	mgr = NewCollectionManager(db)
	col, err = mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("reopen collection: %v", err)
	}
	totalDocs := producers * batchesPerWorker * batchSize
	for _, probe := range []int{0, 1, batchSize - 1, totalDocs / 2, totalDocs - 1} {
		id := []byte(fmt.Sprintf("doc-%06d", probe))
		want := []byte(fmt.Sprintf(`{"age":%d,"email":"u-%06d@example.com"}`, probe%50, probe))
		got, err := col.Get(id)
		if err != nil {
			t.Fatalf("reopen get %q: %v", id, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("reopen get %q = %q want %q", id, got, want)
		}
	}
	matches, err := col.FindByIndex("email_idx", "shared-unique@example.com")
	if err != nil {
		t.Fatalf("reopen find shared unique email: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("shared unique email matches=%d want 1", len(matches))
	}
}

func BenchmarkCollectionConcurrentInsertBatchScaling(b *testing.B) {
	for _, producers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("prod_%d_batch_1000_idx_2", producers), func(b *testing.B) {
			const batchSize = 1000
			dir := b.TempDir()
			db, err := backenddb.Open(backenddb.Options{
				Dir:        dir,
				Durability: backenddb.DurabilityWALOnRelaxed,
				CommandWAL: true,
			})
			if err != nil {
				b.Fatalf("open db: %v", err)
			}
			defer func() { _ = db.Close() }()
			mgr := NewCollectionManager(db)
			if _, err := mgr.CreateCollection(&CollectionMeta{
				Name: "users",
				Indexes: []IndexDefinition{
					{Name: "age_idx", Field: "age", ValueType: IndexValueInt64},
					{Name: "email_idx", Field: "email", ValueType: IndexValueString, Unique: true},
				},
			}); err != nil {
				b.Fatalf("create collection: %v", err)
			}
			col, err := mgr.OpenCollection("users")
			if err != nil {
				b.Fatalf("open collection: %v", err)
			}

			var seq atomic.Uint64
			batches := max(1, (b.N+batchSize-1)/batchSize)
			jobs := make(chan int, batches)
			for i := 0; i < batches; i++ {
				jobs <- i
			}
			close(jobs)

			b.ResetTimer()
			var wg sync.WaitGroup
			errs := make(chan error, producers)
			for p := 0; p < producers; p++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ids := make([][]byte, batchSize)
					docs := make([][]byte, batchSize)
					for range jobs {
						base := seq.Add(batchSize) - batchSize
						for i := 0; i < batchSize; i++ {
							n := base + uint64(i)
							ids[i] = []byte(fmt.Sprintf("d-%010d", n))
							docs[i] = []byte(fmt.Sprintf(`{"age":%d,"email":"u-%010d@example.com"}`, n%100, n))
						}
						if _, err := col.InsertBatch(ids, docs); err != nil {
							errs <- err
							return
						}
					}
				}()
			}
			wg.Wait()
			b.StopTimer()
			close(errs)
			for err := range errs {
				b.Fatalf("insert batch: %v", err)
			}
			b.ReportMetric(float64(batches*batchSize)/b.Elapsed().Seconds(), "docs/s")
		})
	}
}
