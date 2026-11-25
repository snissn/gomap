package mongogateway

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func parseStandaloneStatUint64(t *testing.T, s *StandaloneServer, key string) uint64 {
	t.Helper()
	raw := s.Stats()[key]
	if raw == "" {
		t.Fatalf("missing standalone stat %q", key)
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse standalone stat %q=%q: %v", key, raw, err)
	}
	return v
}

func createBulkCapacityTestIndexes(t *testing.T, s *Server, dbName, colName string) {
	t.Helper()
	resp := serveCommand(t, s, 1, bson.D{
		{Key: "createIndexes", Value: colName},
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "age", Value: int32(1)}}}, {Key: "name", Value: "age_1"}, {Key: "treedbValueType", Value: "int64"}},
			bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "treedbValueType", Value: "string"}, {Key: "unique", Value: true}},
			bson.D{{Key: "key", Value: bson.D{{Key: "city", Value: int32(1)}}}, {Key: "name", Value: "city_1"}, {Key: "treedbValueType", Value: "string"}},
		}},
		{Key: "$db", Value: dbName},
	})
	assertOK(t, resp)
}

func buildStandaloneBulkInsertDocs(producer, count, padBytes int) bson.A {
	cities := []string{"austin", "boston", "chicago", "denver", "seattle", "miami", "portland", "atlanta"}
	pad := string(bytes.Repeat([]byte("x"), padBytes))
	docs := make(bson.A, count)
	for i := 0; i < count; i++ {
		docs[i] = bson.D{
			{Key: "_id", Value: fmt.Sprintf("p%02d-doc-%07d", producer, i)},
			{Key: "age", Value: int64(18 + ((producer*count + i) % 70))},
			{Key: "email", Value: fmt.Sprintf("user-%02d-%07d@example.com", producer, i)},
			{Key: "city", Value: cities[(producer+i)%len(cities)]},
			{Key: "note", Value: pad},
		}
	}
	return docs
}

func TestStandaloneBulkInsertConcurrentBatch10000NoSingleDocFallback(t *testing.T) {
	const (
		producers = 8
		batchSize = 10000
	)
	standalone, err := OpenStandaloneServer(StandaloneOptions{
		Dir:     t.TempDir(),
		Profile: treedb.ProfileCommandWALRelaxed,
	})
	if err != nil {
		t.Fatalf("OpenStandaloneServer: %v", err)
	}
	defer func() {
		if err := standalone.Close(); err != nil {
			t.Errorf("close standalone: %v", err)
		}
	}()
	createBulkCapacityTestIndexes(t, standalone.Server, "app", "users")

	cmds := make([]bson.D, producers)
	for p := 0; p < producers; p++ {
		cmds[p] = bson.D{
			{Key: "insert", Value: "users"},
			{Key: "documents", Value: buildStandaloneBulkInsertDocs(p, batchSize, 128)},
			{Key: "$db", Value: "app"},
		}
	}

	pubBefore := parseStandaloneStatUint64(t, standalone, "treedb.cache.cow.publications_total")
	start := make(chan struct{})
	responses := make([]bson.Raw, producers)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			<-start
			resp := serveCommand(t, standalone.Server, int32(10+pid), cmds[pid])
			responses[pid] = bson.Raw(resp)
		}(p)
	}
	close(start)
	wg.Wait()

	for p, resp := range responses {
		assertOK(t, resp)
		assertInt32(t, resp, "n", batchSize)
		if !resp.Lookup("writeErrors").IsZero() {
			t.Fatalf("producer %d returned unexpected writeErrors: %s", p, resp)
		}
	}

	pubAfter := parseStandaloneStatUint64(t, standalone, "treedb.cache.cow.publications_total")
	if delta := pubAfter - pubBefore; delta > 25 {
		t.Fatalf("p=%d batch=%d fell back to single-doc inserts: publications_total delta=%d (want <= 25)", producers, batchSize, delta)
	}
}

func tightBulkInsertCOWLimits() treedb.COWMemtableLimits {
	limits := treedb.DefaultCOWMemtableLimits()
	limits.MaxInFlightBytes = 16 << 20
	limits.MaxTotalBytes = 32 << 20
	limits.MaxRetiredBytes = 32 << 20
	limits.MaxGenerationBytes = 4 << 20
	return limits
}

func TestStandaloneBulkInsertTightLimitsChunksSubBatches(t *testing.T) {
	const totalDocs = 2000
	standalone, err := OpenStandaloneServer(StandaloneOptions{
		Dir:               t.TempDir(),
		Profile:           treedb.ProfileCommandWALRelaxed,
		COWMemtableLimits: tightBulkInsertCOWLimits(),
	})
	if err != nil {
		t.Fatalf("OpenStandaloneServer: %v", err)
	}
	defer func() {
		if err := standalone.Close(); err != nil {
			t.Errorf("close standalone: %v", err)
		}
	}()
	createBulkCapacityTestIndexes(t, standalone.Server, "app", "chunked")

	pubBefore := parseStandaloneStatUint64(t, standalone, "treedb.cache.cow.publications_total")
	resp := serveCommand(t, standalone.Server, 2, bson.D{
		{Key: "insert", Value: "chunked"},
		{Key: "documents", Value: buildStandaloneBulkInsertDocs(0, totalDocs, 2048)},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(1)}, {Key: "j", Value: true}}},
		{Key: "$db", Value: "app"},
	})
	assertOK(t, resp)
	assertInt32(t, resp, "n", totalDocs)
	if !bson.Raw(resp).Lookup("writeErrors").IsZero() {
		t.Fatalf("unexpected writeErrors: %s", resp)
	}

	pubAfter := parseStandaloneStatUint64(t, standalone, "treedb.cache.cow.publications_total")
	delta := pubAfter - pubBefore
	if delta <= 1 {
		t.Fatalf("test fixture did not trigger sub-batch chunking: publications_total delta=%d (want > 1)", delta)
	}
	if delta > 64 {
		t.Fatalf("tight limits fell back to single-doc inserts instead of chunked sub-batches: publications_total delta=%d (want <= 64 for %d docs)", delta, totalDocs)
	}
}

func TestStandaloneBulkInsertChunkedOrderedAndUnorderedErrorOffsets(t *testing.T) {
	const (
		totalDocs  = 800
		dupIDIndex = 510
		dupEmIndex = 710
	)
	tightErrLimits := treedb.DefaultCOWMemtableLimits()
	tightErrLimits.MaxInFlightBytes = 8 << 20
	tightErrLimits.MaxTotalBytes = 16 << 20
	tightErrLimits.MaxRetiredBytes = 16 << 20
	tightErrLimits.MaxGenerationBytes = 512 << 10
	for _, ordered := range []bool{true, false} {
		name := "unordered"
		if ordered {
			name = "ordered"
		}
		t.Run(name, func(t *testing.T) {
			standalone, err := OpenStandaloneServer(StandaloneOptions{
				Dir:               t.TempDir(),
				Profile:           treedb.ProfileNoWALFast,
				COWMemtableLimits: tightErrLimits,
			})
			if err != nil {
				t.Fatalf("OpenStandaloneServer: %v", err)
			}
			defer func() {
				if err := standalone.Close(); err != nil {
					t.Errorf("close standalone: %v", err)
				}
			}()
			createBulkCapacityTestIndexes(t, standalone.Server, "app", "chunked_err")

			// Seed two conflicting docs so index 750 hits duplicate _id and index 1650 hits duplicate unique email.
			seed := serveCommand(t, standalone.Server, 2, bson.D{
				{Key: "insert", Value: "chunked_err"},
				{Key: "documents", Value: bson.A{
					bson.D{
						{Key: "_id", Value: fmt.Sprintf("p00-doc-%07d", dupIDIndex)},
						{Key: "age", Value: int64(99)},
						{Key: "email", Value: "seed-dup-id@example.com"},
						{Key: "city", Value: "seed"},
					},
					bson.D{
						{Key: "_id", Value: "seed-email-owner"},
						{Key: "age", Value: int64(99)},
						{Key: "email", Value: fmt.Sprintf("user-00-%07d@example.com", dupEmIndex)},
						{Key: "city", Value: "seed"},
					},
				}},
				{Key: "$db", Value: "app"},
			})
			assertOK(t, seed)
			assertInt32(t, seed, "n", 2)

			pubBefore := parseStandaloneStatUint64(t, standalone, "treedb.cache.cow.publications_total")
			resp := serveCommand(t, standalone.Server, 3, bson.D{
				{Key: "insert", Value: "chunked_err"},
				{Key: "ordered", Value: ordered},
				{Key: "documents", Value: buildStandaloneBulkInsertDocs(0, totalDocs, 3072)},
				{Key: "$db", Value: "app"},
			})
			assertOK(t, resp)

			pubAfter := parseStandaloneStatUint64(t, standalone, "treedb.cache.cow.publications_total")
			delta := pubAfter - pubBefore

			errsVal, ok := bson.Raw(resp).Lookup("writeErrors").ArrayOK()
			if !ok {
				t.Fatalf("expected writeErrors array, got %s", resp)
			}
			vals, err := errsVal.Values()
			if err != nil {
				t.Fatalf("writeErrors values: %v", err)
			}

			if ordered {
				assertInt32(t, resp, "n", dupIDIndex)
				if len(vals) != 1 {
					t.Fatalf("ordered writeErrors len=%d want 1: %s", len(vals), resp)
				}
				idx, _ := vals[0].Document().Lookup("index").Int32OK()
				code, _ := vals[0].Document().Lookup("code").Int32OK()
				if idx != dupIDIndex || code != commandCodeDuplicateKey {
					t.Fatalf("ordered writeErrors[0] index=%d code=%d want index=%d code=%d", idx, code, dupIDIndex, commandCodeDuplicateKey)
				}
				// Doc before failure exists; doc after failure does not.
				findBefore := serveCommand(t, standalone.Server, 4, bson.D{{Key: "find", Value: "chunked_err"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: fmt.Sprintf("p00-doc-%07d", dupIDIndex-1)}}}, {Key: "$db", Value: "app"}})
				if len(cursorFirstBatch(t, findBefore)) != 1 {
					t.Fatalf("ordered doc %d missing", dupIDIndex-1)
				}
				findAfter := serveCommand(t, standalone.Server, 5, bson.D{{Key: "find", Value: "chunked_err"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: fmt.Sprintf("p00-doc-%07d", dupIDIndex+1)}}}, {Key: "$db", Value: "app"}})
				if len(cursorFirstBatch(t, findAfter)) != 0 {
					t.Fatalf("ordered doc %d should not have been inserted", dupIDIndex+1)
				}
				if delta >= dupIDIndex {
					t.Fatalf("ordered chunked insert fell back to single-doc commits for all preceding docs: pubDelta=%d", delta)
				}
			} else {
				assertInt32(t, resp, "n", totalDocs-2)
				if len(vals) != 2 {
					t.Fatalf("unordered writeErrors len=%d want 2: %s", len(vals), resp)
				}
				idx0, _ := vals[0].Document().Lookup("index").Int32OK()
				code0, _ := vals[0].Document().Lookup("code").Int32OK()
				idx1, _ := vals[1].Document().Lookup("index").Int32OK()
				code1, _ := vals[1].Document().Lookup("code").Int32OK()
				if idx0 != dupIDIndex || code0 != commandCodeDuplicateKey || idx1 != dupEmIndex || code1 != commandCodeDuplicateKey {
					t.Fatalf("unordered writeErrors got (%d,%d),(%d,%d) want (%d,%d),(%d,%d)",
						idx0, code0, idx1, code1, dupIDIndex, commandCodeDuplicateKey, dupEmIndex, commandCodeDuplicateKey)
				}
				for _, presentIdx := range []int{0, dupIDIndex - 1, dupIDIndex + 1, dupEmIndex - 1, dupEmIndex + 1, totalDocs - 1} {
					findDoc := serveCommand(t, standalone.Server, 6, bson.D{{Key: "find", Value: "chunked_err"}, {Key: "filter", Value: bson.D{{Key: "_id", Value: fmt.Sprintf("p00-doc-%07d", presentIdx)}}}, {Key: "$db", Value: "app"}})
					if len(cursorFirstBatch(t, findDoc)) != 1 {
						t.Fatalf("unordered doc %d missing", presentIdx)
					}
				}
				// Non-conflicting chunks must stay batched (far below totalDocs single-doc commits).
				if delta >= totalDocs/2 {
					t.Fatalf("unordered chunked insert fell back to single-doc commits across clean chunks: pubDelta=%d (totalDocs=%d)", delta, totalDocs)
				}
			}
		})
	}
}

func prepareStandaloneBSONBatch(t *testing.T, count int) ([][]byte, [][]byte) {
	t.Helper()
	rawDocs := buildStandaloneBulkInsertDocs(0, count, 64)
	wireDocs := make([]wire.Document, count)
	for i, doc := range rawDocs {
		encoded, err := bson.Marshal(doc)
		if err != nil {
			t.Fatalf("marshal doc %d: %v", i, err)
		}
		wireDocs[i] = wire.Document(encoded)
	}
	ids, stored, err := prepareInsertDocuments(wireDocs, collections.DocumentFormatBSON)
	if err != nil {
		t.Fatalf("prepareInsertDocuments: %v", err)
	}
	return ids, stored
}

func TestStandaloneBulkInsertChunkedCommitAmbiguousAborts(t *testing.T) {
	for _, ordered := range []bool{true, false} {
		name := "unordered"
		if ordered {
			name = "ordered"
		}
		t.Run(name, func(t *testing.T) {
			standalone, err := OpenStandaloneServer(StandaloneOptions{
				Dir:     t.TempDir(),
				Profile: treedb.ProfileCommandWALRelaxed,
			})
			if err != nil {
				t.Fatalf("OpenStandaloneServer: %v", err)
			}
			defer standalone.Close()
			createBulkCapacityTestIndexes(t, standalone.Server, "app", "ambiguous")
			col, err := standalone.Collections.OpenCollection("app.ambiguous")
			if err != nil {
				t.Fatalf("open collection: %v", err)
			}

			ids, stored := prepareStandaloneBSONBatch(t, 400)
			budget := standalone.Server.newMongoWriteBudget()
			calls := 0
			var seenLens []int
			budget.insertBatchValidatedBSONHook = func(batchIDs, batchStored [][]byte) error {
				calls++
				seenLens = append(seenLens, len(batchIDs))
				switch calls {
				case 1:
					// Full 400-doc batch returns wrapped pre-publication ErrCOWCapacity.
					return fmt.Errorf("%w: %w", collections.ErrMutationNotAccepted, memtable.ErrCOWCapacity)
				case 2:
					// First 200-doc chunk also returns ErrCOWCapacity to halve to 100.
					return fmt.Errorf("%w: %w", collections.ErrMutationNotAccepted, memtable.ErrCOWCapacity)
				case 3:
					// First 100-doc chunk [0:100] succeeds.
					_, err := col.InsertBatchValidatedBSON(batchIDs, batchStored)
					return err
				case 4:
					// Second 100-doc chunk [100:200] returns ErrCommitAmbiguous.
					return fmt.Errorf("ambiguous test failure: %w", collections.ErrCommitAmbiguous)
				default:
					t.Errorf("unexpected extra chunk call #%d (len=%d) after ErrCommitAmbiguous", calls, len(batchIDs))
					return nil
				}
			}

			resp, err := standalone.Server.runMongoInsertCommandWithJournal("app.ambiguous", col, collections.DocumentFormatBSON, ids, stored, ordered, budget, nil)
			if err != nil {
				t.Fatalf("runMongoInsertCommandWithJournal returned transport err: %v", err)
			}
			assertCommandError(t, resp, "ShutdownInProgress")
			if calls != 4 {
				t.Fatalf("calls=%d (lens=%v) want 4 (must not retry ambiguous chunk or continue to chunks 3/4)", calls, seenLens)
			}
		})
	}
}

func TestStandaloneBulkInsertChunkedBudgetExhaustionMidChunk(t *testing.T) {
	cases := []struct {
		name    string
		exhaust func(budget *mongoWriteBudget)
	}{
		{
			name: "deadline_expired",
			exhaust: func(budget *mongoWriteBudget) {
				budget.deadline = time.Now().Add(-time.Second)
			},
		},
		{
			name: "targets_exhausted_with_terminal_error_slot",
			exhaust: func(budget *mongoWriteBudget) {
				budget.targetsRemaining = 0
				budget.errorsRemaining = 0
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			standalone, err := OpenStandaloneServer(StandaloneOptions{
				Dir:     t.TempDir(),
				Profile: treedb.ProfileCommandWALRelaxed,
			})
			if err != nil {
				t.Fatalf("OpenStandaloneServer: %v", err)
			}
			defer standalone.Close()
			createBulkCapacityTestIndexes(t, standalone.Server, "app", "budget_stop")
			col, err := standalone.Collections.OpenCollection("app.budget_stop")
			if err != nil {
				t.Fatalf("open collection: %v", err)
			}

			ids, stored := prepareStandaloneBSONBatch(t, 400)
			budget := standalone.Server.newMongoWriteBudget()
			initialFailed := false
			budget.insertBatchValidatedBSONHook = func(batchIDs, batchStored [][]byte) error {
				if !initialFailed && len(batchIDs) == len(ids) {
					initialFailed = true
					return fmt.Errorf("%w: %w", collections.ErrMutationNotAccepted, memtable.ErrCOWCapacity)
				}
				_, err := col.InsertBatchValidatedBSON(batchIDs, batchStored)
				return err
			}
			budget.beforeInsertChunkHook = func(offset, chunkLen int) {
				if offset == 200 {
					tc.exhaust(budget)
				}
			}

			resp, err := standalone.Server.runMongoInsertCommandWithJournal("app.budget_stop", col, collections.DocumentFormatBSON, ids, stored, false, budget, nil)
			if err != nil {
				t.Fatalf("runMongoInsertCommandWithJournal err: %v", err)
			}
			assertOK(t, resp)
			assertInt32(t, resp, "n", 200)
			errsVal, ok := bson.Raw(resp).Lookup("writeErrors").ArrayOK()
			if !ok {
				t.Fatalf("expected writeErrors on budget exhaustion, got %s", resp)
			}
			vals, err := errsVal.Values()
			if err != nil || len(vals) != 1 {
				t.Fatalf("writeErrors len=%d err=%v resp=%s", len(vals), err, resp)
			}
			idx, idxOK := vals[0].Document().Lookup("index").Int32OK()
			if !idxOK || idx != 200 {
				t.Fatalf("writeErrors[0].index=%d ok=%v want 200", idx, idxOK)
			}
			// Verify first 200 committed and 200..399 did not.
			if got, err := col.Get(ids[199]); err != nil || len(got) == 0 {
				t.Fatalf("expected doc 199 committed, got len=%d err=%v", len(got), err)
			}
			if got, err := col.Get(ids[200]); err != nil || len(got) != 0 {
				t.Fatalf("expected doc 200 uncommitted, got len=%d err=%v", len(got), err)
			}
		})
	}
}
