package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func buildS19GatewayBSONDocs(tb testing.TB, count int) ([][]byte, [][]byte) {
	tb.Helper()
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	cities := []string{"austin", "boston", "chicago", "denver", "miami", "seattle", "newyork", "atlanta"}
	for i := 0; i < count; i++ {
		idStr := fmt.Sprintf("user-%06d", i)
		email := fmt.Sprintf("user-%06d@example.com", i)
		raw, err := bson.Marshal(bson.D{
			{Key: "_id", Value: idStr},
			{Key: "email", Value: email},
			{Key: "city", Value: cities[i%len(cities)]},
			{Key: "age", Value: int32(18 + (i % 65))},
			{Key: "active", Value: i%2 == 0},
			{Key: "score", Value: float64(i%1000) + 0.5},
			{Key: "bio", Value: "benchmark user profile payload with steady length"},
		})
		if err != nil {
			tb.Fatalf("marshal bson %d: %v", i, err)
		}
		k := make([]byte, 0, 2+4+len(idStr)+1)
		k = append(k, 0x01, byte(bson.TypeString))
		l := uint32(len(idStr) + 1)
		k = append(k, byte(l), byte(l>>8), byte(l>>16), byte(l>>24))
		k = append(k, idStr...)
		k = append(k, 0)
		ids[i] = k
		docs[i] = raw
	}
	return ids, docs
}

func s19IndexDefs(secondaryIndexes int) []IndexDefinition {
	defs := []IndexDefinition{
		{Name: "age_1", Field: "age", ValueType: IndexValueBSONOrderedV2},
	}
	if secondaryIndexes >= 1 {
		defs = append(defs, IndexDefinition{Name: "email_1", Field: "email", Unique: true, ValueType: IndexValueBSONOrderedV2})
	}
	if secondaryIndexes >= 2 {
		defs = append(defs, IndexDefinition{Name: "city_1", Field: "city", ValueType: IndexValueBSONOrderedV2})
	}
	return defs
}

func BenchmarkInsertMany100kBSONSettled(b *testing.B) {
	const (
		totalDocs = 100000
		batchSize = 10000
		producers = 8
	)
	ids, docs := buildS19GatewayBSONDocs(b, totalDocs)

	for _, idx := range []int{0, 1, 2} {
		b.Run(fmt.Sprintf("idx=%d", idx), func(b *testing.B) {
			b.ReportAllocs()
			var totalInsertNs, totalCheckpointNs int64
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				dir := b.TempDir()
				opts := treedb.OptionsForSemantic(treedb.ProfileCommandWALRelaxed, dir)
				opts.IndexOuterLeavesInValueLog = true
				opts.IndexInternalBaseDelta = false
				opts.FreshSystemPagerLeaves = true
				opts.FreshRelaxedStandaloneWALStream = true
				owner, err := treedb.Open(opts)
				if err != nil {
					b.Fatalf("open treedb: %v", err)
				}
				backend, err := owner.NativeSemanticBackend()
				if err != nil {
					_ = owner.Close()
					b.Fatalf("native semantic backend: %v", err)
				}
				mgr := NewCollectionManager(backend)
				meta := &CollectionMeta{
					Name:    "users",
					Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
					Indexes: s19IndexDefs(idx),
				}
				if _, err := mgr.CreateCollection(meta); err != nil {
					_ = mgr.Close()
					_ = owner.Close()
					b.Fatalf("create collection: %v", err)
				}
				col, err := mgr.OpenCollection("users")
				if err != nil {
					_ = mgr.Close()
					_ = owner.Close()
					b.Fatalf("open collection: %v", err)
				}
				b.StartTimer()

				t0 := time.Now()
				batchCh := make(chan int, totalDocs/batchSize)
				for batchIdx := 0; batchIdx < totalDocs/batchSize; batchIdx++ {
					batchCh <- batchIdx
				}
				close(batchCh)
				var wg sync.WaitGroup
				errCh := make(chan error, producers)
				for p := 0; p < producers; p++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for batchIdx := range batchCh {
							lo := batchIdx * batchSize
							hi := lo + batchSize
							if err := col.InsertBatchValidatedBSONNoResultIDs(ids[lo:hi], docs[lo:hi]); err != nil {
								errCh <- err
								return
							}
						}
					}()
				}
				wg.Wait()
				close(errCh)
				for err := range errCh {
					if err != nil {
						b.Fatalf("insert batch: %v", err)
					}
				}
				t1 := time.Now()
				if err := mgr.FlushAll(); err != nil {
					b.Fatalf("flush all: %v", err)
				}
				if err := owner.Checkpoint(); err != nil {
					b.Fatalf("checkpoint: %v", err)
				}
				t2 := time.Now()

				b.StopTimer()
				totalInsertNs += t1.Sub(t0).Nanoseconds()
				totalCheckpointNs += t2.Sub(t1).Nanoseconds()
				if err := mgr.Close(); err != nil {
					b.Fatalf("close manager: %v", err)
				}
				if err := owner.Close(); err != nil {
					b.Fatalf("close owner: %v", err)
				}
			}
			totalNs := totalInsertNs + totalCheckpointNs
			if totalNs > 0 {
				docsPerSec := float64(totalDocs*b.N) / (float64(totalNs) / 1e9)
				b.ReportMetric(docsPerSec, "docs/s")
			}
			b.ReportMetric(float64(totalInsertNs)/float64(b.N)/1e6, "insert_ms/op")
			b.ReportMetric(float64(totalCheckpointNs)/float64(b.N)/1e6, "checkpoint_ms/op")
		})
	}
}

func TestMSDRadixSortSecondaryRootEntriesMatchesSortFunc(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	cases := []struct {
		name string
		keys [][]byte
	}{
		{name: "empty", keys: nil},
		{name: "single", keys: [][]byte{[]byte("only")}},
		{name: "identical", keys: func() [][]byte {
			out := make([][]byte, 128)
			for i := range out {
				out[i] = []byte("same-prefix-and-suffix")
			}
			return out
		}()},
		{name: "shared_long_prefix_over_32_bytes", keys: func() [][]byte {
			prefix := bytes.Repeat([]byte("a"), 40)
			out := make([][]byte, 512)
			for i := range out {
				k := append([]byte(nil), prefix...)
				k = append(k, fmt.Sprintf("%06d", 512-i)...)
				out[i] = k
			}
			return out
		}()},
		{name: "varying_lengths_with_prefixes", keys: func() [][]byte {
			out := make([][]byte, 2000)
			for i := range out {
				n := rng.Intn(48)
				k := make([]byte, n)
				for j := range k {
					if j < 4 {
						k[j] = 'p'
					} else {
						k[j] = byte(rng.Intn(256))
					}
				}
				out[i] = k
			}
			return out
		}()},
		{name: "email_10k_secondary_keys", keys: func() [][]byte {
			out := make([][]byte, 10000)
			for i := range out {
				v := rng.Intn(1000000)
				out[i] = []byte(fmt.Sprintf("\x14user-%06d@example.com\x00user-%06d", v, i))
			}
			return out
		}()},
		{name: "age_10k_secondary_keys", keys: func() [][]byte {
			out := make([][]byte, 10000)
			for i := range out {
				var k [16]byte
				k[0] = 0x10
				binary.BigEndian.PutUint64(k[1:9], uint64(18+(i%65)))
				binary.BigEndian.PutUint64(k[8:16], uint64(10000-i))
				out[i] = append([]byte(nil), k[:]...)
			}
			return out
		}()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := make([]directBufferedSecondaryRootEntry, len(tc.keys))
			want := make([]directBufferedSecondaryRootEntry, len(tc.keys))
			for i, k := range tc.keys {
				got[i] = directBufferedSecondaryRootEntry{key: k}
				want[i] = directBufferedSecondaryRootEntry{key: k}
			}
			sortDirectBufferedSecondaryRootEntriesPureInsert(got)
			slices.SortFunc(want, func(a, b directBufferedSecondaryRootEntry) int {
				return bytes.Compare(a.key, b.key)
			})
			for i := range want {
				if !bytes.Equal(got[i].key, want[i].key) {
					t.Fatalf("index %d mismatch: got=%x want=%x", i, got[i].key, want[i].key)
				}
			}
		})
	}

	// Verify zero heap allocations on a 1024-key sort.
	sample := make([]directBufferedSecondaryRootEntry, 1024)
	base := make([]directBufferedSecondaryRootEntry, 1024)
	for i := range base {
		base[i] = directBufferedSecondaryRootEntry{key: []byte(fmt.Sprintf("\x14user-%06d@example.com", (i*7919)%1024))}
	}
	allocs := testing.AllocsPerRun(5, func() {
		copy(sample, base)
		sortDirectBufferedSecondaryRootEntriesPureInsert(sample)
	})
	if allocs != 0 {
		t.Fatalf("sortDirectBufferedSecondaryRootEntriesPureInsert allocs=%v want 0", allocs)
	}
}

func TestReadBSONElementFastAndIndexStateExtraction(t *testing.T) {
	dec128, err := bson.ParseDecimal128("123.45")
	if err != nil {
		t.Fatalf("ParseDecimal128: %v", err)
	}
	oid := bson.NewObjectID()
	raw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "doc-1"},
		{Key: "dbl", Value: 3.14},
		{Key: "str", Value: "hello"},
		{Key: "doc", Value: bson.D{{Key: "nested", Value: int32(7)}}},
		{Key: "arr", Value: bson.A{int32(1), int32(2)}},
		{Key: "bin", Value: bson.Binary{Subtype: 0, Data: []byte{1, 2, 3}}},
		{Key: "oid", Value: oid},
		{Key: "flag", Value: true},
		{Key: "dt", Value: bson.DateTime(1700000000000)},
		{Key: "nilv", Value: nil},
		{Key: "re", Value: bson.Regex{Pattern: "^abc", Options: "i"}},
		{Key: "i32", Value: int32(42)},
		{Key: "ts", Value: bson.Timestamp{T: 10, I: 2}},
		{Key: "i64", Value: int64(9000000000)},
		{Key: "dec", Value: dec128},
		{Key: "city", Value: "austin"},
		{Key: "age", Value: int32(31)},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	opts := collectionOptions{documentFormat: DocumentFormatBSON}
	for _, defs := range [][]IndexDefinition{
		{{Name: "age_1", Field: "age", ValueType: IndexValueBSONOrderedV2}},
		{
			{Name: "age_1", Field: "age", ValueType: IndexValueBSONOrderedV2},
			{Name: "city_1", Field: "city", ValueType: IndexValueBSONOrderedV2},
			{Name: "missing_1", Field: "missing", ValueType: IndexValueBSONOrderedV2},
			{Name: "oid_1", Field: "oid", ValueType: IndexValueBSONOrderedV2},
			{Name: "i64_1", Field: "i64", ValueType: IndexValueBSONOrderedV2},
			{Name: "flag_1", Field: "flag", ValueType: IndexValueBSONOrderedV2},
		},
	} {
		planner, err := nativeJSONInsertPlanner(CollectionMeta{
			Name:    "test_bson_fast",
			Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
			Indexes: defs,
		})
		if err != nil {
			t.Fatalf("nativeJSONInsertPlanner: %v", err)
		}
		runtimes, err := planner.indexRuntimes()
		if err != nil {
			t.Fatalf("indexRuntimes: %v", err)
		}
		var fastArena, slowArena indexEncodeArena
		gotState, err := bsonOrderedIndexStateForDocumentWithArena(raw, runtimes, opts, &fastArena)
		if err != nil {
			t.Fatalf("fast extraction: %v", err)
		}
		wantState := slowArena.appendState(len(runtimes))
		for i, rt := range runtimes {
			if err := appendBSONIndexRuntimeState(bson.Raw(raw), wantState, i, rt, opts, &slowArena); err != nil {
				t.Fatalf("reference extraction %s: %v", rt.def.name, err)
			}
		}
		for i := range runtimes {
			if len(gotState[i]) != len(wantState[i]) {
				t.Fatalf("index %s count=%d want %d", runtimes[i].def.name, len(gotState[i]), len(wantState[i]))
			}
			for j := range wantState[i] {
				if !bytes.Equal(gotState[i][j], wantState[i][j]) {
					t.Fatalf("index %s[%d]=%x want %x", runtimes[i].def.name, j, gotState[i][j], wantState[i][j])
				}
			}
		}
	}
}

func TestInsertBatchValidatedBSONNoResultIDsConflictAndParallelDomains(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsForSemantic(treedb.ProfileCommandWALRelaxed, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.IndexInternalBaseDelta = false
	opts.FreshSystemPagerLeaves = true
	opts.FreshRelaxedStandaloneWALStream = true
	opts.BackgroundCheckpointInterval = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open treedb: %v", err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		_ = owner.Close()
		t.Fatalf("native semantic backend: %v", err)
	}
	mgr := NewCollectionManager(backend)
	meta := &CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
		Indexes: s19IndexDefs(2),
	}
	if _, err := mgr.CreateCollection(meta); err != nil {
		_ = mgr.Close()
		_ = owner.Close()
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = mgr.Close()
		_ = owner.Close()
		t.Fatalf("open collection: %v", err)
	}

	// Insert two disjoint large batches (> 4096 domain rows total to exercise
	// parallel per-domain COW preparation and gap-jumping prechecks).
	ids, docs := buildS19GatewayBSONDocs(t, 4000)
	if err := col.InsertBatchValidatedBSONNoResultIDs(ids[:2000], docs[:2000]); err != nil {
		t.Fatalf("batch 1: %v", err)
	}
	if err := col.InsertBatchValidatedBSONNoResultIDs(ids[2000:4000], docs[2000:4000]); err != nil {
		t.Fatalf("batch 2: %v", err)
	}

	extraDoc, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "z-extra"},
		{Key: "email", Value: "z-extra@example.com"},
		{Key: "city", Value: "austin"},
		{Key: "age", Value: int32(25)},
	})
	if err != nil {
		t.Fatalf("marshal extra doc: %v", err)
	}
	// Primary key conflict at first/middle/last position against pending COW tree.
	for _, conflictIdx := range []int{0, 1500, 3999} {
		if err := col.InsertBatchValidatedBSONNoResultIDs([][]byte{ids[conflictIdx], []byte("z-extra")}, [][]byte{docs[conflictIdx], extraDoc}); !errors.Is(err, ErrDocumentExists) {
			t.Fatalf("primary conflict at %d: got err=%v want ErrDocumentExists", conflictIdx, err)
		}
	}

	// Unique index conflict against pending COW tree with a fresh primary _id.
	dupEmailDoc, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "user-999999"},
		{Key: "email", Value: "user-002500@example.com"},
		{Key: "city", Value: "austin"},
		{Key: "age", Value: int32(25)},
	})
	if err != nil {
		t.Fatalf("marshal dup email: %v", err)
	}
	freshID := []byte{0x01, byte(bson.TypeString), 12, 0, 0, 0, 'u', 's', 'e', 'r', '-', '9', '9', '9', '9', '9', '9', 0}
	if err := col.InsertBatchValidatedBSONNoResultIDs([][]byte{freshID, []byte("z-unique-ok")}, [][]byte{dupEmailDoc, extraDoc}); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("unique conflict: got err=%v want ErrUniqueIndexConflict", err)
	}

	// Checkpoint #1 and #2: verify single authoritative cleanup scan per checkpoint
	// (no premature pre-fallback WAL scan) and reopen durability.
	parseCount := func(s string) int {
		var v int
		_, _ = fmt.Sscanf(s, "%d", &v)
		return v
	}
	scans0 := parseCount(owner.Stats()["treedb.command_wal.cleanup.scan.count_total"])
	if err := mgr.FlushAll(); err != nil {
		t.Fatalf("FlushAll 1: %v", err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint 1: %v", err)
	}
	scans1 := parseCount(owner.Stats()["treedb.command_wal.cleanup.scan.count_total"])
	// Insert another batch after Checkpoint 1 so Checkpoint 2 has non-zero older_root LSN.
	moreIDs, moreDocs := buildS19GatewayBSONDocs(t, 4500)
	if err := col.InsertBatchValidatedBSONNoResultIDs(moreIDs[4000:4500], moreDocs[4000:4500]); err != nil {
		t.Fatalf("batch 3: %v", err)
	}
	if err := mgr.FlushAll(); err != nil {
		t.Fatalf("FlushAll 2: %v", err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint 2: %v", err)
	}
	st := owner.Stats()
	scans2 := parseCount(st["treedb.command_wal.cleanup.scan.count_total"])
	if got := st["treedb.cache.checkpoint.stage.command_wal_cleanup.samples"]; got != "" && got != "0" {
		t.Fatalf("premature pre-fallback command_wal_cleanup stage ran %s times, want 0", got)
	}
	if scans1 != scans0+1 || scans2 != scans1+1 {
		t.Fatalf("command_wal cleanup scans progression = (%d -> %d -> %d), want exactly +1 per checkpoint", scans0, scans1, scans2)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("close mgr: %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatalf("close owner: %v", err)
	}

	// Reopen and verify all 4500 documents survived.
	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reBackend, err := reopened.NativeSemanticBackend()
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	reMgr := NewCollectionManager(reBackend)
	defer func() { _ = reMgr.Close() }()
	reCol, err := reMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("reopen collection: %v", err)
	}
	for _, checkIdx := range []int{0, 1999, 2000, 3999, 4000, 4499} {
		got, err := reCol.Get(moreIDs[checkIdx])
		if err != nil || !bytes.Equal(got, moreDocs[checkIdx]) {
			t.Fatalf("reopen Get(%d) mismatch: len=%d err=%v", checkIdx, len(got), err)
		}
	}
}
