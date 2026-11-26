package collections_test

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func openSecondaryFastPathTestDB(t *testing.T, dir string) (*backenddb.DB, func() error) {
	t.Helper()
	opts := treedb.OptionsForBenchmark(treedb.ProfileCommandWALRelaxed, dir)
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	return backend, cleanup
}

func TestSecondaryLookupFastPath_ReturnedSliceSafety(t *testing.T) {
	dir := t.TempDir()
	backend, cleanup := openSecondaryFastPathTestDB(t, dir)
	defer func() { _ = cleanup() }()

	mgr := collections.NewCollectionManager(backend)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "users",
		Indexes: []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			{Name: "city_idx", Field: "city", ValueType: collections.IndexValueString},
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	const total = 150
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("doc-%04d", i)
		if i == total-1 {
			// Exercise oversized ID path (> chunkMax/2).
			id = "big-" + strings.Repeat("x", 2500)
		}
		doc := fmt.Sprintf(`{"email":"u-%04d@example.com","city":"honolulu"}`, i)
		if _, err := col.Insert([]byte(id), []byte(doc)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Prime catalog cache.
	if ids, err := col.FindByIndex("email_idx", "u-0001@example.com"); err != nil || len(ids) != 1 || string(ids[0]) != "doc-0001" {
		t.Fatalf("prime unique=(%q, %v)", ids, err)
	}

	// Unknown index returns nil, existing index with no match returns non-nil empty slice.
	if ids, err := col.FindByIndex("missing_idx", "anything"); err != nil || ids != nil {
		t.Fatalf("missing index=(%#v, %v), want (nil, nil)", ids, err)
	}
	if ids, err := col.FindByIndex("email_idx", "nobody@example.com"); err != nil || ids == nil || len(ids) != 0 {
		t.Fatalf("empty unique=(%#v, %v), want non-nil empty slice", ids, err)
	}
	if ids, err := col.FindByIndex("city_idx", "nowhere"); err != nil || ids == nil || len(ids) != 0 {
		t.Fatalf("empty non-unique=(%#v, %v), want non-nil empty slice", ids, err)
	}

	// Mutating and appending to every returned ID must never alias or corrupt
	// sibling IDs or subsequent lookups.
	ids, err := col.FindByIndex("city_idx", "honolulu")
	if err != nil {
		t.Fatalf("FindByIndex city_idx: %v", err)
	}
	if len(ids) != total {
		t.Fatalf("len(ids)=%d want %d", len(ids), total)
	}
	originals := make([][]byte, len(ids))
	for i, id := range ids {
		originals[i] = bytes.Clone(id)
	}
	for i := range ids {
		// Append must not overwrite ids[i+1].
		appended := append(ids[i], []byte("-tail-mutation")...)
		if !bytes.HasPrefix(appended, originals[i]) {
			t.Fatalf("append corrupted prefix at %d", i)
		}
		// In-place mutation must not affect any sibling ID.
		for j := range ids[i] {
			ids[i][j] ^= 0xff
		}
		for k := i + 1; k < len(ids); k++ {
			if !bytes.Equal(ids[k], originals[k]) {
				t.Fatalf("mutating/appending ids[%d] corrupted ids[%d]: got %q want %q", i, k, ids[k], originals[k])
			}
		}
	}

	// Re-query must still see intact original IDs via FindByIndex, FindByIndexRange, and ScanIndexRange.
	again, err := col.FindByIndex("city_idx", "honolulu")
	if err != nil || len(again) != total {
		t.Fatalf("re-query FindByIndex=(%d, %v) want %d", len(again), err, total)
	}
	for i := range again {
		if !bytes.Equal(again[i], originals[i]) {
			t.Fatalf("re-query mismatch at %d: got %q want %q", i, again[i], originals[i])
		}
	}

	ranged, truncated, err := col.FindByIndexRange("city_idx", collections.IndexRangeOptions{
		Lower: collections.IndexRangeBound{Value: "honolulu", Inclusive: true},
		Upper: collections.IndexRangeBound{Value: "honolulu", Inclusive: true},
		Limit: 10,
	})
	if err != nil || !truncated || len(ranged) != 10 {
		t.Fatalf("FindByIndexRange limit=(%d, truncated=%v, err=%v)", len(ranged), truncated, err)
	}

	var scanned [][]byte
	scanTrunc, err := col.ScanIndexRange("city_idx", collections.IndexRangeOptions{
		Lower: collections.IndexRangeBound{Value: "honolulu", Inclusive: true},
		Upper: collections.IndexRangeBound{Value: "honolulu", Inclusive: true},
		Limit: 5,
	}, func(id []byte) (bool, error) {
		scanned = append(scanned, id)
		return true, nil
	})
	if err != nil || !scanTrunc || len(scanned) != 5 {
		t.Fatalf("ScanIndexRange=(%d, truncated=%v, err=%v)", len(scanned), scanTrunc, err)
	}
	for i := range scanned {
		if !bytes.Equal(scanned[i], originals[i]) {
			t.Fatalf("ScanIndexRange mismatch at %d: got %q want %q", i, scanned[i], originals[i])
		}
	}
}

func TestSecondaryLookupFastPath_UniqueTombstonesAndReopen(t *testing.T) {
	dir := t.TempDir()
	backend, cleanup := openSecondaryFastPathTestDB(t, dir)

	mgr := collections.NewCollectionManager(backend)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "accounts",
		Indexes: []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", ValueType: collections.IndexValueString, Unique: true},
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("accounts")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	// Insert, checkpoint, delete (leaving tombstone sorted before new doc ID),
	// re-insert same unique value with lexicographically greater doc ID, checkpoint.
	if _, err := col.Insert([]byte("doc-a"), []byte(`{"email":"shared@example.com"}`)); err != nil {
		t.Fatalf("Insert doc-a: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint 1: %v", err)
	}
	if err := col.Delete([]byte("doc-a")); err != nil {
		t.Fatalf("Delete doc-a: %v", err)
	}
	if _, err := col.Insert([]byte("doc-z"), []byte(`{"email":"shared@example.com"}`)); err != nil {
		t.Fatalf("Insert doc-z: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint 2: %v", err)
	}

	// Prime + repeat settled lookups (exercises SeekFirstLiveKeyAtRoot skipping tombstones).
	for i := 0; i < 3; i++ {
		ids, err := col.FindByIndex("email_idx", "shared@example.com")
		if err != nil {
			t.Fatalf("FindByIndex iter %d: %v", i, err)
		}
		if len(ids) != 1 || string(ids[0]) != "doc-z" {
			t.Fatalf("FindByIndex iter %d=%q want [doc-z]", i, ids)
		}
	}

	if err := cleanup(); err != nil {
		t.Fatalf("close backend: %v", err)
	}

	// Reopen and verify pointer & secondary index durability.
	reopenedDB, cleanup2 := openSecondaryFastPathTestDB(t, dir)
	defer func() { _ = cleanup2() }()
	reopenedMgr := collections.NewCollectionManager(reopenedDB)
	reopenedCol, err := reopenedMgr.OpenCollection("accounts")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	for i := 0; i < 3; i++ {
		ids, err := reopenedCol.FindByIndex("email_idx", "shared@example.com")
		if err != nil {
			t.Fatalf("reopened FindByIndex iter %d: %v", i, err)
		}
		if len(ids) != 1 || string(ids[0]) != "doc-z" {
			t.Fatalf("reopened FindByIndex iter %d=%q want [doc-z]", i, ids)
		}
	}
}

func TestSecondaryLookupFastPath_AllScalarTypesAndEdgeKeys(t *testing.T) {
	dir := t.TempDir()
	backend, cleanup := openSecondaryFastPathTestDB(t, dir)
	defer func() { _ = cleanup() }()

	mgr := collections.NewCollectionManager(backend)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "typed",
		Indexes: []collections.IndexDefinition{
			{Name: "s_idx", Field: "s", ValueType: collections.IndexValueString},
			{Name: "b_idx", Field: "b", ValueType: collections.IndexValueBool},
			{Name: "i_idx", Field: "i", ValueType: collections.IndexValueInt64},
			{Name: "d_idx", Field: "d", ValueType: collections.IndexValueDouble},
			{Name: "tags_idx", Field: "tags", ValueType: collections.IndexValueString, MultiKey: true},
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("typed")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	// Mismatched type must error even before any rows exist (empty root).
	if _, err := col.FindByIndex("i_idx", "not-an-int"); err == nil {
		t.Fatal("FindByIndex string on int64 index unexpectedly succeeded on empty root")
	}

	if _, err := col.Insert([]byte("d1"), []byte(`{"s":"a\u0000b","b":true,"i":-42,"d":-0.0,"tags":["go","go","db"]}`)); err != nil {
		t.Fatalf("Insert d1: %v", err)
	}
	if _, err := col.Insert([]byte("d2"), []byte(`{"s":"plain","b":false,"i":100,"d":3.5,"tags":["go"]}`)); err != nil {
		t.Fatalf("Insert d2: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Prime catalog cache, then verify every scalar type on the settled fast path.
	for round := 0; round < 2; round++ {
		if ids, err := col.FindByIndex("s_idx", "a\x00b"); err != nil || len(ids) != 1 || string(ids[0]) != "d1" {
			t.Fatalf("round %d nul string=(%q, %v)", round, ids, err)
		}
		if ids, err := col.FindByIndexValue("b_idx", true); err != nil || len(ids) != 1 || string(ids[0]) != "d1" {
			t.Fatalf("round %d bool=(%q, %v)", round, ids, err)
		}
		if ids, err := col.FindByIndexValue("i_idx", int64(-42)); err != nil || len(ids) != 1 || string(ids[0]) != "d1" {
			t.Fatalf("round %d int64=(%q, %v)", round, ids, err)
		}
		if ids, err := col.FindByIndexValue("d_idx", 0.0); err != nil || len(ids) != 1 || string(ids[0]) != "d1" {
			t.Fatalf("round %d double 0=(%q, %v)", round, ids, err)
		}
		if ids, trunc, err := col.FindByIndexRange("d_idx", collections.IndexRangeOptions{
			Lower: collections.IndexRangeBound{Unbounded: true},
			Upper: collections.IndexRangeBound{Value: math.NaN(), Inclusive: false},
		}); err != nil || trunc || len(ids) != 0 {
			t.Fatalf("round %d double upper NaN exclusive=(%q, %v, %v)", round, ids, trunc, err)
		}
		// MultiKey dedupe: d1 has ["go","go","db"] -> should appear once.
		if ids, err := col.FindByIndex("tags_idx", "go"); err != nil || len(ids) != 2 || string(ids[0]) != "d1" || string(ids[1]) != "d2" {
			t.Fatalf("round %d multikey=(%q, %v)", round, ids, err)
		}
		if _, err := col.FindByIndex("i_idx", "still-not-an-int"); err == nil {
			t.Fatalf("round %d: FindByIndex string on int64 index unexpectedly succeeded", round)
		}
	}
}

func TestSecondaryLookupFastPath_ConcurrentMutationsAndFlushes(t *testing.T) {
	dir := t.TempDir()
	backend, cleanup := openSecondaryFastPathTestDB(t, dir)
	defer func() { _ = cleanup() }()

	mgr := collections.NewCollectionManager(backend)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "concurrent_idx",
		Options: collections.CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			{Name: "city_idx", Field: "city", ValueType: collections.IndexValueString},
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("concurrent_idx")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	const initialDocs = 128
	for i := 0; i < initialDocs; i++ {
		id := fmt.Sprintf("init-%04d", i)
		doc := fmt.Sprintf(`{"email":"init-%04d@example.com","city":"city-%02d"}`, i, i%8)
		if _, err := col.Insert([]byte(id), []byte(doc)); err != nil {
			t.Fatalf("seed Insert %d: %v", i, err)
		}
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatalf("initial Checkpoint: %v", err)
	}

	var stop atomic.Bool
	var errMu sync.Mutex
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	var wg sync.WaitGroup
	// Writer goroutine interleaving inserts, replaces, deletes, and checkpoints.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for step := 0; step < 80; step++ {
			id := fmt.Sprintf("mut-%04d", step)
			email := fmt.Sprintf("mut-%04d@example.com", step)
			city := fmt.Sprintf("city-%02d", step%8)
			if _, err := col.Insert([]byte(id), []byte(fmt.Sprintf(`{"email":%q,"city":%q}`, email, city))); err != nil {
				recordErr(fmt.Errorf("insert %d: %w", step, err))
				return
			}
			if step%3 == 0 {
				if _, err := col.Replace([]byte(id), []byte(fmt.Sprintf(`{"email":%q,"city":%q}`, email, fmt.Sprintf("city-%02d", (step+1)%8)))); err != nil {
					recordErr(fmt.Errorf("replace %d: %w", step, err))
					return
				}
			}
			if step%5 == 0 {
				if err := col.Delete([]byte(id)); err != nil {
					recordErr(fmt.Errorf("delete %d: %w", step, err))
					return
				}
			}
			if step%10 == 0 {
				if err := backend.Checkpoint(); err != nil {
					recordErr(fmt.Errorf("checkpoint %d: %w", step, err))
					return
				}
			}
		}
		stop.Store(true)
	}()

	// Concurrent readers hammering every secondary lookup path.
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; !stop.Load() || i < 50; i++ {
				idx := (worker*17 + i) % initialDocs
				email := fmt.Sprintf("init-%04d@example.com", idx)
				ids, err := col.FindByIndex("email_idx", email)
				if err != nil {
					recordErr(fmt.Errorf("FindByIndex unique: %w", err))
					return
				}
				wantID := fmt.Sprintf("init-%04d", idx)
				if len(ids) != 1 || string(ids[0]) != wantID {
					recordErr(fmt.Errorf("FindByIndex(%q)=%q want [%s]", email, ids, wantID))
					return
				}
				city := fmt.Sprintf("city-%02d", (worker+i)%8)
				cityIDs, err := col.FindByIndex("city_idx", city)
				if err != nil {
					recordErr(fmt.Errorf("FindByIndex non-unique: %w", err))
					return
				}
				if len(cityIDs) < initialDocs/8 {
					recordErr(fmt.Errorf("FindByIndex(%q) len=%d < %d", city, len(cityIDs), initialDocs/8))
					return
				}
				limIDs, _, err := col.FindByIndexValueLimit("city_idx", city, 4)
				if err != nil || len(limIDs) != 4 {
					recordErr(fmt.Errorf("FindByIndexValueLimit(%q)=(%d, %v)", city, len(limIDs), err))
					return
				}
				rngIDs, _, err := col.FindByIndexRange("city_idx", collections.IndexRangeOptions{
					Lower: collections.IndexRangeBound{Value: city, Inclusive: true},
					Upper: collections.IndexRangeBound{Value: city, Inclusive: true},
					Limit: 8,
				})
				if err != nil || len(rngIDs) != 8 {
					recordErr(fmt.Errorf("FindByIndexRange(%q)=(%d, %v)", city, len(rngIDs), err))
					return
				}
			}
		}(r)
	}

	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
}

func TestSecondaryLookupFastPath_AllocsBudget(t *testing.T) {
	dir := t.TempDir()
	backend, cleanup := openSecondaryFastPathTestDB(t, dir)
	defer func() { _ = cleanup() }()

	mgr := collections.NewCollectionManager(backend)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "alloc_budget",
		Indexes: []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			{Name: "city_idx", Field: "city", ValueType: collections.IndexValueString},
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("alloc_budget")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	for i := 0; i < 256; i++ {
		id := fmt.Sprintf("doc-%06d", i)
		doc := fmt.Sprintf(`{"email":"user-%06d@example.com","city":"city-%02d"}`, i, i%4)
		if _, err := col.Insert([]byte(id), []byte(doc)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Prime catalog cache and pools.
	if ids, err := col.FindByIndex("email_idx", "user-000010@example.com"); err != nil || len(ids) != 1 {
		t.Fatalf("prime unique=(%q, %v)", ids, err)
	}
	if ids, err := col.FindByIndex("city_idx", "city-01"); err != nil || len(ids) != 64 {
		t.Fatalf("prime non-unique=(%d, %v)", len(ids), err)
	}

	uniqueAllocs := testing.AllocsPerRun(100, func() {
		ids, err := col.FindByIndex("email_idx", "user-000010@example.com")
		if err != nil || len(ids) != 1 {
			t.Fatalf("FindByIndex unique=(%q, %v)", ids, err)
		}
	})
	if uniqueAllocs > 2 {
		t.Fatalf("settled unique FindByIndex allocs/op=%.0f want <= 2", uniqueAllocs)
	}

	nonUniqueAllocs := testing.AllocsPerRun(100, func() {
		ids, err := col.FindByIndex("city_idx", "city-01")
		if err != nil || len(ids) != 64 {
			t.Fatalf("FindByIndex non-unique=(%d, %v)", len(ids), err)
		}
	})
	if nonUniqueAllocs > 5 {
		t.Fatalf("settled non-unique (64 rows) FindByIndex allocs/op=%.0f want <= 5", nonUniqueAllocs)
	}
}
