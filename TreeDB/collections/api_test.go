package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionErrorsAreClassifiable(t *testing.T) {
	if !errors.Is(ErrCollectionNotFound, ErrCollectionNotFound) {
		t.Fatal("ErrCollectionNotFound should be errors.Is-compatible")
	}
	for _, err := range []error{
		ErrDocumentExists,
		ErrDuplicateDocumentID,
		fmt.Errorf("wrapped: %w", ErrUniqueIndexConflict),
	} {
		if !IsDuplicateKeyError(err) {
			t.Fatalf("IsDuplicateKeyError(%v)=false want true", err)
		}
	}
	if IsDuplicateKeyError(ErrCollectionNotFound) {
		t.Fatal("ErrCollectionNotFound classified as duplicate key")
	}
}

func TestCollectionGetNilDBReturnsError(t *testing.T) {
	col := &Collection{}
	_, err := col.Get([]byte("u1"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, errCollectionDBNil) {
		t.Fatalf("Get nil db error=%v", err)
	}
}

func TestCollectionManagerOpenCollectionNilDBReturnsErrCollectionDBNil(t *testing.T) {
	mgr := NewCollectionManager(nil)
	_, err := mgr.OpenCollection("users")
	if !errors.Is(err, errCollectionDBNil) {
		t.Fatalf("OpenCollection nil db err=%v want errCollectionDBNil", err)
	}
}

func TestCollectionManagerOpenCollectionCacheRejectsClosedDB(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.OpenCollection("users"); err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	if _, err := mgr.OpenCollection("users"); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("OpenCollection after close err=%v want ErrClosed", err)
	}
	if _, err := mgr.OpenCollection(""); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("OpenCollection invalid name after close err=%v want ErrClosed", err)
	}
}

func TestCollectionManagerOpenCollectionDoesNotRetainHandles(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	for i := 0; i < 8; i++ {
		col, err := mgr.OpenCollection("users")
		if err != nil {
			t.Fatalf("open collection %d: %v", i, err)
		}
		if col.manager != mgr {
			t.Fatalf("open collection %d manager=%p want %p", i, col.manager, mgr)
		}
	}

	mgr.collectionsMu.RLock()
	got := len(mgr.collections)
	mgr.collectionsMu.RUnlock()
	if got != 0 {
		t.Fatalf("OpenCollection retained %d collection handles without vector indexes", got)
	}
}

func TestCollectionRegisterVectorIndexDoesNotRetainCleanHandle(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	first, err := newVectorIndex(col, VectorIndexOptions{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
	})
	if err != nil {
		t.Fatalf("new first vector index: %v", err)
	}
	second, err := newVectorIndex(col, VectorIndexOptions{
		Name:       "summary_embedding",
		Field:      "summary_embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
	})
	if err != nil {
		t.Fatalf("new second vector index: %v", err)
	}

	if err := col.RegisterVectorIndex(first); err != nil {
		t.Fatalf("register first vector index: %v", err)
	}
	if got := collectionManagerHandleCount(mgr); got != 0 {
		t.Fatalf("RegisterVectorIndex retained %d clean handles want 0", got)
	}
	if err := col.RegisterVectorIndex(second); err != nil {
		t.Fatalf("register second vector index: %v", err)
	}
	if got := collectionManagerHandleCount(mgr); got != 0 {
		t.Fatalf("second RegisterVectorIndex retained %d clean handles want 0", got)
	}
	col.UnregisterVectorIndex("embedding")
	if got := collectionManagerHandleCount(mgr); got != 0 {
		t.Fatalf("UnregisterVectorIndex retained %d clean handles want 0", got)
	}
	col.UnregisterVectorIndex("summary_embedding")
	if got := collectionManagerHandleCount(mgr); got != 0 {
		t.Fatalf("last UnregisterVectorIndex left %d tracked handles want 0", got)
	}
}

func TestCollectionRegisterVectorIndexReportsCatalogRefreshFailure(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	index, err := newVectorIndex(col, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	if err := col.RegisterVectorIndex(index); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("register after close error=%v want ErrClosed", err)
	}
	if got := col.registeredVectorIndex(index.name); got != nil {
		t.Fatalf("failed registration published runtime %p", got)
	}
}

func TestCollectionUnregisterNativeVectorIndexReleasesHandleWithAdHocRemaining(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 2,
		}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	nativeIndex, err := newVectorIndex(col, VectorIndexOptions{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
	})
	if err != nil {
		t.Fatalf("new native vector index: %v", err)
	}
	adHocIndex, err := newVectorIndex(col, VectorIndexOptions{
		Name:       "adhoc_embedding",
		Field:      "adhoc_embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
	})
	if err != nil {
		t.Fatalf("new ad hoc vector index: %v", err)
	}

	if err := col.RegisterVectorIndex(nativeIndex); err != nil {
		t.Fatalf("register native vector index: %v", err)
	}
	if err := col.RegisterVectorIndex(adHocIndex); err != nil {
		t.Fatalf("register ad hoc vector index: %v", err)
	}
	mgr.registerCollectionHandle(col)
	if got := collectionManagerHandleCount(mgr); got != 1 {
		t.Fatalf("registered handles=%d want 1", got)
	}
	col.UnregisterVectorIndex("embedding")
	if got := collectionManagerHandleCount(mgr); got != 0 {
		t.Fatalf("native unregister retained %d handles with only ad hoc indexes left; want 0", got)
	}
}

func TestCollectionAdHocVectorIndexWritesDoNotRetainManagerHandle(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	index, err := newVectorIndex(col, VectorIndexOptions{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	if err := col.RegisterVectorIndex(index); err != nil {
		t.Fatalf("register vector index: %v", err)
	}

	if _, err := col.Insert([]byte("a"), []byte(`{"embedding":[1,0]}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := collectionManagerHandleCount(mgr); got != 0 {
		t.Fatalf("ad hoc vector write retained %d manager handles; want 0", got)
	}
	results, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1})
	if err != nil {
		t.Fatalf("search ad hoc index: %v", err)
	}
	requireVectorResultIDs(t, results, "a")
}

func TestCollectionDeclaredNativeVectorIndexesLoadedRejectsExtraNativeRuntime(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 2,
		}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	declared, err := newVectorIndex(col, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2})
	if err != nil {
		t.Fatalf("new declared index: %v", err)
	}
	stale, err := newVectorIndex(col, VectorIndexOptions{Name: "stale_embedding", Field: "stale_embedding", Metric: VectorMetricCosine, Dimensions: 2})
	if err != nil {
		t.Fatalf("new stale index: %v", err)
	}
	if err := col.RegisterVectorIndex(declared); err != nil {
		t.Fatalf("register declared vector index: %v", err)
	}
	if err := col.RegisterVectorIndex(stale); err != nil {
		t.Fatalf("register stale vector index: %v", err)
	}
	stale.setNativePersistent(true)

	if col.declaredNativeVectorIndexesLoadedForCurrentCatalog() {
		t.Fatal("declared native check accepted an extra stale native runtime")
	}
}

func collectionManagerHandleCount(mgr *CollectionManager) int {
	mgr.collectionsMu.RLock()
	defer mgr.collectionsMu.RUnlock()
	return len(mgr.collections)
}

func TestCollectionMetaReturnsDefensiveIndexCopyAcrossHandles(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	left, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open left collection: %v", err)
	}
	right, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open right collection: %v", err)
	}

	meta := left.Meta()
	meta.Indexes[0].Name = "mutated"
	if got := left.Meta().Indexes[0].Name; got != "email" {
		t.Fatalf("left Meta leaked mutation: got index %q want email", got)
	}
	if got := right.Meta().Indexes[0].Name; got != "email" {
		t.Fatalf("right Meta leaked mutation: got index %q want email", got)
	}
}

func TestCollectionInsertBatchBridge_RoundTripWithSecondaryIndexes(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if got, want := meta.Name, "users"; got != want {
		t.Fatalf("collection name=%q want %q", got, want)
	}

	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	ids, err := col.InsertBatch(
		[][]byte{[]byte("u2"), []byte("u1")},
		[][]byte{
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
		},
	)
	if err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if len(ids) != 2 || !bytes.Equal(ids[0], []byte("u2")) || !bytes.Equal(ids[1], []byte("u1")) {
		t.Fatalf("result ids=%q", ids)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("u1=%q want %q", got, want)
	}

	emailIDs, err := col.FindByIndex("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u2")) {
		t.Fatalf("email ids=%q want u2", emailIDs)
	}

	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, cityIDs, []byte("u1"), []byte("u2"))
	if err := col.Flush(); err != nil {
		t.Fatalf("flush indexed memtables: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	} {
		if got := catalog.rootID(rootName); got == 0 {
			t.Fatalf("root %q was not persisted", rootName)
		}
	}
}

func TestCollectionValueLogRewriteOffline_RoundTripWithCompressedSecondaryIndexes(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.IndexOuterLeavesInValueLog = true
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closeDB := collectionMaintenanceCloseOnce(cleanup)
	t.Cleanup(func() { _ = closeDB() })

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DataRootStoragePolicy:   RootStorageCompressed,
			IndexStateStoragePolicy: RootStorageCompressed,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true, StoragePolicy: RootStorageCompressed},
			{Name: "city", Field: "city", ValueType: IndexValueString, StoragePolicy: RootStorageCompressed},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	docs := [][]byte{
		[]byte(`{"email":"ada@example.com","city":"hnl","pad":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`),
		[]byte(`{"email":"grace@example.com","city":"hnl","pad":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`),
		[]byte(`{"email":"linus@example.com","city":"hel","pad":"cccccccccccccccccccccccccccccccc"}`),
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1"), []byte("u2"), []byte("u3")}, docs); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	requireCollectionMaintenanceReads(t, col)
	if err := closeDB(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	rewriteStats, err := treedb.ValueLogRewriteOffline(opts)
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if rewriteStats.RecordsCopied == 0 {
		t.Fatalf("expected offline rewrite to copy collection leaf records, stats=%+v", rewriteStats)
	}

	reopened, reopenedCleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopenedCleanup() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection after maintenance: %v", err)
	}
	requireCollectionMaintenanceReads(t, reopenedCol)
}

func TestCollectionValueLogGC_RoundTripWithCompressedSecondaryIndexes(t *testing.T) {
	opts := treedb.Options{
		Dir:                        t.TempDir(),
		Durability:                 treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	}
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	var closeOnce sync.Once
	var closeErr error
	closeDB := func() error {
		closeOnce.Do(func() {
			closeErr = cleanup()
		})
		return closeErr
	}
	t.Cleanup(func() { _ = closeDB() })

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DataRootStoragePolicy:   RootStorageCompressed,
			IndexStateStoragePolicy: RootStorageCompressed,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true, StoragePolicy: RootStorageCompressed},
			{Name: "city", Field: "city", ValueType: IndexValueString, StoragePolicy: RootStorageCompressed},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	docs := [][]byte{
		[]byte(`{"email":"ada@example.com","city":"hnl","pad":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`),
		[]byte(`{"email":"grace@example.com","city":"hnl","pad":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`),
		[]byte(`{"email":"linus@example.com","city":"hel","pad":"cccccccccccccccccccccccccccccccc"}`),
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1"), []byte("u2"), []byte("u3")}, docs); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	requireCollectionMaintenanceReads(t, col)

	valueLogDir := backenddb.ValueLogDirPath(d.Dir())
	syntheticLane, staleSeq := chooseStandaloneValueLogSegmentStart(t, valueLogDir)
	stalePath := writeStandaloneValueLogSegment(t, valueLogDir, syntheticLane, staleSeq, []byte("unreferenced collection-api gc segment"))
	_ = writeStandaloneValueLogSegment(t, valueLogDir, syntheticLane, staleSeq+1, []byte("newer unreferenced collection-api gc segment"))
	if err := d.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	gcCtx, gcCancel := collectionMaintenanceTestContext(t)
	defer gcCancel()
	stats, err := d.ValueLogGC(gcCtx, backenddb.ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("expected GC to delete a stale segment, stats=%+v", stats)
	}
	if _, err := os.Stat(stalePath); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected stale segment to be deleted, err=%v", err)
	}
	if err := closeDB(); err != nil {
		t.Fatalf("close db after value-log GC: %v", err)
	}
	reopened, reopenedCleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("reopen db after value-log GC: %v", err)
	}
	defer func() { _ = reopenedCleanup() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection after value-log GC: %v", err)
	}
	requireCollectionMaintenanceReads(t, reopenedCol)
}

func TestCollectionLeafGenerationPackGC_RoundTripWithTemplateV1SecondaryIndexes(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	if testing.Short() {
		t.Skip("loads enough documents to exercise collection leaf generation pack/GC")
	}
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.Generational.LeafSegmentTargetBytes = 16 << 10

	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closeDB := collectionMaintenanceCloseOnce(cleanup)
	t.Cleanup(func() { _ = closeDB() })

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:          DocumentFormatTemplateV1,
			DataRootStoragePolicy:   RootStorageCompressed,
			IndexStateStoragePolicy: RootStorageCompressed,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true, StoragePolicy: RootStorageCompressed},
			{Name: "city", Field: "city", ValueType: IndexValueString, StoragePolicy: RootStorageCompressed},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	var encoder TemplateV1Encoder
	const (
		documents = 4_000
		batchSize = 500
	)
	for start := 0; start < documents; start += batchSize {
		ids, docs := collectionMaintenanceTemplateBatch(t, &encoder, start, batchSize)
		if _, err := col.InsertBatch(ids, docs); err != nil {
			t.Fatalf("insert batch at %d: %v", start, err)
		}
	}
	requireCollectionMaintenanceTemplateReads(t, col)
	if err := col.Flush(); err != nil {
		t.Fatalf("flush indexed memtables: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	packCtx, packCancel := collectionMaintenanceTestContext(t)
	packStats, err := d.LeafGenerationPackFromPlan(packCtx, backenddb.LeafGenerationPackFromPlanOptions{
		Force:          true,
		MaxGenerations: 1,
		Sync:           true,
	})
	packCancel()
	if err != nil {
		t.Fatalf("LeafGenerationPackFromPlan: %v", err)
	}
	if got := packStats.LeafPagesCopied; got <= 0 {
		t.Fatalf("LeafPagesCopied=%d, want collection leaves copied (stats=%+v)", got, packStats)
	}
	// SourceBytesLive counts the stored representation, which can be very small
	// for compressed collection leaves. Require a real positive source rather
	// than an encoding-dependent byte floor; copied pages plus the read/GC/reopen
	// assertions below prove the maintenance path operated on collection data.
	if got := packStats.SourceBytesLive; got <= 0 {
		t.Fatalf("SourceBytesLive=%d, want real collection live bytes copied (stats=%+v)", got, packStats)
	}
	requireCollectionMaintenanceTemplateReads(t, col)
	// The pre-pack durable slot remains independently recovery-selectable until
	// a later root publication overwrites it. Checkpoint alone does not advance
	// CommitSeq, so recommit the packed root without publishing a new cached
	// leaf-log dependency before asserting physical reclamation.
	if err := d.ForceCommit(d.State().RootPageID); err != nil {
		t.Fatalf("commit packed durable-slot successor: %v", err)
	}

	gcCtx, gcCancel := collectionMaintenanceTestContext(t)
	gcStats, err := d.LeafGenerationGC(gcCtx, backenddb.LeafGenerationGCOptions{})
	gcCancel()
	if err != nil {
		t.Fatalf("LeafGenerationGC: %v", err)
	}
	if gcStats.BytesDeleted <= 0 && gcStats.FilesDeleted <= 0 {
		t.Fatalf("LeafGenerationGC stats=%+v, want deleted packed source generation bytes/files", gcStats)
	}
	requireCollectionMaintenanceTemplateReads(t, col)
	if err := closeDB(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, reopenedCleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopenedCleanup() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection after leaf generation maintenance: %v", err)
	}
	requireCollectionMaintenanceTemplateReads(t, reopenedCol)
}

type failSecondCollectionValueLogAppender struct {
	calls int
}

func (a *failSecondCollectionValueLogAppender) AppendValues(values [][]byte) ([]page.ValuePtr, error) {
	a.calls++
	if a.calls == 2 {
		return nil, errors.New("injected second root append failure")
	}
	ptrs := make([]page.ValuePtr, len(values))
	for i, value := range values {
		ptrs[i] = page.ValuePtr{
			FileID: page.ValueLogFileID(1),
			Offset: uint64(i + 1),
			Length: uint32(len(value)),
		}
	}
	return ptrs, nil
}

func (*failSecondCollectionValueLogAppender) Flush() error { return nil }
func (*failSecondCollectionValueLogAppender) Sync() error  { return nil }
func (*failSecondCollectionValueLogAppender) CurrentValueLogSegment() (string, uint32, bool) {
	return "", 0, false
}

func TestPointerizeInsertBatchPlanRunsLaterFailurePreservesPlanOwnership(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()
	appender := &failSecondCollectionValueLogAppender{}
	d.SetValueLogAppender(appender)
	defer d.SetValueLogAppender(nil)

	newRun := func(rootName string) collectionRootRun {
		table := newCollectionRunTable(1)
		setCollectionRunValue(table, []byte("large"), bytes.Repeat([]byte("v"), page.PageSize+1))
		table.Freeze()
		return collectionRootRun{name: rootName, kind: collectionRootSecondary, table: table}
	}
	plan := &insertBatchPlan{runs: []collectionRootRun{
		newRun(collectionSecondaryRootName("docs", "first")),
		newRun(collectionSecondaryRootName("docs", "second")),
	}}
	originals := []memtable.Table{plan.runs[0].table, plan.runs[1].table}
	defer func() {
		for i, original := range originals {
			if plan.runs[i].table != original {
				resetCollectionRunTable(plan.runs[i].table)
			}
			resetCollectionRunTable(original)
		}
	}()

	obsolete, err := pointerizeInsertBatchPlanRuns(d, CollectionMeta{Name: "docs"}, plan)
	if err == nil || !strings.Contains(err.Error(), "injected second root append failure") {
		t.Fatalf("pointerize err=%v want injected later failure", err)
	}
	if obsolete != nil {
		t.Fatalf("obsolete tables=%v want nil on failure", obsolete)
	}
	if appender.calls != 2 {
		t.Fatalf("value-log append calls=%d want 2", appender.calls)
	}
	for i, original := range originals {
		if plan.runs[i].table != original {
			t.Fatalf("run %d table ownership changed on failure", i)
		}
		if original.Len() != 1 {
			t.Fatalf("run %d original table was released on failure", i)
		}
	}
}

func TestCollectionFastJSONLargeDocumentsUseValueLogPointers(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileFast, t.TempDir())
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closeDB := collectionMaintenanceCloseOnce(cleanup)
	t.Cleanup(func() { _ = closeDB() })

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "bluesky",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatJSON,
			DataRootStoragePolicy: RootStorageFast,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("bluesky")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	const documents = collectionPointerizeBatchMaxValues + 7
	ids := make([][]byte, documents)
	docs := make([][]byte, documents)
	for i := 0; i < documents; i++ {
		ids[i] = []byte(fmt.Sprintf("at://did:example:%06d", i))
		docs[i] = collectionLargeJSONDocumentForTest(i, 8<<10)
	}
	wantFirst := bytes.Clone(docs[0])
	wantLast := bytes.Clone(docs[documents-1])
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("insert large JSON batch: %v", err)
	}
	for i := range docs {
		for j := range docs[i] {
			docs[i][j] ^= 0xff
		}
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush large JSON batch: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	got, err := col.Get(ids[documents-1])
	if err != nil {
		t.Fatalf("get large JSON doc: %v", err)
	}
	if !bytes.Equal(got, wantLast) {
		t.Fatalf("large JSON doc mismatch: got %d bytes want %d", len(got), len(wantLast))
	}
	requireCollectionPrimaryEntryPointer(t, d, "bluesky", ids[documents-1])

	if err := closeDB(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	reopened, reopenedCleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopenedCleanup() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("bluesky")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	reopenedDoc, err := reopenedCol.Get(ids[0])
	if err != nil {
		t.Fatalf("get reopened large JSON doc: %v", err)
	}
	if !bytes.Equal(reopenedDoc, wantFirst) {
		t.Fatalf("reopened large JSON doc mismatch: got %d bytes want %d", len(reopenedDoc), len(wantFirst))
	}
}

func TestCollectionOversizedTextIndexValueUsesValueLogPointer(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:             t.TempDir(),
		ResolvedProfile: backenddb.ProfileCommandWALDurable,
		CommandWAL:      true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "docs",
		Indexes: []IndexDefinition{{Name: "kind", Field: "kind", ValueType: IndexValueString}},
		TextIndexes: []TextIndexDefinition{{
			Name:    "lexical",
			Version: TextIndexVersionV2,
			Fields:  []TextIndexField{{Field: "body"}},
		}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	const documents = int(textV2DefaultDocMapBlockSize)
	ids := make([][]byte, documents)
	values := make([][]byte, documents)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%03d-%s", i, strings.Repeat("x", 56)))
		values[i] = []byte(`{"kind":"test","body":"pointerize this text index block"}`)
	}
	if _, err := col.InsertBatch(ids, values); err != nil {
		t.Fatalf("insert oversized text block: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, "docs")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	rootName := collectionTextV2DocMapRootName("docs", "lexical")
	rootID := catalog.rootID(rootName)
	key := encodeTextV2BlockKey(1)
	entry, err := snap.GetEntryAtRoot(rootID, key)
	if err != nil {
		t.Fatalf("get docmap entry: %v", err)
	}
	value, err := snap.GetAtRoot(rootID, key)
	if err != nil {
		t.Fatalf("read docmap value: %v", err)
	}
	maxInline := page.PageSize - node.NodeHeaderSize - node.DirectoryEntrySize - 7 - page.EntryRevisionSize - len(key)
	if len(value) <= maxInline {
		t.Fatalf("docmap value len=%d unexpectedly fits one leaf entry with max=%d", len(value), maxInline)
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		t.Fatalf("docmap entry flags=%#x ptr=%+v want value-log pointer", entry.Flags, entry.ValuePtr)
	}
}

func TestCollectionCreateTextIndexPointerizesOversizedBackfillValue(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileFast, t.TempDir())
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = cleanup() }()
	if !d.HasValueLogAppender() {
		t.Fatal("database has no value-log appender")
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	const documents = int(textV2DefaultDocMapBlockSize)
	ids := make([][]byte, documents)
	values := make([][]byte, documents)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%03d-%s", i, strings.Repeat("x", 56)))
		values[i] = []byte(`{"body":"pointerize this text index backfill block"}`)
	}
	if _, err := col.InsertBatch(ids, values); err != nil {
		t.Fatalf("insert backfill source: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name:    "lexical",
		Version: TextIndexVersionV2,
		Fields:  []TextIndexField{{Field: "body"}},
	}); err != nil {
		t.Fatalf("create text index: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, "docs")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	rootID := catalog.rootID(collectionTextV2DocMapRootName("docs", "lexical"))
	key := encodeTextV2BlockKey(1)
	entry, err := snap.GetEntryAtRoot(rootID, key)
	if err != nil {
		t.Fatalf("get docmap entry: %v", err)
	}
	value, err := snap.GetAtRoot(rootID, key)
	if err != nil {
		t.Fatalf("read docmap value: %v", err)
	}
	maxInline := page.PageSize - node.NodeHeaderSize - node.DirectoryEntrySize - 7 - page.EntryRevisionSize - len(key)
	if len(value) <= maxInline {
		t.Fatalf("docmap value len=%d unexpectedly fits one leaf entry with max=%d", len(value), maxInline)
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		t.Fatalf("docmap entry flags=%#x ptr=%+v want value-log pointer", entry.Flags, entry.ValuePtr)
	}
}

func TestCollectionCreateIndexPointerizesOversizedBackfillValue(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileFast, t.TempDir())
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = cleanup() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	tags := make([]string, 160)
	for i := range tags {
		tags[i] = fmt.Sprintf("tag-%03d-%s", i, strings.Repeat("x", 24))
	}
	document, err := json.Marshal(map[string]any{"tags": tags})
	if err != nil {
		t.Fatalf("marshal document: %v", err)
	}
	documentID := []byte("doc")
	if _, err := col.InsertBatch([][]byte{documentID}, [][]byte{document}); err != nil {
		t.Fatalf("insert backfill source: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{
		Name:      "tags",
		Field:     "tags",
		ValueType: IndexValueString,
		MultiKey:  true,
	}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, "docs")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	rootID := catalog.rootID(collectionIndexStateRootName("docs"))
	entry, err := snap.GetEntryAtRoot(rootID, documentID)
	if err != nil {
		t.Fatalf("get index-state entry: %v", err)
	}
	value, err := snap.GetAtRoot(rootID, documentID)
	if err != nil {
		t.Fatalf("read index-state value: %v", err)
	}
	maxInline := page.PageSize - node.NodeHeaderSize - node.DirectoryEntrySize - 7 - page.EntryRevisionSize - len(documentID)
	if len(value) <= maxInline {
		t.Fatalf("index-state value len=%d unexpectedly fits one leaf entry with max=%d", len(value), maxInline)
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		t.Fatalf("index-state entry flags=%#x ptr=%+v want value-log pointer", entry.Flags, entry.ValuePtr)
	}
}

func TestCollectionFastJSONBufferedIndexedLargeDocumentsUseValueLogPointers(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileFast, t.TempDir())
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closeDB := collectionMaintenanceCloseOnce(cleanup)
	t.Cleanup(func() { _ = closeDB() })

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "bluesky",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatJSON,
			DataRootStoragePolicy: RootStorageFast,
		},
		Indexes: []IndexDefinition{{
			Name:      "event",
			Field:     "commit.collection",
			ValueType: IndexValueString,
		}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("bluesky")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	const documents = 32
	ids := make([][]byte, documents)
	docs := make([][]byte, documents)
	for i := 0; i < documents; i++ {
		ids[i] = []byte(fmt.Sprintf("at://did:example:%06d", i))
		docs[i] = collectionLargeJSONDocumentForTest(i, 8<<10)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("buffered indexed insert large JSON batch: %v", err)
	}
	eventIDs, err := col.FindByIndex("event", "app.bsky.feed.post")
	if err != nil {
		t.Fatalf("find event before flush: %v", err)
	}
	if len(eventIDs) != documents {
		t.Fatalf("event ids before flush=%d want %d", len(eventIDs), documents)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered indexed writes: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	got, err := col.Get(ids[documents-1])
	if err != nil {
		t.Fatalf("get large JSON doc after flush: %v", err)
	}
	if !bytes.Equal(got, docs[documents-1]) {
		t.Fatalf("large JSON doc mismatch after flush: got %d bytes want %d", len(got), len(docs[documents-1]))
	}
	requireCollectionPrimaryEntryPointer(t, d, "bluesky", ids[documents-1])
}

func TestCollectionFastJSONUpdateBatchLargeDocumentsUseValueLogPointers(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileFast, t.TempDir())
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closeDB := collectionMaintenanceCloseOnce(cleanup)
	t.Cleanup(func() { _ = closeDB() })

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "bluesky",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatJSON,
			DataRootStoragePolicy: RootStorageFast,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("bluesky")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	const documents = 32
	ids := make([][]byte, documents)
	initialDocs := make([][]byte, documents)
	updatedDocs := make([][]byte, documents)
	items := make([]UpdateBatchItem, documents)
	for i := 0; i < documents; i++ {
		id := []byte(fmt.Sprintf("at://did:example:%06d", i))
		updated := collectionLargeJSONDocumentForTest(i, 8<<10)
		ids[i] = id
		initialDocs[i] = []byte(fmt.Sprintf(`{"id":%d,"commit":{"collection":"app.bsky.feed.post"}}`, i))
		updatedDocs[i] = updated
		items[i] = UpdateBatchItem{
			DocumentID: id,
			Update: func([]byte) ([]byte, bool, error) {
				return bytes.Clone(updated), true, nil
			},
		}
	}
	if _, err := col.InsertBatch(ids, initialDocs); err != nil {
		t.Fatalf("insert initial JSON batch: %v", err)
	}
	results, err := col.UpdateBatch(items)
	if err != nil {
		t.Fatalf("UpdateBatch large JSON documents: %v", err)
	}
	if len(results) != documents {
		t.Fatalf("UpdateBatch results=%d want %d", len(results), documents)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	got, err := col.Get(ids[documents-1])
	if err != nil {
		t.Fatalf("get updated large JSON doc: %v", err)
	}
	if !bytes.Equal(got, updatedDocs[documents-1]) {
		t.Fatalf("updated large JSON doc mismatch: got %d bytes want %d", len(got), len(updatedDocs[documents-1]))
	}
	requireCollectionPrimaryEntryPointer(t, d, "bluesky", ids[documents-1])
}

func TestCollectionFastJSONMaintenanceVacuumUsesValueLogLeaves(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	opts := treedb.OptionsFor(treedb.ProfileFast, t.TempDir())
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = cleanup() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "bluesky",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatJSON,
			DataRootStoragePolicy: RootStorageCompressed,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("bluesky")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	const (
		documents = 20_000
		batchSize = 1_000
	)
	for start := 0; start < documents; start += batchSize {
		n := min(batchSize, documents-start)
		ids := make([][]byte, n)
		docs := make([][]byte, n)
		for i := 0; i < n; i++ {
			row := start + i
			ids[i] = []byte(fmt.Sprintf("did:example:%06d", row))
			docs[i] = []byte(fmt.Sprintf(`{"event":"app.bsky.feed.post","seq":%d}`, row))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			t.Fatalf("insert batch at %d: %v", start, err)
		}
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after load: %v", err)
	}
	requireCollectionPrimaryRootLeafLogChildren(t, d, "bluesky")

	ctx := context.Background()
	if _, err := col.CompactRootOverlays(ctx); err != nil {
		t.Fatalf("compact root overlays: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after overlay compaction: %v", err)
	}
	if _, err := d.ValueLogRewriteOnline(ctx, backenddb.ValueLogRewriteOnlineOptions{
		BatchSize:      1024,
		SyncEachBatch:  true,
		LocalityPolicy: backenddb.ValueLogRewriteLocalityGrouped,
	}); err != nil {
		t.Fatalf("value-log rewrite: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after value-log rewrite: %v", err)
	}
	if _, err := d.ValueLogGC(ctx, backenddb.ValueLogGCOptions{}); err != nil {
		t.Fatalf("value-log GC: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after value-log GC: %v", err)
	}
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum index online: %v", err)
	}

	got, err := col.Get([]byte("did:example:019999"))
	if err != nil {
		t.Fatalf("get after vacuum: %v", err)
	}
	if !bytes.Contains(got, []byte(`"seq":19999`)) {
		t.Fatalf("post-vacuum doc=%s want seq 19999", got)
	}
	requireCollectionPrimaryRootLeafLogChildren(t, d, "bluesky")
}

func collectionLargeJSONDocumentForTest(i, payloadBytes int) []byte {
	return []byte(fmt.Sprintf(
		`{"id":%d,"commit":{"collection":"app.bsky.feed.post"},"payload":"%s"}`,
		i,
		strings.Repeat("x", payloadBytes),
	))
}

func BenchmarkCollectionLargeDocumentPointerizedInsertBatchFlush(b *testing.B) {
	prevLog := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(prevLog) })

	opts := treedb.OptionsFor(treedb.ProfileFast, b.TempDir())
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	b.Cleanup(func() {
		if err := cleanup(); err != nil {
			b.Errorf("close db: %v", err)
		}
	})

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "bluesky",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatJSON,
			DataRootStoragePolicy: RootStorageFast,
		},
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("bluesky")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}

	const targetBatchSize = 256
	b.ReportAllocs()
	b.ResetTimer()
	for inserted := 0; inserted < b.N; {
		b.StopTimer()
		batchSize := targetBatchSize
		if remaining := b.N - inserted; remaining < batchSize {
			batchSize = remaining
		}
		ids := make([][]byte, batchSize)
		docs := make([][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			docNum := inserted + i
			ids[i] = []byte(fmt.Sprintf("at://did:example:%06d", docNum))
			docs[i] = collectionLargeJSONDocumentForTest(docNum, 8<<10)
		}
		b.StartTimer()
		if _, err := col.InsertBatch(ids, docs); err != nil {
			b.Fatalf("insert large JSON batch: %v", err)
		}
		if err := col.Flush(); err != nil {
			b.Fatalf("flush large JSON batch: %v", err)
		}
		inserted += batchSize
	}
	b.StopTimer()
	b.ReportMetric(float64(targetBatchSize), "target_docs/batch")
}

func requireCollectionPrimaryEntryPointer(t *testing.T, d *backenddb.DB, collectionName string, id []byte) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		t.Fatalf("load collection catalog: %v", err)
	}
	if catalog == nil {
		t.Fatalf("collection %q not found", collectionName)
	}
	rootID := catalog.rootID(collectionPrimaryRootName(collectionName))
	entry, err := snap.GetEntryAtRoot(rootID, id)
	if err != nil {
		t.Fatalf("GetEntryAtRoot(%q): %v", id, err)
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		t.Fatalf("entry %q flags=%#x ptr=%+v, want value-log pointer", id, entry.Flags, entry.ValuePtr)
	}
}

func requireCollectionPrimaryRootLeafLogChildren(t *testing.T, d *backenddb.DB, collectionName string) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load collection catalog: %v", err)
	}
	if catalog == nil {
		_ = snap.Close()
		t.Fatalf("collection %q not found", collectionName)
	}
	rootID := catalog.rootID(collectionPrimaryRootName(collectionName))
	_ = snap.Close()
	if rootID == 0 {
		t.Fatalf("collection %q primary root is empty", collectionName)
	}
	if !collectionRootHasLeafLogChild(t, d, rootID, make(map[uint64]bool)) {
		t.Fatalf("collection %q primary root %d has no leaf-log children", collectionName, rootID)
	}
}

func collectionRootHasLeafLogChild(t *testing.T, d *backenddb.DB, rootID uint64, seen map[uint64]bool) bool {
	t.Helper()
	if rootID == 0 || seen[rootID] {
		return false
	}
	seen[rootID] = true
	p := d.Pager()
	if p == nil {
		t.Fatal("missing pager")
	}
	data, err := p.Get(rootID)
	if err != nil {
		t.Fatalf("pager get root %d: %v", rootID, err)
	}
	n := node.NewNode(data)
	switch n.Type() {
	case page.PageTypeInternal:
		for i := uint16(0); i < n.Count(); i++ {
			_, childRef, err := n.GetInternalEntryRefView(i)
			if err != nil {
				t.Fatalf("internal child %d at root %d: %v", i, rootID, err)
			}
			if childRef.Kind == page.ChildRefLeafLog {
				return true
			}
			if collectionRootHasLeafLogChild(t, d, childRef.Page, seen) {
				return true
			}
		}
		return false
	case page.PageTypeLeaf:
		return false
	default:
		t.Fatalf("unexpected page type %d at root %d", n.Type(), rootID)
		return false
	}
}

func TestCollectionManagerListCollections(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create users: %v", err)
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "orders",
		Indexes: []IndexDefinition{{Name: "user_id", Field: "user_id", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create orders: %v", err)
	}

	metas, err := mgr.ListCollections()
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	if len(metas) != 2 {
		t.Fatalf("collection count=%d want 2: %+v", len(metas), metas)
	}
	if metas[0].Name != "orders" || metas[1].Name != "users" {
		t.Fatalf("collection order=%q,%q want orders,users", metas[0].Name, metas[1].Name)
	}
	if len(metas[0].Indexes) != 1 || metas[0].Indexes[0].Name != "user_id" {
		t.Fatalf("orders indexes=%+v want user_id", metas[0].Indexes)
	}
}

type collectionMetaIteratorTestEntry struct {
	key, value []byte
	deleted    bool
}

type collectionMetaIteratorTestFake struct {
	entries []collectionMetaIteratorTestEntry
	idx     int
	err     error
}

func (it *collectionMetaIteratorTestFake) Valid() bool { return it.idx < len(it.entries) }
func (it *collectionMetaIteratorTestFake) Next()       { it.idx++ }
func (it *collectionMetaIteratorTestFake) UnsafeKey() []byte {
	return it.entries[it.idx].key
}
func (it *collectionMetaIteratorTestFake) ValueCopy(dst []byte) []byte {
	return append(dst, it.entries[it.idx].value...)
}
func (it *collectionMetaIteratorTestFake) IsDeleted() bool { return it.entries[it.idx].deleted }
func (it *collectionMetaIteratorTestFake) Error() error    { return it.err }

func TestListCollectionMetasBoundedChargesTombstones(t *testing.T) {
	prefix := []byte(systemCollectionMetaPrefix)
	encoded := func(name string) []byte {
		value, err := encodeCollectionMeta(CollectionMeta{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	entries := []collectionMetaIteratorTestEntry{
		{key: append(append([]byte(nil), prefix...), 'a'), deleted: true},
		{key: append(append([]byte(nil), prefix...), 'b'), value: encoded("b")},
		{key: append(append([]byte(nil), prefix...), 'c'), value: encoded("c")},
	}
	for _, tc := range []struct {
		name      string
		max       int
		wantNames []string
		truncated bool
	}{
		{name: "over_cap", max: 2, wantNames: []string{"b"}, truncated: true},
		{name: "at_cap", max: 3, wantNames: []string{"b", "c"}},
		{name: "legacy_unbounded", max: 0, wantNames: []string{"b", "c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, truncated, err := listCollectionMetasBounded(&collectionMetaIteratorTestFake{entries: entries}, prefix, tc.max)
			if err != nil || truncated != tc.truncated || len(got) != len(tc.wantNames) {
				t.Fatalf("metas=%+v truncated=%t err=%v", got, truncated, err)
			}
			for i, name := range tc.wantNames {
				if got[i].Name != name {
					t.Fatalf("meta[%d]=%q want %q", i, got[i].Name, name)
				}
			}
		})
	}
	_, truncated, err := listCollectionMetasBounded(&collectionMetaIteratorTestFake{entries: entries, err: errors.New("cut-point failure")}, prefix, 2)
	if err == nil || truncated {
		t.Fatalf("cut-point iterator error truncated=%t err=%v", truncated, err)
	}
}

func TestCollectionInsertBatchStatsExposeIndexRunShape(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	_, requestStats, err := col.InsertBatchWithStats(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"sfo"}`),
		},
	)
	if err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	stats := col.LastInsertStats()
	if got, want := stats.Documents, 2; got != want {
		t.Fatalf("stats documents=%d want %d", got, want)
	}
	if got, want := stats.Indexes, 2; got != want {
		t.Fatalf("stats indexes=%d want %d", got, want)
	}
	if got, minRuns, maxRuns := stats.Runs, 2+len(stats.SecondaryRuns), 3+len(stats.SecondaryRuns); got < minRuns || got > maxRuns {
		t.Fatalf("stats runs=%d want between %d and %d", got, minRuns, maxRuns)
	}
	if got, want := len(stats.SecondaryRuns), 2; got != want {
		t.Fatalf("stats secondary runs=%d want %d", got, want)
	}
	if got, want := stats.SecondaryEntries, 4; got != want {
		t.Fatalf("stats secondary entries=%d want %d", got, want)
	}
	if stats.SecondaryKeyBytes == 0 {
		t.Fatal("stats secondary key bytes=0 want positive")
	}
	stats.SecondaryRuns[0].IndexName = "mutated"
	again := col.LastInsertStats()
	if got := again.SecondaryRuns[0].IndexName; got == "mutated" {
		t.Fatal("LastInsertStats did not return an owned secondary-run slice")
	}
	requestStats.SecondaryRuns[0].IndexName = "request-mutated"
	if got := col.LastInsertStats().SecondaryRuns[0].IndexName; got == "request-mutated" {
		t.Fatal("InsertBatchWithStats result shares its secondary-run slice with LastInsertStats")
	}
	if ids, err := col.InsertBatch(nil, nil); err != nil {
		t.Fatalf("empty insert batch: %v", err)
	} else if ids != nil {
		t.Fatalf("empty insert ids=%v want nil", ids)
	}
	empty := col.LastInsertStats()
	if got, want := empty.Documents, 0; got != want {
		t.Fatalf("empty stats documents=%d want %d", got, want)
	}
	if got, want := empty.Indexes, 2; got != want {
		t.Fatalf("empty stats indexes=%d want %d", got, want)
	}
	if got := empty.Runs; got != 0 {
		t.Fatalf("empty stats runs=%d want 0", got)
	}
	if got := len(empty.SecondaryRuns); got != 0 {
		t.Fatalf("empty stats secondary runs=%d want 0", got)
	}
}

func TestCollectionInsertBatchWithStatsOwnsConcurrentResults(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users", Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	type result struct {
		stats CollectionInsertStats
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, batch := range [][][]byte{
		{[]byte(`{"email":"one@example.com"}`)},
		{[]byte(`{"email":"two@example.com"}`), []byte(`{"email":"three@example.com"}`)},
	} {
		batch := batch
		go func() {
			<-start
			ids := make([][]byte, len(batch))
			for i := range ids {
				ids[i] = []byte(fmt.Sprintf("u-%d-%d", len(batch), i))
			}
			_, stats, err := col.InsertBatchWithStats(ids, batch)
			results <- result{stats: stats, err: err}
		}()
	}
	close(start)
	seen := map[int]bool{}
	for range 2 {
		got := <-results
		if got.err != nil {
			t.Fatalf("InsertBatchWithStats: %v", got.err)
		}
		seen[got.stats.Documents] = true
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("request stats documents=%v want distinct 1 and 2", seen)
	}
}

func TestCollectionUpdateBatchStatsExposeIndexRunShape(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"sfo"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	results, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: func(current []byte) ([]byte, bool, error) {
			return []byte(`{"email":"ada@example.com","city":"lhr"}`), true, nil
		}},
		{DocumentID: []byte("u2"), Update: func(current []byte) ([]byte, bool, error) {
			return []byte(`{"email":"grace@example.com","city":"ord"}`), true, nil
		}},
	})
	if err != nil {
		t.Fatalf("update batch: %v", err)
	}
	for i, result := range results {
		if !result.Matched || !result.Modified {
			t.Fatalf("result %d=%+v want matched+modified", i, result)
		}
	}

	stats := col.LastUpdateStats()
	if got, want := stats.Items, 2; got != want {
		t.Fatalf("stats items=%d want %d", got, want)
	}
	if got, want := stats.Matched, 2; got != want {
		t.Fatalf("stats matched=%d want %d", got, want)
	}
	if got, want := stats.Modified, 2; got != want {
		t.Fatalf("stats modified=%d want %d", got, want)
	}
	if got, want := stats.Indexes, 2; got != want {
		t.Fatalf("stats indexes=%d want %d", got, want)
	}
	if got := stats.Runs; got < 2 {
		t.Fatalf("stats runs=%d want at least primary+secondary", got)
	}
	if got := stats.SecondaryDeleteEntries; got < 2 {
		t.Fatalf("stats secondary deletes=%d want at least 2", got)
	}
	if got := stats.SecondarySetEntries; got < 2 {
		t.Fatalf("stats secondary sets=%d want at least 2", got)
	}
	if stats.SecondaryKeyBytes == 0 {
		t.Fatal("stats secondary key bytes=0 want positive")
	}
	if stats.CurrentRead != 0 || stats.Callback != 0 || stats.StructuredUpdateApply != 0 ||
		stats.OldIndexStateExtract != 0 || stats.NewIndexStateExtract != 0 ||
		stats.BufferStage != 0 ||
		stats.BufferStagePrecheck != 0 ||
		stats.BufferStageLockWait != 0 || stats.BufferStageLockHold != 0 ||
		stats.BufferStageValidation != 0 || stats.BufferStageRootScan != 0 ||
		stats.BufferStageDomainPrepare != 0 || stats.BufferStageFreeze != 0 ||
		stats.BufferStageRootTable != 0 ||
		stats.BufferStagePrimaryIdx != 0 || stats.BufferStageUniqueIdx != 0 ||
		stats.BufferStagePrimaryAppend != 0 || stats.BufferStageSecondaryAppend != 0 ||
		stats.BufferStageRootAppend != 0 || stats.BufferStageFlush != 0 {
		t.Fatalf("default update timings=%+v want zero unless detailed stats enabled", stats)
	}

	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: func(current []byte) ([]byte, bool, error) {
			time.Sleep(time.Millisecond)
			return []byte(`{"email":"ada@example.com","city":"sea"}`), true, nil
		}},
		{DocumentID: []byte("u2"), Update: func(current []byte) ([]byte, bool, error) {
			return []byte(`{"email":"grace@example.com","city":"bos"}`), true, nil
		}},
	}); err != nil {
		t.Fatalf("timed update batch: %v", err)
	}
	timedStats := col.LastUpdateStats()
	if timedStats.Callback <= 0 {
		t.Fatalf("timed callback=%s want positive with detailed stats enabled", timedStats.Callback)
	}
	if timedStats.BufferStageFlush != 0 {
		t.Fatalf("timed flush stage=%s want zero without threshold flush", timedStats.BufferStageFlush)
	}

	managerStats := mgr.StatsSnapshot()
	if got := managerStats.UpdateBatchCalls; got == 0 {
		t.Fatal("manager update batch calls=0 want positive")
	}
	if got := managerStats.UpdateBatchItems; got < 2 {
		t.Fatalf("manager update batch items=%d want at least 2", got)
	}
	if got := managerStats.UpdateBatchModified; got < 2 {
		t.Fatalf("manager update batch modified=%d want at least 2", got)
	}
	if got := managerStats.UpdateBatchSecondarySets; got < 2 {
		t.Fatalf("manager update batch secondary sets=%d want at least 2", got)
	}
	if got := managerStats.UpdateBatchIndexValueChanges; got < 2 {
		t.Fatalf("manager update batch index value changes=%d want at least 2", got)
	}
	if got := managerStats.UpdateBatchIndexValueUnchanged; got < 2 {
		t.Fatalf("manager update batch index value unchanged=%d want at least 2", got)
	}
	if got := managerStats.UpdateBatchUniqueCheckSkips; got < 2 {
		t.Fatalf("manager update batch unique check skips=%d want at least 2", got)
	}
	exported := mgr.Stats()
	if _, ok := exported["treedb.collections.write_domain.update_batch.secondary_sets_total"]; !ok {
		t.Fatalf("manager stats missing update batch secondary set counter: keys=%v", exported)
	}
	for _, key := range []string{
		"treedb.collections.write_domain.update_batch.index_value_changes_total",
		"treedb.collections.write_domain.update_batch.index_value_unchanged_total",
		"treedb.collections.write_domain.update_batch.changed_index_fast_mask_fallbacks_total",
		"treedb.collections.write_domain.update_batch.unique_checks_total",
		"treedb.collections.write_domain.update_batch.unique_check_skips_total",
	} {
		if _, ok := exported[key]; !ok {
			t.Fatalf("manager stats missing %s: keys=%v", key, exported)
		}
	}
	for _, key := range []string{
		"treedb.collections.write_domain.update_batch.old_index_state_extract_ns_total",
		"treedb.collections.write_domain.update_batch.new_index_state_extract_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_precheck_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_lock_wait_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_lock_hold_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_validation_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_root_scan_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_domain_prepare_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_freeze_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_root_table_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_primary_index_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_unique_index_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_primary_append_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_secondary_append_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_root_append_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_flush_ns_total",
	} {
		if _, ok := exported[key]; !ok {
			t.Fatalf("manager stats missing %s: keys=%v", key, exported)
		}
	}
}

func TestCollectionUpdateBatchStatsCountFastMaskFallbacks(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	indexes := make([]IndexDefinition, 65)
	for i := range indexes {
		field := fmt.Sprintf("f%d", i)
		indexes[i] = IndexDefinition{
			Name:      fmt.Sprintf("idx_%02d", i),
			Field:     field,
			ValueType: IndexValueString,
		}
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "wide", Indexes: indexes}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("wide")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	document := func(value64 string) []byte {
		var b strings.Builder
		b.WriteByte('{')
		for i := range indexes {
			if i > 0 {
				b.WriteByte(',')
			}
			value := fmt.Sprintf("v%d", i)
			if i == 64 {
				value = value64
			}
			fmt.Fprintf(&b, "%q:%q", fmt.Sprintf("f%d", i), value)
		}
		b.WriteByte('}')
		return []byte(b.String())
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{document("v64")}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			return document("changed"), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("update batch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one matched modified update", results)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.MaskFallbacks, 1; got != want {
		t.Fatalf("last update fast-mask fallbacks=%d want %d", got, want)
	}
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("last update changed indexes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 64; got != want {
		t.Fatalf("last update unchanged indexes=%d want %d", got, want)
	}
	managerStats := mgr.StatsSnapshot()
	if got, want := managerStats.UpdateBatchMaskFallbacks, uint64(1); got != want {
		t.Fatalf("manager fast-mask fallbacks=%d want %d", got, want)
	}
	exported := mgr.Stats()
	if got, want := exported["treedb.collections.write_domain.update_batch.changed_index_fast_mask_fallbacks_total"], "1"; got != want {
		t.Fatalf("exported fast-mask fallback counter=%q want %q", got, want)
	}
	ids, err := col.FindByIndexValue("idx_64", "changed")
	if err != nil {
		t.Fatalf("find updated ordinal 64 index: %v", err)
	}
	if len(ids) != 1 || string(ids[0]) != "u1" {
		t.Fatalf("idx_64 changed ids=%q want [u1]", ids)
	}
}

func TestCollectionUpdateBufferFlushTimingCountsAsyncSchedule(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl","flag":false}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}

	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: func(current []byte) ([]byte, bool, error) {
			return []byte(`{"email":"ada@example.com","city":"hnl","flag":true}`), true, nil
		}},
	}); err != nil {
		t.Fatalf("update batch: %v", err)
	} else if !batched {
		t.Fatal("update batch batched=false want true")
	}
	stats := col.LastUpdateStats()
	if got := stats.BufferStageFlush; got <= 0 {
		t.Fatalf("async schedule flush timing=%s want positive", got)
	}
	if got := mgr.StatsSnapshot().IndexedAsyncFlushScheduled; got == 0 {
		t.Fatal("async flush scheduled=0 want positive")
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("drain async update: %v", err)
	}
}

func TestCollectionUpdateBufferBreakdownStatsSnapshotAndAdd(t *testing.T) {
	cases := []struct {
		name string
		key  string
		set  func(*CollectionUpdateStats, time.Duration)
		get  func(CollectionManagerStats) time.Duration
	}{
		{"precheck", "treedb.collections.write_domain.update_batch.buffer_stage_precheck_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStagePrecheck = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferPrecheck }},
		{"lock_wait", "treedb.collections.write_domain.update_batch.buffer_stage_lock_wait_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStageLockWait = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferLockWait }},
		{"lock_hold", "treedb.collections.write_domain.update_batch.buffer_stage_lock_hold_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStageLockHold = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferLockHold }},
		{"validation", "treedb.collections.write_domain.update_batch.buffer_stage_validation_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStageValidation = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferValidation }},
		{"root_scan", "treedb.collections.write_domain.update_batch.buffer_stage_root_scan_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStageRootScan = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferRootScan }},
		{"domain_prepare", "treedb.collections.write_domain.update_batch.buffer_stage_domain_prepare_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStageDomainPrepare = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferDomainPrepare }},
		{"freeze", "treedb.collections.write_domain.update_batch.buffer_stage_freeze_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStageFreeze = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferFreeze }},
		{"root_table", "treedb.collections.write_domain.update_batch.buffer_stage_root_table_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStageRootTable = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferRootTable }},
		{"primary_index", "treedb.collections.write_domain.update_batch.buffer_stage_primary_index_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStagePrimaryIdx = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferPrimaryIdx }},
		{"unique_index", "treedb.collections.write_domain.update_batch.buffer_stage_unique_index_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStageUniqueIdx = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferUniqueIdx }},
		{"primary_append", "treedb.collections.write_domain.update_batch.buffer_stage_primary_append_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStagePrimaryAppend = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferPrimaryAppend }},
		{"secondary_append", "treedb.collections.write_domain.update_batch.buffer_stage_secondary_append_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStageSecondaryAppend = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferSecondaryAppend }},
		{"root_append", "treedb.collections.write_domain.update_batch.buffer_stage_root_append_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStageRootAppend = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferRootAppend }},
		{"flush", "treedb.collections.write_domain.update_batch.buffer_stage_flush_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStageFlush = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferFlush }},
	}

	var updateStats CollectionUpdateStats
	for i, tc := range cases {
		tc.set(&updateStats, time.Duration(i+1)*time.Nanosecond)
	}
	domain := &collectionWriteDomain{}
	domain.observeUpdateBatchStats(updateStats)
	snapshot := domain.statsSnapshot()
	var merged CollectionManagerStats
	merged.add(snapshot)
	exported := (&CollectionManager{domains: map[string]*collectionWriteDomain{"test": domain}}).Stats()
	for i, tc := range cases {
		want := time.Duration(i+1) * time.Nanosecond
		if got := tc.get(snapshot); got != want {
			t.Fatalf("snapshot %s=%s want %s", tc.name, got, want)
		}
		if got := tc.get(merged); got != want {
			t.Fatalf("merged %s=%s want %s", tc.name, got, want)
		}
		if got, want := exported[tc.key], fmt.Sprintf("%d", want.Nanoseconds()); got != want {
			t.Fatalf("exported %s=%q want %q", tc.key, got, want)
		}
	}
}

func TestCollectionUpdateIndexStateBreakdownStatsSnapshotAndAdd(t *testing.T) {
	updateStats := CollectionUpdateStats{
		IndexStateExtraction: 11 * time.Nanosecond,
		OldIndexStateExtract: 5 * time.Nanosecond,
		NewIndexStateExtract: 6 * time.Nanosecond,
	}
	domain := &collectionWriteDomain{}
	domain.observeUpdateBatchStats(updateStats)
	snapshot := domain.statsSnapshot()
	var merged CollectionManagerStats
	merged.add(snapshot)
	exported := (&CollectionManager{domains: map[string]*collectionWriteDomain{"test": domain}}).Stats()
	cases := []struct {
		name string
		key  string
		want time.Duration
		get  func(CollectionManagerStats) time.Duration
	}{
		{"total", "treedb.collections.write_domain.update_batch.index_state_extract_ns_total", 11 * time.Nanosecond, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchIndexStateExtract }},
		{"old", "treedb.collections.write_domain.update_batch.old_index_state_extract_ns_total", 5 * time.Nanosecond, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchOldIndexStateExtract }},
		{"new", "treedb.collections.write_domain.update_batch.new_index_state_extract_ns_total", 6 * time.Nanosecond, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchNewIndexStateExtract }},
	}
	for _, tc := range cases {
		if got := tc.get(snapshot); got != tc.want {
			t.Fatalf("snapshot %s=%s want %s", tc.name, got, tc.want)
		}
		if got := tc.get(merged); got != tc.want {
			t.Fatalf("merged %s=%s want %s", tc.name, got, tc.want)
		}
		if got, want := exported[tc.key], fmt.Sprintf("%d", tc.want.Nanoseconds()); got != want {
			t.Fatalf("exported %s=%q want %q", tc.key, got, want)
		}
	}
}

func TestCollectionUpdateStructuredApplyStatsSnapshotAndAdd(t *testing.T) {
	updateStats := CollectionUpdateStats{
		Callback:              3 * time.Nanosecond,
		StructuredUpdateApply: 5 * time.Nanosecond,
	}
	domain := &collectionWriteDomain{}
	domain.observeUpdateBatchStats(updateStats)
	snapshot := domain.statsSnapshot()
	var merged CollectionManagerStats
	merged.add(snapshot)
	exported := (&CollectionManager{domains: map[string]*collectionWriteDomain{"test": domain}}).Stats()
	if got := snapshot.UpdateBatchCallback; got != updateStats.Callback {
		t.Fatalf("snapshot callback=%s want %s", got, updateStats.Callback)
	}
	if got := snapshot.UpdateBatchStructuredApply; got != updateStats.StructuredUpdateApply {
		t.Fatalf("snapshot structured apply=%s want %s", got, updateStats.StructuredUpdateApply)
	}
	if got := merged.UpdateBatchCallback; got != updateStats.Callback {
		t.Fatalf("merged callback=%s want %s", got, updateStats.Callback)
	}
	if got := merged.UpdateBatchStructuredApply; got != updateStats.StructuredUpdateApply {
		t.Fatalf("merged structured apply=%s want %s", got, updateStats.StructuredUpdateApply)
	}
	if got, want := exported["treedb.collections.write_domain.update_batch.callback_ns_total"], fmt.Sprintf("%d", updateStats.Callback.Nanoseconds()); got != want {
		t.Fatalf("exported callback=%q want %q", got, want)
	}
	if got, want := exported["treedb.collections.write_domain.update_batch.structured_apply_ns_total"], fmt.Sprintf("%d", updateStats.StructuredUpdateApply.Nanoseconds()); got != want {
		t.Fatalf("exported structured apply=%q want %q", got, want)
	}
}

func TestUpdateBatchStatsSinceClampsSubResolutionDurations(t *testing.T) {
	if got := updateBatchStatsSince(false, time.Now().Add(time.Second)); got != 0 {
		t.Fatalf("disabled stats duration=%s want 0", got)
	}
	if got := updateBatchStatsSince(true, time.Now().Add(time.Second)); got != time.Nanosecond {
		t.Fatalf("sub-resolution stats duration=%s want 1ns", got)
	}
}

func TestCollectionUpdateCombineTimingStatsSnapshotAndAdd(t *testing.T) {
	domain := &collectionWriteDomain{}
	domain.updateBatchDetailedStats.Store(true)
	domain.observeUpdateCombineRequest(1)
	domain.observeUpdateCombineRequest(9)
	domain.observeUpdateCombineInline()
	domain.observeUpdateCombineBatch(2, false)
	domain.observeUpdateCombineBatch(300, true)
	domain.observeUpdateCombineEnqueue(3 * time.Nanosecond)
	domain.observeUpdateCombineWait(5 * time.Nanosecond)
	domain.observeUpdateCombineQueueWait(13 * time.Nanosecond)
	domain.observeUpdateCombineDrain(7 * time.Nanosecond)
	domain.observeUpdateCombineRun(11 * time.Nanosecond)
	domain.observeUpdateCombineResultDelivery(17 * time.Nanosecond)
	snapshot := domain.statsSnapshot()
	var merged CollectionManagerStats
	merged.add(snapshot)
	exported := (&CollectionManager{domains: map[string]*collectionWriteDomain{"test": domain}}).Stats()
	if snapshot.UpdateCombineRequests != 2 {
		t.Fatalf("snapshot requests=%d want 2", snapshot.UpdateCombineRequests)
	}
	if snapshot.UpdateCombineBatches != 2 {
		t.Fatalf("snapshot batches=%d want 2", snapshot.UpdateCombineBatches)
	}
	if snapshot.UpdateCombineBatchedRequests != 302 {
		t.Fatalf("snapshot batched requests=%d want 302", snapshot.UpdateCombineBatchedRequests)
	}
	if snapshot.UpdateCombineFallbackRequests != 300 {
		t.Fatalf("snapshot fallback requests=%d want 300", snapshot.UpdateCombineFallbackRequests)
	}
	if snapshot.UpdateCombineInlineRequests != 1 {
		t.Fatalf("snapshot inline requests=%d want 1", snapshot.UpdateCombineInlineRequests)
	}
	if merged.UpdateCombineInlineRequests != 1 {
		t.Fatalf("merged inline requests=%d want 1", merged.UpdateCombineInlineRequests)
	}
	if got, want := exported["treedb.collections.write_domain.update_combine.inline_requests_total"], "1"; got != want {
		t.Fatalf("exported inline requests=%q want %q", got, want)
	}
	cases := []struct {
		name string
		key  string
		want time.Duration
		get  func(CollectionManagerStats) time.Duration
	}{
		{"enqueue", "treedb.collections.write_domain.update_combine.enqueue_ns_total", 3 * time.Nanosecond, func(s CollectionManagerStats) time.Duration { return s.UpdateCombineEnqueue }},
		{"wait", "treedb.collections.write_domain.update_combine.wait_ns_total", 5 * time.Nanosecond, func(s CollectionManagerStats) time.Duration { return s.UpdateCombineWait }},
		{"queue wait", "treedb.collections.write_domain.update_combine.queue_wait_ns_total", 13 * time.Nanosecond, func(s CollectionManagerStats) time.Duration { return s.UpdateCombineQueueWait }},
		{"drain", "treedb.collections.write_domain.update_combine.drain_ns_total", 7 * time.Nanosecond, func(s CollectionManagerStats) time.Duration { return s.UpdateCombineDrain }},
		{"run", "treedb.collections.write_domain.update_combine.run_ns_total", 11 * time.Nanosecond, func(s CollectionManagerStats) time.Duration { return s.UpdateCombineRun }},
		{"result delivery", "treedb.collections.write_domain.update_combine.result_delivery_ns_total", 17 * time.Nanosecond, func(s CollectionManagerStats) time.Duration { return s.UpdateCombineResultDelivery }},
	}
	for _, tc := range cases {
		if got := tc.get(snapshot); got != tc.want {
			t.Fatalf("snapshot %s=%s want %s", tc.name, got, tc.want)
		}
		if got := tc.get(merged); got != tc.want {
			t.Fatalf("merged %s=%s want %s", tc.name, got, tc.want)
		}
		if got, want := exported[tc.key], fmt.Sprintf("%d", tc.want.Nanoseconds()); got != want {
			t.Fatalf("exported %s=%q want %q", tc.key, got, want)
		}
	}
	queueDepthLe1 := collectionUpdateCombineBucketIndex(1)
	queueDepthLe16 := collectionUpdateCombineBucketIndex(9)
	batchSizeLe2 := collectionUpdateCombineBucketIndex(2)
	batchSizeGt256 := collectionUpdateCombineBucketIndex(300)
	if got := snapshot.UpdateCombineQueueDepthBuckets[queueDepthLe1]; got != 1 {
		t.Fatalf("snapshot queue-depth le_1 bucket=%d want 1", got)
	}
	if got := merged.UpdateCombineQueueDepthBuckets[queueDepthLe16]; got != 1 {
		t.Fatalf("merged queue-depth le_16 bucket=%d want 1", got)
	}
	if got := snapshot.UpdateCombineBatchSizeBuckets[batchSizeLe2]; got != 1 {
		t.Fatalf("snapshot batch-size le_2 bucket=%d want 1", got)
	}
	if got := merged.UpdateCombineBatchSizeBuckets[batchSizeGt256]; got != 1 {
		t.Fatalf("merged batch-size gt_256 bucket=%d want 1", got)
	}
	if got, want := exported["treedb.collections.write_domain.update_combine.queue_depth_bucket_le_1_total"], "1"; got != want {
		t.Fatalf("exported queue-depth le_1 bucket=%q want %q", got, want)
	}
	if got, want := exported["treedb.collections.write_domain.update_combine.batch_size_bucket_gt_256_total"], "1"; got != want {
		t.Fatalf("exported batch-size gt_256 bucket=%q want %q", got, want)
	}
}

func TestCollectionUpdateIndexStatsSnapshotAndExport(t *testing.T) {
	updateStats := CollectionUpdateStats{
		IndexStatsCount: 2,
		IndexStats: [maxCollectionUpdateInlineIndexStats]CollectionUpdateIndexStats{
			{
				IndexName:        "email",
				Unique:           true,
				Unchanged:        1,
				UniqueCheckSkips: 1,
			},
			{
				IndexName:         "city/name",
				Changed:           2,
				SecondaryRuns:     2,
				SecondaryDeletes:  2,
				SecondarySets:     2,
				SecondaryKeyBytes: 128,
			},
		},
	}
	domain := &collectionWriteDomain{
		meta: CollectionMeta{Name: "users", Indexes: []IndexDefinition{
			{Name: "email", Unique: true},
			{Name: "city/name"},
		}},
	}
	domain.observeUpdateBatchStats(updateStats)
	domain.observeUpdateBatchStats(updateStats)

	snapshot := domain.statsSnapshot()
	if got, want := snapshot.UpdateBatchIndexStatsCount, 2; got != want {
		t.Fatalf("snapshot index stats count=%d want %d", got, want)
	}
	indexStatByName := func(stats []CollectionUpdateIndexStats, name string) CollectionUpdateIndexStats {
		t.Helper()
		for _, stat := range stats {
			if stat.IndexName == name {
				return stat
			}
		}
		t.Fatalf("missing index stat %q in %+v", name, stats)
		return CollectionUpdateIndexStats{}
	}
	stats := snapshot.UpdateBatchIndexStats[:snapshot.UpdateBatchIndexStatsCount]
	email := indexStatByName(stats, "email")
	if email.CollectionName != "users" || email.IndexOrdinal != 0 || !email.Unique || email.Unchanged != 2 || email.UniqueCheckSkips != 2 {
		t.Fatalf("email aggregate=%+v want unchanged/skips 2", email)
	}
	city := indexStatByName(stats, "city/name")
	if city.CollectionName != "users" || city.IndexOrdinal != 1 || city.Changed != 4 || city.SecondaryRuns != 4 || city.SecondaryDeletes != 4 || city.SecondarySets != 4 || city.SecondaryKeyBytes != 256 {
		t.Fatalf("city aggregate=%+v want doubled secondary work", city)
	}

	var merged CollectionManagerStats
	merged.add(snapshot)
	mergedStats := merged.UpdateBatchIndexStats[:merged.UpdateBatchIndexStatsCount]
	if got := indexStatByName(mergedStats, "city/name").SecondaryKeyBytes; got != 256 {
		t.Fatalf("merged city key bytes=%d want 256", got)
	}

	exported := (&CollectionManager{domains: map[string]*collectionWriteDomain{"test": domain}}).Stats()
	emailPrefix := "treedb.collections.write_domain.update_batch.collection." + collectionStatsMetricToken("users") + ".index.0." + collectionStatsMetricToken("email") + "."
	cityPrefix := "treedb.collections.write_domain.update_batch.collection." + collectionStatsMetricToken("users") + ".index.1." + collectionStatsMetricToken("city/name") + "."
	if got, want := exported[emailPrefix+"unique"], "1"; got != want {
		t.Fatalf("exported email unique=%q want %q", got, want)
	}
	if got, want := exported[emailPrefix+"unchanged_total"], "2"; got != want {
		t.Fatalf("exported email unchanged=%q want %q", got, want)
	}
	if got, want := exported[emailPrefix+"unique_check_skips_total"], "2"; got != want {
		t.Fatalf("exported email skips=%q want %q", got, want)
	}
	if got, want := exported[cityPrefix+"changed_total"], "4"; got != want {
		t.Fatalf("exported city changed=%q want %q", got, want)
	}
	if got, want := exported[cityPrefix+"secondary_key_bytes_total"], "256"; got != want {
		t.Fatalf("exported city key bytes=%q want %q", got, want)
	}
}

func TestCollectionUpdateStatsForMergeKeepsPerIndexBreakdown(t *testing.T) {
	left := CollectionUpdateStats{
		Items:           3,
		Matched:         3,
		Modified:        3,
		SecondaryRuns:   []CollectionUpdateSecondaryRunStats{{IndexName: "city", Deletes: 1, Sets: 2, KeyBytes: 64}},
		IndexStatsCount: 2,
		IndexStats: [maxCollectionUpdateInlineIndexStats]CollectionUpdateIndexStats{
			{
				CollectionName:   "users",
				IndexName:        "email",
				IndexOrdinal:     0,
				Unique:           true,
				Unchanged:        3,
				UniqueCheckSkips: 3,
			},
			{
				CollectionName:    "users",
				IndexName:         "city",
				IndexOrdinal:      1,
				Changed:           3,
				SecondaryRuns:     1,
				SecondaryDeletes:  1,
				SecondarySets:     2,
				SecondaryKeyBytes: 64,
			},
		},
	}
	right := CollectionUpdateStats{
		Items:           4,
		Matched:         4,
		Modified:        4,
		SecondaryRuns:   []CollectionUpdateSecondaryRunStats{{IndexName: "city", Deletes: 3, Sets: 4, KeyBytes: 96}},
		IndexStatsCount: 2,
		IndexStats: [maxCollectionUpdateInlineIndexStats]CollectionUpdateIndexStats{
			{
				CollectionName:   "users",
				IndexName:        "email",
				IndexOrdinal:     0,
				Unique:           true,
				Unchanged:        4,
				UniqueCheckSkips: 4,
			},
			{
				CollectionName:    "users",
				IndexName:         "city",
				IndexOrdinal:      1,
				Changed:           4,
				SecondaryRuns:     1,
				SecondaryDeletes:  3,
				SecondarySets:     4,
				SecondaryKeyBytes: 96,
			},
		},
	}

	var merged CollectionUpdateStats
	addCollectionUpdateStatsForMerge(&merged, left)
	addCollectionUpdateStatsForMerge(&merged, right)

	if got, want := merged.Items, 7; got != want {
		t.Fatalf("merged items=%d want %d", got, want)
	}
	if got, want := len(merged.SecondaryRuns), 1; got != want {
		t.Fatalf("secondary runs=%d want %d: %+v", got, want, merged.SecondaryRuns)
	}
	if city := merged.SecondaryRuns[0]; city.IndexName != "city" || city.Deletes != 4 || city.Sets != 6 || city.KeyBytes != 160 {
		t.Fatalf("merged secondary city=%+v want deletes/sets/key bytes 4/6/160", city)
	}
	if got, want := merged.IndexStatsCount, 2; got != want {
		t.Fatalf("index stats count=%d want %d", got, want)
	}
	email := merged.IndexStats[0]
	if email.IndexName != "email" || !email.Unique || email.Unchanged != 7 || email.UniqueCheckSkips != 7 {
		t.Fatalf("merged email=%+v want unchanged/skips 7", email)
	}
	city := merged.IndexStats[1]
	if city.IndexName != "city" || city.Changed != 7 || city.SecondaryRuns != 2 || city.SecondaryDeletes != 4 || city.SecondarySets != 6 || city.SecondaryKeyBytes != 160 {
		t.Fatalf("merged city index=%+v want changed/runs/deletes/sets/key bytes 7/2/4/6/160", city)
	}
}

func TestCollectionUpdateIndexStatsDoNotMergeOverlappingIndexNames(t *testing.T) {
	newDomain := func(collection string, changed int) *collectionWriteDomain {
		domain := &collectionWriteDomain{
			meta: CollectionMeta{
				Name:    collection,
				Indexes: []IndexDefinition{{Name: "email", Unique: true}},
			},
		}
		domain.observeUpdateBatchStats(CollectionUpdateStats{
			IndexStatsCount: 1,
			IndexStats: [maxCollectionUpdateInlineIndexStats]CollectionUpdateIndexStats{
				{CollectionName: collection, IndexName: "email", IndexOrdinal: 0, Unique: true, Changed: changed},
			},
		})
		return domain
	}
	mgr := &CollectionManager{domains: map[string]*collectionWriteDomain{
		"users":  newDomain("users", 1),
		"orders": newDomain("orders", 2),
	}}
	stats := mgr.StatsSnapshot()
	indexStat := func(collection, index string) CollectionUpdateIndexStats {
		t.Helper()
		for _, stat := range stats.UpdateBatchIndexStats[:stats.UpdateBatchIndexStatsCount] {
			if stat.CollectionName == collection && stat.IndexName == index {
				return stat
			}
		}
		t.Fatalf("missing %s/%s in %+v", collection, index, stats.UpdateBatchIndexStats)
		return CollectionUpdateIndexStats{}
	}
	if got := indexStat("users", "email").Changed; got != 1 {
		t.Fatalf("users/email changed=%d want 1", got)
	}
	if got := indexStat("orders", "email").Changed; got != 2 {
		t.Fatalf("orders/email changed=%d want 2", got)
	}

	exported := mgr.Stats()
	usersPrefix := "treedb.collections.write_domain.update_batch.collection." + collectionStatsMetricToken("users") + ".index.0." + collectionStatsMetricToken("email") + "."
	ordersPrefix := "treedb.collections.write_domain.update_batch.collection." + collectionStatsMetricToken("orders") + ".index.0." + collectionStatsMetricToken("email") + "."
	if usersPrefix == ordersPrefix {
		t.Fatalf("collection-qualified prefixes collided: %q", usersPrefix)
	}
	if got, want := exported[usersPrefix+"changed_total"], "1"; got != want {
		t.Fatalf("users exported changed=%q want %q", got, want)
	}
	if got, want := exported[ordersPrefix+"changed_total"], "2"; got != want {
		t.Fatalf("orders exported changed=%q want %q", got, want)
	}
}

func TestCollectionUpdateIndexStatsSnapshotCapsAtInlineIndexLimit(t *testing.T) {
	indexes := make([]IndexDefinition, maxCollectionUpdateInlineIndexStats+1)
	for i := range indexes {
		indexes[i] = IndexDefinition{Name: fmt.Sprintf("idx_%02d", i)}
	}
	domain := &collectionWriteDomain{
		meta: CollectionMeta{Name: "wide", Indexes: indexes},
	}
	var updateStats CollectionUpdateStats
	updateStats.IndexStatsCount = maxCollectionUpdateInlineIndexStats
	for i := 0; i < maxCollectionUpdateInlineIndexStats; i++ {
		updateStats.IndexStats[i] = CollectionUpdateIndexStats{
			CollectionName: "wide",
			IndexName:      indexes[i].Name,
			IndexOrdinal:   i,
			Changed:        i + 1,
		}
	}
	domain.observeUpdateBatchStats(updateStats)
	stats := domain.statsSnapshot()
	if got, want := stats.UpdateBatchIndexStatsCount, maxCollectionUpdateInlineIndexStats; got != want {
		t.Fatalf("index stats count=%d want cap %d", got, want)
	}
	last := stats.UpdateBatchIndexStats[maxCollectionUpdateInlineIndexStats-1]
	if got, want := last.IndexName, indexes[maxCollectionUpdateInlineIndexStats-1].Name; got != want {
		t.Fatalf("last retained index=%q want %q", got, want)
	}
}

func TestUpdateBatchPlanUniqueSecondaryIndexByRootAvoidsMapAllocation(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "sku", Field: "sku", ValueType: IndexValueString, Unique: true},
		},
	}
	plan := &updateBatchPlan{
		meta:                       meta,
		rootNames:                  []string{"users/primary", "users/index/email", "users/index/city", "users/index/sku"},
		uniqueSecondaryIndexByRoot: []int{-1, 0, -1, 2},
	}
	idx, ok := updateBatchPlanUniqueSecondaryIndex(plan, 1)
	if !ok {
		t.Fatal("unique root metadata did not find email index")
	}
	if got, want := idx.Name, "email"; got != want {
		t.Fatalf("unique root metadata index=%q want %q", got, want)
	}
	if _, ok := updateBatchPlanUniqueSecondaryIndex(plan, 2); ok {
		t.Fatal("non-unique city index reported as unique")
	}
	if _, ok := updateBatchPlanUniqueSecondaryIndex(plan, 0); ok {
		t.Fatal("primary root reported as unique secondary index")
	}
	if idx, ok := updateBatchPlanUniqueSecondaryIndex(plan, 3); !ok || idx.Name != "sku" {
		t.Fatalf("unique root metadata sku=(%q,%v) want sku,true", idx.Name, ok)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		_, _ = updateBatchPlanUniqueSecondaryIndex(plan, 1)
	}); allocs != 0 {
		t.Fatalf("unique root metadata allocs=%v want zero", allocs)
	}
}

func TestDeleteSecondaryEntriesForDocumentOwnsGeneratedKeys(t *testing.T) {
	encoded, err := encodeIndexScalar(IndexValueString, "hnl")
	if err != nil {
		t.Fatalf("encode city: %v", err)
	}
	documentID := []byte("u1")
	wantKey, err := indexEntryKey(encoded, documentID)
	if err != nil {
		t.Fatalf("index key: %v", err)
	}
	state := documentIndexState{"city": {encoded}}
	runtime := indexRuntime{def: indexDefinition{name: "city", valueType: IndexValueString}}
	table := newCollectionRunTable(1)
	if err := deleteSecondaryEntriesForDocument(table, runtime, state, documentID); err != nil {
		t.Fatalf("delete secondary entries: %v", err)
	}

	for i := range encoded {
		encoded[i] = 0
	}
	for i := range documentID {
		documentID[i] = 0
	}
	table.Freeze()
	defer resetCollectionRunTable(table)

	it := table.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		t.Fatal("delete table is empty")
	}
	if got := it.UnsafeKey(); !bytes.Equal(got, wantKey) {
		t.Fatalf("delete key=%q want %q", got, wantKey)
	}
	if !it.IsDeleted() {
		t.Fatal("delete entry is not a tombstone")
	}
	it.Next()
	if it.Valid() {
		t.Fatalf("unexpected extra delete entry %q", it.UnsafeKey())
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
}

func TestCollectionCatalogCachesIndexRuntimesAndRootNames(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "profile.email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}
	catalog := newCollectionCatalog(meta, map[string]uint64{
		collectionPrimaryRootName(meta.Name): 11,
	})
	runtimes, err := catalog.cachedIndexRuntimes()
	if err != nil {
		t.Fatalf("cached index runtimes: %v", err)
	}
	if got, want := len(runtimes), 2; got != want {
		t.Fatalf("cached runtime count=%d want %d", got, want)
	}
	if got, want := catalog.primaryRootName, "users/primary"; got != want {
		t.Fatalf("primary root name=%q want %q", got, want)
	}
	if got, want := runtimes[0].secondaryRootName, "users/index/email"; got != want {
		t.Fatalf("secondary root name=%q want %q", got, want)
	}
	if got, want := strings.Join(runtimes[0].path, "."), "profile.email"; got != want {
		t.Fatalf("runtime path=%q want %q", got, want)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		runtimes, _ := catalog.cachedIndexRuntimes()
		_ = runtimeSecondaryRootName(meta.Name, runtimes[0])
	}); allocs != 0 {
		t.Fatalf("cached runtime/root lookup allocs=%v want zero", allocs)
	}
}

func TestCollectionManagerStatsExposeIndexedWriteDomainMetrics(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	stats := mgr.StatsSnapshot()
	if got, want := stats.Domains, 1; got != want {
		t.Fatalf("stats domains=%d want %d", got, want)
	}
	if got, want := stats.PendingDocuments, 2; got != want {
		t.Fatalf("stats pending documents=%d want %d", got, want)
	}
	if stats.PendingBytes == 0 {
		t.Fatal("stats pending bytes=0 want positive")
	}
	if stats.PendingRootRuns == 0 {
		t.Fatal("stats pending root runs=0 want positive")
	}
	if got, want := stats.OverlayMutableDocuments, 2; got != want {
		t.Fatalf("stats overlay mutable docs=%d want %d", got, want)
	}
	if got := stats.OverlayQueuedIndexedFlushUnits; got != 0 {
		t.Fatalf("stats overlay queued flush units=%d want 0", got)
	}
	if got := stats.OverlayActiveIndexedFlushUnits; got != 0 {
		t.Fatalf("stats overlay active flush units=%d want 0", got)
	}
	if got, want := stats.OverlayVisibleDepth, 1; got != want {
		t.Fatalf("stats overlay visible depth=%d want %d", got, want)
	}
	if got, want := stats.IndexedStageBatches, uint64(1); got != want {
		t.Fatalf("stats indexed stage batches=%d want %d", got, want)
	}
	if got, want := stats.IndexedStageDocs, uint64(2); got != want {
		t.Fatalf("stats indexed stage docs=%d want %d", got, want)
	}
	if stats.IndexedStageBytes == 0 || stats.IndexedStageRootRuns == 0 {
		t.Fatalf("stats indexed stage bytes/root-runs=%d/%d want positive", stats.IndexedStageBytes, stats.IndexedStageRootRuns)
	}
	if stats.MutationLockCalls == 0 {
		t.Fatal("stats mutation lock calls=0 want positive")
	}
	exported := mgr.Stats()
	for _, key := range []string{
		"treedb.collections.write_domain.pending_docs",
		"treedb.collections.write_domain.pending_indexed_flush_units",
		"treedb.collections.write_domain.overlay.mutable_docs",
		"treedb.collections.write_domain.overlay.queued_indexed_flush_units",
		"treedb.collections.write_domain.overlay.active_indexed_flush_units",
		"treedb.collections.write_domain.overlay.visible_depth",
		"treedb.collections.write_domain.indexed_stage.batches_total",
		"treedb.collections.write_domain.insert.validation_preflight_reused_total",
		"treedb.collections.write_domain.insert.validation_preflight_rechecked_total",
		"treedb.collections.write_domain.indexed_async_flush.scheduled_total",
		"treedb.collections.write_domain.indexed_async_flush.wait_ns_total",
		"treedb.collections.write_domain.mutation_lock.calls_total",
	} {
		if exported[key] == "" {
			t.Fatalf("exported stats missing %s from %#v", key, exported)
		}
	}
	domain := col.writeDomain
	domain.mu.Lock()
	if !rotateIndexedMutableToFlushUnitLocked(domain) {
		domain.mu.Unlock()
		t.Fatal("rotate mutable indexed runs to queued unit returned false")
	}
	domain.mu.Unlock()
	stats = mgr.StatsSnapshot()
	if got, want := stats.OverlayMutableDocuments, 0; got != want {
		t.Fatalf("stats overlay mutable docs after rotate=%d want %d", got, want)
	}
	if got, want := stats.OverlayQueuedIndexedFlushUnits, 1; got != want {
		t.Fatalf("stats overlay queued flush units after rotate=%d want %d", got, want)
	}
	if got := stats.OverlayActiveIndexedFlushUnits; got != 0 {
		t.Fatalf("stats overlay active flush units after rotate=%d want 0", got)
	}
	if got, want := stats.OverlayVisibleDepth, 1; got != want {
		t.Fatalf("stats overlay visible depth after rotate=%d want %d", got, want)
	}

	if err := mgr.FlushAll(); err != nil {
		t.Fatalf("flush all: %v", err)
	}
	stats = mgr.StatsSnapshot()
	if got := stats.PendingDocuments; got != 0 {
		t.Fatalf("stats pending documents after flush=%d want 0", got)
	}
	if got, want := stats.IndexedFlushCalls, uint64(1); got != want {
		t.Fatalf("stats indexed flush calls=%d want %d", got, want)
	}
	if got, want := stats.IndexedFlushUnits, uint64(1); got != want {
		t.Fatalf("stats indexed flush units=%d want %d", got, want)
	}
	if got, want := stats.IndexedFlushDocs, uint64(2); got != want {
		t.Fatalf("stats indexed flush docs=%d want %d", got, want)
	}
	if stats.IndexedFlushBytes == 0 || stats.IndexedFlushRootRuns == 0 || stats.IndexedFlushRoots == 0 {
		t.Fatalf("stats indexed flush bytes/root-runs/roots=%d/%d/%d want positive", stats.IndexedFlushBytes, stats.IndexedFlushRootRuns, stats.IndexedFlushRoots)
	}
	if stats.IndexedFlushDuration <= 0 || stats.IndexedFlushMaterialize <= 0 || stats.IndexedFlushPointerize <= 0 || stats.IndexedFlushPublish <= 0 {
		t.Fatalf("stats indexed flush duration/materialize/pointerize/publish=%s/%s/%s/%s want positive", stats.IndexedFlushDuration, stats.IndexedFlushMaterialize, stats.IndexedFlushPointerize, stats.IndexedFlushPublish)
	}
	if stats.RootDeltaPlanPrimaryRoots == 0 || stats.RootDeltaPlanSecondaryRoots == 0 || stats.RootDeltaPlanEntries == 0 {
		t.Fatalf("stats root delta primary/secondary/entries=%d/%d/%d want positive", stats.RootDeltaPlanPrimaryRoots, stats.RootDeltaPlanSecondaryRoots, stats.RootDeltaPlanEntries)
	}
	if stats.RootDeltaPlanKeyBytes == 0 || stats.RootDeltaPlanValueBytes == 0 {
		t.Fatalf("stats root delta key/value bytes=%d/%d want positive", stats.RootDeltaPlanKeyBytes, stats.RootDeltaPlanValueBytes)
	}
	exported = mgr.Stats()
	for _, key := range []string{
		"treedb.collections.write_domain.indexed_flush.units_total",
		"treedb.collections.write_domain.indexed_flush.forced_drains_total",
		"treedb.collections.write_domain.indexed_flush.duration_ns_total",
		"treedb.collections.write_domain.indexed_flush.materialize_ns_total",
		"treedb.collections.write_domain.indexed_flush.pointerize_ns_total",
		"treedb.collections.write_domain.indexed_flush.publish_ns_total",
		"treedb.collections.write_domain.root_delta_plan.roots.primary_total",
		"treedb.collections.write_domain.root_delta_plan.roots.template_total",
		"treedb.collections.write_domain.root_delta_plan.roots.index_state_total",
		"treedb.collections.write_domain.root_delta_plan.roots.secondary_total",
		"treedb.collections.write_domain.root_delta_plan.entries_total",
		"treedb.collections.write_domain.root_delta_plan.key_bytes_total",
		"treedb.collections.write_domain.root_delta_plan.value_bytes_total",
		"treedb.collections.write_domain.root_delta_plan.tombstones_total",
		"treedb.collections.write_domain.primary_only.buffered_calls_total",
	} {
		if exported[key] == "" {
			t.Fatalf("exported stats missing %s from %#v", key, exported)
		}
	}
}

func TestCollectionRootDeltaPlanStatsCountsPointerValueBytes(t *testing.T) {
	delta := batch.New(nil, 0)
	defer func() { _ = delta.Close() }()
	ptr := page.ValuePtr{FileID: page.ValueLogFileID(1), Offset: 128, Length: 64}
	if err := delta.SetPointer([]byte("u1"), ptr); err != nil {
		t.Fatalf("set pointer delta: %v", err)
	}
	stats := collectionRootDeltaPlanStatsFromOrdered("users",
		[]string{collectionPrimaryRootName("users")},
		[]backenddb.OrderedRootDeltaBatchPublishInput{{Delta: delta}},
	)
	if got, want := stats.valueBytes, uint64(page.ValuePtrSize); got != want {
		t.Fatalf("root delta value bytes=%d want pointer payload %d", got, want)
	}
	if got, want := stats.primaryValueBytes, uint64(page.ValuePtrSize); got != want {
		t.Fatalf("primary root delta value bytes=%d want pointer payload %d", got, want)
	}
}

func TestPrimaryOnlyNoIndexUpdateCounters(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada","city":"hnl"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	statsBefore := mgr.StatsSnapshot()
	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"name":"ada","city":"sea"}`), true, nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("update matched/modified=%v/%v want true/true", matched, modified)
	}
	stats := mgr.StatsSnapshot()
	if got, want := stats.PrimaryOnlyUpdateCalls-statsBefore.PrimaryOnlyUpdateCalls, uint64(1); got != want {
		t.Fatalf("primary-only update calls delta=%d want %d", got, want)
	}
	if got, want := stats.PrimaryOnlyMatched-statsBefore.PrimaryOnlyMatched, uint64(1); got != want {
		t.Fatalf("primary-only matched delta=%d want %d", got, want)
	}
	if got, want := stats.PrimaryOnlyModified-statsBefore.PrimaryOnlyModified, uint64(1); got != want {
		t.Fatalf("primary-only modified delta=%d want %d", got, want)
	}
	if got, want := stats.PrimaryOnlyRootPublishes-statsBefore.PrimaryOnlyRootPublishes, uint64(1); got != want {
		t.Fatalf("primary-only root publishes delta=%d want %d", got, want)
	}
	if got, want := stats.PrimaryOnlyRootDeltaEntries-statsBefore.PrimaryOnlyRootDeltaEntries, uint64(1); got != want {
		t.Fatalf("primary-only root delta entries delta=%d want %d", got, want)
	}
	if got := stats.PrimaryOnlyRootDeltaKeyBytes - statsBefore.PrimaryOnlyRootDeltaKeyBytes; got != uint64(len("u1")) {
		t.Fatalf("primary-only root delta key bytes delta=%d want %d", got, len("u1"))
	}
	if got := stats.PrimaryOnlyRootDeltaValueBytes - statsBefore.PrimaryOnlyRootDeltaValueBytes; got == 0 {
		t.Fatal("primary-only root delta value bytes delta=0 want positive")
	}
	if got, want := stats.PrimaryOnlyCoalescedDocs-statsBefore.PrimaryOnlyCoalescedDocs, uint64(1); got != want {
		t.Fatalf("primary-only coalesced docs delta=%d want %d", got, want)
	}
	exported := mgr.Stats()
	for _, key := range []string{
		"treedb.collections.write_domain.primary_only.update_calls_total",
		"treedb.collections.write_domain.primary_only.matched_total",
		"treedb.collections.write_domain.primary_only.modified_total",
		"treedb.collections.write_domain.primary_only.root_publishes_total",
		"treedb.collections.write_domain.primary_only.root_delta_entries_total",
		"treedb.collections.write_domain.primary_only.root_delta_key_bytes_total",
		"treedb.collections.write_domain.primary_only.root_delta_value_bytes_total",
		"treedb.collections.write_domain.primary_only.coalesced_docs_total",
	} {
		if exported[key] == "" {
			t.Fatalf("exported stats missing %s from %#v", key, exported)
		}
	}
}

func TestCollectionManagerResetUpdateCombineQueueDepthMax(t *testing.T) {
	domain := &collectionWriteDomain{}
	mgr := &CollectionManager{
		domains: map[string]*collectionWriteDomain{"users": domain},
	}
	domain.observeUpdateCombineRequest(12)
	if got := mgr.StatsSnapshot().UpdateCombineQueueDepthMax; got != 12 {
		t.Fatalf("queue depth max before reset=%d want 12", got)
	}
	mgr.ResetUpdateCombineQueueDepthMax()
	if got := mgr.StatsSnapshot().UpdateCombineQueueDepthMax; got != 0 {
		t.Fatalf("queue depth max after reset=%d want 0", got)
	}
	domain.observeUpdateCombineRequest(5)
	if got := mgr.StatsSnapshot().UpdateCombineQueueDepthMax; got != 5 {
		t.Fatalf("queue depth max after new observation=%d want 5", got)
	}
}

func TestCollectionManagerResetUpdateCombinersForProfiling(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"name":"ada"}`)}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	domain := col.writeDomain
	if domain == nil {
		t.Fatal("collection write domain is nil")
	}
	first := col.updateCombiner()
	if first == nil {
		t.Fatal("first update combiner is nil")
	}
	domain.updateCombineMu.Lock()
	draining := domain.updateDraining
	domain.updateCombineMu.Unlock()
	if draining != nil {
		t.Fatal("unexpected draining combiner before reset")
	}

	mgr.ResetUpdateCombinersForProfiling()
	domain.updateCombineMu.Lock()
	afterReset := domain.updateCombiner
	afterResetDraining := domain.updateDraining
	domain.updateCombineMu.Unlock()
	if afterReset != nil || afterResetDraining != nil {
		t.Fatalf("combiner after reset=%p draining=%p want nil/nil", afterReset, afterResetDraining)
	}

	second := col.updateCombiner()
	if second == nil {
		t.Fatal("second update combiner is nil")
	}
	if second == first {
		t.Fatal("second update reused stopped combiner after profiling reset")
	}
}

func writeStandaloneValueLogSegment(t *testing.T, valueDir string, lane, seq uint32, value []byte) string {
	t.Helper()
	if err := os.MkdirAll(valueDir, 0o755); err != nil {
		t.Fatalf("mkdir value log dir: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID(%d,%d): %v", lane, seq, err)
	}
	path := valuelog.SegmentPath(valueDir, fileID)
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter(%s): %v", path, err)
	}
	if _, err := writer.Append(0, nil, uint64(seq), value); err != nil {
		_ = writer.Close()
		t.Fatalf("append value-log record: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close value-log writer: %v", err)
	}
	return path
}

func chooseStandaloneValueLogSegmentStart(t *testing.T, valueDir string) (uint32, uint32) {
	t.Helper()
	if err := os.MkdirAll(valueDir, 0o755); err != nil {
		t.Fatalf("mkdir value log dir: %v", err)
	}
	mgr, err := valuelog.NewManager(valueDir)
	if err != nil {
		t.Fatalf("new value-log manager for synthetic segment lane: %v", err)
	}
	defer func() { _ = mgr.Close() }()
	set := mgr.CurrentSet()
	defer func() { _ = mgr.Release(set) }()
	lane, startSeq := chooseStandaloneValueLogSegmentStartFromSet(set)
	if startSeq >= ^uint32(0)-1 {
		t.Fatalf("value-log lane %d sequence exhausted near %d", lane, startSeq)
	}
	return lane, startSeq + 1
}

func chooseStandaloneValueLogSegmentStartFromSet(set *valuelog.Set) (uint32, uint32) {
	maxSeqByLane := make(map[uint32]uint32)
	usedLane := make(map[uint32]struct{})
	var maxObservedLane uint32
	haveObservedLane := false
	if set != nil {
		for fileID := range set.Files {
			lane, seq := valuelog.DecodeFileID(fileID)
			if lane == valuelog.ReservedLeafLogLaneID {
				continue
			}
			if !haveObservedLane || lane > maxObservedLane {
				maxObservedLane = lane
				haveObservedLane = true
			}
			usedLane[lane] = struct{}{}
			if seq > maxSeqByLane[lane] {
				maxSeqByLane[lane] = seq
			}
		}
	}
	if valuelog.ReservedLeafLogLaneID > 0 {
		for lane := uint32(valuelog.ReservedLeafLogLaneID - 1); ; lane-- {
			if _, used := usedLane[lane]; !used {
				return lane, 0
			}
			if lane == 0 {
				break
			}
		}
		return 0, maxSeqByLane[0]
	}
	if !haveObservedLane || maxObservedLane == ^uint32(0) {
		return 1, 0
	}
	return maxObservedLane + 1, 0
}

func TestChooseStandaloneValueLogSegmentStartFromSetSkipsReservedLeafLane(t *testing.T) {
	leafID, err := valuelog.EncodeFileID(valuelog.ReservedLeafLogLaneID, 3)
	if err != nil {
		t.Fatalf("EncodeFileID reserved lane: %v", err)
	}
	userID, err := valuelog.EncodeFileID(valuelog.ReservedLeafLogLaneID-1, 7)
	if err != nil {
		t.Fatalf("EncodeFileID user lane: %v", err)
	}
	set := &valuelog.Set{Files: map[uint32]*valuelog.File{
		leafID: &valuelog.File{ID: leafID},
		userID: &valuelog.File{ID: userID},
	}}

	lane, seq := chooseStandaloneValueLogSegmentStartFromSet(set)
	if want := uint32(valuelog.ReservedLeafLogLaneID - 2); lane != want || seq != 0 {
		t.Fatalf("lane=%d seq=%d want lane=%d seq=0", lane, seq, want)
	}
}

func TestChooseStandaloneValueLogSegmentStartFromSetFallsBackToLaneZero(t *testing.T) {
	files := make(map[uint32]*valuelog.File)
	for lane := uint32(0); lane < uint32(valuelog.ReservedLeafLogLaneID); lane++ {
		seq := uint32(1)
		if lane == 0 {
			seq = 9
		}
		id, err := valuelog.EncodeFileID(lane, seq)
		if err != nil {
			t.Fatalf("EncodeFileID lane=%d seq=%d: %v", lane, seq, err)
		}
		files[id] = &valuelog.File{ID: id}
	}

	lane, seq := chooseStandaloneValueLogSegmentStartFromSet(&valuelog.Set{Files: files})
	if lane != 0 || seq != 9 {
		t.Fatalf("lane=%d seq=%d want lane=0 seq=9", lane, seq)
	}
}

func TestChooseStandaloneValueLogSegmentStartFromSetTracksSeqZero(t *testing.T) {
	fileID, err := valuelog.EncodeFileID(valuelog.ReservedLeafLogLaneID-1, 0)
	if err != nil {
		t.Fatalf("EncodeFileID seq zero: %v", err)
	}
	set := &valuelog.Set{Files: map[uint32]*valuelog.File{
		fileID: {ID: fileID},
	}}

	lane, seq := chooseStandaloneValueLogSegmentStartFromSet(set)
	if want := uint32(valuelog.ReservedLeafLogLaneID - 2); lane != want || seq != 0 {
		t.Fatalf("lane=%d seq=%d want lane=%d seq=0", lane, seq, want)
	}
}

func requireCollectionMaintenanceReads(t *testing.T, col *Collection) {
	t.Helper()
	got, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if want := []byte(`{"email":"grace@example.com","city":"hnl","pad":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`); !bytes.Equal(got, want) {
		t.Fatalf("u2=%q want %q", got, want)
	}
	emailIDs, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u1")) {
		t.Fatalf("email ids=%q want [u1]", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, cityIDs, []byte("u1"), []byte("u2"))
}

func collectionMaintenanceTemplateBatch(t *testing.T, encoder *TemplateV1Encoder, start, count int) ([][]byte, [][]byte) {
	t.Helper()
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		n := start + i
		ids[i] = collectionMaintenanceTemplateID(n)
		doc, err := encoder.EncodeDocument(
			[]string{"name", "email", "city", "pad"},
			[]any{
				fmt.Sprintf("user-%09d", n),
				fmt.Sprintf("user-%09d@example.com", n),
				fmt.Sprintf("city-%02d", n%32),
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
		)
		if err != nil {
			t.Fatalf("encode template-v1 doc %d: %v", n, err)
		}
		docs[i] = doc
	}
	return ids, docs
}

func collectionMaintenanceTemplateID(n int) []byte {
	return []byte(fmt.Sprintf("u-%09d", n))
}

func requireCollectionMaintenanceTemplateReads(t *testing.T, col *Collection) {
	t.Helper()
	id := collectionMaintenanceTemplateID(2)
	got, err := col.Get(id)
	if err != nil {
		t.Fatalf("get %q: %v", id, err)
	}
	if !bytes.HasPrefix(got, []byte(templateV1StoredMagic)) {
		prefixLen := len(got)
		if prefixLen > len(templateV1StoredMagic) {
			prefixLen = len(templateV1StoredMagic)
		}
		t.Fatalf("stored %q prefix=%q want template-v1 stored magic", id, got[:prefixLen])
	}
	emailIDs, err := col.FindByIndex("email", "user-000000001@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], collectionMaintenanceTemplateID(1)) {
		t.Fatalf("email ids=%q want [u-000000001]", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city", "city-02")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if !collectionMaintenanceContainsID(cityIDs, id) {
		t.Fatalf("city ids=%q do not include %q", cityIDs, id)
	}
}

func collectionMaintenanceContainsID(ids [][]byte, want []byte) bool {
	for _, id := range ids {
		if bytes.Equal(id, want) {
			return true
		}
	}
	return false
}

// Non-unique index scans do not promise result ordering; assert set membership.
func collectionMaintenanceRequireUnorderedIDs(t *testing.T, got [][]byte, want ...[]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ids=%q want %q", got, want)
	}
	counts := make(map[string]int, len(got))
	for _, id := range got {
		counts[string(id)]++
	}
	for _, id := range want {
		key := string(id)
		if counts[key] == 0 {
			t.Fatalf("ids=%q missing %q", got, id)
		}
		counts[key]--
	}
}

func collectionMaintenanceTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	timeout := 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}
	return context.WithTimeout(context.Background(), timeout)
}

const collectionTestDeadlineBuffer = 500 * time.Millisecond

func collectionTestTimeout(t *testing.T, fallback time.Duration) time.Duration {
	t.Helper()
	if deadline, ok := t.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return time.Nanosecond
		}
		if remaining <= collectionTestDeadlineBuffer {
			return remaining
		}
		remaining -= collectionTestDeadlineBuffer
		if remaining > 0 && remaining < fallback {
			return remaining
		}
	}
	return fallback
}

func collectionMaintenanceCloseOnce(cleanup func() error) func() error {
	var once sync.Once
	var closeErr error
	return func() error {
		once.Do(func() {
			if cleanup != nil {
				closeErr = cleanup()
			}
		})
		return closeErr
	}
}

func TestCollectionInsertBatchStatsExposeNoIndexFastPath(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	stats := col.LastInsertStats()
	if got, want := stats.Documents, 2; got != want {
		t.Fatalf("stats documents=%d want %d", got, want)
	}
	if got, want := stats.Indexes, 0; got != want {
		t.Fatalf("stats indexes=%d want %d", got, want)
	}
	if got, want := stats.Runs, 1; got != want {
		t.Fatalf("stats runs=%d want %d", got, want)
	}
	if got := len(stats.SecondaryRuns); got != 0 {
		t.Fatalf("stats secondary runs=%d want 0", got)
	}
}

func TestCollectionInsertBatchBuffersNoIndexBSONBeforeFlush(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closeDB := collectionMaintenanceCloseOnce(d.Close)
	defer func() { _ = closeDB() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	writer, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	reader, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	docs := [][]byte{
		mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "ada"}}),
		mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "grace"}}),
	}
	wantU1 := bytes.Clone(docs[0])
	wantU2 := bytes.Clone(docs[1])
	if _, err := writer.InsertBatchValidatedBSON([][]byte{[]byte("u1"), []byte("u2")}, docs); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	docs[0][len(docs[0])-1] ^= 0xff
	if writer.writeDomain == nil {
		t.Fatal("missing write domain")
	}
	if writer.writeDomain.count != 2 || writer.writeDomain.table == nil || writer.writeDomain.table.Len() != 2 {
		t.Fatalf("write domain count=%d table=%v want two pending rows", writer.writeDomain.count, writer.writeDomain.table)
	}
	got, err := reader.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("reader get buffered BSON doc: %v", err)
	}
	if !bytes.Equal(got, wantU1) {
		t.Fatalf("buffered BSON doc=%v want %v", got, wantU1)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := writer.writeDomain.count; got != 0 {
		t.Fatalf("pending docs after flush=%d want 0", got)
	}
	if err := closeDB(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	got, err = reopenedCol.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if !bytes.Equal(got, wantU2) {
		t.Fatalf("reopened BSON doc=%v want %v", got, wantU2)
	}
}

func TestCollectionNoIndexBufferedInsertRejectsDuplicateAcrossOnlineVacuumSnapshot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	original := []byte(`{"name":"original"}`)
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{original}); err != nil {
		t.Fatalf("insert original: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush original: %v", err)
	}

	stale := d.AcquireSnapshot()
	if stale == nil {
		t.Fatal("acquire pre-vacuum snapshot")
	}
	defer func() { _ = stale.Close() }()
	stalePager := stale.Pager()
	staleCatalog, err := loadCollectionCatalog(stale, "users")
	if err != nil {
		t.Fatalf("load pre-vacuum catalog: %v", err)
	}
	options, err := collectionPlannerOptionsForDB(d, staleCatalog.meta)
	if err != nil {
		t.Fatalf("planner options: %v", err)
	}
	if err := d.VacuumIndexOnline(t.Context()); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	current := d.AcquireSnapshot()
	if current == nil {
		t.Fatal("acquire post-vacuum snapshot")
	}
	if current.Pager() == stalePager {
		_ = current.Close()
		t.Fatal("vacuum did not replace pager")
	}
	_ = current.Close()

	_, buffered, err := col.bufferNoIndexInsertBatch(
		col.writeDomain,
		staleCatalog,
		stale,
		options,
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"replacement"}`)},
		insertBatchExecutionOptions{},
	)
	if !buffered || !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("stale-snapshot duplicate buffered=%t err=%v want ErrDocumentExists", buffered, err)
	}
	if col.writeDomain.count != 0 {
		t.Fatalf("buffered duplicate count=%d want 0", col.writeDomain.count)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get original after rejected insert: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("document after rejected insert=%s want %s", got, original)
	}
}

func TestCollectionInsertBatchBuffersNoIndexJSONBeforeFlush(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	closeDB := collectionMaintenanceCloseOnce(d.Close)
	defer func() { _ = closeDB() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	writer, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	reader, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	firstDocs := [][]byte{
		[]byte(`{"name":"ada"}`),
		[]byte(`{"name":"grace"}`),
	}
	firstIDs := [][]byte{[]byte("u1"), []byte("u2")}
	secondDocs := [][]byte{
		[]byte(`{"name":"katherine"}`),
	}
	secondIDs := [][]byte{[]byte("u3")}
	wantPending := len(firstDocs) + len(secondDocs)
	wantU1 := bytes.Clone(firstDocs[0])
	wantU2 := bytes.Clone(firstDocs[1])
	wantU3 := bytes.Clone(secondDocs[0])
	insertedIDs, err := writer.InsertBatch(firstIDs, firstDocs)
	if err != nil {
		t.Fatalf("first insert batch: %v", err)
	}
	if len(insertedIDs) != len(firstIDs) || !bytes.Equal(insertedIDs[0], firstIDs[0]) || !bytes.Equal(insertedIDs[1], firstIDs[1]) {
		t.Fatalf("inserted ids=%q want %q", insertedIDs, firstIDs)
	}
	insertedIDs, err = writer.InsertBatch(secondIDs, secondDocs)
	if err != nil {
		t.Fatalf("second insert batch: %v", err)
	}
	if len(insertedIDs) != len(secondIDs) || !bytes.Equal(insertedIDs[0], secondIDs[0]) {
		t.Fatalf("second inserted ids=%q want %q", insertedIDs, secondIDs)
	}
	firstDocs[0][len(firstDocs[0])-2] = 'x'
	secondDocs[0][len(secondDocs[0])-2] = 'x'
	insertedIDs[0][0] = 'x'
	if writer.writeDomain == nil {
		t.Fatal("missing write domain")
	}
	if writer.writeDomain.count != wantPending || writer.writeDomain.table == nil || writer.writeDomain.table.Len() != wantPending {
		t.Fatalf("write domain count=%d table=%v want %d pending rows", writer.writeDomain.count, writer.writeDomain.table, wantPending)
	}
	got, err := reader.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("reader get buffered JSON doc: %v", err)
	}
	if !bytes.Equal(got, wantU1) {
		t.Fatalf("buffered JSON doc=%v want %v", got, wantU1)
	}
	got, err = reader.Get([]byte("u3"))
	if err != nil {
		t.Fatalf("reader get second buffered JSON doc: %v", err)
	}
	if !bytes.Equal(got, wantU3) {
		t.Fatalf("second buffered JSON doc=%v want %v", got, wantU3)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := writer.writeDomain.count; got != 0 {
		t.Fatalf("pending docs after flush=%d want 0", got)
	}
	if err := closeDB(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	got, err = reopenedCol.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if !bytes.Equal(got, wantU2) {
		t.Fatalf("reopened JSON doc=%v want %v", got, wantU2)
	}
}

func TestCollectionInsertBatchValidatedBSONNoIndexDurablePublishesBeforeReturn(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	before := d.State()
	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "ada"}}),
			mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "grace"}}),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if got := col.writeDomain.count; got != 0 {
		t.Fatalf("pending docs=%d want durable publish before ack", got)
	}
	after := d.State()
	if after.CommitSeq < before.CommitSeq+1 {
		t.Fatalf("commit seq advanced by %d want at least 1", after.CommitSeq-before.CommitSeq)
	}
}

func TestCollectionInsertBatchBridge_ReturnedIDsAndDocumentsAreOwned(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	inputID := []byte("u1")
	inputDocument := []byte(`{"name":"ada"}`)
	ids, err := col.InsertBatch(
		[][]byte{inputID},
		[][]byte{inputDocument},
	)
	if err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	inputID[0] = 'x'
	inputDocument[9] = 'x'
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("returned ids=%q want owned u1", ids)
	}

	ids[0][0] = 'z'
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get original id after mutating returned id: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("original id value=%q want %q", got, want)
	}
	if got, err := col.Get([]byte("z1")); err != nil || got != nil {
		t.Fatalf("mutated returned id lookup got=%q err=%v want missing", got, err)
	}
}

func TestCollectionInsertBatchBridge_IndexedReturnedIDsAreOwned(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	inputID := []byte("u1")
	inputDocument := []byte(`{"email":"ada@example.com","city":"hnl"}`)
	ids, err := col.InsertBatch(
		[][]byte{inputID},
		[][]byte{inputDocument},
	)
	if err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	inputID[0] = 'x'
	inputDocument[10] = 'x'
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("returned ids=%q want owned u1", ids)
	}

	ids[0][0] = 'z'
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get original id after mutating returned id: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("original id value=%q want %q", got, want)
	}
	if got, err := col.Get([]byte("z1")); err != nil || got != nil {
		t.Fatalf("mutated returned id lookup got=%q err=%v want missing", got, err)
	}
}

func TestCollectionInsertBatchBridge_ValidatedBSONReturnedIDsAreOwned(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	inputID := []byte("u1")
	inputDocument, err := bson.Marshal(bson.D{{Key: "name", Value: "ada"}})
	if err != nil {
		t.Fatalf("marshal bson: %v", err)
	}
	ids, err := col.InsertBatchValidatedBSON(
		[][]byte{inputID},
		[][]byte{inputDocument},
	)
	if err != nil {
		t.Fatalf("insert batch validated bson: %v", err)
	}
	inputID[0] = 'x'
	inputDocument[len(inputDocument)-2] = 'x'
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("returned ids=%q want owned u1", ids)
	}

	ids[0][0] = 'z'
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get original id after mutating returned id: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("get original id returned empty document")
	}
	if got, err := col.Get([]byte("z1")); err != nil || got != nil {
		t.Fatalf("mutated returned id lookup got=%q err=%v want missing", got, err)
	}
}

func TestCollectionInsertBatchSingleDirectBufferedBSONStats(t *testing.T) {
	_, col := newSingleDirectBufferedBSONUsersCollection(t)
	indexDefs := singleDirectBufferedUserIndexes()
	wantIndexes := len(indexDefs)

	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "email", Value: "ada@example.com"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert single direct-buffered bson: %v", err)
	}

	stats := col.LastInsertStats()
	if stats.Documents != 1 || stats.Indexes != wantIndexes {
		t.Fatalf("stats documents/indexes=%d/%d want 1/%d", stats.Documents, stats.Indexes, wantIndexes)
	}
	if stats.BufferedIndexedBatches != 1 || stats.BufferedIndexedBypassBatches != 0 {
		t.Fatalf("buffered stats batches=%d bypass=%d want 1/0", stats.BufferedIndexedBatches, stats.BufferedIndexedBypassBatches)
	}
	if wantRuns := 1 + wantIndexes; stats.Runs != wantRuns {
		t.Fatalf("runs=%d want %d primary plus secondary roots", stats.Runs, wantRuns)
	}
	if stats.SecondaryEntries != wantIndexes || stats.SecondarySortedRuns != wantIndexes || stats.SecondaryUnsortedRuns != 0 {
		t.Fatalf("secondary stats entries=%d sorted=%d unsorted=%d want %d/%d/0", stats.SecondaryEntries, stats.SecondarySortedRuns, stats.SecondaryUnsortedRuns, wantIndexes, wantIndexes)
	}
	if len(stats.SecondaryRuns) != wantIndexes {
		t.Fatalf("secondary run stats=%d want %d", len(stats.SecondaryRuns), wantIndexes)
	}
	for _, run := range stats.SecondaryRuns {
		if run.Entries != 1 || !run.AlreadySorted {
			t.Fatalf("secondary run %+v want one already-sorted entry", run)
		}
	}
}

func TestCollectionInsertBatchSingleDirectBufferedBSONRejectsPendingDocumentID(t *testing.T) {
	_, col := newSingleDirectBufferedBSONUsersCollection(t)

	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: "ada@example.com"}, {Key: "city", Value: "hnl"}})},
	); err != nil {
		t.Fatalf("insert pending document: %v", err)
	}
	_, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: "grace@example.com"}, {Key: "city", Value: "sea"}})},
	)
	if !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("duplicate pending document id err=%v want ErrDocumentExists", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if gotCity := bson.Raw(got).Lookup("city").StringValue(); gotCity != "hnl" {
		t.Fatalf("u1 city=%q want original pending document", gotCity)
	}
}

func TestCollectionInsertBatchSingleDirectBufferedBSONRejectsPendingUniqueConflict(t *testing.T) {
	_, col := newSingleDirectBufferedBSONUsersCollection(t)

	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: "ada@example.com"}, {Key: "city", Value: "hnl"}})},
	); err != nil {
		t.Fatalf("insert pending unique document: %v", err)
	}
	_, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("u2")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "email", Value: "ada@example.com"}, {Key: "city", Value: "sea"}})},
	)
	if !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate pending unique err=%v want ErrUniqueIndexConflict", err)
	}
	got, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if got != nil {
		t.Fatalf("u2=%q want no staged document after unique conflict", got)
	}
}

func TestCollectionInsertBatchSingleDirectBufferedBSONRejectsOversizedSecondaryKey(t *testing.T) {
	_, col := newSingleDirectBufferedBSONUsersCollection(t)

	_, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "email", Value: "ada@example.com"},
			{Key: "city", Value: strings.Repeat("x", 70000)},
		})},
	)
	if err == nil || !strings.Contains(err.Error(), "index key too large") {
		t.Fatalf("oversized secondary key err=%v want index key too large", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if got != nil {
		t.Fatalf("u1=%q want no staged document after oversized secondary key", got)
	}
}

func TestCollectionInsertBatchSingleDirectBufferedTemplateV1StagesExpectedRoots(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	mgr := NewCollectionManager(d)
	opts := bufferedIndexedUpdateNoAsyncHighThresholdOptionsForTests()
	opts.DocumentFormat = DocumentFormatTemplateV1
	indexDefs := singleDirectBufferedUserIndexes()
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: opts,
		Indexes: indexDefs,
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustTemplateV1Document(t, []string{"email", "city"}, []any{"ada@example.com", "hnl"})},
	); err != nil {
		t.Fatalf("insert single template-v1 document: %v", err)
	}
	stats := col.LastInsertStats()
	wantIndexes := len(indexDefs)
	wantRuns := 2 + wantIndexes
	if stats.Documents != 1 || stats.Indexes != wantIndexes || stats.Runs != wantRuns {
		t.Fatalf("stats documents/indexes/runs=%d/%d/%d want 1/%d/%d", stats.Documents, stats.Indexes, stats.Runs, wantIndexes, wantRuns)
	}
	if stats.SecondaryEntries != wantIndexes || stats.SecondarySortedRuns != wantIndexes || stats.SecondaryUnsortedRuns != 0 {
		t.Fatalf("secondary stats entries=%d sorted=%d unsorted=%d want %d/%d/0", stats.SecondaryEntries, stats.SecondarySortedRuns, stats.SecondaryUnsortedRuns, wantIndexes, wantIndexes)
	}

	expectedRoots := []string{
		collectionTemplateRootName("users"),
		collectionPrimaryRootName("users"),
	}
	for _, indexDef := range indexDefs {
		expectedRoots = append(expectedRoots, collectionSecondaryRootName("users", indexDef.Name))
	}
	col.writeDomain.mu.RLock()
	defer col.writeDomain.mu.RUnlock()
	for _, rootName := range expectedRoots {
		if got := len(col.writeDomain.rootRuns[rootName]); got != 1 {
			t.Fatalf("pending root %q runs=%d want 1", rootName, got)
		}
	}
	if got := len(col.writeDomain.uniqueValueRuns["email"]); got != 1 {
		t.Fatalf("pending email unique runs=%d want 1", got)
	}
	if got := col.writeDomain.count; got != 1 {
		t.Fatalf("pending count=%d want 1", got)
	}
}

func TestCollectionNativewireInsertBatchNoResultIDsUpdatesVectorIndex(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	index, err := newVectorIndex(col, VectorIndexOptions{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
	})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	if err := col.RegisterVectorIndex(index); err != nil {
		t.Fatalf("register vector index: %v", err)
	}

	inputID := []byte("a")
	if err := col.NativewireInsertBatchNoResultIDs(
		[][]byte{inputID},
		[][]byte{[]byte(`{"embedding":[1,0]}`)},
		false,
	); err != nil {
		t.Fatalf("nativewire insert no result ids: %v", err)
	}
	inputID[0] = 'z'

	results, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a")
	if got, err := col.Get([]byte("z")); err != nil || got != nil {
		t.Fatalf("mutated request id lookup got=%q err=%v want missing", got, err)
	}
}

func singleDirectBufferedUserIndexes() []IndexDefinition {
	return []IndexDefinition{
		{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
		{Name: "city", Field: "city", ValueType: IndexValueString},
	}
}

func newSingleDirectBufferedBSONUsersCollection(t *testing.T) (*backenddb.DB, *Collection) {
	t.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	mgr := NewCollectionManager(d)
	opts := bufferedIndexedUpdateNoAsyncHighThresholdOptionsForTests()
	opts.DocumentFormat = DocumentFormatBSON
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: opts,
		Indexes: singleDirectBufferedUserIndexes(),
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	return d, col
}

func TestCollectionSingleInsertBufferedNoIndexReadsBeforeFlush(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	writer, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	reader, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}

	id := []byte("u1")
	doc := []byte(`{"name":"ada"}`)
	gotID, err := writer.Insert(id, doc)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	id[0] = 'x'
	doc[9] = 'x'
	if !bytes.Equal(gotID, []byte("u1")) {
		t.Fatalf("returned id=%q want u1", gotID)
	}
	gotID[0] = 'z'

	got, err := reader.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("reader get buffered doc: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("buffered doc=%q want %q", got, want)
	}
	got[9] = 'x'
	again, err := writer.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("writer get buffered doc again: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(again, want) {
		t.Fatalf("buffered doc after caller mutation=%q want %q", again, want)
	}
}

func TestCollectionCheckReadableRejectsClosedBackendWithBufferedDocument(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := newCollectionManager(d, collectionManagerOptions{})
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte(" "), []byte(`{"sentinel":true}`)); err != nil {
		t.Fatalf("insert buffered sentinel: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	if got, err := col.Get([]byte(" ")); err != nil || got == nil {
		t.Fatalf("buffered sentinel after close got=%q err=%v", got, err)
	}
	if err := col.CheckReadable(); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("CheckReadable err=%v want backend closed", err)
	}
}

func TestCollectionGetIntoReusesCallerBuffer(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"ada"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	buf := make([]byte, 0, 64)
	base := buf[:cap(buf)]
	got, found, err := col.GetInto([]byte("u1"), buf)
	if err != nil {
		t.Fatalf("GetInto: %v", err)
	}
	if !found {
		t.Fatal("GetInto found=false want true")
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("GetInto=%q want %q", got, want)
	}
	if len(got) == 0 || &got[0] != &base[0] {
		t.Fatal("GetInto did not reuse caller buffer")
	}

	got[9] = 'x'
	again, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get after caller mutation: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(again, want) {
		t.Fatalf("stored document mutated through GetInto result: got %q want %q", again, want)
	}
}

func TestCollectionGetPreservesEmptyDocument(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte{}},
	); err != nil {
		t.Fatalf("insert empty document: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get empty document: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("Get empty document got=%v len=%d want non-nil empty", got, len(got))
	}

	gotInto, found, err := col.GetInto([]byte("u1"), []byte("stale"))
	if err != nil {
		t.Fatalf("GetInto empty document: %v", err)
	}
	if !found || len(gotInto) != 0 {
		t.Fatalf("GetInto empty document got=%q found=%t want empty true", gotInto, found)
	}
}

func TestCollectionGetIntoBufferedReusesCallerBuffer(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	writer, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	reader, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	if _, err := writer.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	buf := make([]byte, 0, 64)
	base := buf[:cap(buf)]
	got, found, err := reader.GetInto([]byte("u1"), buf)
	if err != nil {
		t.Fatalf("GetInto buffered: %v", err)
	}
	if !found {
		t.Fatal("GetInto buffered found=false want true")
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("GetInto buffered=%q want %q", got, want)
	}
	if len(got) == 0 || &got[0] != &base[0] {
		t.Fatal("GetInto buffered did not reuse caller buffer")
	}
	got[9] = 'x'
	again, err := writer.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get after buffered caller mutation: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(again, want) {
		t.Fatalf("buffered document mutated through GetInto result: got %q want %q", again, want)
	}
}

func TestCollectionGetIntoMissingAndDeleted(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	buf := []byte("stale")
	got, found, err := col.GetInto([]byte("missing"), buf)
	if err != nil {
		t.Fatalf("GetInto missing: %v", err)
	}
	if found || len(got) != 0 {
		t.Fatalf("GetInto missing got=%q found=%t want empty false", got, found)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"ada"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, found, err = col.GetInto([]byte("u1"), buf)
	if err != nil {
		t.Fatalf("GetInto deleted: %v", err)
	}
	if found || len(got) != 0 {
		t.Fatalf("GetInto deleted got=%q found=%t want empty false", got, found)
	}
}

func TestCollectionSingleInsertBufferedNoIndexFlushPersistsAfterReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	got, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("reopened doc=%q want %q", got, want)
	}
}

func TestCollectionIndexedWriteMemtablesReadUniqueAndFlush(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2"), []byte("u1")},
		[][]byte{
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get buffered u1: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("buffered u1=%q want %q", got, want)
	}
	emailIDs, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find buffered email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u1")) {
		t.Fatalf("buffered email ids=%q want [u1]", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find buffered city: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, cityIDs, []byte("u1"), []byte("u2"))

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if got := catalog.rootID(collectionPrimaryRootName("users")); got != 0 {
		t.Fatalf("primary root persisted before flush: %d", got)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u3")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"sea"}`)},
	); err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("buffered duplicate unique err=%v want unique index conflict", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u4")},
		[][]byte{[]byte(`{"email":"grace@example.com","city":"sea"}`)},
	); err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("buffered duplicate unique after iterator advance err=%v want unique index conflict", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"new@example.com","city":"sea"}`)},
	); err == nil || !strings.Contains(err.Error(), "document already exists") {
		t.Fatalf("buffered duplicate id err=%v want document exists", err)
	}

	if err := col.Flush(); err != nil {
		t.Fatalf("flush indexed buffer: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	got, err = reopenedCol.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get reopened u2: %v", err)
	}
	if want := []byte(`{"email":"grace@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("reopened u2=%q want %q", got, want)
	}
	emailIDs, err = reopenedCol.FindByIndex("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find reopened email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u2")) {
		t.Fatalf("reopened email ids=%q want [u2]", emailIDs)
	}
}

func TestCollectionIndexedFlushUnitsReadUniqueUpdateAndFlush(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl","score":1}`),
			[]byte(`{"email":"grace@example.com","city":"hnl","score":2}`),
		},
	); err != nil {
		t.Fatalf("insert initial batch: %v", err)
	}

	domain := col.writeDomain
	domain.mu.Lock()
	if !rotateIndexedMutableToFlushUnitLocked(domain) {
		t.Fatal("rotate indexed mutable state returned false")
	}
	if got := len(domain.rootRuns); got != 0 {
		t.Fatalf("mutable root runs after rotate=%d want 0", got)
	}
	if got := len(domain.indexedFlushUnits); got != 1 {
		t.Fatalf("flush units after rotate=%d want 1", got)
	}
	domain.mu.Unlock()

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get flush-unit u1: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"hnl","score":1}`); !bytes.Equal(got, want) {
		t.Fatalf("flush-unit u1=%q want %q", got, want)
	}
	emailIDs, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find flush-unit email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u1")) {
		t.Fatalf("flush-unit email ids=%q want [u1]", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find flush-unit city: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, cityIDs, []byte("u1"), []byte("u2"))

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u3")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"sea","score":3}`)},
	); err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("duplicate unique against flush unit err=%v want unique index conflict", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"new@example.com","city":"sea","score":4}`)},
	); err == nil || !strings.Contains(err.Error(), "document already exists") {
		t.Fatalf("duplicate primary against flush unit err=%v want document exists", err)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u3")},
		[][]byte{[]byte(`{"email":"katherine@example.com","city":"hnl","score":3}`)},
	); err != nil {
		t.Fatalf("insert mutable batch after flush unit: %v", err)
	}
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			if !bytes.Contains(current, []byte(`"city":"hnl"`)) {
				return nil, false, fmt.Errorf("update saw current document %s, want buffered hnl document", current)
			}
			return []byte(`{"email":"ada@example.com","city":"sea","score":10}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("update buffered flush-unit document: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("update results=%+v want matched modified", results)
	}

	cityIDs, err = col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find hnl after buffered update: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, cityIDs, []byte("u2"), []byte("u3"))
	cityIDs, err = col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea after buffered update: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, cityIDs, []byte("u1"))

	if err := col.Flush(); err != nil {
		t.Fatalf("flush indexed flush units: %v", err)
	}
	if deleted, err := col.DeleteDocument([]byte("u2")); err != nil {
		t.Fatalf("delete after flush-unit flush: %v", err)
	} else if !deleted {
		t.Fatal("delete after flush-unit flush deleted=false want true")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	if _, found, err := reopenedCol.GetInto([]byte("u2"), nil); err != nil {
		t.Fatalf("get deleted u2 after reopen: %v", err)
	} else if found {
		t.Fatal("deleted u2 found after reopen")
	}
	cityIDs, err = reopenedCol.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find reopened sea: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, cityIDs, []byte("u1"))
	cityIDs, err = reopenedCol.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find reopened hnl: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, cityIDs, []byte("u3"))
}

func TestCollectionIndexedFlushUnitCloseFlushesRotatedState(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"email":"ada@example.com"}`)}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	col.writeDomain.mu.Lock()
	if !rotateIndexedMutableToFlushUnitLocked(col.writeDomain) {
		t.Fatal("rotate indexed mutable state returned false")
	}
	col.writeDomain.mu.Unlock()
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find reopened email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("reopened email ids=%q want [u1]", ids)
	}
}

func TestCollectionCatalogOverlayRootsAreVisibleAfterReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedAsyncFlush:        true,
			BufferedIndexedWriteMaxDocuments: 1,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush base collection: %v", err)
	}

	primaryOverlay := newCollectionRunTable(1)
	setCollectionRunValue(primaryOverlay, []byte("u1"), []byte(`{"email":"ada@example.com","city":"sea"}`))
	primaryOverlay.Freeze()
	defer resetCollectionRunTable(primaryOverlay)

	cityOverlay := newCollectionRunTable(2)
	oldCity, err := encodeIndexScalar(IndexValueString, "hnl")
	if err != nil {
		t.Fatalf("encode old city: %v", err)
	}
	if _, err := deleteCollectionSecondaryIndexEntry(cityOverlay, oldCity, []byte("u1")); err != nil {
		t.Fatalf("delete old city entry: %v", err)
	}
	newCity, err := encodeIndexScalar(IndexValueString, "sea")
	if err != nil {
		t.Fatalf("encode new city: %v", err)
	}
	if _, err := setCollectionSecondaryIndexEntry(cityOverlay, newCity, []byte("u1")); err != nil {
		t.Fatalf("set new city entry: %v", err)
	}
	cityOverlay.Freeze()
	defer resetCollectionRunTable(cityOverlay)

	primaryRootName := collectionPrimaryRootName("users")
	cityRootName := collectionSecondaryRootName("users", "city")
	_, overlayRootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]backenddb.OrderedRootPublishInput{
		{BaseRoot: 0, Iter: primaryOverlay.NewIterator(nil, nil)},
		{BaseRoot: 0, Iter: cityOverlay.NewIterator(nil, nil)},
	}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		snap := db.AcquireSnapshot()
		if snap == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = snap.Close() }()
		return buildSystemTargetIterator(snap, map[string][]byte{
			systemCollectionRootOverlayKey(primaryRootName): encodeRootIDList([]uint64{rootIDs[0]}),
			systemCollectionRootOverlayKey(cityRootName):    encodeRootIDList([]uint64{rootIDs[1]}),
		})
	})
	if err != nil {
		t.Fatalf("publish overlay roots: %v", err)
	}
	if len(overlayRootIDs) != 2 {
		t.Fatalf("overlay root ids len=%d want 2", len(overlayRootIDs))
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	snap := reopened.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire reopened snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load reopened catalog: %v", err)
	}
	if got := catalog.overlayRootIDs(cityRootName); !reflect.DeepEqual(got, []uint64{overlayRootIDs[1]}) {
		t.Fatalf("city overlay roots=%v want [%d]", got, overlayRootIDs[1])
	}
	if !catalog.overlayRootMayContainKey(primaryRootName, overlayRootIDs[0], []byte("definitely-missing")) {
		t.Fatalf("legacy overlay descriptor without filter must fall back to maybe-present")
	}
	rawOverlayIt, err := snap.IteratorAtRootWithOptions(overlayRootIDs[1], oldCity, prefixEnd(oldCity), backenddb.IteratorOptions{IncludeTombstones: true})
	if err != nil {
		t.Fatalf("open raw old city overlay iterator: %v", err)
	}
	if !rawOverlayIt.Valid() || !rawOverlayIt.IsDeleted() {
		t.Fatalf("raw old city overlay iterator valid/deleted=%v/%v key=%x", rawOverlayIt.Valid(), rawOverlayIt.Valid() && rawOverlayIt.IsDeleted(), rawOverlayIt.UnsafeKey())
	}
	_ = rawOverlayIt.Close()
	oldCityIt, err := collectionIteratorAtCatalogRoot(snap, catalog, cityRootName, oldCity, prefixEnd(oldCity), true)
	if err != nil {
		t.Fatalf("open old city overlay iterator: %v", err)
	}
	if oldCityIt == nil || !oldCityIt.Valid() || !oldCityIt.IsDeleted() {
		t.Fatalf("old city overlay iterator valid/deleted=%v/%v", oldCityIt != nil && oldCityIt.Valid(), oldCityIt != nil && oldCityIt.IsDeleted())
	}
	_ = oldCityIt.Close()
	hiddenOldCityIt, err := collectionIteratorAtCatalogRoot(snap, catalog, cityRootName, oldCity, prefixEnd(oldCity), false)
	if err != nil {
		t.Fatalf("open hidden old city overlay iterator: %v", err)
	}
	if hiddenOldCityIt != nil {
		hiddenOldCityIt.Seek(oldCity)
		if hiddenOldCityIt.Valid() {
			t.Fatalf("hidden old city iterator key=%x deleted=%v, want no visible base row after overlay tombstone", hiddenOldCityIt.UnsafeKey(), hiddenOldCityIt.IsDeleted())
		}
		_ = hiddenOldCityIt.Close()
	}
	_ = snap.Close()
	if _, err := reopenedCol.Insert([]byte("u2"), []byte(`{"email":"grace@example.com","city":"sea"}`)); !errors.Is(err, errCollectionRootOverlaysRequireCompaction) {
		t.Fatalf("insert with overlay roots err=%v want %v", err, errCollectionRootOverlaysRequireCompaction)
	}
	if matched, modified, err := reopenedCol.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"email":"ada@example.com","city":"bos"}`), true, nil
	}); !errors.Is(err, errCollectionRootOverlaysRequireCompaction) || matched || modified {
		t.Fatalf("update with overlay roots matched=%v modified=%v err=%v want %v", matched, modified, err, errCollectionRootOverlaysRequireCompaction)
	}
	if deleted, err := reopenedCol.DeleteDocument([]byte("u1")); !errors.Is(err, errCollectionRootOverlaysRequireCompaction) || deleted {
		t.Fatalf("delete with overlay roots deleted=%v err=%v want %v", deleted, err, errCollectionRootOverlaysRequireCompaction)
	}
	if _, err := reopenedCol.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString}); !errors.Is(err, errCollectionRootOverlaysRequireCompaction) {
		t.Fatalf("create index with overlay roots err=%v want %v", err, errCollectionRootOverlaysRequireCompaction)
	}
	got, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get overlay document: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"sea"}`); !bytes.Equal(got, want) {
		t.Fatalf("overlay document=%q want %q", got, want)
	}
	hnlIDs, err := reopenedCol.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find old city: %v", err)
	}
	if len(hnlIDs) != 0 {
		t.Fatalf("old city ids=%q want none", hnlIDs)
	}
	seaIDs, err := reopenedCol.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find new city: %v", err)
	}
	if len(seaIDs) != 1 || !bytes.Equal(seaIDs[0], []byte("u1")) {
		t.Fatalf("new city ids=%q want [u1]", seaIDs)
	}
	docs, truncated, err := reopenedCol.ScanDocuments(10)
	if err != nil {
		t.Fatalf("scan documents: %v", err)
	}
	if truncated || len(docs) != 1 || !bytes.Equal(docs[0].Document, []byte(`{"email":"ada@example.com","city":"sea"}`)) {
		t.Fatalf("scan docs=%+v truncated=%v", docs, truncated)
	}
}

func TestCollectionIndexedOverlayRootFlushSupportsReadsUpdatesAndUniqueChecks(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedOverlayRoots:      true,
			BufferedIndexedWriteMaxDocuments: 1,
			BufferedIndexedWriteMaxRootRuns:  1,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"ada@example.com","city":"sea"}`)); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("insert duplicate email err=%v want %v", err, ErrUniqueIndexConflict)
	}
	results, buffered, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			return []byte(`{"email":"ada@example.com","city":"sea"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("update city: %v", err)
	}
	if !buffered || len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("update results=%+v buffered=%v", results, buffered)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush overlays: %v", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get updated doc: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"sea"}`); !bytes.Equal(got, want) {
		t.Fatalf("updated doc=%q want %q", got, want)
	}
	hnlIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find old city: %v", err)
	}
	if len(hnlIDs) != 0 {
		t.Fatalf("old city ids=%q want none", hnlIDs)
	}
	seaIDs, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find new city: %v", err)
	}
	if len(seaIDs) != 1 || !bytes.Equal(seaIDs[0], []byte("u1")) {
		t.Fatalf("new city ids=%q want [u1]", seaIDs)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog: %v", err)
	}
	if got := len(catalog.overlayRootIDs(collectionPrimaryRootName("users"))); got != 1 {
		_ = snap.Close()
		t.Fatalf("primary overlay roots=%d want 1 coalesced overlay", got)
	}
	primaryRootName := collectionPrimaryRootName("users")
	primaryOverlays := catalog.overlayRootIDs(primaryRootName)
	if !catalog.overlayRootMayContainKey(primaryRootName, primaryOverlays[0], []byte("u1")) {
		_ = snap.Close()
		t.Fatalf("primary overlay filter does not contain updated document")
	}
	if catalog.overlayRootMayContainKey(primaryRootName, primaryOverlays[0], []byte("definitely-missing")) {
		_ = snap.Close()
		t.Fatalf("primary overlay filter contains unrelated document")
	}
	_ = snap.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	reopenedDoc, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get reopened doc: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"sea"}`); !bytes.Equal(reopenedDoc, want) {
		t.Fatalf("reopened doc=%q want %q", reopenedDoc, want)
	}
	reopenedSeaIDs, err := reopenedCol.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find reopened city: %v", err)
	}
	if len(reopenedSeaIDs) != 1 || !bytes.Equal(reopenedSeaIDs[0], []byte("u1")) {
		t.Fatalf("reopened city ids=%q want [u1]", reopenedSeaIDs)
	}
	reopenedSnap := reopened.AcquireSnapshot()
	if reopenedSnap == nil {
		t.Fatal("acquire reopened snapshot")
	}
	reopenedCatalog, err := loadCollectionCatalog(reopenedSnap, "users")
	if err != nil {
		_ = reopenedSnap.Close()
		t.Fatalf("load reopened catalog: %v", err)
	}
	reopenedPrimaryOverlays := reopenedCatalog.overlayRootIDs(primaryRootName)
	if len(reopenedPrimaryOverlays) != 1 {
		_ = reopenedSnap.Close()
		t.Fatalf("reopened primary overlay roots=%d want 1", len(reopenedPrimaryOverlays))
	}
	if !reopenedCatalog.overlayRootMayContainKey(primaryRootName, reopenedPrimaryOverlays[0], []byte("u1")) {
		_ = reopenedSnap.Close()
		t.Fatalf("reopened primary overlay filter does not contain updated document")
	}
	if !reopenedCatalog.overlayRootMayContainKey(primaryRootName, reopenedPrimaryOverlays[0], []byte("definitely-missing")) {
		_ = reopenedSnap.Close()
		t.Fatalf("reopened primary overlay without in-memory filter must fall back to maybe-present")
	}
	_ = reopenedSnap.Close()
}

func TestCollectionIndexedOverlayRootColdFlushPublishesBatchRootsInParallel(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedOverlayRoots:      true,
			BufferedIndexedWriteMaxDocuments: 1024,
			BufferedIndexedWriteMaxRootRuns:  1024,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	statUint := func(stats map[string]string, key string) uint64 {
		t.Helper()
		raw, ok := stats[key]
		if !ok {
			t.Fatalf("missing stat %s", key)
		}
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("parse stat %s=%q: %v", key, raw, err)
		}
		return value
	}
	const parallelGroupsKey = "treedb.publish.ordered_root_delta_group.root_apply_parallel_groups_total"
	const parallelRootsKey = "treedb.publish.ordered_root_delta_group.root_apply_parallel_roots_total"
	before := db.Stats()
	beforeParallelGroups := statUint(before, parallelGroupsKey)
	beforeParallelRoots := statUint(before, parallelRootsKey)

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"city":"hnl"}`),
			[]byte(`{"city":"sea"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush overlays: %v", err)
	}
	after := db.Stats()
	if got := statUint(after, parallelGroupsKey) - beforeParallelGroups; got != 1 {
		t.Fatalf("parallel groups delta=%d want 1", got)
	}
	if got := statUint(after, parallelRootsKey) - beforeParallelRoots; got < 2 {
		t.Fatalf("parallel roots delta=%d want at least 2", got)
	}
	if got, err := col.Get([]byte("u1")); err != nil || !bytes.Contains(got, []byte(`"hnl"`)) {
		t.Fatalf("get u1 doc=%q err=%v", got, err)
	}
	ids, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("city ids=%q want [u2]", ids)
	}
}

func TestCollectionIndexedOverlayRootColdFlushPreservesDeletes(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedOverlayRoots:      true,
			BufferedIndexedWriteMaxDocuments: 1024,
			BufferedIndexedWriteMaxRootRuns:  1024,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert base doc: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush base doc: %v", err)
	}
	if _, err := col.CompactRootOverlays(context.Background()); err != nil {
		t.Fatalf("compact base overlay: %v", err)
	}
	deleted, err := col.DeleteDocument([]byte("u1"))
	if err != nil {
		t.Fatalf("delete base doc: %v", err)
	}
	if !deleted {
		t.Fatal("delete base doc deleted=false want true")
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush cold delete overlay: %v", err)
	}
	if got, err := col.Get([]byte("u1")); err != nil {
		t.Fatalf("get deleted doc: %v", err)
	} else if got != nil {
		t.Fatalf("deleted doc=%s want nil", got)
	}
	ids, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city after delete: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("city ids=%q want empty after delete", ids)
	}
}

func TestCollectionIndexedOverlayRootFilterUnionsDeltaBase(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedOverlayRoots:      true,
			BufferedIndexedWriteMaxDocuments: 1,
			BufferedIndexedWriteMaxRootRuns:  1,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if _, err := col.Insert([]byte("u2"), []byte(`{"city":"sea"}`)); err != nil {
		t.Fatalf("insert u2: %v", err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog: %v", err)
	}
	primaryRootName := collectionPrimaryRootName("users")
	primaryOverlays := catalog.overlayRootIDs(primaryRootName)
	if len(primaryOverlays) != 1 {
		_ = snap.Close()
		t.Fatalf("primary overlay roots=%d want 1 coalesced overlay", len(primaryOverlays))
	}
	for _, id := range []string{"u1", "u2"} {
		if !catalog.overlayRootMayContainKey(primaryRootName, primaryOverlays[0], []byte(id)) {
			_ = snap.Close()
			t.Fatalf("primary overlay filter does not contain %s after delta-over-overlay publish", id)
		}
	}
	_ = snap.Close()

	results, buffered, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			return []byte(`{"city":"bos"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("update u1: %v", err)
	}
	if !buffered || len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("update results=%+v buffered=%v", results, buffered)
	}
}

func TestCollectionRootOverlayFilterSkipsBaseOnlyUnionWhenDeltaFilterDisabled(t *testing.T) {
	primaryRootName := collectionPrimaryRootName("users")
	var baseFilter collectionRootOverlayFilter
	baseFilter.addKey([]byte("u1"))

	filters, err := buildCollectionRootOverlayFilters(
		[]string{primaryRootName},
		map[string][]memtable.Table{primaryRootName: nil},
		map[string][]uint64{primaryRootName: []uint64{123}},
		map[string]map[uint64]collectionRootOverlayFilter{
			primaryRootName: map[uint64]collectionRootOverlayFilter{123: baseFilter},
		},
	)
	if err != nil {
		t.Fatalf("build overlay filters: %v", err)
	}
	if len(filters) != 0 {
		t.Fatalf("filters=%v, want no base-only filter when delta filter is disabled", filters)
	}
}

func TestCollectionOverlayPrimaryProbeMissDoesNotFallThroughToBase(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	for _, doc := range []struct {
		id  string
		raw string
	}{
		{id: "u1", raw: `{"city":"hnl"}`},
		{id: "u2", raw: `{"city":"sea"}`},
		{id: "u3", raw: `{"city":"lax"}`},
	} {
		if _, err := col.Insert([]byte(doc.id), []byte(doc.raw)); err != nil {
			t.Fatalf("insert %s: %v", doc.id, err)
		}
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush base: %v", err)
	}

	primaryRootName := collectionPrimaryRootName("users")
	primaryOverlay := newCollectionRunTable(2)
	setCollectionRunValue(primaryOverlay, []byte("u2"), []byte(`{"city":"bos"}`))
	primaryOverlay.DeleteSteal([]byte("u3"))
	primaryOverlay.Freeze()
	defer resetCollectionRunTable(primaryOverlay)
	_, overlayRootIDs, err := db.PublishOrderedRootGroupWithSystemBuilder([]backenddb.OrderedRootPublishInput{{
		BaseRoot: 0,
		Iter:     primaryOverlay.NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		snap := db.AcquireSnapshot()
		if snap == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = snap.Close() }()
		return buildSystemTargetIterator(snap, map[string][]byte{
			systemCollectionRootOverlayKey(primaryRootName): encodeRootIDList([]uint64{rootIDs[0]}),
		})
	})
	if err != nil {
		t.Fatalf("publish primary overlay: %v", err)
	}
	if len(overlayRootIDs) != 1 {
		t.Fatalf("overlay roots=%d want 1", len(overlayRootIDs))
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	value, overlayFound, documentFound, err := collectionGetAppendAtCatalogOverlayRoot(snap, catalog, primaryRootName, []byte("u1"), nil)
	if err != nil {
		t.Fatalf("overlay miss probe: %v", err)
	}
	if overlayFound || documentFound || len(value) != 0 {
		t.Fatalf("overlay miss value=%q overlayFound=%v documentFound=%v, want no overlay result", value, overlayFound, documentFound)
	}
	value, overlayFound, documentFound, err = collectionGetAppendAtCatalogOverlayRoot(snap, catalog, primaryRootName, []byte("u2"), nil)
	if err != nil {
		t.Fatalf("overlay hit probe: %v", err)
	}
	if !overlayFound || !documentFound || !bytes.Equal(value, []byte(`{"city":"bos"}`)) {
		t.Fatalf("overlay hit value=%q overlayFound=%v documentFound=%v", value, overlayFound, documentFound)
	}
	value, overlayFound, documentFound, err = collectionGetAppendAtCatalogOverlayRoot(snap, catalog, primaryRootName, []byte("u3"), nil)
	if err != nil {
		t.Fatalf("overlay tombstone probe: %v", err)
	}
	if !overlayFound || documentFound || len(value) != 0 {
		t.Fatalf("overlay tombstone value=%q overlayFound=%v documentFound=%v, want overlay tombstone", value, overlayFound, documentFound)
	}
}

func TestCollectionIndexedOverlayRootValidationRejectsStaleOverlayDescriptors(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedOverlayRoots:      true,
			DisableBufferedIndexedAsyncFlush: true,
			BufferedIndexedWriteMaxDocuments: 1,
			BufferedIndexedWriteMaxRootRuns:  1,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush u1: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog: %v", err)
	}
	primaryRootName := collectionPrimaryRootName("users")
	if got := len(catalog.overlayRootIDs(primaryRootName)); got != 1 {
		_ = snap.Close()
		t.Fatalf("primary overlay roots=%d want 1", got)
	}
	stalePlan := &updateBatchPlan{
		meta:           catalog.meta,
		catalog:        catalog,
		baseCommitSeq:  snapshotCommitSeq(snap),
		baseSystemRoot: snapshotSystemRoot(snap),
		rootNames:      []string{primaryRootName},
		baseRootIDs: map[string]uint64{
			primaryRootName: catalog.rootID(primaryRootName),
		},
	}
	_ = snap.Close()

	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"grace@example.com","city":"sea"}`)); err != nil {
		t.Fatalf("insert u2: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush u2: %v", err)
	}
	if err := col.validateUpdateBatchPlanRootDescriptors(stalePlan); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("stale overlay validation err=%v want %v", err, ErrConcurrentMutation)
	}
}

func TestCollectionCompactRootOverlaysFoldsIntoBaseRoots(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedOverlayRoots:      true,
			DisableBufferedIndexedAsyncFlush: true,
			BufferedIndexedWriteMaxDocuments: 1,
			BufferedIndexedWriteMaxRootRuns:  1,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if _, _, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			return []byte(`{"email":"ada@example.com","city":"sea"}`), true, nil
		},
	}}); err != nil {
		t.Fatalf("update city: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush overlays: %v", err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog before compact: %v", err)
	}
	if got := len(catalog.overlayRootNames()); got == 0 {
		_ = snap.Close()
		t.Fatal("overlay root names before compact=0")
	}
	_ = snap.Close()

	stats, err := col.CompactRootOverlays(context.Background())
	if err != nil {
		t.Fatalf("compact root overlays: %v", err)
	}
	if stats.Roots == 0 || stats.OverlayRoots == 0 {
		t.Fatalf("compact stats=%+v want nonzero roots and overlays", stats)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get compacted doc: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"sea"}`); !bytes.Equal(got, want) {
		t.Fatalf("compacted doc=%q want %q", got, want)
	}
	hnlIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find compacted old city: %v", err)
	}
	if len(hnlIDs) != 0 {
		t.Fatalf("old city ids=%q want none", hnlIDs)
	}
	seaIDs, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find compacted new city: %v", err)
	}
	if len(seaIDs) != 1 || !bytes.Equal(seaIDs[0], []byte("u1")) {
		t.Fatalf("new city ids=%q want [u1]", seaIDs)
	}
	snap = db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot after compact")
	}
	catalog, err = loadCollectionCatalog(snap, "users")
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog after compact: %v", err)
	}
	if got := len(catalog.overlayRootNames()); got != 0 {
		_ = snap.Close()
		t.Fatalf("overlay root names after compact=%d want 0", got)
	}
	_ = snap.Close()
	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"email":"ada@example.com","city":"lax"}`), true, nil
	})
	if err != nil {
		t.Fatalf("direct update after compact: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("direct update after compact matched=%v modified=%v", matched, modified)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	reopenedIDs, err := reopenedCol.FindByIndex("city", "lax")
	if err != nil {
		t.Fatalf("find reopened direct update city: %v", err)
	}
	if len(reopenedIDs) != 1 || !bytes.Equal(reopenedIDs[0], []byte("u1")) {
		t.Fatalf("reopened city ids=%q want [u1]", reopenedIDs)
	}
}

func TestCollectionCompactStorageFoldedRootsUseCurrentDescriptorScan(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedOverlayRoots:      true,
			DisableBufferedIndexedAsyncFlush: true,
			BufferedIndexedWriteMaxDocuments: 1,
			BufferedIndexedWriteMaxRootRuns:  1,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if _, _, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			return []byte(`{"email":"ada@example.com","city":"sea"}`), true, nil
		},
	}}); err != nil {
		t.Fatalf("update city: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush overlays: %v", err)
	}

	result, err := col.compactRootOverlays(context.Background())
	if err != nil {
		t.Fatalf("compact root overlays: %v", err)
	}
	if result.stats.Roots == 0 || result.stats.OverlayRoots == 0 {
		t.Fatalf("compact result stats=%+v want nonzero roots and overlays", result.stats)
	}
	if len(result.rootIDs) == 0 {
		t.Fatal("compact result rootIDs=0 want folded roots")
	}
	if result.systemRootID == 0 {
		t.Fatal("compact result systemRootID=0 want published system root")
	}
	if err := checkpointCollectionCompactStorageFoldedRoots(db, result); err != nil {
		t.Fatalf("checkpoint folded roots: %v", err)
	}
	if _, err := db.CompactStoragePlan(context.Background(), backenddb.CompactStorageOptions{}); err != nil {
		t.Fatalf("CompactStoragePlan with current descriptor scan: %v", err)
	}
}

func TestCollectionCompactStorageFoldsRootsAndCleansStorage(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true

	db, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = cleanup() }()

	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedOverlayRoots:      true,
			BufferedIndexedWriteMaxDocuments: 1,
			BufferedIndexedWriteMaxRootRuns:  1,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	doc := []byte(`{"city":"hnl","payload":"` + strings.Repeat("x", 512) + `"}`)
	if _, err := col.Insert([]byte("u1"), doc); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, _, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			return []byte(`{"city":"sea","payload":"` + strings.Repeat("y", 512) + `"}`), true, nil
		},
	}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush overlays: %v", err)
	}

	valueLogDir := backenddb.ValueLogDirPath(dir)
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("mkdir value_vlog: %v", err)
	}
	emptyPath := filepath.Join(valueLogDir, "value-l42-000001.log")
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
		t.Fatalf("write empty value-log file: %v", err)
	}

	stats, err := col.CompactStorage(context.Background(), CompactStorageOptions{
		LeafPackMinExpectedReclaimBytes: 1,
		LeafPackMinReclaimPerCopyPPM:    1,
	})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if rootStats, ok := stats.RootOverlays["users"]; !ok || rootStats.OverlayRoots == 0 {
		t.Fatalf("root overlay compaction stats=%+v", stats.RootOverlays)
	}
	if !stats.Storage.FullyCompacted {
		t.Fatalf("storage FullyCompacted=false debt=%+v", stats.Storage.RemainingDebt)
	}
	if _, err := os.Stat(emptyPath); !os.IsNotExist(err) {
		t.Fatalf("empty value-log file remains or stat failed: %v", err)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after compact storage: %v", err)
	}
	want := []byte(`{"city":"sea","payload":"` + strings.Repeat("y", 512) + `"}`)
	if !bytes.Equal(got, want) {
		t.Fatalf("doc after compact storage=%q want %q", got, want)
	}
	ids, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find by index after compact storage: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("ids after compact storage=%q want [u1]", ids)
	}

	again, err := col.CompactStoragePlan(context.Background(), CompactStorageOptions{
		LeafPackMinExpectedReclaimBytes: 1,
		LeafPackMinReclaimPerCopyPPM:    1,
	})
	if err != nil {
		t.Fatalf("CompactStoragePlan after: %v", err)
	}
	if !again.Storage.FullyCompacted {
		t.Fatalf("post-compact plan FullyCompacted=false debt=%+v", again.Storage.RemainingDebt)
	}
}

func TestCollectionIndexedWriteMemtablesReadFlushedDocumentWithBufferedRuns(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert flushed document: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush document: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"city":"sea"}`)},
	); err != nil {
		t.Fatalf("insert buffered document: %v", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get flushed document with buffered runs: %v", err)
	}
	if want := []byte(`{"city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("flushed document=%q want %q", got, want)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get buffered document: %v", err)
	}
	if want := []byte(`{"city":"sea"}`); !bytes.Equal(got, want) {
		t.Fatalf("buffered document=%q want %q", got, want)
	}
}

func TestCollectionIndexedWriteMemtablesDefaultForIndexedSchemas(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if !meta.Options.BufferedIndexedWrites {
		t.Fatal("indexed collection did not enable native write memtables by default")
	}
	if !meta.Options.BufferedIndexedAsyncFlush {
		t.Fatal("indexed collection did not enable async threshold publish by default")
	}
	if got := meta.Options.BufferedIndexedWriteMaxDocuments; got != DefaultIndexedWriteMemtableAsyncFlushMaxDocuments {
		t.Fatalf("default max docs=%d want %d", got, DefaultIndexedWriteMemtableAsyncFlushMaxDocuments)
	}
	if got := meta.Options.BufferedIndexedWriteMaxBytes; got != 0 {
		t.Fatalf("default max bytes=%d want 0", got)
	}
	if got := meta.Options.BufferedIndexedWriteMaxRootRuns; got != DefaultIndexedWriteMemtableAsyncFlushMaxRootRuns {
		t.Fatalf("default max root runs=%d want %d", got, DefaultIndexedWriteMemtableAsyncFlushMaxRootRuns)
	}
	if got := meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits; got != DefaultIndexedWriteMemtableAsyncFlushMaxQueuedUnits {
		t.Fatalf("default async max queued units=%d want %d", got, DefaultIndexedWriteMemtableAsyncFlushMaxQueuedUnits)
	}

	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get buffered doc: %v", err)
	}
	if want := []byte(`{"city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("buffered doc=%q want %q", got, want)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if got := catalog.rootID(collectionPrimaryRootName("users")); got != 0 {
		t.Fatalf("primary root persisted before flush: %d", got)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush default indexed memtable: %v", err)
	}
	snap = d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected post-flush snapshot")
	}
	catalog, err = loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load post-flush catalog: %v", err)
	}
	if got := catalog.rootID(collectionPrimaryRootName("users")); got == 0 {
		t.Fatal("primary root was not persisted after flush")
	}
}

func TestCollectionIndexedWriteMemtablesPreserveDocumentDefaultWithRootRunLimit(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	meta, err := NewCollectionManager(d).CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWriteMaxRootRuns: 8,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if got := meta.Options.BufferedIndexedWriteMaxDocuments; got != DefaultIndexedWriteMemtableAsyncFlushMaxDocuments {
		t.Fatalf("max documents=%d want default %d", got, DefaultIndexedWriteMemtableAsyncFlushMaxDocuments)
	}
	if got := meta.Options.BufferedIndexedWriteMaxRootRuns; got != 8 {
		t.Fatalf("max root runs=%d want 8", got)
	}
}

func TestCollectionIndexedWriteMemtablesAsyncFlushDefaultsQueueLimit(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	meta, err := NewCollectionManager(d).CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if !meta.Options.BufferedIndexedAsyncFlush {
		t.Fatal("async indexed flush was not preserved for indexed collection")
	}
	if got := meta.Options.BufferedIndexedWriteMaxDocuments; got != DefaultIndexedWriteMemtableAsyncFlushMaxDocuments {
		t.Fatalf("async max documents=%d want %d", got, DefaultIndexedWriteMemtableAsyncFlushMaxDocuments)
	}
	if got := meta.Options.BufferedIndexedWriteMaxRootRuns; got != DefaultIndexedWriteMemtableAsyncFlushMaxRootRuns {
		t.Fatalf("async max root runs=%d want %d", got, DefaultIndexedWriteMemtableAsyncFlushMaxRootRuns)
	}
	if got := meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits; got != DefaultIndexedWriteMemtableAsyncFlushMaxQueuedUnits {
		t.Fatalf("async max queued units=%d want %d", got, DefaultIndexedWriteMemtableAsyncFlushMaxQueuedUnits)
	}
}

func TestCollectionIndexedWriteMemtablesCanDisableDefaultAsyncFlush(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	meta, err := NewCollectionManager(d).CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if meta.Options.BufferedIndexedAsyncFlush {
		t.Fatal("disabled async indexed flush was enabled")
	}
	if got := meta.Options.BufferedIndexedWriteMaxDocuments; got != DefaultIndexedWriteMemtableMaxDocuments {
		t.Fatalf("foreground max documents=%d want %d", got, DefaultIndexedWriteMemtableMaxDocuments)
	}
	if got := meta.Options.BufferedIndexedWriteMaxRootRuns; got != DefaultIndexedWriteMemtableMaxRootRuns {
		t.Fatalf("foreground max root runs=%d want %d", got, DefaultIndexedWriteMemtableMaxRootRuns)
	}
	if got := meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits; got != 0 {
		t.Fatalf("disabled async max queued units=%d want 0", got)
	}
}

func TestCollectionIndexedWriteMemtablesRejectConflictingAsyncFlushOptions(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	_, err = NewCollectionManager(d).CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedAsyncFlush:        true,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
	})
	if err == nil {
		t.Fatal("create collection with conflicting async flush options succeeded")
	}
	if !strings.Contains(err.Error(), "both enabled and disabled") {
		t.Fatalf("err=%q want conflicting async flush options", err)
	}
}

func TestCollectionIndexedWriteMemtablesCanDisableRootRunLimitWithDocumentLimit(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	meta, err := NewCollectionManager(d).CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWriteMaxDocuments: DefaultIndexedWriteMemtableMaxDocuments,
			BufferedIndexedWriteMaxRootRuns:  0,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if got := meta.Options.BufferedIndexedWriteMaxDocuments; got != DefaultIndexedWriteMemtableMaxDocuments {
		t.Fatalf("max documents=%d want default %d", got, DefaultIndexedWriteMemtableMaxDocuments)
	}
	if got := meta.Options.BufferedIndexedWriteMaxRootRuns; got != 0 {
		t.Fatalf("max root runs=%d want disabled", got)
	}
}

func TestCollectionIndexedWriteMemtablesDefaultSkipsNoIndexSchemas(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	meta, err := NewCollectionManager(d).CreateCollection(&CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if meta.Options.BufferedIndexedWrites {
		t.Fatal("no-index collection enabled indexed write memtables")
	}
	if meta.Options.BufferedIndexedWriteMaxDocuments != 0 || meta.Options.BufferedIndexedWriteMaxBytes != 0 || meta.Options.BufferedIndexedWriteMaxRootRuns != 0 {
		t.Fatalf("no-index buffered limits docs=%d bytes=%d rootRuns=%d want zero",
			meta.Options.BufferedIndexedWriteMaxDocuments, meta.Options.BufferedIndexedWriteMaxBytes, meta.Options.BufferedIndexedWriteMaxRootRuns)
	}
	if meta.Options.DisableBufferedIndexedAsyncFlush || meta.Options.BufferedIndexedAsyncFlush || meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits != 0 {
		t.Fatalf("no-index async flush fields disable=%v enabled=%v maxQueued=%d want false/false/0",
			meta.Options.DisableBufferedIndexedAsyncFlush, meta.Options.BufferedIndexedAsyncFlush, meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits)
	}
}

func TestCollectionIndexedWriteMemtablesPreserveNoIndexAsyncFlushOptOutForFutureIndexes(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DisableBufferedIndexedAsyncFlush: true,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if meta.Options.BufferedIndexedWrites {
		t.Fatal("no-index collection enabled indexed write memtables")
	}
	if !meta.Options.DisableBufferedIndexedAsyncFlush || meta.Options.BufferedIndexedAsyncFlush {
		t.Fatalf("no-index async flags disable=%v enabled=%v want true/false",
			meta.Options.DisableBufferedIndexedAsyncFlush, meta.Options.BufferedIndexedAsyncFlush)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	meta, err = col.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	if !meta.Options.BufferedIndexedWrites {
		t.Fatal("indexed collection did not enable indexed write memtables")
	}
	if !meta.Options.DisableBufferedIndexedAsyncFlush || meta.Options.BufferedIndexedAsyncFlush {
		t.Fatalf("indexed async flags disable=%v enabled=%v want true/false",
			meta.Options.DisableBufferedIndexedAsyncFlush, meta.Options.BufferedIndexedAsyncFlush)
	}
}

func TestCollectionIndexedWriteMemtablesRejectDisabledAsyncFlushQueueLimit(t *testing.T) {
	_, err := normalizeCollectionMeta(CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DisableBufferedIndexedAsyncFlush:        true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 2,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
	})
	if err == nil {
		t.Fatal("normalize disabled async flush queue limit err=nil want error")
	}
	if !strings.Contains(err.Error(), "max queued units") {
		t.Fatalf("err=%q want max queued units error", err)
	}
}

func TestCollectionIndexedWriteMemtablesPreserveNoIndexThresholdsForFutureIndexes(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWriteMaxDocuments: 1234,
			BufferedIndexedWriteMaxBytes:     5678,
			BufferedIndexedWriteMaxRootRuns:  90,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if meta.Options.BufferedIndexedWrites {
		t.Fatal("no-index collection enabled indexed write memtables")
	}
	if meta.Options.BufferedIndexedWriteMaxDocuments != 1234 || meta.Options.BufferedIndexedWriteMaxBytes != 5678 || meta.Options.BufferedIndexedWriteMaxRootRuns != 90 {
		t.Fatalf("no-index buffered limits docs=%d bytes=%d rootRuns=%d want 1234/5678/90",
			meta.Options.BufferedIndexedWriteMaxDocuments, meta.Options.BufferedIndexedWriteMaxBytes, meta.Options.BufferedIndexedWriteMaxRootRuns)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	meta, err = col.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	if !meta.Options.BufferedIndexedWrites {
		t.Fatal("indexed collection did not enable indexed write memtables")
	}
	if meta.Options.BufferedIndexedWriteMaxDocuments != 1234 || meta.Options.BufferedIndexedWriteMaxBytes != 5678 || meta.Options.BufferedIndexedWriteMaxRootRuns != 90 {
		t.Fatalf("indexed buffered limits docs=%d bytes=%d rootRuns=%d want 1234/5678/90",
			meta.Options.BufferedIndexedWriteMaxDocuments, meta.Options.BufferedIndexedWriteMaxBytes, meta.Options.BufferedIndexedWriteMaxRootRuns)
	}
}

func TestCollectionIndexedWriteMemtablesCanBeDisabled(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if meta.Options.BufferedIndexedWrites {
		t.Fatal("disabled indexed write memtables reported enabled")
	}
	if meta.Options.BufferedIndexedWriteMaxDocuments != 0 || meta.Options.BufferedIndexedWriteMaxBytes != 0 || meta.Options.BufferedIndexedWriteMaxRootRuns != 0 {
		t.Fatalf("disabled buffered limits docs=%d bytes=%d rootRuns=%d want zero",
			meta.Options.BufferedIndexedWriteMaxDocuments, meta.Options.BufferedIndexedWriteMaxBytes, meta.Options.BufferedIndexedWriteMaxRootRuns)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if got := catalog.rootID(collectionPrimaryRootName("users")); got == 0 {
		t.Fatal("disabled indexed write memtables did not publish primary root immediately")
	}
}

func TestCollectionIndexedWriteMemtablesBypassDefaultLargeBatches(t *testing.T) {
	col := &Collection{writeDomain: &collectionWriteDomain{}}
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: DefaultIndexedWriteMemtableMaxDocuments,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}
	if !col.shouldBufferIndexedInsertBatch(meta, DefaultIndexedWriteMemtableDirectBatchDocuments-1) {
		t.Fatal("default indexed memtable path bypassed a below-threshold batch")
	}
	if col.shouldBufferIndexedInsertBatch(meta, DefaultIndexedWriteMemtableDirectBatchDocuments) {
		t.Fatal("default indexed memtable path buffered a large direct-publish batch")
	}
	meta.Options.BufferedIndexedAsyncFlush = true
	meta.Options.BufferedIndexedWriteMaxDocuments = DefaultIndexedWriteMemtableAsyncFlushMaxDocuments
	if !col.shouldBufferIndexedInsertBatch(meta, DefaultIndexedWriteMemtableDirectBatchDocuments-1) {
		t.Fatal("async default indexed memtable path bypassed a below-threshold batch")
	}
	if col.shouldBufferIndexedInsertBatch(meta, DefaultIndexedWriteMemtableDirectBatchDocuments) {
		t.Fatal("async default indexed memtable path buffered a large direct-publish batch")
	}
	meta.Options.BufferedIndexedAsyncFlush = false
	meta.Options.BufferedIndexedWriteMaxDocuments = 2
	if !col.shouldBufferIndexedInsertBatch(meta, 2) {
		t.Fatal("explicit small flush threshold should not trigger the default large-batch bypass")
	}
}

func TestCollectionInsertPlanningKeepsLockForIndexedMemtableBypass(t *testing.T) {
	tests := []struct {
		name                    string
		documentFormat          DocumentFormat
		indexedMemtablesEnabled bool
		bufferIndexedInserts    bool
		keepDirectPlanningLock  bool
		wantUnlock              bool
	}{
		{
			name:           "json-no-indexed-memtables",
			documentFormat: DocumentFormatJSON,
			wantUnlock:     true,
		},
		{
			name:                    "json-buffered-indexed-memtables-large-batch",
			documentFormat:          DocumentFormatJSON,
			indexedMemtablesEnabled: true,
			bufferIndexedInserts:    true,
			wantUnlock:              true,
		},
		{
			name:                    "json-buffered-indexed-memtables-direct-accumulator",
			documentFormat:          DocumentFormatJSON,
			indexedMemtablesEnabled: true,
			bufferIndexedInserts:    true,
			keepDirectPlanningLock:  true,
			wantUnlock:              false,
		},
		{
			name:                    "json-direct-indexed-memtable-bypass",
			documentFormat:          DocumentFormatJSON,
			indexedMemtablesEnabled: true,
			bufferIndexedInserts:    false,
			wantUnlock:              false,
		},
		{
			name:           "bson-no-indexed-memtables",
			documentFormat: DocumentFormatBSON,
			wantUnlock:     true,
		},
		{
			name:                    "bson-direct-indexed-memtable-bypass",
			documentFormat:          DocumentFormatBSON,
			indexedMemtablesEnabled: true,
			bufferIndexedInserts:    false,
			wantUnlock:              false,
		},
		{
			name:                    "template-v1-buffered-indexed-memtables-direct-accumulator",
			documentFormat:          DocumentFormatTemplateV1,
			indexedMemtablesEnabled: true,
			bufferIndexedInserts:    true,
			keepDirectPlanningLock:  true,
			wantUnlock:              false,
		},
		{
			name:           "template-v1",
			documentFormat: DocumentFormatTemplateV1,
			wantUnlock:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldUnlockInsertPlanning(
				collectionOptions{documentFormat: tt.documentFormat},
				tt.indexedMemtablesEnabled,
				tt.bufferIndexedInserts,
				tt.keepDirectPlanningLock,
			)
			if got != tt.wantUnlock {
				t.Fatalf("shouldUnlockInsertPlanning()=%v want %v", got, tt.wantUnlock)
			}
		})
	}
}

func TestCollectionIndexedInsertAccumulatorThresholds(t *testing.T) {
	if !shouldUseDirectBufferedInsertAccumulators(1) {
		t.Fatal("single-document insert should use direct buffered accumulators")
	}
	if !shouldUseDirectBufferedInsertAccumulators(DefaultIndexedWriteMemtableAccumulatorBatchDocuments) {
		t.Fatal("accumulator threshold should be inclusive")
	}
	if shouldUseDirectBufferedInsertAccumulators(DefaultIndexedWriteMemtableAccumulatorBatchDocuments + 1) {
		t.Fatal("large insert should keep frozen-run path")
	}
	if !shouldKeepDirectBufferedInsertPlanningLocked(collectionOptions{documentFormat: DocumentFormatJSON}, 1, 0) {
		t.Fatal("single-document JSON accumulator insert should stay locked when uncontended")
	}
	if !shouldKeepDirectBufferedInsertPlanningLocked(collectionOptions{documentFormat: DocumentFormatDefault}, 1, 0) {
		t.Fatal("single-document default accumulator insert should stay locked when uncontended")
	}
	if !shouldKeepDirectBufferedInsertPlanningLocked(collectionOptions{documentFormat: DocumentFormatBSON}, 1, 0) {
		t.Fatal("single-document BSON accumulator insert should stay locked when uncontended")
	}
	if shouldKeepDirectBufferedInsertPlanningLocked(collectionOptions{documentFormat: DocumentFormatJSON}, 1, indexedInsertPlanningUnlockMinWait) {
		t.Fatal("single-document JSON accumulator insert should unlock planning after contention")
	}
	if shouldKeepDirectBufferedInsertPlanningLocked(collectionOptions{documentFormat: DocumentFormatDefault}, 1, indexedInsertPlanningUnlockMinWait) {
		t.Fatal("single-document default accumulator insert should unlock planning after contention")
	}
	if shouldKeepDirectBufferedInsertPlanningLocked(collectionOptions{documentFormat: DocumentFormatBSON}, 1, indexedInsertPlanningUnlockMinWait) {
		t.Fatal("single-document BSON accumulator insert should unlock planning after contention")
	}
	if !shouldKeepDirectBufferedInsertPlanningLocked(collectionOptions{documentFormat: DocumentFormatTemplateV1}, 1, indexedInsertPlanningUnlockMinWait) {
		t.Fatal("single-document template-v1 accumulator insert should keep planning lock")
	}
	if shouldKeepDirectBufferedInsertPlanningLocked(collectionOptions{documentFormat: DocumentFormatTemplateV1}, 2, indexedInsertPlanningUnlockMinWait) {
		t.Fatal("multi-document accumulator insert should release planning lock")
	}
}

func TestCollectionInsertNoIndexBSONRejectsClosedWriteDomain(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if col.writeDomain == nil {
		t.Fatal("missing write domain")
	}
	col.writeDomain.closingWrites.Store(true)
	_, err = col.Insert(
		[]byte("u1"),
		mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "score", Value: int32(1)},
		}),
	)
	if !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("insert err=%v want %v", err, backenddb.ErrClosed)
	}
}

func TestCollectionInsertRetryRetriesWrappedConcurrentMutation(t *testing.T) {
	attempts := 0
	result, err := retryInsertBatchMutation(func() ([][]byte, error) {
		attempts++
		if attempts < 3 {
			return nil, fmt.Errorf("wrapped attempt %d: %w", attempts, ErrConcurrentMutation)
		}
		return [][]byte{[]byte("u1")}, nil
	})
	if err != nil {
		t.Fatalf("retryInsertBatchMutation err=%v want nil", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
	if len(result) != 1 || !bytes.Equal(result[0], []byte("u1")) {
		t.Fatalf("result=%q want [u1]", result)
	}
}

func TestCollectionInsertRetryRetriesWrappedTransientEOF(t *testing.T) {
	attempts := 0
	result, err := retryInsertBatchMutation(func() ([][]byte, error) {
		attempts++
		switch attempts {
		case 1:
			return nil, fmt.Errorf("collections: load catalog \"users\" meta: %w", io.EOF)
		case 2:
			return nil, fmt.Errorf("collections: load catalog \"users\" root \"users/primary\": %w", io.ErrUnexpectedEOF)
		default:
			return [][]byte{[]byte("u1")}, nil
		}
	})
	if err != nil {
		t.Fatalf("retryInsertBatchMutation err=%v want nil", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
	if len(result) != 1 || !bytes.Equal(result[0], []byte("u1")) {
		t.Fatalf("result=%q want [u1]", result)
	}
}

func TestCollectionInsertRetryDoesNotRetryNonCatalogEOF(t *testing.T) {
	attempts := 0
	wantErr := fmt.Errorf("document decode: %w", io.EOF)
	_, err := retryInsertBatchMutation(func() ([][]byte, error) {
		attempts++
		return nil, wantErr
	})
	if !errors.Is(err, io.EOF) {
		t.Fatalf("retryInsertBatchMutation err=%v want EOF", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
}

func TestCollectionInsertRetryReturnsNonRetryableImmediately(t *testing.T) {
	attempts := 0
	wantErr := errors.New("boom")
	_, err := retryInsertBatchMutation(func() ([][]byte, error) {
		attempts++
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("retryInsertBatchMutation err=%v want %v", err, wantErr)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
}

func TestCollectionInsertRetryExhaustionWrapsLastConcurrentMutation(t *testing.T) {
	attempts := 0
	lastErr := fmt.Errorf("last stale root: %w", ErrConcurrentMutation)
	_, err := retryInsertBatchMutation(func() ([][]byte, error) {
		attempts++
		return nil, lastErr
	})
	if !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("retryInsertBatchMutation err=%v want ErrConcurrentMutation", err)
	}
	if !strings.Contains(err.Error(), lastErr.Error()) {
		t.Fatalf("retryInsertBatchMutation err=%q want last error %q", err, lastErr)
	}
	if got := strings.Count(err.Error(), ErrConcurrentMutation.Error()); got != 1 {
		t.Fatalf("retryInsertBatchMutation err=%q contains ErrConcurrentMutation text %d times, want 1", err, got)
	}
	if attempts != maxCollectionMutationRetries {
		t.Fatalf("attempts=%d want %d", attempts, maxCollectionMutationRetries)
	}
}

func TestCollectionIndexedWriteMemtablesReadAfterDomainTableAllocated(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	if deleted, err := col.DeleteDocument([]byte("u1")); err != nil {
		t.Fatalf("delete seed: %v", err)
	} else if !deleted {
		t.Fatal("delete seed deleted=false want true")
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("buffer insert after delete: %v", err)
	}
	got, found, err := col.GetInto([]byte("u2"), nil)
	if err != nil {
		t.Fatalf("get buffered u2: %v", err)
	}
	if !found {
		t.Fatal("get buffered u2 found=false want true")
	}
	if want := []byte(`{"city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("get buffered u2=%q want %q", got, want)
	}
}

func TestCollectionIndexedWriteMemtablesFindLimitExactBufferedCount(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	ids, truncated, err := col.FindByIndexValueLimit("city", "hnl", 1)
	if err != nil {
		t.Fatalf("find exact buffered limit: %v", err)
	}
	if truncated {
		t.Fatalf("exact buffered limit truncated=true ids=%q", ids)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("exact buffered limit ids=%q want [u1]", ids)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert u2: %v", err)
	}
	ids, truncated, err = col.FindByIndexValueLimit("city", "hnl", 1)
	if err != nil {
		t.Fatalf("find over buffered limit: %v", err)
	}
	if !truncated {
		t.Fatalf("over buffered limit truncated=false ids=%q", ids)
	}
	if len(ids) != 1 {
		t.Fatalf("over buffered limit returned %d ids want 1", len(ids))
	}
}

func TestCollectionIndexedWriteMemtablesFindLimitMergesBufferedAndPersistedOrder(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u0")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert persisted row: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush persisted row: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert buffered row: %v", err)
	}
	ids, truncated, err := col.FindByIndexValueLimit("city", "hnl", 1)
	if err != nil {
		t.Fatalf("find limited merged index rows: %v", err)
	}
	if !truncated || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u0")) {
		t.Fatalf("limited ids=%q truncated=%v want [u0]/true", ids, truncated)
	}
	ids, err = col.FindByIndexValue("city", "hnl")
	if err != nil {
		t.Fatalf("find merged index rows: %v", err)
	}
	if len(ids) != 2 || !bytes.Equal(ids[0], []byte("u0")) || !bytes.Equal(ids[1], []byte("u1")) {
		t.Fatalf("merged ids=%q want [u0 u1]", ids)
	}
}

func TestCollectionIndexedWriteMemtablesFindSkipsBufferedSecondaryTombstone(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"city":"hnl"}`), []byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert persisted rows: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush persisted rows: %v", err)
	}

	oldEncoded, err := encodeIndexScalar(IndexValueString, "hnl")
	if err != nil {
		t.Fatalf("encode old city: %v", err)
	}
	oldKey, err := indexEntryKey(oldEncoded, []byte("u1"))
	if err != nil {
		t.Fatalf("old index key: %v", err)
	}
	newEncoded, err := encodeIndexScalar(IndexValueString, "sea")
	if err != nil {
		t.Fatalf("encode new city: %v", err)
	}
	newKey, err := indexEntryKey(newEncoded, []byte("u1"))
	if err != nil {
		t.Fatalf("new index key: %v", err)
	}
	table := newCollectionRunTable(2)
	table.DeleteSteal(oldKey)
	setCollectionRunValue(table, newKey, nil)
	table.Freeze()
	domain := col.writeDomain
	domain.mu.Lock()
	domain.count = 1
	domain.meta = col.Meta()
	domain.rootRuns = map[string][]memtable.Table{
		collectionSecondaryRootName("users", "city"): {table},
	}
	domain.mu.Unlock()

	ids, err := col.FindByIndexValue("city", "hnl")
	if err != nil {
		t.Fatalf("find old city: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("old city ids=%q want [u2]", ids)
	}
	ids, truncated, err := col.FindByIndexValueLimit("city", "hnl", 1)
	if err != nil {
		t.Fatalf("find old city limited: %v", err)
	}
	if truncated || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("limited old city ids=%q truncated=%v want [u2]/false", ids, truncated)
	}
	ids, err = col.FindByIndexValue("city", "sea")
	if err != nil {
		t.Fatalf("find new city: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("new city ids=%q want [u1]", ids)
	}
}

func TestCollectionIndexedWriteMemtablesFindLimitFiltersOnlyBufferedTombstone(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert persisted row: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush persisted row: %v", err)
	}

	encoded, err := encodeIndexScalar(IndexValueString, "hnl")
	if err != nil {
		t.Fatalf("encode city: %v", err)
	}
	key, err := indexEntryKey(encoded, []byte("u1"))
	if err != nil {
		t.Fatalf("index key: %v", err)
	}
	table := newCollectionRunTable(1)
	table.DeleteSteal(key)
	table.Freeze()
	domain := col.writeDomain
	domain.mu.Lock()
	domain.count = 1
	domain.meta = col.Meta()
	domain.rootRuns = map[string][]memtable.Table{
		collectionSecondaryRootName("users", "city"): {table},
	}
	domain.mu.Unlock()

	ids, truncated, err := col.FindByIndexValueLimit("city", "hnl", 1)
	if err != nil {
		t.Fatalf("find limited city: %v", err)
	}
	if truncated || len(ids) != 0 {
		t.Fatalf("limited ids=%q truncated=%v want empty/false", ids, truncated)
	}
}

func TestBufferedIndexTableLockedLimitsLiveMaterialization(t *testing.T) {
	encoded, err := encodeIndexScalar(IndexValueString, "hnl")
	if err != nil {
		t.Fatalf("encode city: %v", err)
	}
	_, prefix, err := appendIndexValuePrefixSlice(nil, encoded)
	if err != nil {
		t.Fatalf("index prefix: %v", err)
	}
	table := newCollectionRunTable(4)
	for _, id := range [][]byte{[]byte("u0"), []byte("u1"), []byte("u2"), []byte("u3")} {
		key, err := indexEntryKey(encoded, id)
		if err != nil {
			t.Fatalf("index key %q: %v", id, err)
		}
		if bytes.Equal(id, []byte("u0")) {
			table.DeleteSteal(key)
		} else {
			setCollectionRunValue(table, key, nil)
		}
	}
	table.Freeze()
	domain := &collectionWriteDomain{
		count: 4,
		meta:  CollectionMeta{Name: "users"},
		rootRuns: map[string][]memtable.Table{
			collectionSecondaryRootName("users", "city"): {table},
		},
	}

	buffered, err := bufferedIndexTableLocked(domain, "users", "city", false, prefix, 1)
	if err != nil {
		t.Fatalf("buffered index table: %v", err)
	}
	defer resetCollectionRunTable(buffered)
	if buffered == nil {
		t.Fatal("buffered index table nil")
	}
	if got := buffered.Len(); got != 3 {
		t.Fatalf("buffered table len=%d want tombstone plus two live rows", got)
	}
}

func TestBufferedIndexTableLockedUsesUniqueValueIndexForMisses(t *testing.T) {
	hnlEncoded, err := encodeIndexScalar(IndexValueString, "hnl@example.com")
	if err != nil {
		t.Fatalf("encode hnl email: %v", err)
	}
	_, hnlPrefix, err := appendIndexValuePrefixSlice(nil, hnlEncoded)
	if err != nil {
		t.Fatalf("hnl prefix: %v", err)
	}
	seaEncoded, err := encodeIndexScalar(IndexValueString, "sea@example.com")
	if err != nil {
		t.Fatalf("encode sea email: %v", err)
	}
	_, seaPrefix, err := appendIndexValuePrefixSlice(nil, seaEncoded)
	if err != nil {
		t.Fatalf("sea prefix: %v", err)
	}
	key, err := indexEntryKey(seaEncoded, []byte("u2"))
	if err != nil {
		t.Fatalf("sea index key: %v", err)
	}
	table := newCollectionRunTable(1)
	setCollectionRunValue(table, key, nil)
	table.Freeze()
	defer resetCollectionRunTable(table)

	pending := newBufferedUniqueValueIndex(1)
	pending.add(seaPrefix)
	domain := &collectionWriteDomain{
		count: 1,
		meta:  CollectionMeta{Name: "users"},
		rootRuns: map[string][]memtable.Table{
			collectionSecondaryRootName("users", "email"): {table},
		},
		uniqueValueIndex: map[string]*bufferedUniqueValueIndex{
			"email": pending,
		},
	}

	missing, err := bufferedIndexTableLocked(domain, "users", "email", true, hnlPrefix, 0)
	if err != nil {
		t.Fatalf("buffered unique miss: %v", err)
	}
	if missing != nil {
		defer resetCollectionRunTable(missing)
		t.Fatalf("buffered unique miss table len=%d want nil", missing.Len())
	}
	buffered, err := bufferedIndexTableLocked(domain, "users", "email", true, seaPrefix, 0)
	if err != nil {
		t.Fatalf("buffered unique hit: %v", err)
	}
	if buffered == nil {
		t.Fatal("buffered unique hit table nil")
	}
	defer resetCollectionRunTable(buffered)
	if got := buffered.Len(); got != 1 {
		t.Fatalf("buffered unique hit len=%d want 1", got)
	}
}

func TestBufferedRootRunsIteratorSingleRunHidesTombstones(t *testing.T) {
	table := newCollectionRunTable(2)
	table.DeleteSteal([]byte("a"))
	setCollectionRunValue(table, []byte("b"), []byte("value"))
	table.Freeze()

	it := newBufferedRootRunsIterator([]memtable.Table{table}, nil, nil)
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		t.Fatal("iterator invalid, want live key b")
	}
	if got := it.UnsafeKey(); !bytes.Equal(got, []byte("b")) {
		t.Fatalf("first key=%q want b", got)
	}
	it.Next()
	if it.Valid() {
		t.Fatalf("iterator has extra key %q", it.UnsafeKey())
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
}

func TestBufferedRootRunsIteratorSingleRunIncludesTombstones(t *testing.T) {
	table := newCollectionRunTable(2)
	table.DeleteSteal([]byte("a"))
	setCollectionRunValue(table, []byte("b"), []byte("value"))
	table.Freeze()

	it := newBufferedRootRunsIteratorWithDeleted([]memtable.Table{table}, nil, nil, true)
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		t.Fatal("iterator invalid, want tombstone key a")
	}
	if got := it.UnsafeKey(); !bytes.Equal(got, []byte("a")) || !it.IsDeleted() {
		t.Fatalf("first key=%q deleted=%v want tombstone a", got, it.IsDeleted())
	}
	it.Next()
	if !it.Valid() {
		t.Fatal("iterator missing live key b")
	}
	if got := it.UnsafeKey(); !bytes.Equal(got, []byte("b")) || it.IsDeleted() {
		t.Fatalf("second key=%q deleted=%v want live b", got, it.IsDeleted())
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
}

func TestBufferedRootRunsIteratorWorkCapBoundsRawSingleRootForwardAndReverse(t *testing.T) {
	// A catalog root without overlays is deliberately wrapped in this iterator
	// for direct compound scans. Keep this lower-level fixture non-vacuous: it
	// has more physical records than the cap, rather than relying on a compacted
	// collection where historical tombstones have already been discarded.
	table := newCollectionRunTable(65)
	defer resetCollectionRunTable(table)
	for i := 0; i < 65; i++ {
		key := []byte(fmt.Sprintf("%03d", i))
		if i%2 == 0 {
			table.DeleteSteal(key)
		} else {
			setCollectionRunValue(table, key, []byte("live"))
		}
	}
	table.Freeze()
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "forward", true: "reverse"}[reverse], func(t *testing.T) {
			var source iterator.UnsafeIterator
			if reverse {
				source = table.NewReverseIterator(nil, nil)
			} else {
				source = table.NewIterator(nil, nil)
			}
			it := newBufferedRootRunIteratorSourcesIteratorWithDeletedDirectionWorkCap(
				[]bufferedRootRunIteratorSource{{iter: source}}, nil, nil, true, false, reverse, 64,
			)
			defer func() { _ = it.Close() }()
			count := 0
			for it.Valid() {
				count++
				it.Next()
			}
			if count != 64 {
				t.Fatalf("physical entries returned=%d want 64", count)
			}
			if !errors.Is(it.Error(), errCollectionIndexScanWorkCap) {
				t.Fatalf("iterator error=%v want work cap", it.Error())
			}
		})
	}
}

func TestBufferedRootRunsIteratorWorkCapRejectsOverlaySourceFanout(t *testing.T) {
	// The capped compositor must reject before opening/seeking an arbitrary
	// number of overlay roots. Each table has one physical entry; a 64-entry
	// budget therefore cannot admit 65 sources.
	tables := make([]memtable.Table, 65)
	for i := range tables {
		table := newCollectionRunTable(1)
		setCollectionRunValue(table, []byte(fmt.Sprintf("%03d", i)), []byte("value"))
		table.Freeze()
		tables[i] = table
	}
	defer resetCollectionTables(tables)
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "forward", true: "reverse"}[reverse], func(t *testing.T) {
			it := newBufferedRootRunsIteratorWithDeletedDirectionWorkCap(tables, nil, nil, true, reverse, 64)
			defer func() { _ = it.Close() }()
			if it.Valid() {
				t.Fatalf("iterator valid despite source fanout above cap")
			}
			if !errors.Is(it.Error(), errCollectionIndexScanWorkCap) {
				t.Fatalf("iterator error=%v want work cap", it.Error())
			}
		})
	}
}

func TestBufferedRootRunsIteratorMultiRunIncludesNewestTombstone(t *testing.T) {
	older := newCollectionRunTable(2)
	setCollectionRunValue(older, []byte("a"), []byte("older"))
	setCollectionRunValue(older, []byte("b"), []byte("older"))
	older.Freeze()

	newer := newCollectionRunTable(2)
	newer.DeleteSteal([]byte("a"))
	setCollectionRunValue(newer, []byte("c"), []byte("newer"))
	newer.Freeze()

	it := newBufferedRootRunsIteratorWithDeleted([]memtable.Table{older, newer}, nil, nil, true)
	defer func() { _ = it.Close() }()
	if !it.Valid() {
		t.Fatal("iterator invalid, want tombstone key a")
	}
	if got := it.UnsafeKey(); !bytes.Equal(got, []byte("a")) || !it.IsDeleted() {
		t.Fatalf("first key=%q deleted=%v want newest tombstone a", got, it.IsDeleted())
	}
	it.Next()
	if !it.Valid() {
		t.Fatal("iterator missing live key b")
	}
	if got := it.UnsafeKey(); !bytes.Equal(got, []byte("b")) || it.IsDeleted() {
		t.Fatalf("second key=%q deleted=%v want live b", got, it.IsDeleted())
	}
	it.Next()
	if !it.Valid() {
		t.Fatal("iterator missing live key c")
	}
	if got := it.UnsafeKey(); !bytes.Equal(got, []byte("c")) || it.IsDeleted() {
		t.Fatalf("third key=%q deleted=%v want live c", got, it.IsDeleted())
	}
	it.Next()
	if it.Valid() {
		t.Fatalf("iterator has extra key %q", it.UnsafeKey())
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
}

func TestBufferedRootRunsIteratorDirectPathPreservesDisjointRunOrder(t *testing.T) {
	front := newCollectionRunTable(3)
	defer resetCollectionRunTable(front)
	setCollectionRunValue(front, []byte("a"), []byte("front-a"))
	setCollectionRunValue(front, []byte("c"), []byte("front-c"))
	setCollectionRunValue(front, []byte("e"), []byte("front-e"))
	front.Freeze()

	back := newCollectionRunTable(1)
	defer resetCollectionRunTable(back)
	setCollectionRunValue(back, []byte("z"), []byte("back-z"))
	back.Freeze()

	it := newBufferedRootRunsIteratorWithDeleted([]memtable.Table{front, back}, nil, nil, true)
	defer func() { _ = it.Close() }()

	var got []string
	for it.Valid() {
		got = append(got, string(it.UnsafeKey())+"="+string(it.UnsafeValue()))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	want := []string{"a=front-a", "c=front-c", "e=front-e", "z=back-z"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("entries=%v want %v", got, want)
	}
}

func TestBufferedRootRunsIteratorDirectPathFallsBackForEqualKeys(t *testing.T) {
	older := newCollectionRunTable(2)
	defer resetCollectionRunTable(older)
	setCollectionRunValue(older, []byte("a"), []byte("older-a"))
	setCollectionRunValue(older, []byte("b"), []byte("older-b"))
	older.Freeze()

	newer := newCollectionRunTable(1)
	defer resetCollectionRunTable(newer)
	setCollectionRunValue(newer, []byte("b"), []byte("newer-b"))
	newer.Freeze()

	it := newBufferedRootRunsIteratorWithDeleted([]memtable.Table{older, newer}, nil, nil, true)
	defer func() { _ = it.Close() }()

	if !it.Valid() {
		t.Fatal("iterator invalid, want key a")
	}
	if got := string(it.UnsafeKey()) + "=" + string(it.UnsafeValue()); got != "a=older-a" {
		t.Fatalf("first entry=%s want a=older-a", got)
	}
	it.Next()
	if !it.Valid() {
		t.Fatal("iterator missing shadowed key b")
	}
	if got := string(it.UnsafeKey()) + "=" + string(it.UnsafeValue()); got != "b=newer-b" {
		t.Fatalf("second entry=%s want b=newer-b", got)
	}
	it.Next()
	if it.Valid() {
		t.Fatalf("iterator has extra key %q", it.UnsafeKey())
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
}

func TestBufferedRootRunsIteratorDirectPathSkipsMiddleTombstone(t *testing.T) {
	table := newCollectionRunTable(3)
	defer resetCollectionRunTable(table)
	setCollectionRunValue(table, []byte("a"), []byte("live-a"))
	table.DeleteSteal([]byte("b"))
	setCollectionRunValue(table, []byte("c"), []byte("live-c"))
	table.Freeze()

	it := newBufferedRootRunsIterator([]memtable.Table{table}, nil, nil)
	defer func() { _ = it.Close() }()

	var got []string
	for it.Valid() {
		got = append(got, string(it.UnsafeKey()))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	want := []string{"a", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys=%v want %v", got, want)
	}
}

func TestBufferedRootRunsIteratorMultiRunStableUnsafeSlices(t *testing.T) {
	oldAKey := []byte("a")
	oldBKey := []byte("b")
	oldBValue := []byte("older")
	newAKey := []byte("a")
	newCKey := []byte("c")
	newCValue := []byte("newer")

	older := newCollectionRunTable(2)
	defer resetCollectionRunTable(older)
	setCollectionRunValue(older, oldAKey, []byte("older"))
	setCollectionRunValue(older, oldBKey, oldBValue)
	older.Freeze()

	newer := newCollectionRunTable(2)
	defer resetCollectionRunTable(newer)
	newer.DeleteSteal(newAKey)
	setCollectionRunValue(newer, newCKey, newCValue)
	newer.Freeze()

	it := newBufferedRootRunsIteratorWithDeleted([]memtable.Table{older, newer}, nil, nil, true)
	stable, ok := it.(interface {
		StableUnsafeIteratorSlices() bool
	})
	if !ok {
		t.Fatalf("buffered root iterator does not expose StableUnsafeIteratorSlices")
	}
	if !stable.StableUnsafeIteratorSlices() {
		t.Fatalf("buffered root iterator did not preserve stable memtable slices")
	}
	lenHint, ok := it.(interface {
		Len() int
	})
	if !ok {
		t.Fatalf("buffered root iterator does not expose Len hint")
	}
	if got, wantAtLeast := lenHint.Len(), 3; got < wantAtLeast {
		t.Fatalf("buffered root iterator Len hint=%d want at least %d", got, wantAtLeast)
	}

	delta, err := backenddb.OrderedRootDeltaBatchFromIterator(it)
	if err != nil {
		t.Fatalf("materialize delta: %v", err)
	}
	defer func() { _ = delta.Close() }()
	if err := it.Close(); err != nil {
		t.Fatalf("close iterator: %v", err)
	}

	entries := delta.SortedEntries()
	if got, want := len(entries), 3; got != want {
		t.Fatalf("delta entries=%d want %d", got, want)
	}
	if got := entries[0]; string(got.Key) != "a" || got.Type != batch.OpDelete {
		t.Fatalf("entry[0]=%+v want tombstone a", got)
	}
	if unsafe.SliceData(entries[0].Key) != unsafe.SliceData(newAKey) {
		t.Fatal("tombstone key was copied instead of borrowed from the newer run")
	}
	if got := entries[1]; string(got.Key) != "b" || string(got.Value) != "older" {
		t.Fatalf("entry[1]=%+v want b=older", got)
	}
	if unsafe.SliceData(entries[1].Key) != unsafe.SliceData(oldBKey) {
		t.Fatal("older run key was copied instead of borrowed")
	}
	if unsafe.SliceData(entries[1].Value) != unsafe.SliceData(oldBValue) {
		t.Fatal("older run value was copied instead of borrowed")
	}
	if got := entries[2]; string(got.Key) != "c" || string(got.Value) != "newer" {
		t.Fatalf("entry[2]=%+v want c=newer", got)
	}
	if unsafe.SliceData(entries[2].Key) != unsafe.SliceData(newCKey) {
		t.Fatal("newer run key was copied instead of borrowed")
	}
	if unsafe.SliceData(entries[2].Value) != unsafe.SliceData(newCValue) {
		t.Fatal("newer run value was copied instead of borrowed")
	}
}

func TestBuildBufferedRootDeltaBatchPublishInputsParallelPreservesRootOrderAndTombstones(t *testing.T) {
	primaryName := collectionPrimaryRootName("users")
	stateName := collectionIndexStateRootName("users")
	cityName := collectionSecondaryRootName("users", "city")

	primaryTable := newCollectionRunTable(2)
	primaryTable.DeleteSteal([]byte("u1"))
	setCollectionRunValue(primaryTable, []byte("u2"), []byte(`{"city":"sea"}`))
	primaryTable.Freeze()

	stateTable := newCollectionRunTable(1)
	setCollectionRunValue(stateTable, []byte("u2"), []byte("state"))
	stateTable.Freeze()

	cityTable := newCollectionRunTable(1)
	setCollectionRunValue(cityTable, []byte("city:sea/u2"), nil)
	cityTable.Freeze()
	defer resetCollectionTables([]memtable.Table{primaryTable, stateTable, cityTable})

	rootNames := []string{stateName, primaryName, cityName}
	rootRuns := map[string][]memtable.Table{
		primaryName: {primaryTable},
		stateName:   {stateTable},
		cityName:    {cityTable},
	}
	rootBaseIDs := map[string]uint64{
		primaryName: 42,
		stateName:   43,
		cityName:    44,
	}
	rootPolicies := map[string]backenddb.OrderedRootStoragePolicy{
		primaryName: backenddb.OrderedRootStorageValueLogLeaves,
		stateName:   backenddb.OrderedRootStoragePagerLeaves,
		cityName:    backenddb.OrderedRootStoragePagerLeaves,
	}

	ordered, cleanup, err := buildBufferedRootDeltaBatchPublishInputs(rootNames, rootRuns, rootBaseIDs, rootPolicies)
	if err != nil {
		t.Fatalf("build buffered root deltas: %v", err)
	}
	defer cleanup()

	if got, want := len(ordered), len(rootNames); got != want {
		t.Fatalf("ordered roots=%d want %d", got, want)
	}
	for i, rootName := range rootNames {
		if ordered[i].BaseRoot != rootBaseIDs[rootName] {
			t.Fatalf("ordered[%d] base=%d want %d", i, ordered[i].BaseRoot, rootBaseIDs[rootName])
		}
		if ordered[i].StoragePolicy != rootPolicies[rootName] {
			t.Fatalf("ordered[%d] policy=%d want %d", i, ordered[i].StoragePolicy, rootPolicies[rootName])
		}
		if !ordered[i].ParallelApply {
			t.Fatalf("ordered[%d] ParallelApply=false want true", i)
		}
		if ordered[i].IncludeDeletedOnColdBuild {
			t.Fatalf("ordered[%d] IncludeDeletedOnColdBuild=true want false", i)
		}
	}

	primaryEntries := ordered[1].Delta.SortedEntries()
	if got, want := len(primaryEntries), 2; got != want {
		t.Fatalf("primary entries=%d want %d", got, want)
	}
	if entry := primaryEntries[0]; string(entry.Key) != "u1" || entry.Type != batch.OpDelete {
		t.Fatalf("primary entry[0]=%+v want tombstone u1", entry)
	}
	if entry := primaryEntries[1]; string(entry.Key) != "u2" || string(entry.Value) != `{"city":"sea"}` {
		t.Fatalf("primary entry[1]=%+v want u2 document", entry)
	}

	stateEntries := ordered[0].Delta.SortedEntries()
	if got, want := len(stateEntries), 1; got != want || string(stateEntries[0].Key) != "u2" || string(stateEntries[0].Value) != "state" {
		t.Fatalf("state entries=%+v want u2=state", stateEntries)
	}
	cityEntries := ordered[2].Delta.SortedEntries()
	if got, want := len(cityEntries), 1; got != want || string(cityEntries[0].Key) != "city:sea/u2" || cityEntries[0].Type != batch.OpPut {
		t.Fatalf("city entries=%+v want city:sea/u2 put", cityEntries)
	}
}

func TestBuildBufferedRootOverlayDeltaBatchPublishInputsPreservesColdTombstones(t *testing.T) {
	primaryName := collectionPrimaryRootName("users")
	cityName := collectionSecondaryRootName("users", "city")

	primaryTable := newCollectionRunTable(2)
	primaryTable.DeleteSteal([]byte("u1"))
	setCollectionRunValue(primaryTable, []byte("u2"), []byte(`{"city":"sea"}`))
	primaryTable.Freeze()

	cityTable := newCollectionRunTable(1)
	cityTable.DeleteSteal([]byte("city:old/u1"))
	cityTable.Freeze()
	defer resetCollectionTables([]memtable.Table{primaryTable, cityTable})

	rootNames := []string{primaryName, cityName}
	rootRuns := map[string][]memtable.Table{
		primaryName: {primaryTable},
		cityName:    {cityTable},
	}
	rootPolicies := map[string]backenddb.OrderedRootStoragePolicy{
		primaryName: backenddb.OrderedRootStorageValueLogLeaves,
		cityName:    backenddb.OrderedRootStoragePagerLeaves,
	}
	rootOverlays := map[string][]uint64{
		primaryName: {102},
		cityName:    nil,
	}

	ordered, cleanup, err := buildBufferedRootOverlayDeltaBatchPublishInputs(rootNames, rootRuns, rootPolicies, rootOverlays)
	if err != nil {
		t.Fatalf("build overlay root deltas: %v", err)
	}
	defer cleanup()

	if got, want := len(ordered), len(rootNames); got != want {
		t.Fatalf("ordered roots=%d want %d", got, want)
	}
	if ordered[0].BaseRoot != 102 {
		t.Fatalf("primary overlay base=%d want latest overlay 102", ordered[0].BaseRoot)
	}
	if ordered[1].BaseRoot != 0 {
		t.Fatalf("city overlay base=%d want cold overlay 0", ordered[1].BaseRoot)
	}
	for i := range ordered {
		if !ordered[i].IncludeDeletedOnColdBuild {
			t.Fatalf("ordered[%d] IncludeDeletedOnColdBuild=false want true", i)
		}
		if !ordered[i].ParallelApply {
			t.Fatalf("ordered[%d] ParallelApply=false want true", i)
		}
	}

	primaryEntries := ordered[0].Delta.SortedEntries()
	if got, want := len(primaryEntries), 2; got != want {
		t.Fatalf("primary entries=%d want %d", got, want)
	}
	if entry := primaryEntries[0]; string(entry.Key) != "u1" || entry.Type != batch.OpDelete {
		t.Fatalf("primary entry[0]=%+v want tombstone u1", entry)
	}

	cityEntries := ordered[1].Delta.SortedEntries()
	if got, want := len(cityEntries), 1; got != want {
		t.Fatalf("city entries=%d want %d", got, want)
	}
	if entry := cityEntries[0]; string(entry.Key) != "city:old/u1" || entry.Type != batch.OpDelete {
		t.Fatalf("city entry[0]=%+v want tombstone city:old/u1", entry)
	}
}

func BenchmarkBufferedRootRunsIteratorBuildManyRuns(b *testing.B) {
	const runCount = 8192
	runs := make([]memtable.Table, 0, runCount)
	for i := 0; i < runCount; i++ {
		table := newCollectionRunTable(1)
		setCollectionRunValue(table, []byte(fmt.Sprintf("k%08d", i)), []byte("value"))
		table.Freeze()
		runs = append(runs, table)
	}
	defer resetCollectionTables(runs)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := newBufferedRootRunsIteratorWithDeleted(runs, nil, nil, true)
		if !it.Valid() {
			b.Fatal("iterator invalid")
		}
		if err := it.Close(); err != nil {
			b.Fatalf("close iterator: %v", err)
		}
	}
}

func TestCollectMergedCollectionIndexIDsSkipsPersistedTombstones(t *testing.T) {
	encoded, err := encodeIndexScalar(IndexValueString, "hnl")
	if err != nil {
		t.Fatalf("encode city: %v", err)
	}
	_, prefix, err := appendIndexValuePrefixSlice(nil, encoded)
	if err != nil {
		t.Fatalf("index prefix: %v", err)
	}
	key, err := indexEntryKey(encoded, []byte("u1"))
	if err != nil {
		t.Fatalf("index key: %v", err)
	}
	table := newCollectionRunTable(1)
	table.DeleteSteal(key)
	table.Freeze()
	it := table.NewIterator(prefix, prefixEnd(prefix))
	defer func() { _ = it.Close() }()

	ids, truncated, err := collectMergedCollectionIndexIDs(nil, it, prefix, 1)
	if err != nil {
		t.Fatalf("collect merged ids: %v", err)
	}
	if truncated || len(ids) != 0 {
		t.Fatalf("ids=%q truncated=%v want empty/false", ids, truncated)
	}
}

func TestCollectionFindByIndexValueLimitMaxIntDoesNotOverflow(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	ids, truncated, err := col.FindByIndexValueLimit("city", "hnl", int(^uint(0)>>1))
	if err != nil {
		t.Fatalf("find max-int limit: %v", err)
	}
	if truncated || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("max-int limited ids=%q truncated=%v want [u1]/false", ids, truncated)
	}
}

func TestCollectionIndexedWriteMemtablesRejectPersistedUniqueConflict(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"email":"ada@example.com"}`)}); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	_, err = col.InsertBatch(
		[][]byte{[]byte("u2"), []byte("u3")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	)
	if err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("persisted duplicate unique err=%v want unique index conflict", err)
	}
	got, err := col.Get([]byte("u3"))
	if err != nil {
		t.Fatalf("get failed insert: %v", err)
	}
	if got != nil {
		t.Fatalf("failed insert left buffered doc=%q", got)
	}
}

func TestCollectionIndexedWriteMemtablesAutoFlushMaxDocuments(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 2,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("first insert batch: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog after first batch: %v", err)
	}
	if got := catalog.rootID(collectionPrimaryRootName("users")); got != 0 {
		t.Fatalf("primary root persisted before threshold flush: %d", got)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"email":"grace@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("second insert batch: %v", err)
	}
	snap = d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot after threshold flush")
	}
	catalog, err = loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog after threshold flush: %v", err)
	}
	if got := catalog.rootID(collectionPrimaryRootName("users")); got == 0 {
		t.Fatal("primary root was not persisted after threshold flush")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	ids, err := reopenedCol.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city after threshold flush: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, ids, []byte("u1"), []byte("u2"))
}

func TestCollectionIndexedWriteMemtablesAsyncAutoFlushDrainsOnFlush(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        2,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("first insert batch: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog after first batch: %v", err)
	}
	if got := catalog.rootID(collectionPrimaryRootName("users")); got != 0 {
		t.Fatalf("primary root persisted before threshold flush: %d", got)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"email":"grace@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("second insert batch: %v", err)
	}
	stats := mgr.StatsSnapshot()
	if got, want := stats.IndexedAutoFlushes, uint64(1); got != want {
		t.Fatalf("indexed auto flushes=%d want %d", got, want)
	}
	if got, want := stats.IndexedAsyncFlushScheduled, uint64(1); got != want {
		t.Fatalf("async flush scheduled=%d want %d", got, want)
	}
	if got, err := col.Get([]byte("u2")); err != nil {
		t.Fatalf("get queued async doc: %v", err)
	} else if want := []byte(`{"email":"grace@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("queued async doc=%q want %q", got, want)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush async indexed writes: %v", err)
	}
	stats = mgr.StatsSnapshot()
	if got := stats.PendingDocuments; got != 0 {
		t.Fatalf("pending docs after async flush drain=%d want 0", got)
	}
	if got := stats.PendingIndexedFlushUnits; got != 0 {
		t.Fatalf("pending async flush units after drain=%d want 0", got)
	}
	if stats.IndexedFlushCalls == 0 {
		t.Fatal("indexed flush calls=0 want background or drain flush")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	ids, err := reopenedCol.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city after async flush: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, ids, []byte("u1"), []byte("u2"))
}

func TestCollectionIndexedWriteMemtablesAsyncBackpressurePublishesSynchronously(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 1,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	stats := mgr.StatsSnapshot()
	if got, want := stats.IndexedAsyncFlushBackpressure, uint64(1); got != want {
		t.Fatalf("async backpressure sync=%d want %d", got, want)
	}
	if got := stats.IndexedAsyncFlushScheduled; got != 0 {
		t.Fatalf("async scheduled=%d want 0 when backpressure syncs", got)
	}
	if got := stats.PendingDocuments; got != 0 {
		t.Fatalf("pending docs after backpressure sync=%d want 0", got)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot after backpressure sync")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog after backpressure sync: %v", err)
	}
	if got := catalog.rootID(collectionPrimaryRootName("users")); got == 0 {
		t.Fatal("primary root was not persisted after backpressure sync")
	}
}

func TestCollectionIndexedWriteMemtablesAsyncBackpressureWaitsForPublishingUnit(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        100,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 1,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com"}`)},
	); err != nil {
		t.Fatalf("seed insert batch: %v", err)
	}

	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	var asyncFinished atomic.Bool
	finishAsync := func(err error) {
		if asyncFinished.CompareAndSwap(false, true) {
			col.writeDomain.finishIndexedAsyncFlush(err)
		}
	}
	if !col.writeDomain.beginIndexedAsyncFlush() {
		t.Fatal("begin async flush returned false")
	}
	t.Cleanup(func() {
		if !asyncFinished.Load() && work.pin != nil {
			_ = work.pin.Close()
			work.pin = nil
		}
		finishAsync(errors.New("test cleanup"))
	})

	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"email":"grace@example.com"}`)},
	); err != nil {
		t.Fatalf("second insert batch: %v", err)
	}

	backpressureDone := make(chan error, 1)
	go func() {
		col.writeDomain.mu.Lock()
		_, _, _, err := col.flushBufferedIndexedAfterThresholdLocked(col.writeDomain, CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 1,
		})
		col.writeDomain.mu.Unlock()
		backpressureDone <- err
	}()
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if mgr.StatsSnapshot().IndexedAsyncFlushBackpressure > 0 {
			break
		}
		select {
		case err := <-backpressureDone:
			t.Fatalf("backpressure flush returned before in-flight async publish drained: %v", err)
		case <-time.After(time.Millisecond):
		}
	}
	if got := mgr.StatsSnapshot().IndexedAsyncFlushBackpressure; got == 0 {
		t.Fatal("async backpressure did not wait for in-flight publishing unit")
	}

	publishDone := make(chan error, 1)
	go func() {
		err := col.publishPreparedIndexedFlush(work)
		finishAsync(err)
		publishDone <- err
	}()
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publish prepared async flush: %v", err)
		}
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out publishing prepared async flush")
	}
	select {
	case err := <-backpressureDone:
		if err != nil {
			t.Fatalf("backpressure flush: %v", err)
		}
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out waiting for backpressure flush")
	}

	stats := mgr.StatsSnapshot()
	if got := stats.PendingIndexedFlushUnits; got != 0 {
		t.Fatalf("pending indexed flush units after backpressure drain=%d want 0", got)
	}
	if got := stats.PendingDocuments; got != 0 {
		t.Fatalf("pending documents after backpressure drain=%d want 0", got)
	}
	if got := stats.IndexedAsyncFlushWait; got <= 0 {
		t.Fatalf("async flush wait=%s want positive", got)
	}
	if got := stats.IndexedFlushForcedDrains; got == 0 {
		t.Fatal("indexed flush forced drains=0 want positive")
	}
	for _, id := range []string{"u1", "u2"} {
		got, err := col.Get([]byte(id))
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if len(got) == 0 {
			t.Fatalf("get %s returned empty document", id)
		}
	}
}

func TestCollectionIndexedWriteMemtablesAsyncScheduleRacePublishesSynchronously(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if !col.writeDomain.beginIndexedAsyncFlush() {
		t.Fatal("begin async flush returned false")
	}
	defer col.writeDomain.finishIndexedAsyncFlush(nil)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	stats := mgr.StatsSnapshot()
	if got := stats.IndexedAsyncFlushScheduled; got != 0 {
		t.Fatalf("async scheduled=%d want 0 when scheduling race falls back", got)
	}
	if got := stats.IndexedFlushCalls; got != 1 {
		t.Fatalf("indexed flush calls=%d want sync fallback flush", got)
	}
	if got := stats.IndexedFlushForcedDrains; got != 1 {
		t.Fatalf("indexed flush forced drains=%d want 1", got)
	}
	if got := stats.PendingDocuments; got != 0 {
		t.Fatalf("pending docs after sync fallback=%d want 0", got)
	}
}

func TestCollectionIndexedWriteMemtablesAsyncQueuedUnitsParticipateInUniqueChecks(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        2,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if got := mgr.StatsSnapshot().IndexedAsyncFlushScheduled; got != 1 {
		t.Fatalf("async flush scheduled=%d want 1", got)
	}
	_, err = col.InsertBatch(
		[][]byte{[]byte("u3")},
		[][]byte{[]byte(`{"email":"grace@example.com"}`)},
	)
	if err == nil || !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate queued unique insert err=%v want ErrUniqueIndexConflict", err)
	}
}

func TestCollectionIndexedWriteMemtablesAsyncPublishingUnitsParticipateInReadsAndUniqueChecks(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1024,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	if got := mgr.StatsSnapshot().PendingIndexedFlushUnits; got != 1 {
		t.Fatalf("pending publishing units=%d want 1", got)
	}
	got, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get publishing doc: %v", err)
	}
	if want := []byte(`{"email":"grace@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("publishing doc=%q want %q", got, want)
	}
	ids, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find publishing city: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, ids, []byte("u1"), []byte("u2"))
	_, err = col.InsertBatch(
		[][]byte{[]byte("u3")},
		[][]byte{[]byte(`{"email":"grace@example.com","city":"sfo"}`)},
	)
	if err == nil || !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate publishing unique insert err=%v want ErrUniqueIndexConflict", err)
	}
	if err := col.publishPreparedIndexedFlush(work); err != nil {
		t.Fatalf("publish prepared async flush: %v", err)
	}
	if got := mgr.StatsSnapshot().PendingDocuments; got != 0 {
		t.Fatalf("pending docs after publish=%d want 0", got)
	}
}

func TestCollectionIndexedWriteMemtablesFlushWaitsForPublishingUnits(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1024,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	var asyncFinished atomic.Bool
	finishAsync := func(err error) {
		if asyncFinished.CompareAndSwap(false, true) {
			col.writeDomain.finishIndexedAsyncFlush(err)
		}
	}
	if !col.writeDomain.beginIndexedAsyncFlush() {
		t.Fatal("begin async flush returned false")
	}
	t.Cleanup(func() {
		if !asyncFinished.Load() && work.pin != nil {
			_ = work.pin.Close()
			work.pin = nil
		}
		finishAsync(errors.New("test cleanup"))
	})

	waitEntered, releaseWait := collectionWaitIndexedAsyncFlushGateForTest(t)
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- col.flushBufferedWrites()
	}()
	select {
	case <-waitEntered:
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out waiting for flush to reach indexed async drain")
	}
	select {
	case err := <-flushDone:
		t.Fatalf("flush returned after entering indexed async drain but before publish finished: %v", err)
	default:
	}
	releaseWait()
	publishDone := make(chan error, 1)
	go func() {
		err := col.publishPreparedIndexedFlush(work)
		finishAsync(err)
		publishDone <- err
	}()
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publish prepared async flush: %v", err)
		}
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out publishing prepared async flush")
	}
	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatalf("flush after in-flight async publish: %v", err)
		}
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out waiting for flush after publish")
	}
	if got := mgr.StatsSnapshot().PendingDocuments; got != 0 {
		t.Fatalf("pending docs after async publish=%d want 0", got)
	}
}

func TestCollectionIndexedWriteMemtablesFlushRetriesAsyncScheduledDuringWaitGap(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1024,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	domain := col.writeDomain
	waitEntered, releaseWait := collectionWaitIndexedAsyncFlushGateForTest(t)
	domain.mu.Lock()
	scanDone := make(chan error, 1)
	go func() {
		_, err := col.ScanDocumentsFunc(16, func(DocumentRecord) (bool, error) {
			return true, nil
		})
		scanDone <- err
	}()
	if !domain.beginIndexedAsyncFlush() {
		domain.mu.Unlock()
		t.Fatal("begin async flush returned false")
	}
	work, err := col.prepareIndexedAsyncPublishLocked(domain)
	if err != nil {
		domain.mu.Unlock()
		domain.finishIndexedAsyncFlush(err)
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		domain.mu.Unlock()
		domain.finishIndexedAsyncFlush(nil)
		t.Fatal("prepare async publish returned nil work")
	}
	domain.mu.Unlock()

	var asyncFinished atomic.Bool
	finishAsync := func(err error) {
		if asyncFinished.CompareAndSwap(false, true) {
			domain.finishIndexedAsyncFlush(err)
		}
	}
	t.Cleanup(func() {
		if !asyncFinished.Load() && work.pin != nil {
			_ = work.pin.Close()
			work.pin = nil
		}
		finishAsync(errors.New("test cleanup"))
	})

	select {
	case <-waitEntered:
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out waiting for scan to reach indexed async drain")
	}
	select {
	case err := <-scanDone:
		t.Fatalf("scan returned after entering indexed async drain but before publish finished: %v", err)
	default:
	}
	releaseWait()
	publishDone := make(chan error, 1)
	go func() {
		err := col.publishPreparedIndexedFlush(work)
		finishAsync(err)
		publishDone <- err
	}()
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publish prepared async flush: %v", err)
		}
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out publishing prepared async flush")
	}
	select {
	case err := <-scanDone:
		if err != nil {
			t.Fatalf("scan after scheduled async publish: %v", err)
		}
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out waiting for scan after publish")
	}
}

func TestCollectionIndexedWriteMemtablesCreateIndexWaitsForPublishingUnits(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1024,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	var asyncFinished atomic.Bool
	finishAsync := func(err error) {
		if asyncFinished.CompareAndSwap(false, true) {
			col.writeDomain.finishIndexedAsyncFlush(err)
		}
	}
	if !col.writeDomain.beginIndexedAsyncFlush() {
		t.Fatal("begin async flush returned false")
	}
	t.Cleanup(func() {
		if !asyncFinished.Load() && work.pin != nil {
			_ = work.pin.Close()
			work.pin = nil
		}
		finishAsync(errors.New("test cleanup"))
	})

	waitEntered, releaseWait := collectionWaitIndexedAsyncFlushGateForTest(t)
	createDone := make(chan error, 1)
	go func() {
		_, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city", ValueType: IndexValueString})
		createDone <- err
	}()
	select {
	case <-waitEntered:
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out waiting for CreateIndex to reach indexed async drain")
	}
	select {
	case err := <-createDone:
		t.Fatalf("CreateIndex returned after entering indexed async drain but before publish finished: %v", err)
	default:
	}
	releaseWait()
	publishDone := make(chan error, 1)
	go func() {
		err := col.publishPreparedIndexedFlush(work)
		finishAsync(err)
		publishDone <- err
	}()
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publish prepared async flush: %v", err)
		}
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out publishing prepared async flush")
	}
	select {
	case err := <-createDone:
		if err != nil {
			t.Fatalf("CreateIndex after in-flight async publish: %v", err)
		}
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("timed out waiting for CreateIndex after publish")
	}
	ids, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city after CreateIndex: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, ids, []byte("u1"))
}

func collectionWaitIndexedAsyncFlushGateForTest(tb testing.TB) (<-chan struct{}, func()) {
	tb.Helper()
	waitEntered := make(chan struct{})
	allowWait := make(chan struct{})
	var waitEnteredOnce sync.Once
	var releaseOnce sync.Once
	restoreWaitHook := setCollectionWaitIndexedAsyncFlushHookForTest(func() {
		waitEnteredOnce.Do(func() {
			close(waitEntered)
			<-allowWait
		})
	})
	release := func() {
		releaseOnce.Do(func() {
			close(allowWait)
			restoreWaitHook()
		})
	}
	tb.Cleanup(release)
	return waitEntered, release
}

func TestCollectionIndexedWriteMemtablesAsyncSuccessClearsPriorError(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1024,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	col.writeDomain.finishIndexedAsyncFlush(errors.New("prior async failure"))
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	if err := col.publishPreparedIndexedFlush(work); err != nil {
		t.Fatalf("publish prepared async flush: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush after successful async publish surfaced stale error: %v", err)
	}
}

func TestCollectionIndexedWriteMemtablesAsyncPublishRetargetsMutableRuns(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1024,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl","score":1}`)},
	); err != nil {
		t.Fatalf("insert first batch: %v", err)
	}
	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"city":"hnl","score":2}`)},
	); err != nil {
		t.Fatalf("insert while publish is in flight: %v", err)
	}
	if err := col.publishPreparedIndexedFlush(work); err != nil {
		t.Fatalf("publish prepared async flush: %v", err)
	}
	if got := mgr.StatsSnapshot().PendingDocuments; got != 1 {
		t.Fatalf("pending docs after publish=%d want mutable doc", got)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush mutable runs after publish retarget: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	ids, err := reopenedCol.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city after retargeted flush: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, ids, []byte("u1"), []byte("u2"))
}

func TestCollectionIndexedWriteMemtablesAsyncUpdateAndDeleteDrainCorrectly(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1,
			BufferedIndexedAsyncFlush:               true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 8,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"city":"hnl","score":0}`),
			[]byte(`{"city":"hnl","score":0}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"city":"sfo","score":1}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("update batch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("update results=%+v want matched modified", results)
	}
	if got := mgr.StatsSnapshot().IndexedAsyncFlushScheduled; got == 0 {
		t.Fatal("async flush scheduled=0 want update to schedule a flush")
	}
	ids, err := col.FindByIndex("city", "sfo")
	if err != nil {
		t.Fatalf("find updated city before drain: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, ids, []byte("u1"))
	deleted, err := col.DeleteDocument([]byte("u1"))
	if err != nil {
		t.Fatalf("delete after queued update: %v", err)
	}
	if !deleted {
		t.Fatal("delete after queued update reported not found")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	if got, err := reopenedCol.Get([]byte("u1")); err != nil {
		t.Fatalf("get deleted doc after reopen: %v", err)
	} else if got != nil {
		t.Fatalf("deleted doc after reopen=%s want nil", got)
	}
	ids, err = reopenedCol.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find hnl after reopen: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, ids, []byte("u2"))
}

func TestCollectionIndexedWriteMemtablesAutoFlushMaxBytes(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxBytes:     1,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot after byte threshold flush")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog after byte threshold flush: %v", err)
	}
	if got := catalog.rootID(collectionPrimaryRootName("users")); got == 0 {
		t.Fatal("primary root was not persisted after byte threshold flush")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find email after byte threshold flush: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("email ids=%q want [u1]", ids)
	}
}

func TestCollectionIndexedWriteMemtablesCompactRootRunsBeforeDocumentFlush(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 5,
			BufferedIndexedWriteMaxRootRuns:  6,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	for i, id := range []string{"u1", "u2"} {
		doc := fmt.Sprintf(`{"email":"user%d@example.com"}`, i+1)
		if _, err := col.InsertBatch([][]byte{[]byte(id)}, [][]byte{[]byte(doc)}); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot before document threshold")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog before document threshold: %v", err)
	}
	if got := catalog.rootID(collectionPrimaryRootName("users")); got != 0 {
		t.Fatalf("primary root persisted before document threshold: %d", got)
	}

	col.writeDomain.mu.RLock()
	rootRunCount := col.writeDomain.rootRunCount
	rootNames := len(col.writeDomain.rootRuns)
	uniqueRuns := len(col.writeDomain.uniqueValueRuns["email"])
	col.writeDomain.mu.RUnlock()
	if rootRunCount != rootNames {
		t.Fatalf("rootRunCount=%d rootNames=%d, want compacted one run per root", rootRunCount, rootNames)
	}
	if uniqueRuns != 1 {
		t.Fatalf("unique value runs=%d want 1 compacted run", uniqueRuns)
	}
	got, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get pending compacted doc: %v", err)
	}
	if got == nil {
		t.Fatal("pending compacted doc not visible")
	}
	ids, err := col.FindByIndex("email", "user2@example.com")
	if err != nil {
		t.Fatalf("find pending compacted email: %v", err)
	}
	if !reflect.DeepEqual(ids, [][]byte{[]byte("u2")}) {
		t.Fatalf("pending compacted email ids=%q want [u2]", ids)
	}

	for i, id := range []string{"u3", "u4", "u5"} {
		doc := fmt.Sprintf(`{"email":"user%d@example.com"}`, i+3)
		if _, err := col.InsertBatch([][]byte{[]byte(id)}, [][]byte{[]byte(doc)}); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}
	snap = d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot after document threshold")
	}
	catalog, err = loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog after document threshold: %v", err)
	}
	if got := catalog.rootID(collectionPrimaryRootName("users")); got == 0 {
		t.Fatal("primary root was not persisted after document threshold")
	}
}

func TestCollectionIndexedInsertBatchUsesMutableBufferedRootAccumulators(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 100,
			BufferedIndexedWriteMaxRootRuns:  100,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	for i, id := range []string{"u1", "u2", "u3"} {
		doc := fmt.Sprintf(`{"email":"user%d@example.com","city":"hnl"}`, i+1)
		if _, err := col.InsertBatch([][]byte{[]byte(id)}, [][]byte{[]byte(doc)}); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}

	primaryRoot := collectionPrimaryRootName("users")
	indexStateRoot := collectionIndexStateRootName("users")
	emailRoot := collectionSecondaryRootName("users", "email")
	cityRoot := collectionSecondaryRootName("users", "city")

	col.writeDomain.mu.RLock()
	rootRunCount := col.writeDomain.rootRunCount
	rootRuns := map[string]int{
		primaryRoot:    len(col.writeDomain.rootRuns[primaryRoot]),
		indexStateRoot: len(col.writeDomain.rootRuns[indexStateRoot]),
		emailRoot:      len(col.writeDomain.rootRuns[emailRoot]),
		cityRoot:       len(col.writeDomain.rootRuns[cityRoot]),
	}
	mutableRuns := len(col.writeDomain.rootMutableRuns)
	uniqueRuns := len(col.writeDomain.uniqueValueRuns["email"])
	uniqueMutableRuns := len(col.writeDomain.uniqueValueMutableRuns)
	col.writeDomain.mu.RUnlock()

	if rootRunCount != 4 {
		t.Fatalf("rootRunCount=%d want 4 mutable root accumulators", rootRunCount)
	}
	for rootName, count := range rootRuns {
		if count != 1 {
			t.Fatalf("root %q runs=%d want 1 mutable accumulator", rootName, count)
		}
	}
	if mutableRuns != 4 {
		t.Fatalf("mutable root accumulators=%d want 4", mutableRuns)
	}
	if uniqueRuns != 1 || uniqueMutableRuns != 1 {
		t.Fatalf("unique value runs=%d mutable=%d want 1/1", uniqueRuns, uniqueMutableRuns)
	}
	stats := mgr.StatsSnapshot()
	if got := stats.IndexedStageRootRuns; got != 4 {
		t.Fatalf("indexed staged root runs=%d want 4 newly-created accumulators", got)
	}
	if got := stats.IndexedStageBatches; got != 3 {
		t.Fatalf("indexed staged batches=%d want 3", got)
	}

	got, err := col.Get([]byte("u3"))
	if err != nil {
		t.Fatalf("get pending buffered doc: %v", err)
	}
	if got == nil {
		t.Fatal("pending buffered doc not visible")
	}
	ids, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find pending city: %v", err)
	}
	if !reflect.DeepEqual(ids, [][]byte{[]byte("u1"), []byte("u2"), []byte("u3")}) {
		t.Fatalf("pending city ids=%q want [u1 u2 u3]", ids)
	}
	_, err = col.InsertBatch([][]byte{[]byte("u4")}, [][]byte{[]byte(`{"email":"user2@example.com","city":"hnl"}`)})
	if !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate pending unique insert err=%v want ErrUniqueIndexConflict", err)
	}
}

func TestCollectionIndexedInsertBatchKeepsLargeBatchesAsFrozenRuns(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: DefaultIndexedWriteMemtableAccumulatorBatchDocuments * 4,
			BufferedIndexedWriteMaxRootRuns:  DefaultIndexedWriteMemtableAccumulatorBatchDocuments * 4,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	count := DefaultIndexedWriteMemtableAccumulatorBatchDocuments + 1
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("u%04d", i))
		docs[i] = []byte(fmt.Sprintf(`{"email":"user%04d@example.com"}`, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("insert large batch: %v", err)
	}

	primaryRoot := collectionPrimaryRootName("users")
	indexStateRoot := collectionIndexStateRootName("users")
	emailRoot := collectionSecondaryRootName("users", "email")

	col.writeDomain.mu.RLock()
	rootRunCount := col.writeDomain.rootRunCount
	rootRuns := map[string]int{
		primaryRoot:    len(col.writeDomain.rootRuns[primaryRoot]),
		indexStateRoot: len(col.writeDomain.rootRuns[indexStateRoot]),
		emailRoot:      len(col.writeDomain.rootRuns[emailRoot]),
	}
	mutableRuns := len(col.writeDomain.rootMutableRuns)
	uniqueRuns := len(col.writeDomain.uniqueValueRuns["email"])
	uniqueMutableRuns := len(col.writeDomain.uniqueValueMutableRuns)
	col.writeDomain.mu.RUnlock()

	if rootRunCount != 3 {
		t.Fatalf("rootRunCount=%d want 3 frozen runs", rootRunCount)
	}
	for rootName, count := range rootRuns {
		if count != 1 {
			t.Fatalf("root %q runs=%d want 1 frozen run", rootName, count)
		}
	}
	if mutableRuns != 0 {
		t.Fatalf("mutable root accumulators=%d want 0 for large batch", mutableRuns)
	}
	if uniqueRuns != 1 || uniqueMutableRuns != 0 {
		t.Fatalf("unique value runs=%d mutable=%d want 1/0", uniqueRuns, uniqueMutableRuns)
	}
}

func TestCollectionIndexedWriteMemtablesAutoFlushMaxRootRuns(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxRootRuns:  2,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot after root-run threshold flush")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog after root-run threshold flush: %v", err)
	}
	if got := catalog.rootID(collectionPrimaryRootName("users")); got == 0 {
		t.Fatal("primary root was not persisted after root-run threshold flush")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find email after root-run threshold flush: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("email ids=%q want [u1]", ids)
	}
}

func TestBufferedPrimaryIDArenaCapAvoidsOverflow(t *testing.T) {
	if got := bufferedPrimaryIDArenaCap(2); got != 32 {
		t.Fatalf("small arena cap=%d want 32", got)
	}
	if got := bufferedPrimaryIDArenaCap(maxCollectionInt/16 + 1); got != 0 {
		t.Fatalf("overflow arena cap=%d want 0", got)
	}
}

func TestCloneCollectionRunTablesSurvivesSourceReset(t *testing.T) {
	table := newCollectionRunTable(1)
	setCollectionRunValue(table, []byte("k"), []byte("value"))
	table.Freeze()

	cloned, err := cloneCollectionRunTables([]memtable.Table{table})
	if err != nil {
		t.Fatalf("clone tables: %v", err)
	}
	defer resetCollectionTables(cloned)
	resetCollectionRunTable(table)

	value, _, flags, found := cloned[0].GetEntry([]byte("k"))
	if !found || flags&node.FlagTombstone != 0 || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("cloned entry found=%v flags=%d value=%q want live value", found, flags, value)
	}
}

func TestBufferedPrimaryRunIndexFindsNewestTable(t *testing.T) {
	older := newCollectionRunTable(1)
	setCollectionRunValue(older, []byte("u1"), []byte("older"))
	older.Freeze()
	newer := newCollectionRunTable(1)
	setCollectionRunValue(newer, []byte("u1"), []byte("newer"))
	newer.Freeze()
	defer resetCollectionRunTable(older)
	defer resetCollectionRunTable(newer)

	index := newBufferedPrimaryRunIndexWithDirectEntries(0, true)
	if err := addBufferedPrimaryRunIndexEntries(index, older); err != nil {
		t.Fatalf("add older table: %v", err)
	}
	if err := addBufferedPrimaryRunIndexEntries(index, newer); err != nil {
		t.Fatalf("add newer table: %v", err)
	}
	if got := len(index.arenas); got != 0 {
		t.Fatalf("stable primary run index retained arenas=%d want 0", got)
	}
	table, ok := index.lookup([]byte("u1"))
	if !ok {
		t.Fatal("lookup u1 missing")
	}
	value, _, flags, found := table.GetEntry([]byte("u1"))
	if !found || flags&node.FlagTombstone != 0 || !bytes.Equal(value, []byte("newer")) {
		t.Fatalf("lookup found=%v flags=%d value=%q want newer live value", found, flags, value)
	}
	ref, ok := index.lookupRef([]byte("u1"))
	if !ok {
		t.Fatal("lookupRef u1 missing")
	}
	if !ref.entryValid || ref.table != newer || ref.flags&node.FlagTombstone != 0 || !bytes.Equal(ref.value, []byte("newer")) {
		t.Fatalf("lookupRef entryValid=%v table=%p flags=%d value=%q want newer direct entry", ref.entryValid, ref.table, ref.flags, ref.value)
	}
}

func TestBufferedIndexedInsertMaintainsPrimaryRunIndexBeforeRead(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 1024,
			BufferedIndexedWriteMaxRootRuns:  1024,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2"), []byte("u3")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
			[]byte(`{"email":"linus@example.com"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if !collectionHasBufferedPrimaryRunIndexForTest(t, col) {
		t.Fatal("buffered indexed insert did not maintain primary run index before first read")
	}

	var rebuilds atomic.Int32
	restore := setCollectionPrimaryRunIndexRebuildHookForTest(func(string, int, int) {
		rebuilds.Add(1)
	})
	defer restore()
	got, found, err := col.GetInto([]byte("u2"), nil)
	if err != nil {
		t.Fatalf("GetInto: %v", err)
	}
	if !found || !bytes.Equal(got, []byte(`{"email":"grace@example.com"}`)) {
		t.Fatalf("GetInto found=%v value=%q want buffered u2", found, got)
	}
	if got := rebuilds.Load(); got != 0 {
		t.Fatalf("primary run index was rebuilt on read path %d times; want eager maintenance", got)
	}
}

func TestBufferedIndexedUpdateMaintainsPrimaryRunIndexBeforeRead(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 1024,
			BufferedIndexedWriteMaxRootRuns:  1024,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush seed: %v", err)
	}

	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			return []byte(`{"email":"ada@example.com","city":"lhr"}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("UpdateBatch results=%+v want one matched modified", results)
	}
	if !collectionHasBufferedPrimaryRunIndexForTest(t, col) {
		t.Fatal("buffered indexed update did not maintain primary run index before first read")
	}

	var rebuilds atomic.Int32
	restore := setCollectionPrimaryRunIndexRebuildHookForTest(func(string, int, int) {
		rebuilds.Add(1)
	})
	defer restore()
	got, found, err := col.GetInto([]byte("u1"), nil)
	if err != nil {
		t.Fatalf("GetInto: %v", err)
	}
	if !found || !bytes.Equal(got, []byte(`{"email":"ada@example.com","city":"lhr"}`)) {
		t.Fatalf("GetInto found=%v value=%q want updated buffered document", found, got)
	}
	if got := rebuilds.Load(); got != 0 {
		t.Fatalf("primary run index was rebuilt on read path %d times; want eager maintenance", got)
	}
}

func TestBufferedIndexedUpdateMaterializedOverlayEntersPrimaryRunIndex(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 1024,
			BufferedIndexedWriteMaxRootRuns:  1024,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"city":"hnl","score":0}`),
			[]byte(`{"city":"sfo","score":0}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush seed: %v", err)
	}

	if _, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update:     setJSONField("score", 1),
	}}); err != nil {
		t.Fatalf("primary-only UpdateBatch: %v", err)
	}
	if _, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u2"),
		Update: func(current []byte) ([]byte, bool, error) {
			var doc map[string]any
			if err := json.Unmarshal(current, &doc); err != nil {
				return nil, false, err
			}
			doc["city"] = "sea"
			next, err := json.Marshal(doc)
			return next, true, err
		},
	}}); err != nil {
		t.Fatalf("secondary-changing UpdateBatch: %v", err)
	}

	got, found, err := col.GetInto([]byte("u1"), nil)
	if err != nil {
		t.Fatalf("GetInto u1: %v", err)
	}
	if !found || !bytes.Equal(got, []byte(`{"city":"hnl","score":1}`)) {
		t.Fatalf("GetInto u1 found=%v value=%q want materialized overlay update", found, got)
	}
}

func TestBufferedIndexedDeleteTombstoneSurvivesLaterPrimaryRunIndexEnable(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 1024,
			BufferedIndexedWriteMaxRootRuns:  1024,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"city":"hnl","score":0}`),
			[]byte(`{"city":"sfo","score":0}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("u1")}); err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	} else if deleted != 1 {
		t.Fatalf("DeleteBatch deleted=%d want 1", deleted)
	}
	if collectionHasBufferedPrimaryRunIndexForTest(t, col) {
		t.Fatal("delete buffer unexpectedly left primary run index enabled")
	}

	if _, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u2"),
		Update: func(current []byte) ([]byte, bool, error) {
			var doc map[string]any
			if err := json.Unmarshal(current, &doc); err != nil {
				return nil, false, err
			}
			doc["city"] = "sea"
			next, err := json.Marshal(doc)
			return next, true, err
		},
	}}); err != nil {
		t.Fatalf("secondary-changing UpdateBatch: %v", err)
	}

	got, found, err := col.GetInto([]byte("u1"), nil)
	if err != nil {
		t.Fatalf("GetInto deleted u1: %v", err)
	}
	if found || len(got) != 0 {
		t.Fatalf("GetInto deleted u1 found=%v value=%q want missing tombstone", found, got)
	}
}

func TestShouldFlushBufferedIndexedWritesAfterAddingBoundaries(t *testing.T) {
	opts := CollectionOptions{
		BufferedIndexedWriteMaxDocuments: 10,
		BufferedIndexedWriteMaxBytes:     100,
		BufferedIndexedWriteMaxRootRuns:  5,
	}
	cases := []struct {
		name          string
		domain        *collectionWriteDomain
		addedCount    int
		addedBytes    int64
		addedRootRuns int
		want          bool
	}{
		{
			name:       "nil domain",
			addedCount: 1,
			addedBytes: 1,
		},
		{
			name:       "zero added count ignores bytes",
			domain:     &collectionWriteDomain{bufferedBytes: 99},
			addedBytes: 1,
		},
		{
			name:       "just below document limit",
			domain:     &collectionWriteDomain{count: 8},
			addedCount: 1,
		},
		{
			name:       "exactly at document limit",
			domain:     &collectionWriteDomain{count: 9},
			addedCount: 1,
			want:       true,
		},
		{
			name:       "above document limit",
			domain:     &collectionWriteDomain{count: 9},
			addedCount: 2,
			want:       true,
		},
		{
			name:       "count overflow clamps to flush",
			domain:     &collectionWriteDomain{count: maxCollectionInt},
			addedCount: 1,
			want:       true,
		},
		{
			name:       "byte overflow clamps to flush",
			domain:     &collectionWriteDomain{count: 1, bufferedBytes: int64(^uint64(0) >> 1)},
			addedCount: 1,
			addedBytes: 1,
			want:       true,
		},
		{
			name:          "just below root run limit",
			domain:        &collectionWriteDomain{count: 1, rootRuns: map[string][]memtable.Table{"a": make([]memtable.Table, 3)}},
			addedCount:    1,
			addedRootRuns: 1,
		},
		{
			name:          "exactly at root run limit",
			domain:        &collectionWriteDomain{count: 1, rootRuns: map[string][]memtable.Table{"a": make([]memtable.Table, 4)}},
			addedCount:    1,
			addedRootRuns: 1,
			want:          true,
		},
		{
			name:          "root run overflow clamps to flush",
			domain:        &collectionWriteDomain{count: 1},
			addedCount:    1,
			addedRootRuns: maxCollectionInt,
			want:          true,
		},
		{
			name:          "async uses mutable document count after rotated units",
			domain:        &collectionWriteDomain{count: 100, mutableCount: 8, indexedFlushUnits: []indexedFlushUnit{{rootRuns: map[string][]memtable.Table{"a": nil}}}},
			addedCount:    1,
			addedRootRuns: 1,
			want:          false,
		},
		{
			name:          "async mutable document count reaches threshold",
			domain:        &collectionWriteDomain{count: 100, mutableCount: 9, indexedFlushUnits: []indexedFlushUnit{{rootRuns: map[string][]memtable.Table{"a": nil}}}},
			addedCount:    1,
			addedRootRuns: 1,
			want:          true,
		},
		{
			name:          "async uses mutable root run count after rotated units",
			domain:        &collectionWriteDomain{count: 100, mutableCount: 1, rootRunCount: 3, indexedFlushUnits: []indexedFlushUnit{{rootRuns: map[string][]memtable.Table{"a": make([]memtable.Table, 10)}}}},
			addedCount:    1,
			addedRootRuns: 1,
			want:          false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			caseOpts := opts
			if strings.HasPrefix(tc.name, "async ") {
				caseOpts.BufferedIndexedAsyncFlush = true
			}
			got := shouldFlushBufferedIndexedWritesAfterAdding(tc.domain, caseOpts, tc.addedCount, tc.addedBytes, tc.addedRootRuns)
			if got != tc.want {
				t.Fatalf("shouldFlushBufferedIndexedWritesAfterAdding()=%v want %v", got, tc.want)
			}
		})
	}
}

func TestDetachMutableIndexedRunTablesKeepsRunsVisible(t *testing.T) {
	domain := &collectionWriteDomain{}
	primaryName := collectionPrimaryRootName("users")
	secondaryName := collectionSecondaryRootName("users", "city")

	domain.mu.Lock()
	primary, created := mutableRootRunLocked(domain, primaryName)
	if !created || primary == nil {
		domain.mu.Unlock()
		t.Fatal("primary mutable root run was not created")
	}
	city, created := mutableRootRunLocked(domain, secondaryName)
	if !created || city == nil {
		domain.mu.Unlock()
		t.Fatal("city mutable root run was not created")
	}
	domain.mu.Unlock()
	if err := applyCollectionRunEntriesWithFlags(primary, 2, func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error) {
		if i == 0 {
			return []byte("u2"), []byte("old-u2"), page.ValuePtr{}, node.FlagInline, nil
		}
		return []byte("u1"), []byte("value-u1"), page.ValuePtr{}, node.FlagInline, nil
	}); err != nil {
		t.Fatalf("append primary entries: %v", err)
	}
	if err := applyCollectionRunEntriesWithFlags(city, 1, func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error) {
		return []byte("city\x00u1"), nil, page.ValuePtr{}, node.FlagInline, nil
	}); err != nil {
		t.Fatalf("append city entries: %v", err)
	}

	domain.mu.Lock()
	detached := detachMutableIndexedRunTablesLocked(domain)
	if got := len(detached); got != 2 {
		domain.mu.Unlock()
		t.Fatalf("detached tables=%d want 2", got)
	}
	if domain.rootMutableRuns != nil {
		domain.mu.Unlock()
		t.Fatal("rootMutableRuns still has append targets after detach")
	}
	if got := pendingIndexedRootRunsLocked(domain, primaryName); len(got) != 1 || got[0] != primary {
		domain.mu.Unlock()
		t.Fatalf("pending primary runs=%v want original primary table", got)
	}
	if got := pendingIndexedRootRunsLocked(domain, secondaryName); len(got) != 1 || got[0] != city {
		domain.mu.Unlock()
		t.Fatalf("pending city runs=%v want original city table", got)
	}
	domain.mu.Unlock()

	requireFreezeSortRunIterator(t, primary.NewIterator(nil, nil), []string{"u1", "u2"})
	freezeIndexedRunTables(detached)
	requireFreezeSortRunIterator(t, primary.NewIterator(nil, nil), []string{"u1", "u2"})

	resetCollectionTables(detached)
}

func TestFreezeIndexedRunTablesOutsideLockAllowsNilDomain(t *testing.T) {
	table := newFreezeSortRunTable()
	if err := applyCollectionRunEntriesWithFlags(table, 2, func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error) {
		if i == 0 {
			return []byte("b"), []byte("value-b"), page.ValuePtr{}, node.FlagInline, nil
		}
		return []byte("a"), []byte("value-a"), page.ValuePtr{}, node.FlagInline, nil
	}); err != nil {
		t.Fatalf("append entries: %v", err)
	}
	freezeDuration := freezeIndexedRunTablesObserved([]memtable.Table{table})
	if freezeDuration <= 0 {
		t.Fatalf("freeze duration=%s want positive", freezeDuration)
	}
	requireFreezeSortRunIterator(t, table.NewIterator(nil, nil), []string{"a", "b"})
	resetCollectionRunTable(table)
}

func TestIndexedPrepareFreezeWaitsUntilFinished(t *testing.T) {
	domain := &collectionWriteDomain{}
	domain.mu.Lock()
	domain.beginIndexedPrepareFreezeLocked()
	domain.mu.Unlock()

	done := make(chan time.Duration, 1)
	waiterReady := make(chan struct{})
	go func() {
		domain.mu.Lock()
		if domain.indexedPrepareFreezes <= 0 {
			close(waiterReady)
			domain.mu.Unlock()
			done <- 0
			return
		}
		close(waiterReady)
		waited := domain.waitIndexedPrepareFreezeLocked()
		domain.mu.Unlock()
		done <- waited
	}()

	<-waiterReady
	select {
	case waited := <-done:
		t.Fatalf("prepare freeze wait returned before finish: %s", waited)
	default:
	}

	domain.mu.Lock()
	domain.finishIndexedPrepareFreezeLocked()
	domain.mu.Unlock()

	<-done
}

func TestIndexedPrepareFreezeFinishRequiresBegin(t *testing.T) {
	domain := &collectionWriteDomain{}
	domain.mu.Lock()
	defer domain.mu.Unlock()
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("finish without begin did not panic")
		}
	}()
	domain.finishIndexedPrepareFreezeLocked()
}

func TestRollbackBufferedIndexedDomainRestoresMetadata(t *testing.T) {
	catalog := &collectionCatalog{
		meta:  CollectionMeta{Name: "users"},
		roots: map[string]uint64{collectionPrimaryRootName("users"): 42},
	}
	domain := &collectionWriteDomain{
		loaded:          true,
		meta:            catalog.meta,
		catalog:         catalog,
		baseCommitSeq:   7,
		baseSystemRoot:  11,
		primaryRoot:     42,
		count:           3,
		bufferedBytes:   99,
		mutableCount:    2,
		mutableBytes:    88,
		writeGeneration: 12,
		rootRuns: map[string][]memtable.Table{
			collectionPrimaryRootName("users"): nil,
		},
		rootPolicies: map[string]backenddb.OrderedRootStoragePolicy{
			collectionPrimaryRootName("users"): backenddb.OrderedRootStorageDefault,
		},
		rootBaseIDs: map[string]uint64{
			collectionPrimaryRootName("users"): 42,
		},
		uniqueValueRuns: map[string][]memtable.Table{
			collectionSecondaryRootName("users", "email"): nil,
		},
	}
	checkpoint := checkpointBufferedIndexedDomain(domain)

	domain.loaded = false
	domain.meta = CollectionMeta{Name: "other"}
	domain.catalog = &collectionCatalog{meta: CollectionMeta{Name: "other"}}
	domain.baseCommitSeq = 100
	domain.baseSystemRoot = 200
	domain.primaryRoot = 300
	domain.count = 400
	domain.bufferedBytes = 500
	domain.mutableCount = 501
	domain.mutableBytes = 502
	domain.writeGeneration = 600
	domain.rootRuns = map[string][]memtable.Table{collectionPrimaryRootName("other"): nil}
	domain.rootPolicies = nil
	domain.rootBaseIDs = nil
	domain.primaryRunIndex = newBufferedPrimaryRunIndex(1)
	domain.uniqueValueRuns = nil

	rollbackBufferedIndexedDomain(domain, checkpoint)
	if !domain.loaded || domain.meta.Name != "users" || domain.catalog != catalog {
		t.Fatalf("metadata after rollback loaded=%v meta=%+v catalog=%p want users/%p", domain.loaded, domain.meta, domain.catalog, catalog)
	}
	if domain.baseCommitSeq != 7 || domain.baseSystemRoot != 11 || domain.primaryRoot != 42 {
		t.Fatalf("roots after rollback commit=%d system=%d primary=%d", domain.baseCommitSeq, domain.baseSystemRoot, domain.primaryRoot)
	}
	if domain.count != 3 || domain.bufferedBytes != 99 || domain.mutableCount != 2 || domain.mutableBytes != 88 || domain.writeGeneration != 12 {
		t.Fatalf("counters after rollback count=%d bytes=%d mutable=%d/%d generation=%d", domain.count, domain.bufferedBytes, domain.mutableCount, domain.mutableBytes, domain.writeGeneration)
	}
	if _, ok := domain.rootRuns[collectionPrimaryRootName("users")]; !ok {
		t.Fatalf("rootRuns=%v missing users primary root", domain.rootRuns)
	}
	if domain.primaryRunIndex != nil {
		t.Fatal("rollback rebuilt lazy primary run index that was absent at checkpoint")
	}
	if _, ok := domain.uniqueValueRuns[collectionSecondaryRootName("users", "email")]; !ok {
		t.Fatalf("uniqueValueRuns=%v missing users email root", domain.uniqueValueRuns)
	}
}

func TestRollbackBufferedIndexedDomainRestoresPreRotationRuns(t *testing.T) {
	primaryName := collectionPrimaryRootName("users")
	oldTable := newCollectionRunTable(1)
	setCollectionRunValue(oldTable, []byte("u1"), []byte(`{"name":"ada"}`))
	oldTable.Freeze()
	newTable := newCollectionRunTable(1)
	setCollectionRunValue(newTable, []byte("u2"), []byte(`{"name":"grace"}`))
	newTable.Freeze()

	domain := &collectionWriteDomain{
		loaded:         true,
		meta:           CollectionMeta{Name: "users"},
		catalog:        &collectionCatalog{meta: CollectionMeta{Name: "users"}, roots: map[string]uint64{primaryName: 42}},
		baseCommitSeq:  7,
		baseSystemRoot: 11,
		primaryRoot:    42,
		count:          1,
		rootRuns: map[string][]memtable.Table{
			primaryName: {oldTable},
		},
		rootPolicies: map[string]backenddb.OrderedRootStoragePolicy{
			primaryName: backenddb.OrderedRootStorageDefault,
		},
		rootBaseIDs: map[string]uint64{
			primaryName: 42,
		},
		rootRunCount: 1,
	}
	checkpoint := checkpointBufferedIndexedDomain(domain)

	domain.rootRuns[primaryName] = append(domain.rootRuns[primaryName], newTable)
	domain.rootRunCount = 2
	domain.count = 2
	if !rotateIndexedMutableToFlushUnitLocked(domain) {
		t.Fatal("rotate indexed mutable state returned false")
	}
	rollbackBufferedIndexedDomain(domain, checkpoint)
	if len(domain.indexedFlushUnits) != 0 {
		t.Fatalf("flush units after rollback=%d want 0", len(domain.indexedFlushUnits))
	}
	runs := domain.rootRuns[primaryName]
	if len(runs) != 1 || runs[0] != oldTable {
		t.Fatalf("root runs after rollback=%v want only original table", runs)
	}
	value, _, flags, found := runs[0].GetEntry([]byte("u1"))
	if !found || flags&node.FlagTombstone != 0 || !bytes.Equal(value, []byte(`{"name":"ada"}`)) {
		t.Fatalf("original table after rollback found=%v flags=%d value=%q", found, flags, value)
	}
	resetCollectionRunTable(oldTable)
}

func TestRollbackBufferedIndexedDomainPreservesCompactedCheckpointRuns(t *testing.T) {
	primaryName := collectionPrimaryRootName("users")
	emailUniqueName := collectionSecondaryRootName("users", "email")
	primaryOld := newCollectionRunTable(1)
	setCollectionRunValue(primaryOld, []byte("u1"), []byte(`{"email":"a@example.com","city":"nyc"}`))
	primaryOld.Freeze()
	primaryNew := newCollectionRunTable(1)
	setCollectionRunValue(primaryNew, []byte("u1"), []byte(`{"email":"a@example.com","city":"sf"}`))
	primaryNew.Freeze()
	uniqueOld := newCollectionRunTable(1)
	setCollectionRunValue(uniqueOld, []byte("a@example.com"), nil)
	uniqueOld.Freeze()
	uniqueNew := newCollectionRunTable(1)
	setCollectionRunValue(uniqueNew, []byte("b@example.com"), nil)
	uniqueNew.Freeze()

	domain := &collectionWriteDomain{
		loaded:         true,
		meta:           CollectionMeta{Name: "users"},
		catalog:        &collectionCatalog{meta: CollectionMeta{Name: "users"}, roots: map[string]uint64{primaryName: 42, emailUniqueName: 43}},
		baseCommitSeq:  7,
		baseSystemRoot: 11,
		primaryRoot:    42,
		count:          1,
		mutableCount:   1,
		rootRuns: map[string][]memtable.Table{
			primaryName: {primaryOld, primaryNew},
		},
		rootPolicies: map[string]backenddb.OrderedRootStoragePolicy{
			primaryName: backenddb.OrderedRootStorageDefault,
		},
		rootBaseIDs: map[string]uint64{
			primaryName: 42,
		},
		uniqueValueRuns: map[string][]memtable.Table{
			"email": {uniqueOld, uniqueNew},
		},
		rootRunCount: 2,
	}
	checkpoint := checkpointBufferedIndexedDomain(domain)

	obsolete, err := maybeCompactBufferedIndexedMutableRunsLocked(domain, CollectionOptions{
		BufferedIndexedWriteMaxDocuments: 3,
		BufferedIndexedWriteMaxRootRuns:  2,
	})
	if err != nil {
		t.Fatalf("compact mutable runs: %v", err)
	}
	if len(domain.rootRuns[primaryName]) != 1 {
		t.Fatalf("compacted primary run count=%d want 1", len(domain.rootRuns[primaryName]))
	}
	if len(domain.uniqueValueRuns["email"]) != 1 {
		t.Fatalf("compacted unique run count=%d want 1", len(domain.uniqueValueRuns["email"]))
	}
	if len(obsolete) != 4 {
		t.Fatalf("obsolete table count=%d want 4", len(obsolete))
	}

	rollbackBufferedIndexedDomain(domain, checkpoint)
	primaryRuns := domain.rootRuns[primaryName]
	if len(primaryRuns) != 2 || primaryRuns[0] != primaryOld || primaryRuns[1] != primaryNew {
		t.Fatalf("primary runs after rollback=%v want checkpoint tables", primaryRuns)
	}
	value, _, flags, found := getBufferedRunEntry(primaryRuns, []byte("u1"))
	if !found || flags&node.FlagTombstone != 0 || !bytes.Equal(value, []byte(`{"email":"a@example.com","city":"sf"}`)) {
		t.Fatalf("primary value after rollback found=%v flags=%d value=%q", found, flags, value)
	}
	uniqueRuns := domain.uniqueValueRuns["email"]
	if len(uniqueRuns) != 2 || uniqueRuns[0] != uniqueOld || uniqueRuns[1] != uniqueNew {
		t.Fatalf("unique runs after rollback=%v want checkpoint tables", uniqueRuns)
	}
	if _, _, flags, found := getBufferedRunEntry(uniqueRuns, []byte("a@example.com")); !found || flags&node.FlagTombstone != 0 {
		t.Fatalf("unique a@example.com after rollback found=%v flags=%d", found, flags)
	}
	if _, _, flags, found := getBufferedRunEntry(uniqueRuns, []byte("b@example.com")); !found || flags&node.FlagTombstone != 0 {
		t.Fatalf("unique b@example.com after rollback found=%v flags=%d", found, flags)
	}
	resetCollectionTables(obsolete)
}

func TestHasBufferedPrimaryRootRunsIgnoresSecondaryOnlyRuns(t *testing.T) {
	domain := &collectionWriteDomain{
		meta: CollectionMeta{Name: "users"},
		rootRuns: map[string][]memtable.Table{
			collectionSecondaryRootName("users", "email"): {newCollectionRunTable(0)},
		},
	}
	if hasBufferedPrimaryRootRuns(domain, "users") {
		t.Fatal("secondary-only buffered runs reported primary runs")
	}
	domain.rootRuns[collectionPrimaryRootName("users")] = []memtable.Table{newCollectionRunTable(0)}
	if !hasBufferedPrimaryRootRuns(domain, "users") {
		t.Fatal("primary buffered run not detected")
	}
	for _, tables := range domain.rootRuns {
		resetCollectionTables(tables)
	}
}

func TestCollectionIndexedWriteMemtablesCloseFlushes(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"email":"ada@example.com"}`)}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find reopened email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("reopened email ids=%q want [u1]", ids)
	}
}

func TestCollectionSingleInsertBufferedNoIndexCloseFlushes(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	got, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("reopened close-flushed doc=%q want %q", got, want)
	}
}

func TestCollectionSingleInsertBufferedNoIndexRejectsBufferedDuplicate(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"grace"}`)); err == nil || !strings.Contains(err.Error(), "document already exists") {
		t.Fatalf("duplicate err=%v want document already exists", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("u1=%q want %q", got, want)
	}
}

func TestCollectionIndexedDeleteBuffersNonUniqueTombstones(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	writer, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	reader, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	if _, err := writer.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"city":"hnl"}`),
			[]byte(`{"city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush inserts: %v", err)
	}

	deleted, err := writer.DeleteDocument([]byte("u1"))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted {
		t.Fatal("delete reported missing document")
	}
	if writer.writeDomain == nil {
		t.Fatal("missing write domain")
	}
	if writer.writeDomain.count != 1 || !writer.writeDomain.indexedDeletesOnly {
		t.Fatalf("write domain delete state count=%d deleteOnly=%v", writer.writeDomain.count, writer.writeDomain.indexedDeletesOnly)
	}
	got, err := reader.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("reader get pending deleted document: %v", err)
	}
	if got != nil {
		t.Fatalf("reader saw pending deleted document %q", got)
	}
	ids, err := reader.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find by city: %v", err)
	}
	if !reflect.DeepEqual(ids, [][]byte{[]byte("u2")}) {
		t.Fatalf("city ids=%q want [u2]", ids)
	}
	deleted, err = writer.DeleteDocument([]byte("u1"))
	if err != nil {
		t.Fatalf("delete already buffered u1: %v", err)
	}
	if deleted {
		t.Fatal("second delete of pending tombstone reported deleted")
	}
	deleted, err = writer.DeleteDocument([]byte("u2"))
	if err != nil {
		t.Fatalf("delete u2 after primary run index exists: %v", err)
	}
	if !deleted {
		t.Fatal("delete u2 reported missing document")
	}
	got, err = reader.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("reader get second pending deleted document: %v", err)
	}
	if got != nil {
		t.Fatalf("reader saw second pending deleted document %q", got)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush delete: %v", err)
	}
	got, err = reader.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("reader get flushed deleted document: %v", err)
	}
	if got != nil {
		t.Fatalf("reader saw flushed deleted document %q", got)
	}
}

func TestCollectionIndexedDeleteRevalidatesBeforeSkippingFlush(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	writerMgr := NewCollectionManager(d)
	if _, err := writerMgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	writer, err := writerMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	if _, err := writer.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"a@example.com","city":"hnl"}`),
			[]byte(`{"email":"b@example.com","city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush inserts: %v", err)
	}
	if deleted, err := writer.DeleteDocument([]byte("u1")); err != nil || !deleted {
		t.Fatalf("buffer delete deleted=%v err=%v", deleted, err)
	}

	indexMgr := NewCollectionManager(d)
	indexer, err := indexMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open indexer: %v", err)
	}
	if _, err := indexer.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}); err != nil {
		t.Fatalf("create unique index: %v", err)
	}

	if _, err := writer.DeleteDocument([]byte("u2")); err == nil || !strings.Contains(err.Error(), "concurrent schema modification") {
		t.Fatalf("delete after schema change err=%v want concurrent schema modification", err)
	}
	writer.writeDomain.mu.RLock()
	count := writer.writeDomain.count
	deleteOnly := writer.writeDomain.indexedDeletesOnly
	writer.writeDomain.mu.RUnlock()
	if count != 1 || !deleteOnly {
		t.Fatalf("write domain count=%d deleteOnly=%v want pending original delete", count, deleteOnly)
	}
}

func TestCollectionIndexedDeleteThenReinsertFlushesPendingTombstone(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert original: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	deleted, err := col.DeleteDocument([]byte("u1"))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted {
		t.Fatal("delete reported missing document")
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"city":"sea"}`)}); err != nil {
		t.Fatalf("reinsert after buffered delete: %v", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get reinserted document: %v", err)
	}
	if string(got) != `{"city":"sea"}` {
		t.Fatalf("reinserted document=%s want sea", got)
	}
}

func TestCollectionSingleInsertBufferedNoIndexRejectsConcurrentSchemaChange(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	writerMgr := NewCollectionManager(d)
	if _, err := writerMgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	writer, err := writerMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	if _, err := writer.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("buffer insert: %v", err)
	}

	indexMgr := NewCollectionManager(d)
	indexer, err := indexMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open indexer: %v", err)
	}
	if _, err := indexer.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	if _, err := writer.Insert([]byte("u2"), []byte(`{"email":"grace@example.com"}`)); err == nil || !strings.Contains(err.Error(), "concurrent schema modification") {
		t.Fatalf("insert after schema change err=%v want concurrent schema modification", err)
	}
	if err := writer.Flush(); err == nil || !strings.Contains(err.Error(), "concurrent schema modification") {
		t.Fatalf("flush after schema change err=%v want concurrent schema modification", err)
	}
}

func TestCollectionSingleInsertBufferedNoIndexRejectsConcurrentPrimaryRootChange(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	leftMgr := NewCollectionManager(d)
	if _, err := leftMgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	left, err := leftMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open left writer: %v", err)
	}
	if _, err := left.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("left buffer insert: %v", err)
	}

	rightMgr := NewCollectionManager(d)
	right, err := rightMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open right writer: %v", err)
	}
	if _, err := right.Insert([]byte("u2"), []byte(`{"name":"grace"}`)); err != nil {
		t.Fatalf("right insert: %v", err)
	}
	if err := right.Flush(); err != nil {
		t.Fatalf("right flush: %v", err)
	}

	if _, err := left.Insert([]byte("u3"), []byte(`{"name":"katherine"}`)); err == nil || !strings.Contains(err.Error(), "concurrent root modification") {
		t.Fatalf("insert after root change err=%v want concurrent root modification", err)
	}
	if err := left.Flush(); err == nil || !strings.Contains(err.Error(), "concurrent root modification") {
		t.Fatalf("flush after root change err=%v want concurrent root modification", err)
	}
}

func TestRootDescriptorDeltaRejectsConcurrentSchemaChange(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	baseCommitSeq := snapshotCommitSeq(snap)
	baseSystemRoot := snapshotSystemRoot(snap)
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	baseMeta := catalog.meta
	col.meta = baseMeta
	rootName := collectionPrimaryRootName("users")
	baseRoot := catalog.rootID(rootName)

	indexer, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("open indexer: %v", err)
	}
	if _, err := indexer.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	iter, err := col.buildRootDescriptorSystemDeltaIterator(
		baseCommitSeq,
		baseSystemRoot,
		[]string{rootName},
		map[string]uint64{rootName: baseRoot},
		[]uint64{baseRoot + 1},
	)
	if iter != nil {
		_ = iter.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "concurrent schema modification") {
		t.Fatalf("system delta err=%v want concurrent schema modification", err)
	}
}

func TestCreateIndexSystemIteratorRejectsConcurrentPrimaryRootChange(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush u1: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	baseMeta := catalog.meta
	newMeta, normalizedDef, err := addIndexToCollectionMeta(baseMeta, IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString})
	if err != nil {
		t.Fatalf("add index metadata: %v", err)
	}
	primaryRootName := collectionPrimaryRootName("users")
	stateRootName := collectionIndexStateRootName("users")
	secondaryRootName := collectionSecondaryRootName("users", normalizedDef.Name)
	baseRootIDs := map[string]uint64{
		primaryRootName:   catalog.rootID(primaryRootName),
		stateRootName:     catalog.rootID(stateRootName),
		secondaryRootName: catalog.rootID(secondaryRootName),
	}

	other, err := NewCollectionManager(d).OpenCollection("users")
	if err != nil {
		t.Fatalf("open other writer: %v", err)
	}
	if _, err := other.Insert([]byte("u2"), []byte(`{"email":"grace@example.com"}`)); err != nil {
		t.Fatalf("insert u2: %v", err)
	}
	if err := other.Flush(); err != nil {
		t.Fatalf("flush u2: %v", err)
	}

	rootNames := []string{stateRootName, secondaryRootName}
	iter, err := col.buildSchemaAndRootDescriptorSystemIterator(
		baseMeta,
		newMeta,
		rootNames,
		baseRootIDs,
		[]uint64{baseRootIDs[primaryRootName] + 1, baseRootIDs[primaryRootName] + 2},
	)
	if iter != nil {
		_ = iter.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "concurrent root modification") {
		t.Fatalf("schema publish err=%v want concurrent root modification", err)
	}
	if err != nil && !strings.Contains(err.Error(), primaryRootName) {
		t.Fatalf("schema publish err=%v want primary root name %q", err, primaryRootName)
	}
}

func TestCollectionInsertBatchBridge_ReopenUsesPersistedRootDescriptors(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	got, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com"}`); !bytes.Equal(got, want) {
		t.Fatalf("reopen u1=%q want %q", got, want)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find after reopen: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("reopen email ids=%q want u1", ids)
	}
}

func TestCollectionCachedCatalogRefreshesAfterCrossHandleWrite(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	reader, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open reader collection: %v", err)
	}
	writer, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open writer collection: %v", err)
	}

	if got, err := reader.Get([]byte("u1")); err != nil || got != nil {
		t.Fatalf("initial reader get got=%q err=%v want missing", got, err)
	}
	if _, err := writer.Insert([]byte("u1"), []byte(`{"name":"ada"}`)); err != nil {
		t.Fatalf("writer insert: %v", err)
	}
	got, err := reader.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("reader get after writer insert: %v", err)
	}
	if want := []byte(`{"name":"ada"}`); !bytes.Equal(got, want) {
		t.Fatalf("reader saw stale catalog value=%q want %q", got, want)
	}
}

func TestCollectionCachedCatalogUsesSystemRoot(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir())
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = cleanup() }()
	if !d.CommandWALEnabled() {
		t.Fatal("expected command WAL")
	}

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "name", Value: "ada"},
	})
	if err != nil {
		t.Fatalf("marshal doc: %v", err)
	}
	if _, err := col.InsertBatchValidatedBSON([][]byte{[]byte("u1")}, [][]byte{doc}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	rootName := collectionPrimaryRootName("users")

	col.catalogMu.Lock()
	col.catalog = catalog
	col.catalogSystemRoot = snapshotSystemRoot(snap)
	col.catalogCommitSeq = snapshotCommitSeq(snap) + 1
	col.catalogMu.Unlock()

	refreshed, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	if refreshed != catalog {
		t.Fatal("catalog cache missed despite unchanged system root")
	}
	if got, want := refreshed.rootID(rootName), catalog.rootID(rootName); got != want {
		t.Fatalf("refreshed root=%d want %d", got, want)
	}
}

func TestCollectionValidateInsertBatchPlanLockedClassifiesRaces(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"name":"ada"}`)}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	rootName := collectionPrimaryRootName("users")
	plan := &insertBatchPlan{
		resultIDs:   [][]byte{[]byte("u2")},
		primaryKeys: [][]byte{[]byte("u2")},
		runs:        []collectionRootRun{{name: rootName}},
	}
	rootNames, baseRootIDs := insertBatchPlanRootNamesAndBaseIDs(plan, catalog)
	baseRoot, ok := insertBatchBaseRootID(rootNames, baseRootIDs, rootName)
	if !ok {
		t.Fatalf("base root for %q not found", rootName)
	}
	staleRootIDs := append([]uint64(nil), baseRootIDs...)
	for i, name := range rootNames {
		if name == rootName {
			staleRootIDs[i] = baseRoot + 1
		}
	}
	validation := insertBatchValidationContext{
		meta:        catalog.meta,
		rootNames:   rootNames,
		baseRootIDs: staleRootIDs,
		plan:        plan,
	}
	if current, _, err := col.validateInsertBatchPlanLocked(validation); current != nil || !errors.Is(err, ErrConcurrentMutation) {
		if current != nil {
			_ = current.Close()
		}
		t.Fatalf("root mismatch current=%v err=%v want ErrConcurrentMutation", current, err)
	} else if !strings.Contains(err.Error(), `collection="users"`) || !strings.Contains(err.Error(), `root="users/primary"`) {
		t.Fatalf("root mismatch err=%v missing collection/root context", err)
	}
	validation.baseRootIDs = nil
	if current, _, err := col.validateInsertBatchPlanLocked(validation); current != nil || err == nil || errors.Is(err, ErrConcurrentMutation) || !strings.Contains(err.Error(), `collection="users"`) || !strings.Contains(err.Error(), `root="users/primary"`) {
		if current != nil {
			_ = current.Close()
		}
		t.Fatalf("missing base root current=%v err=%v want non-retryable missing base root error", current, err)
	}

	schemaMeta := catalog.meta
	schemaMeta.Options.AllowArrayValuesInIndex = !schemaMeta.Options.AllowArrayValuesInIndex
	validation.meta = schemaMeta
	validation.baseRootIDs = baseRootIDs
	if current, _, err := col.validateInsertBatchPlanLocked(validation); current != nil || err == nil || errors.Is(err, ErrConcurrentMutation) {
		if current != nil {
			_ = current.Close()
		}
		t.Fatalf("schema mismatch current=%v err=%v want non-retryable schema error", current, err)
	}
}

func TestCollectionLockAndValidateInsertBatchPlanAllowsDisjointRootDrift(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"city":"a"}`)}); err != nil {
		t.Fatalf("insert initial: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog: %v", err)
	}
	meta := catalog.meta
	plannerOptions, err := collectionPlannerOptions(meta)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("planner options: %v", err)
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	plan, err := (insertBatchPlanner{
		collection:     meta.Name,
		primaryRoot:    collectionPrimaryRootName(meta.Name),
		templateRoot:   collectionTemplateRootName(meta.Name),
		indexStateRoot: collectionIndexStateRootName(meta.Name),
		indexes:        plannerIndexes(meta.Indexes),
		options:        plannerOptions,
	}).planInsertBatch([][]byte{[]byte("u2")}, [][]byte{[]byte(`{"city":"b"}`)})
	_ = snap.Close()
	if err != nil {
		t.Fatalf("plan insert: %v", err)
	}
	defer resetCollectionRunTables(plan.runs)
	rootNames, baseRootIDs := insertBatchPlanRootNamesAndBaseIDs(plan, catalog)
	oldPrimaryRoot, ok := insertBatchBaseRootID(rootNames, baseRootIDs, collectionPrimaryRootName("users"))
	if !ok {
		t.Fatal("planned insert missing primary root base id")
	}

	if _, err := col.InsertBatch([][]byte{[]byte("u3")}, [][]byte{[]byte(`{"city":"c"}`)}); err != nil {
		t.Fatalf("insert disjoint concurrent row: %v", err)
	}

	mutationLocked := false
	var unlockMutation collectionMutationUnlock
	pin, currentCatalog, _, _, err := col.lockAndValidateInsertBatchPlan(&mutationLocked, &unlockMutation, nil, catalog, meta, rootNames, baseRootIDs, false, 0, 0, false, plan)
	if err != nil {
		t.Fatalf("validate disjoint root drift: %v", err)
	}
	defer unlockMutation.Unlock()
	defer func() { _ = pin.Close() }()
	currentPrimaryRoot := currentCatalog.rootID(collectionPrimaryRootName("users"))
	if currentPrimaryRoot == oldPrimaryRoot {
		t.Fatal("test did not create primary root drift")
	}
	if got, ok := insertBatchBaseRootID(rootNames, baseRootIDs, collectionPrimaryRootName("users")); !ok || got != currentPrimaryRoot {
		t.Fatalf("rebased primary root=%d want %d", got, currentPrimaryRoot)
	}
}

func TestCollectionLockAndValidateInsertBatchPlanRefreshesVacuumedSnapshot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"city":"a"}`)}); err != nil {
		t.Fatalf("insert initial: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	oldPager := snap.Pager()
	catalog, err := loadCollectionCatalog(snap, "users")
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog: %v", err)
	}
	meta := catalog.meta
	plannerOptions, err := collectionPlannerOptions(meta)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("planner options: %v", err)
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	plan, err := (insertBatchPlanner{
		collection:     meta.Name,
		primaryRoot:    collectionPrimaryRootName(meta.Name),
		templateRoot:   collectionTemplateRootName(meta.Name),
		indexStateRoot: collectionIndexStateRootName(meta.Name),
		indexes:        plannerIndexes(meta.Indexes),
		options:        plannerOptions,
	}).planInsertBatch([][]byte{[]byte("u2")}, [][]byte{[]byte(`{"city":"b"}`)})
	if err != nil {
		_ = snap.Close()
		t.Fatalf("plan insert: %v", err)
	}
	defer resetCollectionRunTables(plan.runs)
	rootNames, baseRootIDs := insertBatchPlanRootNamesAndBaseIDs(plan, catalog)

	if err := d.VacuumIndexOnline(t.Context()); err != nil {
		_ = snap.Close()
		t.Fatalf("vacuum: %v", err)
	}
	if err := col.validateMutationRootDescriptors(oldPager, snapshotUserRoot(snap), snapshotSystemRoot(snap), snapshotCommitSeq(snap)); !errors.Is(err, ErrConcurrentMutation) {
		_ = snap.Close()
		t.Fatalf("pre-vacuum mutation preflight err=%v want ErrConcurrentMutation", err)
	}
	mutationUnlock := col.lockMutation()
	mutationLocked := true
	var unlockMutation collectionMutationUnlock
	pin, currentCatalog, _, _, err := col.lockAndValidateInsertBatchPlan(&mutationLocked, &unlockMutation, snap, catalog, meta, rootNames, baseRootIDs, false, 0, 0, true, plan)
	mutationUnlock.Unlock()
	if err != nil {
		t.Fatalf("refresh validation: %v", err)
	}
	defer func() { _ = pin.Close() }()
	if pin.Pager() == oldPager {
		t.Fatal("validation retained the pre-vacuum snapshot")
	}
	for i, rootName := range rootNames {
		if got, want := baseRootIDs[i], currentCatalog.rootID(rootName); got != want {
			t.Fatalf("base root %q=%d want current %d", rootName, got, want)
		}
	}
}

func TestCollectionBufferedIndexedRootsRebaseAfterOnlineVacuum(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 1024,
			DisableBufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("seed")}, [][]byte{[]byte(`{"city":"seed"}`)}); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"city":"a"}`)}); err != nil {
		t.Fatalf("buffer first row: %v", err)
	}
	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare buffered publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare buffered publish returned no work")
	}
	before := d.AcquireSnapshot()
	if before == nil {
		t.Fatal("missing pre-vacuum snapshot")
	}
	beforePager := before.Pager()
	_ = before.Close()
	if err := d.VacuumIndexOnline(t.Context()); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	after := d.AcquireSnapshot()
	if after == nil {
		t.Fatal("missing post-vacuum snapshot")
	}
	if after.Pager() == beforePager {
		_ = after.Close()
		t.Fatal("vacuum did not replace the index pager")
	}
	if err := col.publishPreparedIndexedFlush(work); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("publish prepared work after vacuum err=%v want ErrConcurrentMutation", err)
	}
	freshCatalog, err := loadCollectionCatalog(after, "users")
	_ = after.Close()
	if err != nil {
		t.Fatalf("load post-vacuum catalog: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u2")}, [][]byte{[]byte(`{"city":"b"}`)}); err != nil {
		t.Fatalf("buffer second row after vacuum: %v", err)
	}
	col.writeDomain.mu.RLock()
	for rootName, baseRoot := range col.writeDomain.rootBaseIDs {
		if want := freshCatalog.rootID(rootName); baseRoot != want {
			col.writeDomain.mu.RUnlock()
			t.Fatalf("rebased root %q=%d want %d", rootName, baseRoot, want)
		}
	}
	col.writeDomain.mu.RUnlock()
	if err := col.Flush(); err != nil {
		t.Fatalf("flush rebased rows: %v", err)
	}
	for id, want := range map[string]string{
		"seed": `{"city":"seed"}`,
		"u1":   `{"city":"a"}`,
		"u2":   `{"city":"b"}`,
	} {
		got, err := col.Get([]byte(id))
		if err != nil || string(got) != want {
			t.Fatalf("Get(%q)=(%q,%v) want %q", id, got, err, want)
		}
	}
}

func TestCollectionSchemaBackfillsRetryAfterOnlineVacuumPagerReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	tests := []struct {
		name      string
		operation string
		rootName  string
		create    func(*Collection) error
	}{
		{
			name:      "scalar index",
			operation: "index",
			rootName:  collectionSecondaryRootName("docs", "city"),
			create: func(col *Collection) error {
				_, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city", ValueType: IndexValueString})
				return err
			},
		},
		{
			name:      "text index",
			operation: "text_index",
			rootName:  collectionTextV2DocMapRootName("docs", "lexical"),
			create: func(col *Collection) error {
				_, _, err := col.CreateTextIndex(TextIndexDefinition{
					Name:    "lexical",
					Version: TextIndexVersionV2,
					Fields:  []TextIndexField{{Field: "body"}},
				})
				return err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = d.Close() }()
			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
				t.Fatalf("create collection: %v", err)
			}
			col, err := mgr.OpenCollection("docs")
			if err != nil {
				t.Fatalf("open collection: %v", err)
			}
			if _, err := col.InsertBatch(
				[][]byte{[]byte("d1"), []byte("d2")},
				[][]byte{[]byte(`{"city":"hnl","body":"first document"}`), []byte(`{"city":"nyc","body":"second document"}`)},
			); err != nil {
				t.Fatalf("insert source documents: %v", err)
			}

			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releasePublish := func() {
				releaseOnce.Do(func() { close(release) })
			}
			defer releasePublish()
			var blocked atomic.Bool
			var calls atomic.Int32
			testBeforeSchemaBackfillPublishHook.installMu.Lock()
			testBeforeSchemaBackfillPublishHook.ptr.Store(&testSchemaBackfillPublishHook{fn: func(operation string) {
				if operation != tc.operation {
					return
				}
				calls.Add(1)
				if blocked.CompareAndSwap(false, true) {
					close(entered)
					<-release
				}
			}})
			defer func() {
				testBeforeSchemaBackfillPublishHook.ptr.Store(nil)
				testBeforeSchemaBackfillPublishHook.installMu.Unlock()
			}()

			done := make(chan error, 1)
			go func() {
				done <- tc.create(col)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("schema backfill did not reach pre-publish hook")
			}
			beforePager := d.Pager()
			if err := d.VacuumIndexOnline(t.Context()); err != nil {
				t.Fatalf("vacuum: %v", err)
			}
			if d.Pager() == beforePager {
				t.Fatal("vacuum did not replace pager")
			}
			releasePublish()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("schema backfill retry: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("schema backfill retry did not finish")
			}
			if got := calls.Load(); got < 2 {
				t.Fatalf("schema backfill publish attempts=%d want retry after pager replacement", got)
			}
			snap := d.AcquireSnapshot()
			if snap == nil {
				t.Fatal("missing post-retry snapshot")
			}
			catalog, err := loadCollectionCatalog(snap, "docs")
			_ = snap.Close()
			if err != nil {
				t.Fatalf("load post-retry catalog: %v", err)
			}
			if rootID := catalog.rootID(tc.rootName); rootID == 0 {
				t.Fatalf("post-retry root %q is empty", tc.rootName)
			}
		})
	}
}

func TestCollectionValidateInsertBatchPlanAfterPlanningLockedNilSnapshot(t *testing.T) {
	col := &Collection{}
	pin, catalog, err := col.validateInsertBatchPlanAfterPlanningLocked(true, insertBatchValidationContext{})
	if pin != nil || catalog != nil || !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("pin=%v catalog=%v err=%v want ErrClosed without panic", pin, catalog, err)
	}
}

func TestOpenCollectionWriteDomainCatalogCacheUsesCommitSeq(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"name":"ada"}`)}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	state := d.State()
	domain := mgr.existingWriteDomainForCollection("users")
	cached := cachedWriteDomainCatalogForState(domain, state.SystemRootPageID, state.CommitSeq)
	if cached == nil {
		t.Fatal("expected populated write-domain catalog cache")
	}
	cachedRoot := cached.rootID(collectionPrimaryRootName("users"))
	if cachedRoot == 0 {
		t.Fatal("write-domain catalog cache did not include primary root")
	}
	if stale := cachedWriteDomainCatalogForState(domain, state.SystemRootPageID, state.CommitSeq+1); stale != nil {
		t.Fatal("write-domain catalog cache ignored commit sequence")
	}
	if err := d.Set([]byte("raw/unrelated"), []byte("value")); err != nil {
		t.Fatalf("raw set: %v", err)
	}
	rawState := d.State()
	if rawState.SystemRootPageID != state.SystemRootPageID {
		t.Fatalf("raw write changed system root from %d to %d", state.SystemRootPageID, rawState.SystemRootPageID)
	}
	if rawState.CommitSeq == state.CommitSeq {
		t.Fatalf("raw write did not advance commit seq: before=%d after=%d", state.CommitSeq, rawState.CommitSeq)
	}

	reopened, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("reopen collection: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	reopenedCatalog, err := reopened.catalogForSnapshot(snap)
	if err != nil {
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	if got := reopenedCatalog.rootID(collectionPrimaryRootName("users")); got != cachedRoot {
		t.Fatalf("OpenCollection refreshed root=%d want %d", got, cachedRoot)
	}
	if fresh := cachedWriteDomainCatalogForState(domain, rawState.SystemRootPageID, rawState.CommitSeq); fresh == nil || fresh.rootID(collectionPrimaryRootName("users")) != reopenedCatalog.rootID(collectionPrimaryRootName("users")) {
		t.Fatal("OpenCollection did not refresh write-domain catalog cache for new commit sequence")
	}
	if got, err := reopened.Get([]byte("u1")); err != nil || !bytes.Equal(got, []byte(`{"name":"ada"}`)) {
		t.Fatalf("reopened get got=%q err=%v", got, err)
	}
}

func TestCollectionUpdateBatchUpdatesMultipleDocuments(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"name":"ada","count":0}`), []byte(`{"name":"grace","count":0}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	before := d.State()
	results, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: incrementJSONCount},
		{DocumentID: []byte("u2"), Update: incrementJSONCount},
		{DocumentID: []byte("missing"), Update: incrementJSONCount},
	})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("results=%d want 3", len(results))
	}
	if !results[0].Matched || !results[0].Modified || !results[1].Matched || !results[1].Modified {
		t.Fatalf("unexpected matched/modified results: %+v", results)
	}
	if results[2].Matched || results[2].Modified {
		t.Fatalf("missing document result matched/modified: %+v", results[2])
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("CommitSeq after batch=%d want %d", after.CommitSeq, before.CommitSeq+1)
	}
	for _, id := range []string{"u1", "u2"} {
		got, err := col.Get([]byte(id))
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if !bytes.Contains(got, []byte(`"count":1`)) {
			t.Fatalf("%s document=%s want count 1", id, got)
		}
	}
}

func TestCollectionUpdateBatchRejectsDuplicateIDs(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	_, err = (&Collection{db: d}).UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: incrementJSONCount},
		{DocumentID: []byte("u1"), Update: incrementJSONCount},
	})
	if !errors.Is(err, ErrDuplicateDocumentID) {
		t.Fatalf("UpdateBatch err=%v want ErrDuplicateDocumentID", err)
	}
}

func TestCollectionUpdateBatchValidationErrorsIncludeIndex(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	_, err = (&Collection{db: d}).UpdateBatch([]UpdateBatchItem{{Update: incrementJSONCount}})
	if err == nil || !strings.Contains(err.Error(), "index 0") {
		t.Fatalf("empty id err=%v want index 0", err)
	}
	_, err = (&Collection{db: d}).UpdateBatch([]UpdateBatchItem{{DocumentID: []byte("u1")}})
	if err == nil || !strings.Contains(err.Error(), "index 0") {
		t.Fatalf("nil update err=%v want index 0", err)
	}
	_, err = (&Collection{}).UpdateBatch([]UpdateBatchItem{{DocumentID: []byte("u1"), Update: incrementJSONCount}})
	if !errors.Is(err, errCollectionDBNil) {
		t.Fatalf("nil db err=%v want errCollectionDBNil", err)
	}
}

func TestCollectionUpdateBatchRejectsEmptyChangedReplacementWithIndex(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"count":0}`), []byte(`{"count":0}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, err = col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: incrementJSONCount},
		{DocumentID: []byte("u2"), Update: func([]byte) ([]byte, bool, error) {
			return nil, true, nil
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "update batch index 1") || !strings.Contains(err.Error(), "cannot be empty") {
		t.Fatalf("UpdateBatch err=%v want index 1 empty replacement", err)
	}
	var itemErr *UpdateBatchItemError
	if !errors.As(err, &itemErr) || itemErr.Index != 1 {
		t.Fatalf("UpdateBatch err=%v want typed item index 1", err)
	}
}

func TestCollectionUpdateBatchRejectsUniqueConflictsWithinBatch(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"email":"a@example.com"}`), []byte(`{"email":"b@example.com"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, err = col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONEmail("same@example.com")},
		{DocumentID: []byte("u2"), Update: setJSONEmail("same@example.com")},
	})
	if !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("UpdateBatch err=%v want ErrUniqueIndexConflict", err)
	}
	if !strings.Contains(err.Error(), "batch indexes 0 and 1") {
		t.Fatalf("UpdateBatch err=%v missing conflicting batch indexes", err)
	}
}

func TestCollectionUpdateCombinerRunBatchPublishesDistinctIDsOnce(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"score":0}`), []byte(`{"score":0}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	before := d.State()
	combiner := &collectionUpdateCombiner{maxBatch: 8, domain: col.writeDomain}
	requests := []collectionUpdateCombineRequest{
		{
			collection: col,
			documentID: []byte("u1"),
			update: func([]byte) ([]byte, bool, error) {
				return []byte(`{"score":1}`), true, nil
			},
			done: make(chan collectionUpdateCombineResult, 1),
		},
		{
			collection: col,
			documentID: []byte("u2"),
			update: func([]byte) ([]byte, bool, error) {
				return []byte(`{"score":2}`), true, nil
			},
			done: make(chan collectionUpdateCombineResult, 1),
		},
	}
	combiner.runBatch(requests)
	for i, req := range requests {
		result := <-req.done
		if result.err != nil {
			t.Fatalf("request %d err: %v", i, result.err)
		}
		if !result.matched || !result.modified {
			t.Fatalf("request %d matched=%v modified=%v", i, result.matched, result.modified)
		}
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("combined batch advanced commit seq by %d, want 1", after.CommitSeq-before.CommitSeq)
	}
}

func TestCollectionUpdateCombinerRunBatchStartingWithYieldsForQueuedPeer(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"score":0}`), []byte(`{"score":0}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	first := collectionUpdateCombineRequest{
		collection: col,
		documentID: []byte("u1"),
		update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"score":1}`), true, nil
		},
		done: make(chan collectionUpdateCombineResult, 1),
	}
	second := collectionUpdateCombineRequest{
		collection: col,
		documentID: []byte("u2"),
		update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"score":2}`), true, nil
		},
		done: make(chan collectionUpdateCombineResult, 1),
	}
	before := d.State()
	queuedSecond := false
	yieldCalls := 0
	var combiner *collectionUpdateCombiner
	combiner = &collectionUpdateCombiner{
		maxBatch: 3,
		domain:   col.writeDomain,
		requests: make(chan collectionUpdateCombineRequest, 1),
		drainYield: func() {
			yieldCalls++
			if queuedSecond {
				return
			}
			queuedSecond = true
			combiner.requests <- second
		},
	}
	combiner.runBatchStartingWith(first)
	for i, done := range []chan collectionUpdateCombineResult{first.done, second.done} {
		select {
		case result := <-done:
			if result.err != nil {
				t.Fatalf("request %d err: %v", i, result.err)
			}
			if !result.matched || !result.modified {
				t.Fatalf("request %d matched=%v modified=%v", i, result.matched, result.modified)
			}
		default:
			t.Fatalf("request %d was not included in yielded batch", i)
		}
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("combined batch advanced commit seq by %d, want 1", after.CommitSeq-before.CommitSeq)
	}
	if yieldCalls != 1 {
		t.Fatalf("drainYield calls=%d want exactly one bounded yield", yieldCalls)
	}
}

func TestCollectionUpdateCombinerBatchesWhenSecondaryUniqueValuesAreUnchanged(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"a@example.com","city":"hnl"}`),
			[]byte(`{"email":"b@example.com","city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	before := d.State()
	combiner := &collectionUpdateCombiner{maxBatch: 8, domain: col.writeDomain}
	requests := []collectionUpdateCombineRequest{
		{
			collection: col,
			documentID: []byte("u1"),
			update:     setJSONCity("sea"),
			done:       make(chan collectionUpdateCombineResult, 1),
		},
		{
			collection: col,
			documentID: []byte("u2"),
			update:     setJSONCity("sfo"),
			done:       make(chan collectionUpdateCombineResult, 1),
		},
	}
	combiner.runBatch(requests)
	for i, req := range requests {
		result := <-req.done
		if result.err != nil {
			t.Fatalf("request %d err: %v", i, result.err)
		}
		if !result.matched || !result.modified {
			t.Fatalf("request %d matched=%v modified=%v", i, result.matched, result.modified)
		}
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("combined unique-schema batch advanced commit seq by %d, want 0", after.CommitSeq-before.CommitSeq)
	}
	seaIDs, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city: %v", err)
	}
	if len(seaIDs) != 1 || !bytes.Equal(seaIDs[0], []byte("u1")) {
		t.Fatalf("sea ids=%q want [u1]", seaIDs)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush combined unique-schema batch: %v", err)
	}
	flushed := d.State()
	if flushed.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("flushed combined unique-schema batch advanced commit seq by %d, want 1", flushed.CommitSeq-before.CommitSeq)
	}
}

func TestCollectionUpdateCombinerBuffersSingletonWhenSecondaryUniqueValuesAreUnchanged(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 1 << 20,
			BufferedIndexedWriteMaxRootRuns:  1 << 20,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"a@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	before := d.State()
	statsBefore := mgr.StatsSnapshot()
	combiner := &collectionUpdateCombiner{maxBatch: 8, domain: col.writeDomain}
	req := collectionUpdateCombineRequest{
		collection: col,
		documentID: []byte("u1"),
		update:     setJSONCity("sea"),
		done:       make(chan collectionUpdateCombineResult, 1),
	}
	combiner.runBatch([]collectionUpdateCombineRequest{req})
	result := <-req.done
	if result.err != nil {
		t.Fatalf("request err: %v", result.err)
	}
	if !result.matched || !result.modified {
		t.Fatalf("request matched=%v modified=%v", result.matched, result.modified)
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("singleton safe update advanced commit seq by %d before flush, want buffered", after.CommitSeq-before.CommitSeq)
	}
	stats := mgr.StatsSnapshot()
	if got, want := stats.IndexedStageDocs-statsBefore.IndexedStageDocs, uint64(1); got != want {
		t.Fatalf("indexed staged docs=%d want %d", got, want)
	}
	if got := stats.UpdateCombineFallbackRequests - statsBefore.UpdateCombineFallbackRequests; got != 0 {
		t.Fatalf("fallback requests=%d want 0", got)
	}
	seaIDs, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city: %v", err)
	}
	if len(seaIDs) != 1 || !bytes.Equal(seaIDs[0], []byte("u1")) {
		t.Fatalf("sea ids=%q want [u1]", seaIDs)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush singleton buffered update: %v", err)
	}
	flushed := d.State()
	if flushed.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("flushed singleton update advanced commit seq by %d, want 1", flushed.CommitSeq-before.CommitSeq)
	}
}

func TestCollectionUpdateCombinerRunBatchPreservesIndependentItemErrorOutcomes(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"score":0}`), []byte(`{"score":0}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	itemErr := errors.New("bad update")
	combiner := &collectionUpdateCombiner{maxBatch: 8}
	firstCalls := 0
	secondCalls := 0
	requests := []collectionUpdateCombineRequest{
		{
			collection: col,
			documentID: []byte("u1"),
			update: func([]byte) ([]byte, bool, error) {
				firstCalls++
				return []byte(`{"score":1}`), true, nil
			},
			done: make(chan collectionUpdateCombineResult, 1),
		},
		{
			collection: col,
			documentID: []byte("u2"),
			update: func([]byte) ([]byte, bool, error) {
				secondCalls++
				return nil, false, itemErr
			},
			done: make(chan collectionUpdateCombineResult, 1),
		},
	}
	combiner.runBatch(requests)
	first := <-requests[0].done
	if first.err != nil {
		t.Fatalf("first err=%v want nil", first.err)
	}
	if !first.matched || !first.modified {
		t.Fatalf("first matched=%v modified=%v want true,true", first.matched, first.modified)
	}
	second := <-requests[1].done
	if !errors.Is(second.err, itemErr) {
		t.Fatalf("second err=%v want itemErr", second.err)
	}
	if firstCalls == 0 || secondCalls != 1 {
		t.Fatalf("callback calls first=%d second=%d want first called and second called once", firstCalls, secondCalls)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Contains(got, []byte(`"score":1`)) {
		t.Fatalf("u1 document=%s want updated score", got)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if !bytes.Contains(got, []byte(`"score":0`)) {
		t.Fatalf("u2 document=%s want unchanged score", got)
	}
}

func TestCollectionUpdateCombinerRunBatchRecoversCallbackPanic(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"score":0}`), []byte(`{"score":0}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	combiner := &collectionUpdateCombiner{maxBatch: 8}
	secondCalls := 0
	requests := []collectionUpdateCombineRequest{
		{
			collection: col,
			documentID: []byte("u1"),
			update: func([]byte) ([]byte, bool, error) {
				panic("bad callback")
			},
			done: make(chan collectionUpdateCombineResult, 1),
		},
		{
			collection: col,
			documentID: []byte("u2"),
			update: func([]byte) ([]byte, bool, error) {
				secondCalls++
				return []byte(`{"score":2}`), true, nil
			},
			done: make(chan collectionUpdateCombineResult, 1),
		},
	}
	combiner.runBatch(requests)
	first := <-requests[0].done
	if first.err == nil || !strings.Contains(first.err.Error(), "bad callback") {
		t.Fatalf("first err=%v want recovered panic", first.err)
	}
	assertNoStackTraceInError(t, first.err)
	second := <-requests[1].done
	if second.err != nil {
		t.Fatalf("second err=%v want nil", second.err)
	}
	if !second.matched || !second.modified {
		t.Fatalf("second matched=%v modified=%v want true,true", second.matched, second.modified)
	}
	if secondCalls != 1 {
		t.Fatalf("second callback calls=%d want 1", secondCalls)
	}
	got, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if !bytes.Contains(got, []byte(`"score":2`)) {
		t.Fatalf("u2 document=%s want updated score", got)
	}
}

func TestCollectionUpdateDirectRecoversCallbackPanic(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"score":0}`)}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	directCol := &Collection{db: d, meta: col.Meta()}
	matched, modified, err := directCol.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		panic("bad callback")
	})
	if err == nil || !strings.Contains(err.Error(), "bad callback") {
		t.Fatalf("Update err=%v want recovered panic", err)
	}
	assertNoStackTraceInError(t, err)
	if matched || modified {
		t.Fatalf("matched=%v modified=%v want false,false", matched, modified)
	}
}

func TestCollectionUpdateCombinerMaxBatchOneRecoversCallbackPanic(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"score":0}`)}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	combiner := &collectionUpdateCombiner{maxBatch: 1}
	matched, modified, err := combiner.update(col, []byte("u1"), func([]byte) ([]byte, bool, error) {
		panic("bad callback")
	}, bsonSetUpdate{}, false)
	if err == nil || !strings.Contains(err.Error(), "bad callback") {
		t.Fatalf("Update err=%v want recovered panic", err)
	}
	assertNoStackTraceInError(t, err)
	if matched || modified {
		t.Fatalf("matched=%v modified=%v want false,false", matched, modified)
	}
}

func TestCollectionUpdatePanicErrorOmitsStackTrace(t *testing.T) {
	err := collectionUpdatePanicError("combiner", "bad callback")
	if err == nil || !strings.Contains(err.Error(), "bad callback") {
		t.Fatalf("panic err=%v want recovered panic", err)
	}
	assertNoStackTraceInError(t, err)
}

func assertNoStackTraceInError(tb testing.TB, err error) {
	tb.Helper()
	if err == nil {
		tb.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "\n") || strings.Contains(err.Error(), ".go:") {
		tb.Fatalf("error contains stack trace: %q", err.Error())
	}
}

func TestCollectionUpdateCombinerUpdateReturnsWhenWorkerExits(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"score":0}`)}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	combiner := col.updateCombiner()
	if combiner == nil {
		t.Fatal("expected update combiner")
	}
	matched, modified, err := combiner.update(col, []byte("u1"), func([]byte) ([]byte, bool, error) {
		runtime.Goexit()
		return nil, false, nil
	}, bsonSetUpdate{}, false)
	if !errors.Is(err, errUpdateCombinerStopped) {
		t.Fatalf("update err=%v want errUpdateCombinerStopped", err)
	}
	if matched || modified {
		t.Fatalf("matched=%v modified=%v want false,false", matched, modified)
	}
	if !combiner.isStopped() {
		t.Fatal("worker exit did not mark combiner stopped")
	}
	select {
	case _, ok := <-combiner.requests:
		if ok {
			t.Fatal("worker exit left request channel open")
		}
	default:
		t.Fatal("worker exit did not close request channel")
	}
}

func TestCollectionUpdateCombinerRejectsFullDoneChannel(t *testing.T) {
	combiner := &collectionUpdateCombiner{
		requests: make(chan collectionUpdateCombineRequest, 1),
	}
	done := make(chan collectionUpdateCombineResult, 1)
	done <- collectionUpdateCombineResult{matched: true}
	if combiner.enqueue(collectionUpdateCombineRequest{
		collection: &Collection{db: &backenddb.DB{}},
		documentID: []byte("u1"),
		update:     func([]byte) ([]byte, bool, error) { return nil, false, nil },
		done:       done,
	}) {
		t.Fatal("enqueue accepted a request with a full done channel")
	}
}

func TestCollectionUpdateCombinerDocumentIDInlineStorageDoesNotAllocate(t *testing.T) {
	id := []byte("user-123")
	req := newCollectionUpdateCombineRequest(&Collection{db: &backenddb.DB{}}, id, func([]byte) ([]byte, bool, error) {
		return nil, false, nil
	}, bsonSetUpdate{}, false, make(chan collectionUpdateCombineResult, 1))
	if got := req.documentIDBytes(); !bytes.Equal(got, id) {
		t.Fatalf("inline document id=%q want %q", got, id)
	}
	id[0] = 'X'
	if got := req.documentIDBytes(); !bytes.Equal(got, []byte("user-123")) {
		t.Fatalf("inline document id changed after caller mutation: %q", got)
	}

	allocID := []byte("user-123")
	if allocs := testing.AllocsPerRun(1000, func() {
		req := newCollectionUpdateCombineRequest(nil, allocID, nil, bsonSetUpdate{}, false, nil)
		if !bytes.Equal((&req).documentIDBytes(), allocID) {
			t.Fatal("inline document id mismatch")
		}
	}); allocs != 0 {
		t.Fatalf("inline document id request allocations/run=%0.1f want 0", allocs)
	}
}

func TestCollectionUpdateCombinerDocumentIDLongStorageClonesCallerBytes(t *testing.T) {
	id := bytes.Repeat([]byte("x"), collectionUpdateCombineInlineDocumentIDMax+1)
	req := newCollectionUpdateCombineRequest(&Collection{db: &backenddb.DB{}}, id, func([]byte) ([]byte, bool, error) {
		return nil, false, nil
	}, bsonSetUpdate{}, false, make(chan collectionUpdateCombineResult, 1))
	if req.documentIDInlineLen != 0 {
		t.Fatalf("long document id inline len=%d want 0", req.documentIDInlineLen)
	}
	if len(req.documentID) != len(id) {
		t.Fatalf("long document id len=%d want %d", len(req.documentID), len(id))
	}
	if got := req.documentIDBytes(); !bytes.Equal(got, id) {
		t.Fatalf("long document id=%q want %q", got, id)
	}
	id[0] = 'y'
	if got := req.documentIDBytes(); got[0] != 'x' {
		t.Fatalf("long document id changed after caller mutation: first byte %q", got[0])
	}
}

func TestCollectionUpdateBypassesIdleCombinerWhenNoBatchPressure(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"city":"hnl","score":0}`)}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	idle := col.updateCombiner()
	if idle == nil {
		t.Fatal("expected idle combiner")
	}

	before := mgr.StatsSnapshot()
	matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"city":"sea","score":1}`), true, nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("update matched=%v modified=%v want true,true", matched, modified)
	}
	after := mgr.StatsSnapshot()
	if got := after.UpdateCombineInlineRequests - before.UpdateCombineInlineRequests; got != 1 {
		t.Fatalf("inline requests delta=%d want 1", got)
	}
	if got := after.UpdateCombineRequests - before.UpdateCombineRequests; got != 0 {
		t.Fatalf("combine requests delta=%d want 0", got)
	}
	ids, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find updated city: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, ids, []byte("u1"))
}

func TestCollectionCloseWaitsForInlineUpdateWithoutCombiner(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	mgr := NewCollectionManager(d)
	collectionCloseHookDone := make(chan struct{})
	d.RegisterCloseHook(func() error {
		close(collectionCloseHookDone)
		return nil
	})
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"city":"hnl","score":0}`)}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	enteredUpdate := make(chan struct{})
	releaseUpdate := make(chan struct{})
	updateDone := make(chan error, 1)
	go func() {
		matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
			close(enteredUpdate)
			<-releaseUpdate
			return []byte(`{"city":"sea","score":1}`), true, nil
		})
		if err == nil && (!matched || !modified) {
			err = fmt.Errorf("update matched=%v modified=%v", matched, modified)
		}
		updateDone <- err
	}()
	select {
	case <-enteredUpdate:
	case <-time.After(collectionTestTimeout(t, time.Second)):
		t.Fatal("inline update callback did not start")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- d.Close()
	}()

	closingDeadline := time.NewTimer(collectionTestTimeout(t, time.Second))
	defer closingDeadline.Stop()
	closingTicker := time.NewTicker(time.Millisecond)
	defer closingTicker.Stop()
	for !col.writeDomain.closingWrites.Load() {
		select {
		case err := <-closeDone:
			t.Fatalf("close returned before marking writes closing: %v", err)
		case <-closingDeadline.C:
			t.Fatal("close did not reach write-domain drain")
		case <-closingTicker.C:
		}
	}

	closeBlocked := time.NewTimer(20 * time.Millisecond)
	select {
	case err := <-closeDone:
		closeBlocked.Stop()
		t.Fatalf("close returned while inline update callback was blocked: %v", err)
	case <-closeBlocked.C:
	}
	close(releaseUpdate)

	var updateErr error
	select {
	case updateErr = <-updateDone:
	case <-time.After(collectionTestTimeout(t, time.Second)):
		t.Fatal("inline update did not finish after release")
	}
	select {
	case <-collectionCloseHookDone:
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatal("collection close hook did not finish after inline update completed")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close after inline update: %v", err)
		}
	case <-time.After(collectionTestTimeout(t, 30*time.Second)):
		t.Fatal("close did not finish after inline update completed")
	}
	if updateErr != nil && !errors.Is(updateErr, backenddb.ErrClosed) {
		t.Fatalf("inline update err=%v want nil or ErrClosed", updateErr)
	}
	if updateErr != nil {
		return
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedMgr := NewCollectionManager(reopened)
	reopenedCol, err := reopenedMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	got, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get reopened document: %v", err)
	}
	if !bytes.Contains(got, []byte(`"city":"sea"`)) {
		t.Fatalf("reopened document=%s want city sea", got)
	}
}

func TestCollectionUpdateUsesCombinerWhenInlineUpdateInFlight(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"city":"hnl","score":0}`),
			[]byte(`{"city":"hnl","score":0}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	before := mgr.StatsSnapshot()
	enteredFirstUpdate := make(chan struct{})
	releaseFirstUpdate := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseFirstUpdate) })

	firstDone := make(chan error, 1)
	go func() {
		matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
			enterOnce.Do(func() { close(enteredFirstUpdate) })
			<-releaseFirstUpdate
			return []byte(`{"city":"bos","score":1}`), true, nil
		})
		if err == nil && (!matched || !modified) {
			err = fmt.Errorf("first update matched=%v modified=%v", matched, modified)
		}
		firstDone <- err
	}()

	select {
	case <-enteredFirstUpdate:
	case <-time.After(collectionTestTimeout(t, time.Second)):
		t.Fatal("first update callback did not start")
	}

	secondDone := make(chan error, 1)
	go func() {
		matched, modified, err := col.Update([]byte("u2"), func([]byte) ([]byte, bool, error) {
			return []byte(`{"city":"sea","score":1}`), true, nil
		})
		if err == nil && (!matched || !modified) {
			err = fmt.Errorf("second update matched=%v modified=%v", matched, modified)
		}
		secondDone <- err
	}()

	deadline := time.After(collectionTestTimeout(t, time.Second))
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		stats := mgr.StatsSnapshot()
		if stats.UpdateCombineRequests-before.UpdateCombineRequests > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("second update did not enqueue through combiner")
		case <-ticker.C:
		}
	}

	releaseOnce.Do(func() { close(releaseFirstUpdate) })
	for name, done := range map[string]chan error{
		"first":  firstDone,
		"second": secondDone,
	} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s update: %v", name, err)
			}
		case <-time.After(collectionTestTimeout(t, time.Second)):
			t.Fatalf("%s update did not complete", name)
		}
	}

	after := mgr.StatsSnapshot()
	if got := after.UpdateCombineInlineRequests - before.UpdateCombineInlineRequests; got != 1 {
		t.Fatalf("inline requests delta=%d want 1", got)
	}
	if got := after.UpdateCombineRequests - before.UpdateCombineRequests; got == 0 {
		t.Fatal("combine requests delta=0 want positive")
	}
	ids, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find updated city: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, ids, []byte("u2"))

	col.writeDomain.updateCombineLastRequestUnixNano.Store(time.Now().Add(time.Hour).UnixNano())
	combineBeforeCooldownCheck := after.UpdateCombineRequests
	inlineBeforeCooldownCheck := after.UpdateCombineInlineRequests
	matched, modified, err := col.Update([]byte("u2"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"city":"iad","score":2}`), true, nil
	})
	if err != nil {
		t.Fatalf("cooldown update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("cooldown update matched=%v modified=%v want true,true", matched, modified)
	}
	afterCooldownCheck := mgr.StatsSnapshot()
	if got := afterCooldownCheck.UpdateCombineInlineRequests - inlineBeforeCooldownCheck; got != 0 {
		t.Fatalf("cooldown inline requests delta=%d want 0", got)
	}
	if got := afterCooldownCheck.UpdateCombineRequests - combineBeforeCooldownCheck; got == 0 {
		t.Fatal("cooldown combine requests delta=0 want positive")
	}
}

func TestUpdateBatchBufferedEntryPoolClearsEntries(t *testing.T) {
	updateBatchBufferedEntryPool = sync.Pool{}

	entries, buffer := getUpdateBatchBufferedEntries(4)
	entries[0] = updateBatchBufferedEntry{
		value: []byte("current"),
		flags: node.FlagTombstone,
		found: true,
	}
	entries[3] = updateBatchBufferedEntry{
		value: []byte("stale-capacity"),
		flags: node.FlagTombstone,
		found: true,
	}
	putUpdateBatchBufferedEntries(entries[:2], buffer)

	for i, entry := range entries[:cap(entries)] {
		if entry.value != nil || entry.flags != 0 || entry.found {
			t.Fatalf("pooled entry %d retained data immediately after put: %+v", i, entry)
		}
	}

	reused, reusedBuffer := getUpdateBatchBufferedEntries(2)
	defer putUpdateBatchBufferedEntries(reused, reusedBuffer)
	for i, entry := range reused {
		if entry.value != nil || entry.flags != 0 || entry.found {
			t.Fatalf("entry %d not cleared after pool reuse: %+v", i, entry)
		}
	}
}

func TestCollectionUpdateCombinerCloseRequestsAllowsNilRequests(t *testing.T) {
	combiner := &collectionUpdateCombiner{}
	if !combiner.closeRequests() {
		t.Fatal("closeRequests returned false for fresh combiner")
	}
	if !combiner.isStopped() {
		t.Fatal("closeRequests did not mark combiner stopped")
	}
}

func TestCollectionUpdateCombinerDuplicateIDsPreserveOrder(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"score":0}`)}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	setScore := func(score int32) func([]byte) ([]byte, bool, error) {
		return func([]byte) ([]byte, bool, error) {
			return []byte(fmt.Sprintf(`{"score":%d}`, score)), true, nil
		}
	}
	combiner := &collectionUpdateCombiner{maxBatch: 8}
	requests := []collectionUpdateCombineRequest{
		{collection: col, documentID: []byte("u1"), update: setScore(1), done: make(chan collectionUpdateCombineResult, 1)},
		{collection: col, documentID: []byte("u1"), update: setScore(2), done: make(chan collectionUpdateCombineResult, 1)},
	}
	combiner.runBatch(requests)
	for i, req := range requests {
		result := <-req.done
		if result.err != nil {
			t.Fatalf("request %d err: %v", i, result.err)
		}
		if !result.matched || !result.modified {
			t.Fatalf("request %d matched=%v modified=%v", i, result.matched, result.modified)
		}
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Contains(got, []byte(`"score":2`)) {
		t.Fatalf("u1 document=%s want final score 2", got)
	}
}

func TestCollectionUpdateCombinerDefersDuplicateIDsAndBatchesDistinctPeers(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"score":0}`), []byte(`{"score":0}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	setScore := func(score int32) func([]byte) ([]byte, bool, error) {
		return func([]byte) ([]byte, bool, error) {
			return []byte(fmt.Sprintf(`{"score":%d}`, score)), true, nil
		}
	}
	first := collectionUpdateCombineRequest{
		collection: col,
		documentID: []byte("u1"),
		update:     setScore(1),
		done:       make(chan collectionUpdateCombineResult, 1),
	}
	duplicate := collectionUpdateCombineRequest{
		collection: col,
		documentID: []byte("u1"),
		update:     setScore(2),
		done:       make(chan collectionUpdateCombineResult, 1),
	}
	peer := collectionUpdateCombineRequest{
		collection: col,
		documentID: []byte("u2"),
		update:     setScore(3),
		done:       make(chan collectionUpdateCombineResult, 1),
	}
	combiner := &collectionUpdateCombiner{
		maxBatch: 8,
		domain:   col.writeDomain,
		requests: make(chan collectionUpdateCombineRequest, 2),
	}
	combiner.requests <- duplicate
	combiner.requests <- peer
	beforeState := d.State()
	beforeStats := mgr.StatsSnapshot()

	combiner.runBatchStartingWith(first)
	for name, done := range map[string]chan collectionUpdateCombineResult{
		"first": first.done,
		"peer":  peer.done,
	} {
		result := <-done
		if result.err != nil || !result.matched || !result.modified {
			t.Fatalf("%s result=%+v want matched modified nil err", name, result)
		}
	}
	select {
	case result := <-duplicate.done:
		t.Fatalf("duplicate completed in same batch: %+v", result)
	default:
	}
	if got := len(combiner.deferred); got != 1 {
		t.Fatalf("deferred duplicate count=%d want 1", got)
	}
	midState := d.State()
	if midState.CommitSeq != beforeState.CommitSeq+1 {
		t.Fatalf("first combined batch advanced commit seq by %d, want 1", midState.CommitSeq-beforeState.CommitSeq)
	}
	midStats := mgr.StatsSnapshot()
	if got := midStats.UpdateCombineFallbackRequests - beforeStats.UpdateCombineFallbackRequests; got != 0 {
		t.Fatalf("fallback requests after distinct-peer batch=%d want 0", got)
	}

	deferred, ok := combiner.popDeferredRequest()
	if !ok {
		t.Fatal("missing deferred duplicate")
	}
	combiner.runBatchStartingWith(deferred)
	result := <-duplicate.done
	if result.err != nil || !result.matched || !result.modified {
		t.Fatalf("duplicate result=%+v want matched modified nil err", result)
	}
	afterState := d.State()
	if afterState.CommitSeq != beforeState.CommitSeq+2 {
		t.Fatalf("two serialized batches advanced commit seq by %d, want 2", afterState.CommitSeq-beforeState.CommitSeq)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Contains(got, []byte(`"score":2`)) {
		t.Fatalf("u1 document=%s want final score 2", got)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if !bytes.Contains(got, []byte(`"score":3`)) {
		t.Fatalf("u2 document=%s want score 3", got)
	}
}

func TestCollectionUpdateCombinerShardedIngressPublishesDistinctIDsOnce(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"score":0}`), []byte(`{"score":0}`)},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	setScore := func(score int32) func([]byte) ([]byte, bool, error) {
		return func([]byte) ([]byte, bool, error) {
			return []byte(fmt.Sprintf(`{"score":%d}`, score)), true, nil
		}
	}
	combiner := &collectionUpdateCombiner{
		maxBatch:        8,
		domain:          col.writeDomain,
		shardedRequests: make([]chan collectionUpdateCombineRequest, 4),
		readyShards:     make(chan int, 16),
		done:            make(chan struct{}),
	}
	for i := range combiner.shardedRequests {
		combiner.shardedRequests[i] = make(chan collectionUpdateCombineRequest, 8)
	}
	firstDone := make(chan collectionUpdateCombineResult, 1)
	secondDone := make(chan collectionUpdateCombineResult, 1)
	if !combiner.enqueue(newCollectionUpdateCombineRequest(col, []byte("u1"), setScore(1), bsonSetUpdate{}, false, firstDone)) {
		t.Fatal("enqueue first sharded update")
	}
	if !combiner.enqueue(newCollectionUpdateCombineRequest(col, []byte("u2"), setScore(2), bsonSetUpdate{}, false, secondDone)) {
		t.Fatal("enqueue second sharded update")
	}
	before := d.State()
	go combiner.run()
	defer combiner.stop()
	for name, done := range map[string]chan collectionUpdateCombineResult{
		"first":  firstDone,
		"second": secondDone,
	} {
		select {
		case result := <-done:
			if result.err != nil || !result.matched || !result.modified {
				t.Fatalf("%s result matched=%v modified=%v err=%v want matched modified nil err", name, result.matched, result.modified, result.err)
			}
		case <-time.After(collectionTestTimeout(t, time.Second)):
			t.Fatalf("%s update did not complete", name)
		}
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("sharded ingress batch advanced commit seq by %d, want 1", after.CommitSeq-before.CommitSeq)
	}
}

func TestCollectionUpdateCombinerLaneWorkerEnqueueSkipsReadyShardSignal(t *testing.T) {
	col := &Collection{db: &backenddb.DB{}}
	newCombiner := func(laneWorkers bool) *collectionUpdateCombiner {
		combiner := &collectionUpdateCombiner{
			laneWorkers:     laneWorkers,
			shardedRequests: make([]chan collectionUpdateCombineRequest, 4),
			readyShards:     make(chan int, 4),
		}
		for i := range combiner.shardedRequests {
			combiner.shardedRequests[i] = make(chan collectionUpdateCombineRequest, 1)
		}
		return combiner
	}
	update := func([]byte) ([]byte, bool, error) { return nil, false, nil }

	laneCombiner := newCombiner(true)
	if !laneCombiner.enqueue(newCollectionUpdateCombineRequest(col, []byte("u1"), update, bsonSetUpdate{}, false, make(chan collectionUpdateCombineResult, 1))) {
		t.Fatal("enqueue lane-worker request")
	}
	if got := len(laneCombiner.readyShards); got != 0 {
		t.Fatalf("lane-worker ready shard signals=%d want 0", got)
	}

	mergerCombiner := newCombiner(false)
	if !mergerCombiner.enqueue(newCollectionUpdateCombineRequest(col, []byte("u1"), update, bsonSetUpdate{}, false, make(chan collectionUpdateCombineResult, 1))) {
		t.Fatal("enqueue sharded-merger request")
	}
	if got := len(mergerCombiner.readyShards); got != 1 {
		t.Fatalf("sharded-merger ready shard signals=%d want 1", got)
	}
}

func TestCollectionUpdateCombinerLaneWorkersReadOwnBufferedWrites(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	mgr.SetUpdateCombineShardsForProfiling(4)
	mgr.SetUpdateCombineLaneWorkersForProfiling(true)
	combiner := col.updateCombiner()
	if combiner == nil {
		t.Fatal("expected update combiner")
	}
	defer combiner.stop()

	setCity := func(city string) bsonSetUpdate {
		t.Helper()
		spec, err := newBSONSetUpdate([]BSONSetField{{
			Key:   "city",
			Value: mustBSONRawValue(t, city),
		}})
		if err != nil {
			t.Fatalf("prepare BSON set %q: %v", city, err)
		}
		return spec
	}
	assertCity := func(city string) {
		t.Helper()
		doc, err := col.Get([]byte("u1"))
		if err != nil {
			t.Fatalf("get u1: %v", err)
		}
		if got := bson.Raw(doc).Lookup("city").StringValue(); got != city {
			t.Fatalf("city=%q want %q", got, city)
		}
	}

	before := d.State()
	matched, modified, err := combiner.update(col, []byte("u1"), nil, setCity("sea"), true)
	if err != nil {
		t.Fatalf("first lane-worker update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("first matched=%v modified=%v want true/true", matched, modified)
	}
	assertCity("sea")
	matched, modified, err = combiner.update(col, []byte("u1"), nil, setCity("sfo"), true)
	if err != nil {
		t.Fatalf("second lane-worker update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("second matched=%v modified=%v want true/true", matched, modified)
	}
	assertCity("sfo")
	after := d.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("lane-worker updates advanced commit seq by %d before flush, want buffered", after.CommitSeq-before.CommitSeq)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush lane-worker updates: %v", err)
	}
	assertCity("sfo")
}

func TestCollectionUpdateCombinerLaneWorkersDeferDuplicateDocumentUntilAfterStage(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	setCity := func(city string) bsonSetUpdate {
		t.Helper()
		spec, err := newBSONSetUpdate([]BSONSetField{{
			Key:   "city",
			Value: mustBSONRawValue(t, city),
		}})
		if err != nil {
			t.Fatalf("prepare BSON set %q: %v", city, err)
		}
		return spec
	}
	done1 := make(chan collectionUpdateCombineResult, 1)
	done2 := make(chan collectionUpdateCombineResult, 1)
	req1 := newCollectionUpdateCombineRequest(col, []byte("u1"), nil, setCity("sea"), true, done1)
	req2 := newCollectionUpdateCombineRequest(col, []byte("u1"), nil, setCity("sfo"), true, done2)

	combiner := &collectionUpdateCombiner{
		maxBatch:        8,
		domain:          col.writeDomain,
		laneWorkers:     true,
		shardedRequests: []chan collectionUpdateCombineRequest{make(chan collectionUpdateCombineRequest, 8)},
		preparedBatches: make(chan collectionUpdateCombinePreparedBatch, 8),
	}
	mergerDone := make(chan struct{})
	go func() {
		combiner.runPreparedBatchMerger()
		close(mergerDone)
	}()
	combiner.shardedRequests[0] <- req1
	combiner.shardedRequests[0] <- req2
	close(combiner.shardedRequests[0])
	combiner.runShardWorker(0)
	close(combiner.preparedBatches)
	select {
	case <-mergerDone:
	case <-time.After(collectionTestTimeout(t, time.Second)):
		t.Fatal("prepared batch merger did not exit")
	}

	for name, done := range map[string]chan collectionUpdateCombineResult{"first": done1, "second": done2} {
		select {
		case result := <-done:
			if result.err != nil || !result.matched || !result.modified {
				t.Fatalf("%s result=%+v want matched modified nil err", name, result)
			}
		case <-time.After(collectionTestTimeout(t, time.Second)):
			t.Fatalf("%s update did not complete", name)
		}
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sfo" {
		t.Fatalf("city=%q want sfo", got)
	}
}

func TestCollectionUpdateCombinerSinglePreparedLanePlanStagesBuffered(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:                   DocumentFormatBSON,
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 1 << 20,
			BufferedIndexedWriteMaxRootRuns:  1 << 20,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	combiner := &collectionUpdateCombiner{
		maxBatch:        8,
		domain:          col.writeDomain,
		laneWorkers:     true,
		shardedRequests: make([]chan collectionUpdateCombineRequest, 4),
	}
	for i := range combiner.shardedRequests {
		combiner.shardedRequests[i] = make(chan collectionUpdateCombineRequest, 8)
	}
	spec, err := newBSONSetUpdate([]BSONSetField{{Key: "city", Value: mustBSONRawValue(t, "sea")}})
	if err != nil {
		t.Fatalf("prepare BSON set: %v", err)
	}
	req := newCollectionUpdateCombineRequest(col, []byte("u1"), nil, spec, true, make(chan collectionUpdateCombineResult, 1))
	prepared := combiner.prepareBatchWithScratch([]collectionUpdateCombineRequest{req}, nil)
	if prepared.err != nil {
		t.Fatalf("prepare: %v", prepared.err)
	}
	if prepared.fallbackDirect || prepared.plan == nil || prepared.plan.directBufferedUpdate == nil {
		t.Fatalf("prepare produced fallback/non-direct plan: %+v", prepared)
	}
	beforeState := d.State()
	beforeStats := mgr.StatsSnapshot()
	combiner.stagePreparedBatches([]collectionUpdateCombinePreparedBatch{prepared})
	result := <-req.done
	if result.err != nil || !result.matched || !result.modified {
		t.Fatalf("result=%+v want matched modified nil err", result)
	}
	afterState := d.State()
	if afterState.CommitSeq != beforeState.CommitSeq {
		t.Fatalf("single prepared stage advanced commit seq by %d before flush, want buffered", afterState.CommitSeq-beforeState.CommitSeq)
	}
	afterStats := mgr.StatsSnapshot()
	if got := afterStats.UpdateCombineFallbackRequests - beforeStats.UpdateCombineFallbackRequests; got != 0 {
		t.Fatalf("fallback requests=%d want 0", got)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("city=%q want sea", got)
	}
}

func TestCollectionUpdateCombinerMergedLanePlansPreserveBufferedReadGeneration(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:                   DocumentFormatBSON,
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 1 << 20,
			BufferedIndexedWriteMaxRootRuns:  1 << 20,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "email", Value: "a@example.com"}, {Key: "city", Value: "hnl"}, {Key: "score", Value: int32(0)}}),
			mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "email", Value: "b@example.com"}, {Key: "city", Value: "hnl"}, {Key: "score", Value: int32(0)}}),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	combiner := &collectionUpdateCombiner{
		maxBatch:        8,
		domain:          col.writeDomain,
		laneWorkers:     true,
		shardedRequests: make([]chan collectionUpdateCombineRequest, 4),
	}
	for i := range combiner.shardedRequests {
		combiner.shardedRequests[i] = make(chan collectionUpdateCombineRequest, 8)
	}
	prepareSetScore := func(id string, score int32) collectionUpdateCombinePreparedBatch {
		t.Helper()
		spec, err := newBSONSetUpdate([]BSONSetField{{
			Key:   "score",
			Value: mustBSONRawValue(t, score),
		}})
		if err != nil {
			t.Fatalf("prepare BSON set score=%d: %v", score, err)
		}
		req := newCollectionUpdateCombineRequest(col, []byte(id), nil, spec, true, make(chan collectionUpdateCombineResult, 1))
		prepared := combiner.prepareBatchWithScratch([]collectionUpdateCombineRequest{req}, nil)
		if prepared.err != nil {
			t.Fatalf("prepare %s score=%d: %v", id, score, prepared.err)
		}
		if prepared.fallbackDirect || prepared.plan == nil || prepared.plan.directBufferedUpdate == nil {
			t.Fatalf("prepare %s score=%d produced fallback/non-direct plan: %+v", id, score, prepared)
		}
		return prepared
	}
	requireResult := func(prepared collectionUpdateCombinePreparedBatch, name string) {
		t.Helper()
		result := <-prepared.batch[0].done
		if result.err != nil || !result.matched || !result.modified {
			t.Fatalf("%s result=%+v want matched modified nil err", name, result)
		}
	}
	assertScore := func(id string, score int32) {
		t.Helper()
		doc, err := col.Get([]byte(id))
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if got := bson.Raw(doc).Lookup("score").Int32(); got != score {
			t.Fatalf("%s score=%d want %d", id, got, score)
		}
	}

	beforeState := d.State()
	beforeStats := mgr.StatsSnapshot()
	first := prepareSetScore("u1", 1)
	secondOtherDoc := prepareSetScore("u2", 10)
	combiner.stagePreparedBatches([]collectionUpdateCombinePreparedBatch{first})
	requireResult(first, "first")

	secondSameDoc := prepareSetScore("u1", 2)
	merged, _, err := mergeDirectBufferedPreparedBatches([]collectionUpdateCombinePreparedBatch{secondOtherDoc, secondSameDoc})
	if err != nil {
		t.Fatalf("merge mixed-generation plans: %v", err)
	}
	if merged.directBufferedUpdate == nil || len(merged.directBufferedUpdate.primaryEntryReadGenerations) == 0 {
		t.Fatalf("merged mixed-generation plan missing per-entry read generations: %+v", merged.directBufferedUpdate)
	}
	merged.close()
	combiner.stagePreparedBatches([]collectionUpdateCombinePreparedBatch{secondOtherDoc, secondSameDoc})
	requireResult(secondOtherDoc, "second other doc")
	requireResult(secondSameDoc, "second same doc")

	afterState := d.State()
	if afterState.CommitSeq != beforeState.CommitSeq {
		t.Fatalf("merged lane plans advanced commit seq by %d before flush, want buffered", afterState.CommitSeq-beforeState.CommitSeq)
	}
	afterStats := mgr.StatsSnapshot()
	if got := afterStats.UpdateCombineFallbackRequests - beforeStats.UpdateCombineFallbackRequests; got != 0 {
		t.Fatalf("fallback requests=%d want 0", got)
	}
	assertScore("u1", 2)
	assertScore("u2", 10)
}

func TestMergeDirectBufferedPreparedBatchesRejectsSchemaMismatch(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:                   DocumentFormatBSON,
			BufferedIndexedWrites:            true,
			BufferedIndexedWriteMaxDocuments: 1 << 20,
			BufferedIndexedWriteMaxRootRuns:  1 << 20,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}, {Key: "score", Value: int32(0)}}),
			mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "hnl"}, {Key: "score", Value: int32(0)}}),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	combiner := &collectionUpdateCombiner{
		maxBatch:        8,
		domain:          col.writeDomain,
		laneWorkers:     true,
		shardedRequests: make([]chan collectionUpdateCombineRequest, 4),
	}
	for i := range combiner.shardedRequests {
		combiner.shardedRequests[i] = make(chan collectionUpdateCombineRequest, 8)
	}
	prepareSetScore := func(id string, score int32) collectionUpdateCombinePreparedBatch {
		t.Helper()
		spec, err := newBSONSetUpdate([]BSONSetField{{
			Key:   "score",
			Value: mustBSONRawValue(t, score),
		}})
		if err != nil {
			t.Fatalf("prepare BSON set score=%d: %v", score, err)
		}
		req := newCollectionUpdateCombineRequest(col, []byte(id), nil, spec, true, make(chan collectionUpdateCombineResult, 1))
		prepared := combiner.prepareBatchWithScratch([]collectionUpdateCombineRequest{req}, nil)
		if prepared.err != nil {
			t.Fatalf("prepare %s score=%d: %v", id, score, prepared.err)
		}
		if prepared.fallbackDirect || prepared.plan == nil || prepared.plan.directBufferedUpdate == nil {
			t.Fatalf("prepare %s score=%d produced fallback/non-direct plan: %+v", id, score, prepared)
		}
		return prepared
	}

	first := prepareSetScore("u1", 1)
	defer first.plan.close()
	second := prepareSetScore("u2", 2)
	defer second.plan.close()
	second.plan.meta.Indexes = append(second.plan.meta.Indexes, IndexDefinition{Name: "status", Field: "status", ValueType: IndexValueString})
	if _, _, err := mergeDirectBufferedPreparedBatches([]collectionUpdateCombinePreparedBatch{first, second}); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("merge schema mismatch err=%v want ErrConcurrentMutation", err)
	}
}

func TestGetBufferedDocumentPrefersCurrentRootRunOverDetachedOverlay(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}
	col := &Collection{meta: meta}
	primaryRoot := collectionPrimaryRootName("users")

	detachedOverlay := newBufferedPrimaryOverlay(1)
	detachedOverlay.addEntries([]directBufferedRootEntry{{
		key:   []byte("u1"),
		value: mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "old"}}),
		flags: node.FlagInline,
	}})
	currentPrimary := newCollectionRunTable(1)
	setCollectionRunValue(currentPrimary, []byte("u1"), mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "new"}}))
	defer resetCollectionRunTable(currentPrimary)

	domain := &collectionWriteDomain{
		count:  2,
		loaded: true,
		meta:   meta,
		indexedFlushUnits: []indexedFlushUnit{{
			primaryOverlay: detachedOverlay,
		}},
		rootRuns: map[string][]memtable.Table{
			primaryRoot: {currentPrimary},
		},
	}
	doc, buffered, found, err := col.getBufferedDocumentIntoLocked(domain, []byte("u1"), nil)
	if err != nil {
		t.Fatalf("get buffered document: %v", err)
	}
	if !buffered || !found {
		t.Fatalf("buffered=%v found=%v want true,true", buffered, found)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "new" {
		t.Fatalf("city=%q want new", got)
	}
}

func TestCollectionUpdateCombinerMixedCollectionsFallbackDirect(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "left"}); err != nil {
		t.Fatalf("create left: %v", err)
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "right"}); err != nil {
		t.Fatalf("create right: %v", err)
	}
	left, err := mgr.OpenCollection("left")
	if err != nil {
		t.Fatalf("open left: %v", err)
	}
	right, err := mgr.OpenCollection("right")
	if err != nil {
		t.Fatalf("open right: %v", err)
	}
	if _, err := left.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"score":0}`)}); err != nil {
		t.Fatalf("insert left: %v", err)
	}
	if _, err := right.InsertBatch([][]byte{[]byte("u2")}, [][]byte{[]byte(`{"score":0}`)}); err != nil {
		t.Fatalf("insert right: %v", err)
	}

	setScore := func(score int32) func([]byte) ([]byte, bool, error) {
		return func([]byte) ([]byte, bool, error) {
			return []byte(fmt.Sprintf(`{"score":%d}`, score)), true, nil
		}
	}
	combiner := &collectionUpdateCombiner{maxBatch: 8}
	requests := []collectionUpdateCombineRequest{
		{collection: left, documentID: []byte("u1"), update: setScore(1), done: make(chan collectionUpdateCombineResult, 1)},
		{collection: right, documentID: []byte("u2"), update: setScore(2), done: make(chan collectionUpdateCombineResult, 1)},
	}
	combiner.runBatch(requests)
	for i, req := range requests {
		result := <-req.done
		if result.err != nil {
			t.Fatalf("request %d err: %v", i, result.err)
		}
		if !result.matched || !result.modified {
			t.Fatalf("request %d matched=%v modified=%v want true,true", i, result.matched, result.modified)
		}
	}
	got, err := left.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get left u1: %v", err)
	}
	if !bytes.Contains(got, []byte(`"score":1`)) {
		t.Fatalf("left document=%s want score 1", got)
	}
	got, err = right.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get right u2: %v", err)
	}
	if !bytes.Contains(got, []byte(`"score":2`)) {
		t.Fatalf("right document=%s want score 2", got)
	}
}

func TestCollectionUpdateCombinerEvictsWhenIdle(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	col.writeDomain.updateCombineMu.Lock()
	col.writeDomain.updateCombineTTL = 25 * time.Millisecond
	col.writeDomain.updateCombineMu.Unlock()
	combiner := col.updateCombiner()
	if combiner == nil {
		t.Fatal("expected update combiner")
	}
	select {
	case <-combiner.done:
	case <-time.After(time.Second):
		t.Fatal("combiner was not evicted after idle timeout")
	}
	col.writeDomain.updateCombineMu.Lock()
	stillCached := col.writeDomain.updateCombiner == combiner
	col.writeDomain.updateCombineMu.Unlock()
	combiner.mu.RLock()
	stopped := combiner.stopped
	combiner.mu.RUnlock()
	if stillCached || !stopped {
		t.Fatalf("stillCached=%v stopped=%v want false,true", stillCached, stopped)
	}
}

func TestCollectionManagerCloseStopsUpdateCombiners(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	combiner := col.updateCombiner()
	if combiner == nil {
		t.Fatal("expected update combiner")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	combiner.mu.RLock()
	stopped := combiner.stopped
	combiner.mu.RUnlock()
	if !stopped {
		t.Fatal("combiner was not stopped")
	}
	if got := col.updateCombiner(); got != nil {
		t.Fatal("closed manager created a new combiner")
	}
}

func TestCollectionUpdateCombinerReplacesStoppedCachedCombinerWithoutWaiting(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	stopped := &collectionUpdateCombiner{done: make(chan struct{})}
	stopped.mu.Lock()
	stopped.stopped = true
	stopped.mu.Unlock()
	col.writeDomain.updateCombineMu.Lock()
	col.writeDomain.updateCombiner = stopped
	col.writeDomain.updateCombineMu.Unlock()

	gotCh := make(chan *collectionUpdateCombiner, 1)
	go func() {
		gotCh <- col.updateCombiner()
	}()
	select {
	case got := <-gotCh:
		if got == nil {
			t.Fatal("updateCombiner returned nil")
		}
		if got == stopped {
			t.Fatal("updateCombiner reused stopped combiner")
		}
		got.stop()
	case <-time.After(time.Second):
		t.Fatal("updateCombiner waited for stopped combiner done channel")
	}
}

func TestCollectionUpdateCombinerWaitsForIdleDrain(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	draining := &collectionUpdateCombiner{done: make(chan struct{})}
	col.writeDomain.updateCombineMu.Lock()
	col.writeDomain.updateDraining = draining
	col.writeDomain.updateCombineMu.Unlock()

	started := make(chan struct{})
	gotCh := make(chan *collectionUpdateCombiner, 1)
	go func() {
		close(started)
		gotCh <- col.updateCombiner()
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("updateCombiner goroutine did not start")
	}
	select {
	case got := <-gotCh:
		t.Fatalf("updateCombiner returned %v before idle drain completed", got)
	default:
	}

	col.writeDomain.updateCombineMu.Lock()
	col.writeDomain.updateDraining = nil
	col.writeDomain.updateCombineMu.Unlock()
	close(draining.done)

	select {
	case got := <-gotCh:
		if got == nil || got == draining {
			t.Fatalf("updateCombiner returned %v after idle drain", got)
		}
		got.stop()
	case <-time.After(time.Second):
		t.Fatal("updateCombiner did not resume after idle drain completed")
	}
}

func TestCollectionUpdateCombinerStopWaitsForActiveWorker(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"score":0}`)}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	combiner := &collectionUpdateCombiner{
		maxBatch: 8,
		requests: make(chan collectionUpdateCombineRequest, 4),
		done:     make(chan struct{}),
	}
	go combiner.run()

	started := make(chan struct{})
	release := make(chan struct{})
	resultCh := make(chan collectionUpdateCombineResult, 1)
	if !combiner.enqueue(collectionUpdateCombineRequest{
		collection: col,
		documentID: []byte("u1"),
		update: func([]byte) ([]byte, bool, error) {
			close(started)
			<-release
			return []byte(`{"score":5}`), true, nil
		},
		done: resultCh,
	}) {
		t.Fatal("enqueue update")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("combiner worker did not start update")
	}

	stopReturned := make(chan struct{})
	stopStarted := make(chan struct{})
	go func() {
		close(stopStarted)
		combiner.stop()
		close(stopReturned)
	}()
	select {
	case <-stopStarted:
	case <-time.After(time.Second):
		t.Fatal("stop goroutine did not start")
	}
	for {
		combiner.mu.RLock()
		stopped := combiner.stopped
		combiner.mu.RUnlock()
		if stopped {
			break
		}
		select {
		case <-stopReturned:
			t.Fatal("stop returned before marking combiner stopped")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case <-stopReturned:
		t.Fatal("stop returned before active combiner worker drained")
	default:
	}

	close(release)
	select {
	case <-stopReturned:
	case <-time.After(time.Second):
		t.Fatal("stop did not return after active combiner worker drained")
	}
	var result collectionUpdateCombineResult
	select {
	case result = <-resultCh:
	case <-time.After(time.Second):
		t.Fatal("active update did not complete after release")
	}
	if result.err != nil || !result.matched || !result.modified {
		t.Fatalf("combined result=%+v want matched modified nil err", result)
	}
	select {
	case <-combiner.done:
	case <-time.After(time.Second):
		t.Fatal("combiner worker did not exit after active update completed")
	}
}

func TestCollectionManagerCloseForBackendPreventsCombinerRecreationBeforeDBClosing(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	combiner := col.updateCombiner()
	if combiner == nil {
		t.Fatal("expected update combiner")
	}
	if err := mgr.closeForBackend(); err != nil {
		t.Fatalf("closeForBackend: %v", err)
	}
	if d.IsClosing() {
		t.Fatal("backend DB is closing; test must cover pre-closing close-hook window")
	}
	combiner.mu.RLock()
	stopped := combiner.stopped
	combiner.mu.RUnlock()
	if !stopped {
		t.Fatal("combiner was not stopped")
	}
	if got := col.updateCombiner(); got != nil {
		t.Fatal("close hook window recreated update combiner")
	}
	if _, _, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"score":1}`), true, nil
	}); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("Update after manager close err=%v want ErrClosed", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"score":1}`)}); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("InsertBatch after manager close err=%v want ErrClosed", err)
	}
	if _, err := col.DeleteDocument([]byte("u1")); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("DeleteDocument after manager close err=%v want ErrClosed", err)
	}
	if _, err := mgr.OpenCollection("users"); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("OpenCollection during manager close err=%v want ErrClosed", err)
	}
}

func TestCollectionManagerCloseForBackendDrainsCombinerBeforeFlush(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{[]byte(`{"score":0}`)}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	combiner := col.updateCombiner()
	if combiner == nil {
		t.Fatal("expected update combiner")
	}
	done := make(chan collectionUpdateCombineResult, 1)
	if !combiner.enqueue(collectionUpdateCombineRequest{
		collection: col,
		documentID: []byte("u1"),
		update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"score":9}`), true, nil
		},
		done: done,
	}) {
		t.Fatal("enqueue update")
	}
	if err := mgr.closeForBackend(); err != nil {
		t.Fatalf("closeForBackend: %v", err)
	}
	result := <-done
	if result.err != nil || !result.matched || !result.modified {
		t.Fatalf("combined result=%+v want matched modified nil err", result)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Contains(got, []byte(`"score":9`)) {
		t.Fatalf("u1 document=%s want score 9", got)
	}
}

func TestCollectionUpdateBatchAllowsUniqueHandoff(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"email":"a@example.com"}`), []byte(`{"email":"b@example.com"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	results, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONEmail("c@example.com")},
		{DocumentID: []byte("u2"), Update: setJSONEmail("a@example.com")},
	})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 2 || !results[0].Modified || !results[1].Modified {
		t.Fatalf("results=%+v want both modified", results)
	}
	got, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if !bytes.Contains(got, []byte(`"email":"a@example.com"`)) {
		t.Fatalf("u2 document=%s want handed-off email", got)
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexesDeclinesFreshUniqueCatalog(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	stale, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open stale collection: %v", err)
	}
	if _, err := stale.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"email":"a@example.com"}`), []byte(`{"email":"b@example.com"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	fresh, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open fresh collection: %v", err)
	}
	if _, err := fresh.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	results, batched, err := stale.UpdateBatchIfNoSecondaryUniqueIndexes([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONEmail("c@example.com")},
		{DocumentID: []byte("u2"), Update: setJSONEmail("a@example.com")},
	})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexes: %v", err)
	}
	if batched {
		t.Fatalf("batched=%v results=%+v want declined", batched, results)
	}
	if len(results) != 2 || results[0].Matched || results[0].Modified || results[1].Matched || results[1].Modified {
		t.Fatalf("declined results=%+v want two zero-valued results", results)
	}
	got, err := stale.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Contains(got, []byte(`"email":"a@example.com"`)) {
		t.Fatalf("u1 document=%s want unchanged email", got)
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesBatchesNonUniqueUpdates(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"a@example.com","city":"hnl"}`),
			[]byte(`{"email":"b@example.com","city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := d.State()

	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sea")},
		{DocumentID: []byte("u2"), Update: setJSONCity("sfo")},
	})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatalf("batched=%v results=%+v want batched", batched, results)
	}
	if len(results) != 2 || !results[0].Matched || !results[0].Modified || !results[1].Matched || !results[1].Modified {
		t.Fatalf("results=%+v want two modified rows", results)
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("buffered batch advanced commit seq by %d, want 0", after.CommitSeq-before.CommitSeq)
	}
	seaIDs, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city: %v", err)
	}
	if len(seaIDs) != 1 || !bytes.Equal(seaIDs[0], []byte("u1")) {
		t.Fatalf("sea ids=%q want [u1]", seaIDs)
	}
	hnlIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find hnl city: %v", err)
	}
	if len(hnlIDs) != 0 {
		t.Fatalf("hnl ids=%q want none after buffered city update", hnlIDs)
	}
	emailIDs, err := col.FindByIndex("email", "a@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u1")) {
		t.Fatalf("email ids=%q want [u1]", emailIDs)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered update batch: %v", err)
	}
	flushed := d.State()
	if flushed.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("flush advanced commit seq by %d, want 1", flushed.CommitSeq-before.CommitSeq)
	}
	seaIDs, err = col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city after flush: %v", err)
	}
	if len(seaIDs) != 1 || !bytes.Equal(seaIDs[0], []byte("u1")) {
		t.Fatalf("flushed sea ids=%q want [u1]", seaIDs)
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesAppendsToBufferedUpdates(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"a@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := d.State()

	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sea")},
	}); err != nil {
		t.Fatalf("first UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("first batch was declined")
	}
	afterFirst := d.State()
	if afterFirst.CommitSeq != before.CommitSeq {
		t.Fatalf("first buffered batch advanced commit seq by %d, want 0", afterFirst.CommitSeq-before.CommitSeq)
	}
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sfo")},
	}); err != nil {
		t.Fatalf("second UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("second batch was declined")
	}
	afterSecond := d.State()
	if afterSecond.CommitSeq != before.CommitSeq {
		t.Fatalf("second buffered batch advanced commit seq by %d, want 0", afterSecond.CommitSeq-before.CommitSeq)
	}
	seaIDs, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city: %v", err)
	}
	if len(seaIDs) != 0 {
		t.Fatalf("sea ids=%q want none after second buffered update", seaIDs)
	}
	sfoIDs, err := col.FindByIndex("city", "sfo")
	if err != nil {
		t.Fatalf("find sfo city: %v", err)
	}
	if len(sfoIDs) != 1 || !bytes.Equal(sfoIDs[0], []byte("u1")) {
		t.Fatalf("sfo ids=%q want [u1]", sfoIDs)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered update batches: %v", err)
	}
	flushed := d.State()
	if flushed.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("flush advanced commit seq by %d, want 1", flushed.CommitSeq-before.CommitSeq)
	}
}

func TestCollectionUpdateBatchDirectBufferedBSONAccumulatesRootRuns(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatBSON,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "email", Value: "a@example.com"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := d.State()

	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setBSONField("city", "sea")},
	}); err != nil {
		t.Fatalf("first UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("first batch was declined")
	}
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setBSONField("city", "sfo")},
	}); err != nil {
		t.Fatalf("second UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("second batch was declined")
	}
	afterSecond := d.State()
	if afterSecond.CommitSeq != before.CommitSeq {
		t.Fatalf("buffered BSON updates advanced commit seq by %d, want 0", afterSecond.CommitSeq-before.CommitSeq)
	}
	col.writeDomain.mu.RLock()
	rootRunCount := col.writeDomain.rootRunCount
	primaryRuns := len(col.writeDomain.rootRuns[collectionPrimaryRootName("users")])
	cityRuns := len(col.writeDomain.rootRuns[collectionSecondaryRootName("users", "city")])
	rootMutableRuns := len(col.writeDomain.rootMutableRuns)
	col.writeDomain.mu.RUnlock()
	if rootRunCount != 2 {
		t.Fatalf("rootRunCount=%d want 2 accumulated roots after two BSON update batches", rootRunCount)
	}
	if primaryRuns != 1 || cityRuns != 1 {
		t.Fatalf("runs primary=%d city=%d, want one run per affected root", primaryRuns, cityRuns)
	}
	if rootMutableRuns != 2 {
		t.Fatalf("rootMutableRuns=%d want 2 active root-local accumulators", rootMutableRuns)
	}
	seaIDs, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city: %v", err)
	}
	if len(seaIDs) != 0 {
		t.Fatalf("sea ids=%q want none after second buffered update", seaIDs)
	}
	sfoIDs, err := col.FindByIndex("city", "sfo")
	if err != nil {
		t.Fatalf("find sfo city: %v", err)
	}
	if len(sfoIDs) != 1 || !bytes.Equal(sfoIDs[0], []byte("u1")) {
		t.Fatalf("sfo ids=%q want [u1]", sfoIDs)
	}
	emailIDs, err := col.FindByIndex("email", "a@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u1")) {
		t.Fatalf("email ids=%q want [u1]", emailIDs)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered BSON update batches: %v", err)
	}
}

func TestCollectionUpdateBatchBuffersWhenSecondaryUniqueUnchanged(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatBSON,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "email", Value: "a@example.com"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := d.State()

	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update:     setBSONField("city", "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one modified row", results)
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("buffered ordinary UpdateBatch advanced commit seq by %d, want 0", after.CommitSeq-before.CommitSeq)
	}
	col.writeDomain.mu.RLock()
	primaryRuns := len(col.writeDomain.rootRuns[collectionPrimaryRootName("users")])
	cityRuns := len(col.writeDomain.rootRuns[collectionSecondaryRootName("users", "city")])
	emailRuns := len(col.writeDomain.rootRuns[collectionSecondaryRootName("users", "email")])
	col.writeDomain.mu.RUnlock()
	if primaryRuns == 0 || cityRuns == 0 {
		t.Fatalf("primary/city runs=%d/%d want buffered primary and changed non-unique index", primaryRuns, cityRuns)
	}
	if emailRuns != 0 {
		t.Fatalf("email runs=%d want unchanged unique index not buffered", emailRuns)
	}
	ids, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("city ids=%q want [u1]", ids)
	}
}

func TestCollectionUpdateBatchPublishesWhenSecondaryUniqueChanges(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatBSON,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "email", Value: "a@example.com"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := d.State()

	callbackCalls := 0
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			callbackCalls++
			return setBSONField("email", "b@example.com")(current)
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one modified row", results)
	}
	if callbackCalls != 1 {
		t.Fatalf("callback calls=%d want 1", callbackCalls)
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("unique UpdateBatch advanced commit seq by %d, want 1", after.CommitSeq-before.CommitSeq)
	}
	col.writeDomain.mu.RLock()
	rootRunCount := col.writeDomain.rootRunCount
	col.writeDomain.mu.RUnlock()
	if rootRunCount != 0 {
		t.Fatalf("rootRunCount=%d want no buffered runs after unique-key publish", rootRunCount)
	}
	ids, err := col.FindByIndex("email", "b@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("email ids=%q want [u1]", ids)
	}
}

func TestCollectionUpdateBatchWithPendingIndexedRunsCallsCallbackOnce(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatBSON,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustBSONCollectionDocument(t, bson.D{
				{Key: "_id", Value: "u1"},
				{Key: "email", Value: "a@example.com"},
				{Key: "city", Value: "hnl"},
			}),
			mustBSONCollectionDocument(t, bson.D{
				{Key: "_id", Value: "u2"},
				{Key: "email", Value: "b@example.com"},
				{Key: "city", Value: "hnl"},
			}),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	if _, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u2"),
		Update:     setBSONField("city", "sea"),
	}}); err != nil {
		t.Fatalf("buffered setup UpdateBatch: %v", err)
	}

	callbackCalls := 0
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			callbackCalls++
			return setBSONField("email", "c@example.com")(current)
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch with pending indexed runs: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one modified row", results)
	}
	if callbackCalls != 1 {
		t.Fatalf("callback calls=%d want 1", callbackCalls)
	}
	ids, err := col.FindByIndex("email", "c@example.com")
	if err != nil {
		t.Fatalf("find updated email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("updated email ids=%q want [u1]", ids)
	}
}

func TestCollectionUpdateBSONSetReusesUnchangedIndexState(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatBSON,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "active", Field: "active", ValueType: IndexValueBool},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "email", Value: "a@example.com"},
			{Key: "city", Value: "hnl"},
			{Key: "active", Value: true},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("matched=%v modified=%v want true/true", matched, modified)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.StructuredUpdateApplications, 1; got != want {
		t.Fatalf("structured update applications=%d want %d", got, want)
	}
	if stats.IndexValueChanges != 1 || stats.IndexValueUnchanged != 2 || stats.UniqueIndexCheckSkips != 1 {
		t.Fatalf("index stats changes=%d unchanged=%d unique skips=%d want 1/2/1", stats.IndexValueChanges, stats.IndexValueUnchanged, stats.UniqueIndexCheckSkips)
	}
	col.writeDomain.mu.RLock()
	rootRunCount := col.writeDomain.rootRunCount
	primaryRuns := len(col.writeDomain.rootRuns[collectionPrimaryRootName("users")])
	cityRuns := len(col.writeDomain.rootRuns[collectionSecondaryRootName("users", "city")])
	emailRuns := len(col.writeDomain.rootRuns[collectionSecondaryRootName("users", "email")])
	activeRuns := len(col.writeDomain.rootRuns[collectionSecondaryRootName("users", "active")])
	col.writeDomain.mu.RUnlock()
	if rootRunCount != 2 || primaryRuns != 1 || cityRuns != 1 || emailRuns != 0 || activeRuns != 0 {
		t.Fatalf("runs root=%d primary=%d city=%d email=%d active=%d want 2/1/1/0/0", rootRunCount, primaryRuns, cityRuns, emailRuns, activeRuns)
	}
	ids, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("city ids=%q want [u1]", ids)
	}
	ids, err = col.FindByIndex("email", "a@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("email ids=%q want [u1]", ids)
	}
}

func TestCollectionUpdateBSONSetDirectFallbackReportsStructuredApply(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatBSON,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "email", Value: "a@example.com"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	lastEmail := "b@example.com"
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "email",
		Value: mustBSONRawValue(t, lastEmail),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("matched=%v modified=%v want true/true", matched, modified)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.StructuredUpdateApplications, 1; got != want {
		t.Fatalf("structured update applications=%d want %d", got, want)
	}
	if stats.Callback != 0 {
		t.Fatalf("callback duration=%s want zero for BSON set direct fallback", stats.Callback)
	}
	if got, want := stats.UniqueIndexChecks, 1; got != want {
		t.Fatalf("unique checks=%d want %d", got, want)
	}
	ids, err := col.FindByIndex("email", lastEmail)
	if err != nil {
		t.Fatalf("find updated email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("updated email ids=%q want [u1]", ids)
	}
}

func TestCollectionUpdateBSONSetNoIndexBuffersPrimaryOverlay(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	before := d.State()
	statsBefore := mgr.StatsSnapshot()
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("matched=%v modified=%v want true/true", matched, modified)
	}
	afterUpdate := d.State()
	if afterUpdate.CommitSeq != before.CommitSeq {
		t.Fatalf("UpdateBSONSet advanced commit seq by %d before flush, want buffered", afterUpdate.CommitSeq-before.CommitSeq)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.BufferedBatches, 1; got != want {
		t.Fatalf("buffered batches=%d want %d", got, want)
	}
	statsAfterStage := mgr.StatsSnapshot()
	if got, want := statsAfterStage.PrimaryOnlyUpdateCalls-statsBefore.PrimaryOnlyUpdateCalls, uint64(1); got != want {
		t.Fatalf("primary-only update calls delta=%d want %d", got, want)
	}
	if got, want := statsAfterStage.PrimaryOnlyMatched-statsBefore.PrimaryOnlyMatched, uint64(1); got != want {
		t.Fatalf("primary-only matched delta=%d want %d", got, want)
	}
	if got, want := statsAfterStage.PrimaryOnlyModified-statsBefore.PrimaryOnlyModified, uint64(1); got != want {
		t.Fatalf("primary-only modified delta=%d want %d", got, want)
	}
	if got, want := statsAfterStage.PrimaryOnlyRootPublishes-statsBefore.PrimaryOnlyRootPublishes, uint64(0); got != want {
		t.Fatalf("primary-only root publishes delta=%d want %d before flush", got, want)
	}
	rootCounts, rootRunCount := bufferedRootRunCountsForTest(t, col, collectionPrimaryRootName("users"))
	overlayEntries := bufferedPrimaryOverlayCountForTest(t, col)
	if rootRunCount != 0 || rootCounts[collectionPrimaryRootName("users")] != 0 || overlayEntries != 1 {
		t.Fatalf("root runs=%d primary=%d overlay=%d want 0/0/1", rootRunCount, rootCounts[collectionPrimaryRootName("users")], overlayEntries)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered update: %v", err)
	}
	afterFlush := d.State()
	if afterFlush.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("flush advanced commit seq to %d from %d, want one publish", afterFlush.CommitSeq, before.CommitSeq)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after flush: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("city=%q want sea", got)
	}
}

func TestCollectionUpdateBSONSetNoIndexIgnoresIndexedThresholds(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:                   DocumentFormatBSON,
			BufferedIndexedWriteMaxDocuments: 1,
			BufferedIndexedWriteMaxBytes:     1,
			BufferedIndexedWriteMaxRootRuns:  1,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}

	before := d.State()
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("matched=%v modified=%v want true/true", matched, modified)
	}
	afterUpdate := d.State()
	if afterUpdate.CommitSeq != before.CommitSeq {
		t.Fatalf("UpdateBSONSet advanced commit seq by %d before flush, want buffered", afterUpdate.CommitSeq-before.CommitSeq)
	}
	rootCounts, rootRunCount := bufferedRootRunCountsForTest(t, col, collectionPrimaryRootName("users"))
	overlayEntries := bufferedPrimaryOverlayCountForTest(t, col)
	if rootRunCount != 0 || rootCounts[collectionPrimaryRootName("users")] != 0 || overlayEntries != 1 {
		t.Fatalf("root runs=%d primary=%d overlay=%d want 0/0/1", rootRunCount, rootCounts[collectionPrimaryRootName("users")], overlayEntries)
	}
}

func TestCollectionInsertNoIndexBSONFlushesPendingRootRunsBeforeSingleInsert(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert batch seed: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush seed insert: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("UpdateBSONSet matched=%v modified=%v want true/true", matched, modified)
	}
	if _, err := col.Insert([]byte("u2"), mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u2"},
		{Key: "city", Value: "sfo"},
	})); err != nil {
		t.Fatalf("single insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush mixed buffered writes: %v", err)
	}
	doc1, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if got := bson.Raw(doc1).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("u1 city=%q want sea", got)
	}
	doc2, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if got := bson.Raw(doc2).Lookup("city").StringValue(); got != "sfo" {
		t.Fatalf("u2 city=%q want sfo", got)
	}
}

func TestCollectionInsertBatchNoIndexBSONFlushesPendingRootRunsBeforeBatchInsert(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush seed insert: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("UpdateBSONSet matched=%v modified=%v want true/true", matched, modified)
	}
	beforeBatch := d.State()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2"), []byte("u3")},
		[][]byte{
			mustBSONCollectionDocument(t, bson.D{
				{Key: "_id", Value: "u2"},
				{Key: "city", Value: "sfo"},
			}),
			mustBSONCollectionDocument(t, bson.D{
				{Key: "_id", Value: "u3"},
				{Key: "city", Value: "nyc"},
			}),
		},
	); err != nil {
		t.Fatalf("insert batch after buffered update: %v", err)
	}
	afterBatch := d.State()
	if afterBatch.CommitSeq <= beforeBatch.CommitSeq {
		t.Fatalf("insert batch commit seq=%d before=%d want buffered update flush publish", afterBatch.CommitSeq, beforeBatch.CommitSeq)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush mixed buffered writes: %v", err)
	}
	doc1, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if got := bson.Raw(doc1).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("u1 city=%q want sea", got)
	}
	doc2, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if got := bson.Raw(doc2).Lookup("city").StringValue(); got != "sfo" {
		t.Fatalf("u2 city=%q want sfo", got)
	}
	doc3, err := col.Get([]byte("u3"))
	if err != nil {
		t.Fatalf("get u3: %v", err)
	}
	if got := bson.Raw(doc3).Lookup("city").StringValue(); got != "nyc" {
		t.Fatalf("u3 city=%q want nyc", got)
	}
}

func TestCollectionNativewireInsertBatchNoIndexBSONFlushesPendingPrimaryOverlay(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush seed: %v", err)
	}

	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("UpdateBSONSet matched=%v modified=%v want true/true", matched, modified)
	}
	rootCounts, rootRunCount := bufferedRootRunCountsForTest(t, col, collectionPrimaryRootName("users"))
	overlayEntries := bufferedPrimaryOverlayCountForTest(t, col)
	if rootRunCount != 0 || rootCounts[collectionPrimaryRootName("users")] != 0 || overlayEntries != 1 {
		t.Fatalf("root runs=%d primary=%d overlay=%d want 0/0/1 before trusted insert", rootRunCount, rootCounts[collectionPrimaryRootName("users")], overlayEntries)
	}

	beforeInsert := d.State()
	if err := col.NativewireInsertBatchNoResultIDs(
		[][]byte{[]byte("u2")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u2"},
			{Key: "city", Value: "sfo"},
		})},
		true,
	); err != nil {
		t.Fatalf("trusted nativewire insert after overlay update: %v", err)
	}
	afterInsert := d.State()
	if afterInsert.CommitSeq <= beforeInsert.CommitSeq {
		t.Fatalf("trusted insert commit seq=%d before=%d want pending overlay flushed first", afterInsert.CommitSeq, beforeInsert.CommitSeq)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush trusted insert: %v", err)
	}
	doc1, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if got := bson.Raw(doc1).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("u1 city=%q want sea", got)
	}
	doc2, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if got := bson.Raw(doc2).Lookup("city").StringValue(); got != "sfo" {
		t.Fatalf("u2 city=%q want sfo", got)
	}
}

func TestCollectionUpdateBSONSetNoIndexNoopSkipsDirectBufferedRetryLoop(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	before := d.State()
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "hnl"),
	}})
	if err != nil {
		t.Fatalf("noop UpdateBSONSet existing doc: %v", err)
	}
	if !matched || modified {
		t.Fatalf("noop existing doc matched=%v modified=%v want true/false", matched, modified)
	}
	matched, modified, err = col.UpdateBSONSet([]byte("missing"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("noop UpdateBSONSet missing doc: %v", err)
	}
	if matched || modified {
		t.Fatalf("noop missing doc matched=%v modified=%v want false/false", matched, modified)
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("noop BSON updates advanced commit seq by %d, want 0", after.CommitSeq-before.CommitSeq)
	}
}

func TestCollectionUpdateBSONSetNoIndexWALOnBuffersPrimaryOverlayBeforeAck(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}

	before := d.State()
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("matched=%v modified=%v want true/true", matched, modified)
	}
	afterUpdate := d.State()
	if afterUpdate.CommitSeq != before.CommitSeq {
		t.Fatalf("UpdateBSONSet commit seq=%d before=%d want buffered", afterUpdate.CommitSeq, before.CommitSeq)
	}
	rootCounts, rootRunCount := bufferedRootRunCountsForTest(t, col, collectionPrimaryRootName("users"))
	overlayEntries := bufferedPrimaryOverlayCountForTest(t, col)
	if rootRunCount != 0 || rootCounts[collectionPrimaryRootName("users")] != 0 || overlayEntries != 1 {
		t.Fatalf("root runs=%d primary=%d overlay=%d want 0/0/1", rootRunCount, rootCounts[collectionPrimaryRootName("users")], overlayEntries)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get updated document: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("city=%q want sea", got)
	}
}

func TestCollectionUpdateNoIndexBSONCallbackPublishesSynchronously(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert: %v", err)
	}
	before := d.State()
	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		return mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "city", Value: "sea"},
		}), true, nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("matched=%v modified=%v want true/true", matched, modified)
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("Update commit seq=%d before=%d want synchronous publish", after.CommitSeq, before.CommitSeq)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get updated document: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("city=%q want sea", got)
	}
	col.writeDomain.mu.RLock()
	rootRunCount := col.writeDomain.rootRunCount
	col.writeDomain.mu.RUnlock()
	if rootRunCount != 0 {
		t.Fatalf("callback update root runs=%d want 0", rootRunCount)
	}
}

func TestCollectionUpdateBSONSetRequiresBSONFormat(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err == nil {
		t.Fatalf("UpdateBSONSet err=nil matched=%v modified=%v", matched, modified)
	}
	if !strings.Contains(err.Error(), "requires BSON document format") {
		t.Fatalf("UpdateBSONSet err=%q want BSON format error", err)
	}
}

func TestCollectionUpdateBSONSetBatchEmptyMatchesUpdateBatchResultShape(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	results, batched, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges(nil)
	if err != nil {
		t.Fatalf("UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatal("batched=false want true")
	}
	if results != nil {
		t.Fatalf("results=%v want nil empty-batch result", results)
	}
}

func TestCollectionUpdateBSONSetBatchDuplicateIDMatchesUpdateBatchErrorShape(t *testing.T) {
	_, err := prepareBSONSetUpdateBatchItems([]BSONSetUpdateBatchItem{
		{DocumentID: []byte("u1")},
		{DocumentID: []byte("u1")},
	})
	if !errors.Is(err, ErrDuplicateDocumentID) {
		t.Fatalf("duplicate err=%v want ErrDuplicateDocumentID", err)
	}
	var itemErr *UpdateBatchItemError
	if errors.As(err, &itemErr) {
		t.Fatalf("duplicate err=%T want plain duplicate validation error", err)
	}
}

func TestBuildUpdateBatchPlanBSONSetRejectsInvalidCurrentBSON(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	baseCommitSeq := snapshotCommitSeq(snap)
	baseSystemRoot := snapshotSystemRoot(snap)
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	rootName := collectionPrimaryRootName("users")
	baseRoot := catalog.rootID(rootName)
	table := newCollectionRunTable(1)
	setCollectionRunValue(table, []byte("u1"), []byte{0x05, 0x00, 0x00, 0x00})
	table.Freeze()
	defer resetCollectionRunTable(table)

	_, rootIDs, err := d.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          table.NewIterator(nil, nil),
		StoragePolicy: backenddb.OrderedRootStorageDefault,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return col.buildRootDescriptorSystemDeltaIterator(
			baseCommitSeq,
			baseSystemRoot,
			[]string{rootName},
			map[string]uint64{rootName: baseRoot},
			rootIDs,
		)
	})
	if err != nil {
		t.Fatalf("publish invalid BSON primary root: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("published root IDs=%d want 1", len(rootIDs))
	}

	spec, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("new BSON set update: %v", err)
	}
	plan, err := col.buildUpdateBatchPlan([]updateBatchItem{
		newBSONSetUpdateBatchItem([]byte("u1"), spec),
	}, updateBatchModeNoSecondaryUniqueIndexChanges, false, nil)
	if plan != nil {
		plan.close()
	}
	if err == nil {
		t.Fatal("buildUpdateBatchPlan err=nil want invalid current BSON")
	}
	if !strings.Contains(err.Error(), "current BSON document") {
		t.Fatalf("buildUpdateBatchPlan err=%q want current BSON validation error", err)
	}
}

func TestBSONSetUpdateAppendReplacementGrowsSmallDestination(t *testing.T) {
	spec, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("new BSON set update: %v", err)
	}
	current := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "email", Value: "a@example.com"},
		{Key: "city", Value: "hnl"},
		{Key: "active", Value: true},
	})
	dst := []byte("prefix")
	dst = dst[:len(dst):len(dst)]

	out, replacement, changed, err := spec.appendReplacement(dst, current)
	if err != nil {
		t.Fatalf("appendReplacement: %v", err)
	}
	if !changed {
		t.Fatal("changed=false want true")
	}
	if len(out) <= len(dst) {
		t.Fatalf("out len=%d want appended replacement after prefix len=%d", len(out), len(dst))
	}
	if cap(out) <= cap(dst) {
		t.Fatalf("out cap=%d want growth beyond dst cap=%d", cap(out), cap(dst))
	}
	if !bytes.Equal(out[:len(dst)], dst) {
		t.Fatalf("out prefix=%q want %q", out[:len(dst)], dst)
	}
	if len(replacement) == 0 || &replacement[0] != &out[len(dst)] {
		t.Fatal("replacement does not point at appended output region")
	}
	if got := bson.Raw(replacement).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("city=%q want sea", got)
	}
	if got := bson.Raw(replacement).Lookup("_id").StringValue(); got != "u1" {
		t.Fatalf("_id=%q want u1", got)
	}
}

func TestBSONSetUpdateApplyNoopDoesNotAllocateReplacement(t *testing.T) {
	current := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "city", Value: "hnl"},
	})
	spec, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "hnl"),
	}})
	if err != nil {
		t.Fatalf("new BSON set update: %v", err)
	}
	got, changed, err := spec.apply(current)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if changed {
		t.Fatal("apply changed=true want false")
	}
	if len(got) == 0 || len(current) == 0 || &got[0] != &current[0] {
		t.Fatal("no-op BSON set did not return the original document backing")
	}
	allocs := testing.AllocsPerRun(1000, func() {
		got, changed, err := spec.apply(current)
		if err != nil {
			t.Fatalf("apply during alloc check: %v", err)
		}
		if changed || !bytes.Equal(got, current) {
			t.Fatalf("alloc check changed=%v got=%#v want original %#v", changed, got, current)
		}
	})
	if allocs != 0 {
		t.Fatalf("no-op BSON set allocations=%g want 0", allocs)
	}
}

func TestCollectionUpdateBSONSetRejectsInvalidFieldNames(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  string
		want string
	}{
		{name: "empty", key: "", want: "empty"},
		{name: "id", key: "_id", want: "_id"},
		{name: "dotted", key: "profile.city", want: "top-level"},
		{name: "dollar", key: "$city", want: "$"},
		{name: "nul", key: "city\x00name", want: "NUL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newBSONSetUpdate([]BSONSetField{{
				Key:   tc.key,
				Value: mustBSONRawValue(t, "sea"),
			}})
			if err == nil {
				t.Fatal("newBSONSetUpdate err=nil want error")
			}
			if !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "field") {
				t.Fatalf("err=%q want field context containing %q", err, tc.want)
			}
		})
	}
}

func TestCollectionUpdateBSONSetRejectsDuplicateFields(t *testing.T) {
	_, err := newBSONSetUpdate([]BSONSetField{
		{Key: "city", Value: mustBSONRawValue(t, "sea")},
		{Key: "city", Value: mustBSONRawValue(t, "sfo")},
	})
	if err == nil {
		t.Fatal("newBSONSetUpdate err=nil want duplicate field error")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("newBSONSetUpdate err=%q want duplicate field error", err)
	}
}

func TestCollectionUpdateBSONSetRejectsInvalidRawValue(t *testing.T) {
	_, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: bson.RawValue{Type: bson.TypeString, Value: []byte{0xff}},
	}})
	if err == nil {
		t.Fatal("newBSONSetUpdate err=nil want invalid value error")
	}
	if !strings.Contains(err.Error(), "invalid BSON raw value") {
		t.Fatalf("newBSONSetUpdate err=%q want invalid BSON raw value", err)
	}
}

func TestCollectionUpdateBSONSetValidatesCollectionBeforeFields(t *testing.T) {
	var col *Collection
	_, _, err := col.UpdateBSONSet(nil, []BSONSetField{
		{Key: "", Value: mustBSONRawValue(t, "sea")},
	})
	if !errors.Is(err, errCollectionNil) {
		t.Fatalf("UpdateBSONSet err=%v want %v", err, errCollectionNil)
	}
}

func TestCollectionUpdateBatchDirectBufferedBSONDoesNotReserveUnchangedUnique(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatBSON,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "email", Value: "a@example.com"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert flushed document: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush document: %v", err)
	}
	before := d.State()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u2"},
			{Key: "email", Value: "b@example.com"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert buffered document: %v", err)
	}
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setBSONField("city", "sea")},
	}); err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatal("update batch was not buffered")
	}
	afterUpdate := d.State()
	if afterUpdate.CommitSeq != before.CommitSeq {
		t.Fatalf("buffered BSON update advanced commit seq by %d, want 0", afterUpdate.CommitSeq-before.CommitSeq)
	}

	encodedA, err := encodeIndexScalar(IndexValueString, "a@example.com")
	if err != nil {
		t.Fatalf("encode email: %v", err)
	}
	_, prefixA, err := appendIndexValuePrefixSlice(nil, encodedA)
	if err != nil {
		t.Fatalf("email prefix: %v", err)
	}
	encodedB, err := encodeIndexScalar(IndexValueString, "b@example.com")
	if err != nil {
		t.Fatalf("encode buffered email: %v", err)
	}
	_, prefixB, err := appendIndexValuePrefixSlice(nil, encodedB)
	if err != nil {
		t.Fatalf("buffered email prefix: %v", err)
	}

	emailRoot := collectionSecondaryRootName("users", "email")
	col.writeDomain.mu.RLock()
	pending := col.writeDomain.uniqueValueIndex["email"]
	containsA := pending != nil && pending.contains(prefixA)
	containsB := pending != nil && pending.contains(prefixB)
	uniqueRuns := len(col.writeDomain.uniqueValueRuns["email"])
	emailRuns := len(col.writeDomain.rootRuns[emailRoot])
	col.writeDomain.mu.RUnlock()
	if containsA {
		t.Fatal("direct-buffered BSON update added unchanged persisted unique email to pending unique-value index")
	}
	if !containsB {
		t.Fatal("pending buffered insert unique email missing from pending unique-value index")
	}
	if uniqueRuns != 1 {
		t.Fatalf("unique value runs=%d want only the pending insert run", uniqueRuns)
	}
	if emailRuns != 1 {
		t.Fatalf("email root runs=%d want only the pending insert secondary run", emailRuns)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u3")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u3"},
			{Key: "email", Value: "a@example.com"},
			{Key: "city", Value: "hnl"},
		})},
	); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate persisted unique email err=%v want ErrUniqueIndexConflict", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u4")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u4"},
			{Key: "email", Value: "b@example.com"},
			{Key: "city", Value: "hnl"},
		})},
	); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate pending unique email err=%v want ErrUniqueIndexConflict", err)
	}

	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	col.writeDomain.mu.RLock()
	pending = col.writeDomain.uniqueValueIndex["email"]
	containsA = pending != nil && pending.contains(prefixA)
	containsB = pending != nil && pending.contains(prefixB)
	publishingUniqueRuns := 0
	publishingEmailRuns := 0
	if len(col.writeDomain.indexedPublishingUnits) > 0 {
		publishingUniqueRuns = len(col.writeDomain.indexedPublishingUnits[0].uniqueValueRuns["email"])
		publishingEmailRuns = len(col.writeDomain.indexedPublishingUnits[0].rootRuns[emailRoot])
	}
	col.writeDomain.mu.RUnlock()
	if containsA {
		t.Fatal("rotated direct-buffered BSON update added unchanged persisted unique email to pending unique-value index")
	}
	if !containsB {
		t.Fatal("rotated pending insert unique email missing from pending unique-value index")
	}
	if publishingUniqueRuns != 1 {
		t.Fatalf("publishing unique value runs=%d want only the pending insert run", publishingUniqueRuns)
	}
	if publishingEmailRuns != 1 {
		t.Fatalf("publishing email root runs=%d want only the pending insert secondary run", publishingEmailRuns)
	}
	if err := col.publishPreparedIndexedFlush(work); err != nil {
		t.Fatalf("publish prepared async flush: %v", err)
	}
}

func TestCollectionUpdateBatchDirectBufferedTemplateV1AccumulatesRootRuns(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatTemplateV1,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustTemplateV1Document(t, []string{"email", "city"}, []any{"a@example.com", "hnl"})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := d.State()

	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setTemplateV1JSON(t, `{"email":"a@example.com","city":"sea","score":1}`)},
	}); err != nil {
		t.Fatalf("first UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("first batch was declined")
	}
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setTemplateV1JSON(t, `{"email":"a@example.com","city":"sfo","score":2}`)},
	}); err != nil {
		t.Fatalf("second UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("second batch was declined")
	}
	afterSecond := d.State()
	if afterSecond.CommitSeq != before.CommitSeq {
		t.Fatalf("buffered template-v1 updates advanced commit seq by %d, want 0", afterSecond.CommitSeq-before.CommitSeq)
	}

	col.writeDomain.mu.RLock()
	rootRunCount := col.writeDomain.rootRunCount
	templateRuns := len(col.writeDomain.rootRuns[collectionTemplateRootName("users")])
	primaryRuns := len(col.writeDomain.rootRuns[collectionPrimaryRootName("users")])
	cityRuns := len(col.writeDomain.rootRuns[collectionSecondaryRootName("users", "city")])
	rootMutableRuns := len(col.writeDomain.rootMutableRuns)
	col.writeDomain.mu.RUnlock()
	if rootRunCount != 3 {
		t.Fatalf("rootRunCount=%d want 3 accumulated roots after two template-v1 update batches", rootRunCount)
	}
	if templateRuns != 1 || primaryRuns != 1 || cityRuns != 1 {
		t.Fatalf("runs template=%d primary=%d city=%d, want one run per affected root", templateRuns, primaryRuns, cityRuns)
	}
	if rootMutableRuns != 3 {
		t.Fatalf("rootMutableRuns=%d want 3 active root-local accumulators", rootMutableRuns)
	}

	seaIDs, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city: %v", err)
	}
	if len(seaIDs) != 0 {
		t.Fatalf("sea ids=%q want none after second buffered update", seaIDs)
	}
	sfoIDs, err := col.FindByIndex("city", "sfo")
	if err != nil {
		t.Fatalf("find sfo city: %v", err)
	}
	if len(sfoIDs) != 1 || !bytes.Equal(sfoIDs[0], []byte("u1")) {
		t.Fatalf("sfo ids=%q want [u1]", sfoIDs)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get template-v1 buffered document: %v", err)
	}
	gotJSON, err := col.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("materialize template-v1 buffered document: %v", err)
	}
	for _, want := range [][]byte{[]byte(`"city":"sfo"`), []byte(`"score":2`)} {
		if !bytes.Contains(gotJSON, want) {
			t.Fatalf("buffered document=%s missing %s", gotJSON, want)
		}
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered template-v1 update batches: %v", err)
	}
	flushed := d.State()
	if flushed.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("flush advanced commit seq by %d, want 1", flushed.CommitSeq-before.CommitSeq)
	}
}

func TestCollectionUpdateBatchDirectBufferedTemplateV1RejectsStaleTemplatePlan(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatTemplateV1,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustTemplateV1Document(t, []string{"email", "city"}, []any{"a@example.com", "hnl"}),
			mustTemplateV1Document(t, []string{"email", "city"}, []any{"b@example.com", "hnl"}),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	itemsA, err := prepareUpdateBatchItems([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setTemplateV1JSON(t, `{"email":"a@example.com","city":"hnl","shape_a":1}`)},
	})
	if err != nil {
		t.Fatalf("prepare stale plan A items: %v", err)
	}
	itemsB, err := prepareUpdateBatchItems([]UpdateBatchItem{
		{DocumentID: []byte("u2"), Update: setTemplateV1JSON(t, `{"email":"b@example.com","city":"hnl","shape_b":1}`)},
	})
	if err != nil {
		t.Fatalf("prepare stale plan B items: %v", err)
	}
	planA, err := col.buildUpdateBatchPlan(itemsA, updateBatchModeNoSecondaryUniqueIndexChanges, true, nil)
	if err != nil {
		t.Fatalf("build plan A: %v", err)
	}
	defer planA.close()
	planB, err := col.buildUpdateBatchPlan(itemsB, updateBatchModeNoSecondaryUniqueIndexChanges, true, nil)
	if err != nil {
		t.Fatalf("build plan B: %v", err)
	}
	defer planB.close()
	if planA.directBufferedUpdate == nil || len(planA.directBufferedUpdate.templateEntries) == 0 {
		t.Fatal("plan A did not create direct buffered template entries")
	}
	if planB.directBufferedUpdate == nil || len(planB.directBufferedUpdate.templateEntries) == 0 {
		t.Fatal("plan B did not create direct buffered template entries")
	}

	if err := col.withMutationLock(func() error {
		buffered, err := col.bufferUpdateBatchPlanLocked(planA)
		if err != nil {
			return err
		}
		if !buffered {
			return errors.New("plan A was not buffered")
		}
		return nil
	}); err != nil {
		t.Fatalf("stage plan A: %v", err)
	}
	if err := col.withMutationLock(func() error {
		buffered, err := col.bufferUpdateBatchPlanLocked(planB)
		if !errors.Is(err, ErrConcurrentMutation) {
			return fmt.Errorf("stale plan B buffered=%t err=%v, want ErrConcurrentMutation", buffered, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("stage stale plan B: %v", err)
	}

	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u2"), Update: setTemplateV1JSON(t, `{"email":"b@example.com","city":"hnl","shape_b":1}`)},
	}); err != nil {
		t.Fatalf("replanned public update B: %v", err)
	} else if !batched {
		t.Fatal("replanned public update B was declined")
	}
	for _, tc := range []struct {
		id   []byte
		want []byte
	}{
		{[]byte("u1"), []byte(`"shape_a":1`)},
		{[]byte("u2"), []byte(`"shape_b":1`)},
	} {
		got, err := col.Get(tc.id)
		if err != nil {
			t.Fatalf("get %s: %v", tc.id, err)
		}
		gotJSON, err := col.StoredDocumentJSON(got)
		if err != nil {
			t.Fatalf("materialize %s: %v", tc.id, err)
		}
		if !bytes.Contains(gotJSON, tc.want) {
			t.Fatalf("document %s=%s missing %s", tc.id, gotJSON, tc.want)
		}
	}
}

func TestCollectionUpdateBatchDirectBufferedTemplateV1TemplateOnlySkipsSecondaryRoots(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatTemplateV1,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustTemplateV1Document(t, []string{"email", "city"}, []any{"a@example.com", "hnl"})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := d.State()

	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setTemplateV1JSON(t, `{"email":"a@example.com","city":"hnl","score":1}`)},
	}); err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("batch was declined")
	}
	afterUpdate := d.State()
	if afterUpdate.CommitSeq != before.CommitSeq {
		t.Fatalf("buffered template-v1 update advanced commit seq by %d, want 0", afterUpdate.CommitSeq-before.CommitSeq)
	}

	col.writeDomain.mu.RLock()
	rootRunCount := col.writeDomain.rootRunCount
	templateRuns := len(col.writeDomain.rootRuns[collectionTemplateRootName("users")])
	primaryRuns := len(col.writeDomain.rootRuns[collectionPrimaryRootName("users")])
	emailRuns := len(col.writeDomain.rootRuns[collectionSecondaryRootName("users", "email")])
	cityRuns := len(col.writeDomain.rootRuns[collectionSecondaryRootName("users", "city")])
	rootMutableRuns := len(col.writeDomain.rootMutableRuns)
	uniqueRuns := len(col.writeDomain.uniqueValueRuns["email"])
	col.writeDomain.mu.RUnlock()
	if rootRunCount != 2 {
		t.Fatalf("rootRunCount=%d want only primary and template roots", rootRunCount)
	}
	if templateRuns != 1 || primaryRuns != 1 {
		t.Fatalf("runs template=%d primary=%d, want one run for each", templateRuns, primaryRuns)
	}
	if emailRuns != 0 || cityRuns != 0 {
		t.Fatalf("secondary runs email=%d city=%d, want none for unchanged indexed values", emailRuns, cityRuns)
	}
	if rootMutableRuns != 2 {
		t.Fatalf("rootMutableRuns=%d want two active root-local accumulators", rootMutableRuns)
	}
	if uniqueRuns != 0 {
		t.Fatalf("unique value runs=%d want none for unchanged unique value", uniqueRuns)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get template-v1 buffered document: %v", err)
	}
	gotJSON, err := col.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("materialize template-v1 buffered document: %v", err)
	}
	if !bytes.Contains(gotJSON, []byte(`"score":1`)) {
		t.Fatalf("buffered document=%s missing score update", gotJSON)
	}
	emailIDs, err := col.FindByIndex("email", "a@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u1")) {
		t.Fatalf("email ids=%q want [u1]", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(cityIDs) != 1 || !bytes.Equal(cityIDs[0], []byte("u1")) {
		t.Fatalf("city ids=%q want [u1]", cityIDs)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered template-v1 update: %v", err)
	}
	flushed := d.State()
	if flushed.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("flush advanced commit seq by %d, want 1", flushed.CommitSeq-before.CommitSeq)
	}
}

func TestCollectionUpdateBatchDirectBufferedTemplateV1DoesNotReserveUnchangedUnique(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatTemplateV1,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustTemplateV1Document(t, []string{"email", "city"}, []any{"a@example.com", "hnl"})},
	); err != nil {
		t.Fatalf("insert flushed document: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush document: %v", err)
	}
	before := d.State()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{mustTemplateV1Document(t, []string{"email", "city"}, []any{"b@example.com", "hnl"})},
	); err != nil {
		t.Fatalf("insert buffered document: %v", err)
	}
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setTemplateV1JSON(t, `{"email":"a@example.com","city":"sea","score":1}`)},
	}); err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatal("update batch was not buffered")
	}
	afterUpdate := d.State()
	if afterUpdate.CommitSeq != before.CommitSeq {
		t.Fatalf("buffered template-v1 update advanced commit seq by %d, want 0", afterUpdate.CommitSeq-before.CommitSeq)
	}

	encodedA, err := encodeIndexScalar(IndexValueString, "a@example.com")
	if err != nil {
		t.Fatalf("encode email: %v", err)
	}
	_, prefixA, err := appendIndexValuePrefixSlice(nil, encodedA)
	if err != nil {
		t.Fatalf("email prefix: %v", err)
	}
	encodedB, err := encodeIndexScalar(IndexValueString, "b@example.com")
	if err != nil {
		t.Fatalf("encode buffered email: %v", err)
	}
	_, prefixB, err := appendIndexValuePrefixSlice(nil, encodedB)
	if err != nil {
		t.Fatalf("buffered email prefix: %v", err)
	}

	emailRoot := collectionSecondaryRootName("users", "email")
	col.writeDomain.mu.RLock()
	pending := col.writeDomain.uniqueValueIndex["email"]
	containsA := pending != nil && pending.contains(prefixA)
	containsB := pending != nil && pending.contains(prefixB)
	uniqueRuns := len(col.writeDomain.uniqueValueRuns["email"])
	emailRuns := len(col.writeDomain.rootRuns[emailRoot])
	col.writeDomain.mu.RUnlock()
	if containsA {
		t.Fatal("direct-buffered template-v1 update added unchanged persisted unique email to pending unique-value index")
	}
	if !containsB {
		t.Fatal("pending buffered insert unique email missing from pending unique-value index")
	}
	if uniqueRuns != 1 {
		t.Fatalf("unique value runs=%d want only the pending insert run", uniqueRuns)
	}
	if emailRuns != 1 {
		t.Fatalf("email root runs=%d want only the pending insert secondary run", emailRuns)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u3")},
		[][]byte{mustTemplateV1Document(t, []string{"email", "city"}, []any{"a@example.com", "hnl"})},
	); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate persisted unique email err=%v want ErrUniqueIndexConflict", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u4")},
		[][]byte{mustTemplateV1Document(t, []string{"email", "city"}, []any{"b@example.com", "hnl"})},
	); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate pending unique email err=%v want ErrUniqueIndexConflict", err)
	}

	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	col.writeDomain.mu.RLock()
	pending = col.writeDomain.uniqueValueIndex["email"]
	containsA = pending != nil && pending.contains(prefixA)
	containsB = pending != nil && pending.contains(prefixB)
	publishingUniqueRuns := 0
	publishingEmailRuns := 0
	if len(col.writeDomain.indexedPublishingUnits) > 0 {
		publishingUniqueRuns = len(col.writeDomain.indexedPublishingUnits[0].uniqueValueRuns["email"])
		publishingEmailRuns = len(col.writeDomain.indexedPublishingUnits[0].rootRuns[emailRoot])
	}
	col.writeDomain.mu.RUnlock()
	if containsA {
		t.Fatal("rotated direct-buffered template-v1 update added unchanged persisted unique email to pending unique-value index")
	}
	if !containsB {
		t.Fatal("rotated pending insert unique email missing from pending unique-value index")
	}
	if publishingUniqueRuns != 1 {
		t.Fatalf("publishing unique value runs=%d want only the pending insert run", publishingUniqueRuns)
	}
	if publishingEmailRuns != 1 {
		t.Fatalf("publishing email root runs=%d want only the pending insert secondary run", publishingEmailRuns)
	}
	if err := col.publishPreparedIndexedFlush(work); err != nil {
		t.Fatalf("publish prepared async flush: %v", err)
	}
}

func TestDirectBufferedRootEntriesOwnKeysAndRetainDocumentArena(t *testing.T) {
	scratch := getUpdateBatchPlanScratch(1, 0)
	documentID := []byte("u1")
	document := appendUpdateBatchPlanScratchDocument(scratch, []byte(`{"city":"hnl"}`))
	primaryEntries := buildDirectBufferedPrimaryRootEntries([]preparedBatchUpdate{{
		documentID: documentID,
		document:   document,
	}}, scratch)
	if len(primaryEntries) != 1 {
		t.Fatalf("primary entries=%d want 1", len(primaryEntries))
	}
	documentID[0] = 'x'
	if !bytes.Equal(primaryEntries[0].key, []byte("u1")) {
		t.Fatalf("primary entry key=%q, want owned original key", primaryEntries[0].key)
	}
	if &primaryEntries[0].value[0] != &document[0] {
		t.Fatalf("primary entry value was cloned, want borrowed document arena")
	}

	table := newFreezeSortRunTable()
	if err := applyDirectBufferedRootEntries(table, primaryEntries); err != nil {
		t.Fatalf("apply direct primary entries: %v", err)
	}
	plan := newUpdateBatchPlan()
	plan.scratch = scratch
	plan.directBufferedUpdate = &directBufferedUpdatePlan{primaryEntries: primaryEntries}
	var domain collectionWriteDomain
	retainDirectBufferedDocumentArenaLocked(&domain, plan)
	if got := len(domain.rootValueArenas); got != 2 {
		t.Fatalf("retained key/document arenas=%d want 2", got)
	}
	plan.close()
	reused := getUpdateBatchPlanScratch(1, 0)
	_ = appendUpdateBatchPlanScratchDocument(reused, []byte(`{"city":"koa"}`))
	_ = appendUpdateBatchPlanScratchKey(reused, []byte("u9"))
	putUpdateBatchPlanScratch(reused)
	got, deleted, ok := table.Get([]byte("u1"))
	if !ok || deleted || !bytes.Equal(got, []byte(`{"city":"hnl"}`)) {
		t.Fatalf("staged primary value=%q ok=%v deleted=%v, want retained original", got, ok, deleted)
	}

	templateRecords := []templateV1Record{{
		hash: [32]byte{1, 2, 3},
		id:   7,
		raw:  []byte("template-record"),
	}}
	templateEntries := buildDirectBufferedTemplateRootEntries(templateRecords)
	if len(templateEntries) != 3 {
		t.Fatalf("template entries=%d want 3", len(templateEntries))
	}
	templateRecords[0].hash[0] = 9
	templateRecords[0].raw[0] = 'X'
	if templateEntries[1].key[1] != 1 || !bytes.Equal(templateEntries[2].value, []byte("template-record")) {
		t.Fatalf("template hash key[1]=%d record value=%q, want owned original bytes", templateEntries[1].key[1], templateEntries[2].value)
	}
}

func TestAppendUpdateBatchPlanScratchDocumentPreservesEmptyRetainedPayloadM13C(t *testing.T) {
	scratch := getUpdateBatchPlanScratch(1, 0)
	defer putUpdateBatchPlanScratch(scratch)

	document := appendUpdateBatchPlanScratchDocument(scratch, nil)
	if document == nil {
		t.Fatal("empty retained payload became nil tombstone")
	}
	if len(document) != 0 {
		t.Fatalf("empty retained payload len=%d want 0", len(document))
	}

	entries := buildDirectBufferedPrimaryRootEntries([]preparedBatchUpdate{{
		documentID: []byte("u1"),
		document:   document,
	}}, scratch)
	if len(entries) != 1 {
		t.Fatalf("primary entries=%d want 1", len(entries))
	}
	if entries[0].value == nil || len(entries[0].value) != 0 {
		t.Fatalf("primary value=%v len=%d want non-nil zero-length value", entries[0].value, len(entries[0].value))
	}
}

type bufferedUsersUpdateDoc struct {
	id    string
	email string
	city  string
}

type bufferedUsersUpdateJSONDoc struct {
	Email string `json:"email"`
	City  string `json:"city"`
}

const (
	// These limits are intentionally above this fixture's data size so tests can
	// exercise explicit Flush or forced-drain paths without a threshold flush.
	bufferedIndexedUpdateNoThresholdDocumentLimitForTests = 1 << 20
	bufferedIndexedUpdateNoThresholdByteLimitForTests     = int64(1) << 40
	bufferedIndexedUpdateNoThresholdRootRunLimitForTests  = 1 << 20
	bufferedIndexedUpdateNoThresholdQueueLimitForTests    = 1 << 20
)

type bufferedUsersUpdateFixture struct {
	db         *backenddb.DB
	manager    *CollectionManager
	collection *Collection
}

func bufferedIndexedUpdateHighThresholdOptionsForTests() CollectionOptions {
	return bufferedIndexedUpdateHighThresholdOptionsForTestsWithAsync(true)
}

func bufferedIndexedUpdateNoAsyncHighThresholdOptionsForTests() CollectionOptions {
	return bufferedIndexedUpdateHighThresholdOptionsForTestsWithAsync(false)
}

func bufferedIndexedUpdateHighThresholdOptionsForTestsWithAsync(async bool) CollectionOptions {
	queuedUnits := 0
	if async {
		queuedUnits = bufferedIndexedUpdateNoThresholdQueueLimitForTests
	}
	return CollectionOptions{
		BufferedIndexedWrites:                   true,
		BufferedIndexedWriteMaxDocuments:        bufferedIndexedUpdateNoThresholdDocumentLimitForTests,
		BufferedIndexedWriteMaxBytes:            bufferedIndexedUpdateNoThresholdByteLimitForTests,
		BufferedIndexedWriteMaxRootRuns:         bufferedIndexedUpdateNoThresholdRootRunLimitForTests,
		BufferedIndexedAsyncFlush:               async,
		DisableBufferedIndexedAsyncFlush:        !async,
		BufferedIndexedAsyncFlushMaxQueuedUnits: queuedUnits,
	}
}

func newBufferedUsersUpdateFixtureWithDocs(t *testing.T, opts CollectionOptions, docs []bufferedUsersUpdateDoc) bufferedUsersUpdateFixture {
	return newBufferedUsersUpdateFixtureWithDocsCleanup(t, opts, docs, false)
}

func newBufferedUsersUpdateFixtureWithDocsCleanup(t *testing.T, opts CollectionOptions, docs []bufferedUsersUpdateDoc, allowConcurrentMutationCleanup bool) bufferedUsersUpdateFixture {
	t.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	var mgr *CollectionManager
	t.Cleanup(func() {
		if mgr != nil {
			if err := mgr.FlushAll(); err != nil && !errors.Is(err, backenddb.ErrClosed) && !(allowConcurrentMutationCleanup && errors.Is(err, ErrConcurrentMutation)) {
				t.Errorf("flush buffered user fixture: %v", err)
			}
		}
		if err := d.Close(); err != nil && !errors.Is(err, backenddb.ErrClosed) && !(allowConcurrentMutationCleanup && errors.Is(err, ErrConcurrentMutation)) {
			t.Errorf("close buffered user fixture DB: %v", err)
		}
	})

	mgr = NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: opts,
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if len(docs) > 0 {
		ids := make([][]byte, len(docs))
		values := make([][]byte, len(docs))
		for i, doc := range docs {
			ids[i] = []byte(doc.id)
			values[i], err = json.Marshal(bufferedUsersUpdateJSONDoc{
				Email: doc.email,
				City:  doc.city,
			})
			if err != nil {
				t.Fatalf("marshal user fixture doc: %v", err)
			}
		}
		if _, err := col.InsertBatch(ids, values); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	return bufferedUsersUpdateFixture{
		db:         d,
		manager:    mgr,
		collection: col,
	}
}
func newBufferedUsersUpdateCollection(t *testing.T) (*backenddb.DB, *Collection) {
	t.Helper()
	fixture := newBufferedUsersUpdateFixtureWithDocs(t, CollectionOptions{}, []bufferedUsersUpdateDoc{
		{id: "u1", email: "a@example.com", city: "hnl"},
	})
	return fixture.db, fixture.collection
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesRejectsStaleBufferedPlan(t *testing.T) {
	_, col := newBufferedUsersUpdateCollection(t)
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sea")},
	}); err != nil {
		t.Fatalf("first UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("first batch was declined")
	}

	items, err := prepareUpdateBatchItems([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sfo")},
	})
	if err != nil {
		t.Fatalf("prepare stale plan items: %v", err)
	}
	plan, err := col.buildUpdateBatchPlan(items, updateBatchModeNoSecondaryUniqueIndexChanges, true, nil)
	if err != nil {
		t.Fatalf("build stale plan: %v", err)
	}
	defer plan.close()
	if !plan.bufferedBase || plan.bufferedReadGeneration == 0 {
		t.Fatalf("plan bufferedBase=%v generation=%d want buffered read", plan.bufferedBase, plan.bufferedReadGeneration)
	}
	if !col.bufferedUpdateBatchPlanStillCurrent(plan) {
		t.Fatalf("fresh buffered plan was unexpectedly stale")
	}

	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("oak")},
	}); err != nil {
		t.Fatalf("second UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("second batch was declined")
	}

	err = col.withMutationLock(func() error {
		buffered, err := col.bufferUpdateBatchPlanLocked(plan)
		if buffered {
			t.Fatalf("stale plan buffered successfully")
		}
		return err
	})
	if !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("buffer stale plan err=%v want ErrConcurrentMutation", err)
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesRejectsStalePrimaryOnlyDirectPlan(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "city", Value: "hnl"},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	spec, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sfo"),
	}})
	if err != nil {
		t.Fatalf("prepare stale primary-only bson set spec: %v", err)
	}
	plan, err := col.buildUpdateBatchPlan([]updateBatchItem{
		newBSONSetUpdateBatchItem([]byte("u1"), spec),
	}, updateBatchModeNoSecondaryUniqueIndexChanges, true, nil)
	if err != nil {
		t.Fatalf("build stale primary-only plan: %v", err)
	}
	defer plan.close()
	if plan.bufferedBase {
		t.Fatalf("plan bufferedBase=%v want false when domain was empty", plan.bufferedBase)
	}
	if !plan.canBufferDirectUpdateBatch || plan.directBufferedUpdate == nil || len(plan.directBufferedUpdate.primaryEntries) == 0 {
		t.Fatalf("plan direct buffered path unavailable: canBuffer=%v directPlan=%v primaryEntries=%d", plan.canBufferDirectUpdateBatch, plan.directBufferedUpdate != nil, len(plan.directBufferedUpdate.primaryEntries))
	}

	if _, _, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}}); err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	col.writeDomain.mu.RLock()
	bufferedCount := col.writeDomain.count
	col.writeDomain.mu.RUnlock()
	if bufferedCount == 0 {
		t.Fatalf("expected buffered no-index update to stage pending state")
	}

	err = col.withMutationLock(func() error {
		buffered, err := col.bufferUpdateBatchPlanLocked(plan)
		if buffered {
			t.Fatalf("stale primary-only plan buffered successfully")
		}
		return err
	})
	if !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("buffer stale primary-only plan err=%v want ErrConcurrentMutation", err)
	}
}

func TestCollectionUpdateBSONSetBatchReadsNoIndexBufferedInsertWithoutFlush(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	before := d.State()
	ids := [][]byte{[]byte("u1"), []byte("u2")}
	docs := [][]byte{
		mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}}),
		mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "hnl"}}),
	}
	wantBuffered := len(ids)
	if wantBuffered != len(docs) {
		t.Fatalf("test fixture ids=%d docs=%d", len(ids), len(docs))
	}
	if _, err := col.InsertBatchValidatedBSON(ids, docs); err != nil {
		t.Fatalf("insert buffered BSON documents: %v", err)
	}
	col.writeDomain.mu.RLock()
	bufferedCount := col.writeDomain.count
	bufferedBytes := col.writeDomain.bufferedBytes
	mutableBytes := col.writeDomain.mutableBytes
	tableLen := 0
	if col.writeDomain.table != nil {
		tableLen = col.writeDomain.table.Len()
	}
	col.writeDomain.mu.RUnlock()
	if bufferedCount != wantBuffered || tableLen != wantBuffered {
		t.Fatalf("buffered count=%d tableLen=%d want %d/%d", bufferedCount, tableLen, wantBuffered, wantBuffered)
	}

	results, batched, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{{
		DocumentID: []byte("u1"),
		Fields: []BSONSetField{{
			Key:   "city",
			Value: mustBSONRawValue(t, "sea"),
		}},
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatalf("batched=%v results=%+v want batched", batched, results)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one modified row", results)
	}
	afterUpdate := d.State()
	if afterUpdate.CommitSeq != before.CommitSeq {
		t.Fatalf("buffered insert+update advanced commit seq by %d, want 0", afterUpdate.CommitSeq-before.CommitSeq)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get updated buffered BSON document: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("city=%q want sea", got)
	}
	col.writeDomain.mu.RLock()
	bufferedCount = col.writeDomain.count
	bufferedBytesAfterUpdate := col.writeDomain.bufferedBytes
	mutableBytesAfterUpdate := col.writeDomain.mutableBytes
	col.writeDomain.mu.RUnlock()
	if bufferedCount != wantBuffered {
		t.Fatalf("after update buffered count=%d want %d", bufferedCount, wantBuffered)
	}
	if bufferedBytesAfterUpdate != bufferedBytes {
		t.Fatalf("after same-size update bufferedBytes=%d want %d", bufferedBytesAfterUpdate, bufferedBytes)
	}
	if mutableBytesAfterUpdate != mutableBytes {
		t.Fatalf("after same-size update mutableBytes=%d want %d", mutableBytesAfterUpdate, mutableBytes)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered insert+update: %v", err)
	}
	flushed := d.State()
	if flushed.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("flush advanced commit seq by %d, want 1", flushed.CommitSeq-before.CommitSeq)
	}
	doc, err = col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get flushed BSON document: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("flushed city=%q want sea", got)
	}
}

func TestCollectionUpdateBSONSetBatchNoIndexBufferedInsertInvalidatesStalePlan(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}})},
	); err != nil {
		t.Fatalf("insert first buffered BSON document: %v", err)
	}

	col.writeDomain.mu.RLock()
	initialGeneration := col.writeDomain.writeGeneration
	var firstWriteGeneration uint64
	var firstWriteNoted bool
	if col.writeDomain.primaryWriteIndex != nil {
		firstWriteGeneration, firstWriteNoted = col.writeDomain.primaryWriteIndex.generation([]byte("u1"))
	}
	col.writeDomain.mu.RUnlock()
	if initialGeneration == 0 {
		t.Fatalf("initial buffered insert write generation=0")
	}
	if !firstWriteNoted || firstWriteGeneration != initialGeneration {
		t.Fatalf("primary write generation for u1=%d noted=%v want %d/true", firstWriteGeneration, firstWriteNoted, initialGeneration)
	}

	spec, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("prepare bson set spec: %v", err)
	}
	plan, err := col.buildUpdateBatchPlan([]updateBatchItem{
		newBSONSetUpdateBatchItem([]byte("u1"), spec),
	}, updateBatchModeNoSecondaryUniqueIndexChanges, true, nil)
	if err != nil {
		t.Fatalf("build buffered plan: %v", err)
	}
	defer plan.close()
	if !plan.bufferedBase || plan.bufferedReadGeneration != initialGeneration {
		t.Fatalf("plan bufferedBase=%v generation=%d want true/%d", plan.bufferedBase, plan.bufferedReadGeneration, initialGeneration)
	}
	if !col.bufferedUpdateBatchPlanStillCurrent(plan) {
		t.Fatalf("fresh buffered plan unexpectedly stale")
	}

	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("u2")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "hnl"}})},
	); err != nil {
		t.Fatalf("insert second buffered BSON document: %v", err)
	}

	col.writeDomain.mu.RLock()
	afterGeneration := col.writeDomain.writeGeneration
	var secondWriteGeneration uint64
	var secondWriteNoted bool
	if col.writeDomain.primaryWriteIndex != nil {
		secondWriteGeneration, secondWriteNoted = col.writeDomain.primaryWriteIndex.generation([]byte("u2"))
	}
	col.writeDomain.mu.RUnlock()
	if afterGeneration <= initialGeneration {
		t.Fatalf("second buffered insert write generation=%d want > %d", afterGeneration, initialGeneration)
	}
	if !secondWriteNoted || secondWriteGeneration != afterGeneration {
		t.Fatalf("primary write generation for u2=%d noted=%v want %d/true", secondWriteGeneration, secondWriteNoted, afterGeneration)
	}
	if col.bufferedUpdateBatchPlanStillCurrent(plan) {
		t.Fatalf("buffered update plan stayed current after a later buffered insert")
	}
}

func TestCollectionUpdateBSONSetBatchFlushesStaleNoIndexBufferedInsert(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create users collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open users collection: %v", err)
	}
	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}})},
	); err != nil {
		t.Fatalf("insert buffered BSON document: %v", err)
	}
	beforeSchemaChange := d.State()
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "audit"}); err != nil {
		t.Fatalf("create audit collection: %v", err)
	}
	afterSchemaChange := d.State()
	if afterSchemaChange.SystemRootPageID == beforeSchemaChange.SystemRootPageID {
		t.Fatalf("schema change did not advance system root: before=%d after=%d", beforeSchemaChange.SystemRootPageID, afterSchemaChange.SystemRootPageID)
	}

	results, batched, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{{
		DocumentID: []byte("u1"),
		Fields: []BSONSetField{{
			Key:   "city",
			Value: mustBSONRawValue(t, "sea"),
		}},
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatalf("batched=%v results=%+v want batched", batched, results)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one modified row from flushed buffered insert", results)
	}
	afterUpdate := d.State()
	if afterUpdate.CommitSeq == afterSchemaChange.CommitSeq {
		t.Fatalf("stale buffered insert was not flushed before update: commit seq stayed %d", afterUpdate.CommitSeq)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get updated BSON document: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("city=%q want sea", got)
	}
}

func TestCollectionUpdateBatchGenericFlushesNoIndexBufferedInsertBeforeCallback(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create users collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open users collection: %v", err)
	}
	before := d.State()
	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}})},
	); err != nil {
		t.Fatalf("insert buffered BSON document: %v", err)
	}

	var callbackCalls atomic.Int32
	setCity := setBSONField("city", "sea")
	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			callbackCalls.Add(1)
			return setCity(current)
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatalf("batched=%v results=%+v want batched", batched, results)
	}
	if got := callbackCalls.Load(); got != 1 {
		t.Fatalf("callback calls=%d want 1", got)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one modified row", results)
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq+2 {
		t.Fatalf("generic update advanced commit seq by %d, want buffered flush plus update publish", after.CommitSeq-before.CommitSeq)
	}
	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get updated BSON document: %v", err)
	}
	if got := bson.Raw(doc).Lookup("city").StringValue(); got != "sea" {
		t.Fatalf("city=%q want sea", got)
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesBuffersStaleNonOverlappingPrimaryOnlyDirectPlan(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:        t.TempDir(),
		Durability: backenddb.DurabilityWALOffRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "city", Value: "hnl"}}),
			mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "city", Value: "hnl"}}),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := d.State()

	spec, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sfo"),
	}})
	if err != nil {
		t.Fatalf("prepare stale primary-only bson set spec: %v", err)
	}
	plan, err := col.buildUpdateBatchPlan([]updateBatchItem{
		newBSONSetUpdateBatchItem([]byte("u2"), spec),
	}, updateBatchModeNoSecondaryUniqueIndexChanges, true, nil)
	if err != nil {
		t.Fatalf("build stale non-overlap plan: %v", err)
	}
	defer plan.close()
	if plan.bufferedBase {
		t.Fatalf("plan bufferedBase=%v want false when domain was empty", plan.bufferedBase)
	}
	if !plan.canBufferDirectUpdateBatch || plan.directBufferedUpdate == nil || len(plan.directBufferedUpdate.primaryEntries) == 0 {
		t.Fatalf("plan direct buffered path unavailable: canBuffer=%v directPlan=%v primaryEntries=%d", plan.canBufferDirectUpdateBatch, plan.directBufferedUpdate != nil, len(plan.directBufferedUpdate.primaryEntries))
	}

	if _, _, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}}); err != nil {
		t.Fatalf("UpdateBSONSet u1: %v", err)
	}
	err = col.withMutationLock(func() error {
		buffered, err := col.bufferUpdateBatchPlanLocked(plan)
		if err != nil {
			return err
		}
		if !buffered {
			t.Fatalf("stale non-overlap primary-only plan was not buffered")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("buffer stale non-overlap primary-only plan: %v", err)
	}
	afterStage := d.State()
	if afterStage.CommitSeq != before.CommitSeq {
		t.Fatalf("staged non-overlap updates advanced commit seq by %d before flush, want buffered", afterStage.CommitSeq-before.CommitSeq)
	}
	for _, tc := range []struct {
		id   string
		city string
	}{
		{"u1", "sea"},
		{"u2", "sfo"},
	} {
		doc, err := col.Get([]byte(tc.id))
		if err != nil {
			t.Fatalf("get %s before flush: %v", tc.id, err)
		}
		if got := bson.Raw(doc).Lookup("city").StringValue(); got != tc.city {
			t.Fatalf("%s city=%q want %q before flush", tc.id, got, tc.city)
		}
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush staged updates: %v", err)
	}
	for _, tc := range []struct {
		id   string
		city string
	}{
		{"u1", "sea"},
		{"u2", "sfo"},
	} {
		doc, err := col.Get([]byte(tc.id))
		if err != nil {
			t.Fatalf("get %s after flush: %v", tc.id, err)
		}
		if got := bson.Raw(doc).Lookup("city").StringValue(); got != tc.city {
			t.Fatalf("%s city=%q want %q after flush", tc.id, got, tc.city)
		}
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesRejectsStaleZeroDeltaPlan(t *testing.T) {
	_, col := newBufferedUsersUpdateCollection(t)
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sea")},
	}); err != nil {
		t.Fatalf("first UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("first batch was declined")
	}

	items, err := prepareUpdateBatchItems([]UpdateBatchItem{
		{
			DocumentID: []byte("u1"),
			Update: func(current []byte) ([]byte, bool, error) {
				if !bytes.Contains(current, []byte(`"city":"sea"`)) {
					return nil, false, fmt.Errorf("current document %s did not include buffered city", current)
				}
				return current, false, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("prepare stale zero-delta plan items: %v", err)
	}
	plan, err := col.buildUpdateBatchPlan(items, updateBatchModeNoSecondaryUniqueIndexChanges, true, nil)
	if err != nil {
		t.Fatalf("build stale zero-delta plan: %v", err)
	}
	defer plan.close()
	if len(plan.deltaTables) != 0 || !plan.bufferedBase || plan.bufferedReadGeneration == 0 {
		t.Fatalf("plan deltas=%d bufferedBase=%v generation=%d want zero-delta buffered read", len(plan.deltaTables), plan.bufferedBase, plan.bufferedReadGeneration)
	}
	if !col.bufferedUpdateBatchPlanStillCurrent(plan) {
		t.Fatalf("fresh zero-delta plan was unexpectedly stale")
	}

	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("oak")},
	}); err != nil {
		t.Fatalf("second UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("second batch was declined")
	}
	if col.bufferedUpdateBatchPlanStillCurrent(plan) {
		t.Fatalf("stale zero-delta plan still appeared current")
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesReplansStaleBufferedPlanWithoutFlush(t *testing.T) {
	fixture := newBufferedUsersUpdateFixtureWithDocs(t,
		bufferedIndexedUpdateNoAsyncHighThresholdOptionsForTests(),
		[]bufferedUsersUpdateDoc{
			{id: "u1", email: "a@example.com", city: "hnl"},
			{id: "u2", email: "b@example.com", city: "hnl"},
		},
	)
	mgr := fixture.manager
	col := fixture.collection
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sea")},
	}); err != nil {
		t.Fatalf("first UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("first batch was declined")
	}

	before := mgr.StatsSnapshot()
	var injected atomic.Bool
	setSFO := setJSONCity("sfo")
	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			if !injected.Swap(true) {
				if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
					{DocumentID: []byte("u2"), Update: setJSONCity("oak")},
				}); err != nil {
					return nil, false, err
				} else if !batched {
					return nil, false, errors.New("nested batch was declined")
				}
			}
			return setSFO(current)
		},
	}})
	if err != nil {
		t.Fatalf("stale-replan UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatalf("stale-replan batch was declined")
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one modified row", results)
	}
	if !injected.Load() {
		t.Fatalf("nested batch did not run")
	}
	after := mgr.StatsSnapshot()
	if got := after.IndexedFlushCalls - before.IndexedFlushCalls; got != 0 {
		t.Fatalf("indexed flush calls delta=%d want 0 for stale buffered replan", got)
	}
	if got := after.IndexedFlushForcedDrains - before.IndexedFlushForcedDrains; got != 0 {
		t.Fatalf("indexed forced drain delta=%d want 0 for stale buffered replan", got)
	}

	sfoIDs, err := col.FindByIndex("city", "sfo")
	if err != nil {
		t.Fatalf("find sfo city: %v", err)
	}
	if len(sfoIDs) != 1 || !bytes.Equal(sfoIDs[0], []byte("u1")) {
		t.Fatalf("sfo ids=%q want [u1]", sfoIDs)
	}
	oakIDs, err := col.FindByIndex("city", "oak")
	if err != nil {
		t.Fatalf("find oak city: %v", err)
	}
	if len(oakIDs) != 1 || !bytes.Equal(oakIDs[0], []byte("u2")) {
		t.Fatalf("oak ids=%q want [u2]", oakIDs)
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesBoundsStaleBufferedReplans(t *testing.T) {
	fixture := newBufferedUsersUpdateFixtureWithDocs(t,
		bufferedIndexedUpdateHighThresholdOptionsForTests(),
		[]bufferedUsersUpdateDoc{
			{id: "u1", email: "a@example.com", city: "hnl"},
			{id: "u2", email: "b@example.com", city: "hnl"},
		},
	)
	mgr := fixture.manager
	col := fixture.collection
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sea")},
	}); err != nil {
		t.Fatalf("first UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("first batch was declined")
	}

	before := mgr.StatsSnapshot()
	var injections atomic.Int32
	setSFO := setJSONCity("sfo")
	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			n := int(injections.Add(1))
			if n <= maxUpdateBatchBufferedReadReplans+1 {
				if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
					{DocumentID: []byte("u2"), Update: setJSONCity(fmt.Sprintf("oak-%02d", n))},
				}); err != nil {
					return nil, false, err
				} else if !batched {
					return nil, false, errors.New("nested batch was declined")
				}
			}
			return setSFO(current)
		},
	}})
	if err != nil {
		t.Fatalf("bounded-replan UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatalf("bounded-replan batch was declined")
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one modified row", results)
	}
	if got, want := int(injections.Load()), maxUpdateBatchBufferedReadReplans+2; got != want {
		t.Fatalf("injections=%d want %d", got, want)
	}
	after := mgr.StatsSnapshot()
	if got := after.IndexedFlushForcedDrains - before.IndexedFlushForcedDrains; got != 1 {
		t.Fatalf("indexed forced drain delta=%d want exactly one bounded replan fallback flush", got)
	}

	sfoIDs, err := col.FindByIndex("city", "sfo")
	if err != nil {
		t.Fatalf("find sfo city: %v", err)
	}
	if len(sfoIDs) != 1 || !bytes.Equal(sfoIDs[0], []byte("u1")) {
		t.Fatalf("sfo ids=%q want [u1]", sfoIDs)
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesFlushesRootMismatchInsteadOfReplanning(t *testing.T) {
	fixture := newBufferedUsersUpdateFixtureWithDocsCleanup(t,
		bufferedIndexedUpdateHighThresholdOptionsForTests(),
		[]bufferedUsersUpdateDoc{
			{id: "u1", email: "a@example.com", city: "hnl"},
			{id: "u2", email: "b@example.com", city: "hnl"},
		},
		true,
	)
	d := fixture.db
	mgr := fixture.manager
	col := fixture.collection
	otherMgr := NewCollectionManager(d)
	t.Cleanup(func() {
		if err := otherMgr.FlushAll(); err != nil && !errors.Is(err, backenddb.ErrClosed) && !errors.Is(err, ErrConcurrentMutation) {
			t.Errorf("flush second collection manager: %v", err)
		}
	})
	otherCol, err := otherMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open users from second manager: %v", err)
	}

	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sea")},
	}); err != nil {
		t.Fatalf("first UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("first batch was declined")
	}
	if _, batched, err := otherCol.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u2"), Update: setJSONCity("oak")},
	}); err != nil {
		t.Fatalf("second-manager UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("second-manager batch was declined")
	}
	if err := otherCol.Flush(); err != nil {
		t.Fatalf("flush second-manager buffered update: %v", err)
	}

	before := mgr.StatsSnapshot()
	done := make(chan error, 1)
	go func() {
		_, _, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
			{DocumentID: []byte("u1"), Update: setJSONCity("sfo")},
		})
		done <- err
	}()
	timeout := collectionTestTimeout(t, 30*time.Second)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if !errors.Is(err, ErrConcurrentMutation) {
			t.Fatalf("stale-root UpdateBatchIfNoSecondaryUniqueIndexChanges err=%v want %v", err, ErrConcurrentMutation)
		}
		if !isBufferedRootBaseMismatch(err) {
			t.Fatalf("stale-root UpdateBatchIfNoSecondaryUniqueIndexChanges err=%v want buffered root base mismatch context", err)
		}
		if !isConcurrentRootModification(err) {
			t.Fatalf("stale-root UpdateBatchIfNoSecondaryUniqueIndexChanges err=%v want root modification context", err)
		}
	case <-timer.C:
		_ = d.Close()
		unblockTimer := time.NewTimer(collectionTestTimeout(t, time.Second))
		defer unblockTimer.Stop()
		select {
		case err := <-done:
			t.Fatalf("stale-root UpdateBatchIfNoSecondaryUniqueIndexChanges timed out, likely replanning without flushing; unblocked after close with err=%v", err)
		case <-unblockTimer.C:
			t.Fatal("stale-root UpdateBatchIfNoSecondaryUniqueIndexChanges timed out and did not unblock after DB close")
		}
	}
	after := mgr.StatsSnapshot()
	if got := after.IndexedFlushForcedDrains - before.IndexedFlushForcedDrains; got != 1 {
		t.Fatalf("forced drain delta=%d want 1 for root-base mismatch", got)
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesBatchOneDoesNotFlushBeforeThreshold(t *testing.T) {
	fixture := newBufferedUsersUpdateFixtureWithDocs(t,
		bufferedIndexedUpdateNoAsyncHighThresholdOptionsForTests(),
		[]bufferedUsersUpdateDoc{
			{id: "u1", email: "a@example.com", city: "hnl"},
			{id: "u2", email: "b@example.com", city: "hnl"},
			{id: "u3", email: "c@example.com", city: "hnl"},
		},
	)
	mgr := fixture.manager
	col := fixture.collection

	before := mgr.StatsSnapshot()
	for _, update := range []struct {
		id   string
		city string
	}{
		{id: "u1", city: "sea"},
		{id: "u2", city: "sfo"},
		{id: "u3", city: "oak"},
	} {
		results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
			{DocumentID: []byte(update.id), Update: setJSONCity(update.city)},
		})
		if err != nil {
			t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges %s: %v", update.id, err)
		}
		if !batched {
			t.Fatalf("batch for %s was declined", update.id)
		}
		if len(results) != 1 || !results[0].Matched || !results[0].Modified {
			t.Fatalf("results for %s=%+v want one modified row", update.id, results)
		}
	}
	after := mgr.StatsSnapshot()
	if got := after.IndexedFlushCalls - before.IndexedFlushCalls; got != 0 {
		t.Fatalf("indexed flush calls delta=%d want 0 before explicit Flush", got)
	}
	if got := after.IndexedFlushForcedDrains - before.IndexedFlushForcedDrains; got != 0 {
		t.Fatalf("indexed forced drain delta=%d want 0 before explicit Flush", got)
	}
	if got := after.PendingDocuments; got == 0 {
		t.Fatalf("pending documents=%d want buffered batch-one updates", got)
	}

	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered updates: %v", err)
	}
	flushed := mgr.StatsSnapshot()
	if got := flushed.IndexedFlushCalls - before.IndexedFlushCalls; got == 0 {
		t.Fatalf("indexed flush calls delta=%d want explicit Flush to publish buffered updates", got)
	}
	if got := flushed.PendingDocuments; got != 0 {
		t.Fatalf("pending documents after Flush=%d want 0", got)
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesReadsBufferedAfterRawCommit(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"a@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := d.State()
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sea")},
	}); err != nil {
		t.Fatalf("first UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("first batch was declined")
	}
	if err := d.Set([]byte("raw/unrelated"), []byte("value")); err != nil {
		t.Fatalf("raw set: %v", err)
	}
	rawState := d.State()
	if rawState.SystemRootPageID != before.SystemRootPageID {
		t.Fatalf("raw write changed system root from %d to %d", before.SystemRootPageID, rawState.SystemRootPageID)
	}
	if rawState.CommitSeq == before.CommitSeq {
		t.Fatalf("raw write did not advance commit seq: before=%d after=%d", before.CommitSeq, rawState.CommitSeq)
	}
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sfo")},
	}); err != nil {
		t.Fatalf("second UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("second batch was declined")
	}
	afterSecondState := d.State()
	if afterSecondState.CommitSeq != rawState.CommitSeq {
		t.Fatalf("second buffered update advanced commit seq: raw=%d after=%d", rawState.CommitSeq, afterSecondState.CommitSeq)
	}
	if afterSecondState.SystemRootPageID != rawState.SystemRootPageID {
		t.Fatalf("second buffered update changed system root from %d to %d", rawState.SystemRootPageID, afterSecondState.SystemRootPageID)
	}
	seaIDs, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city: %v", err)
	}
	if len(seaIDs) != 0 {
		t.Fatalf("sea ids=%q want none after raw commit plus second buffered update", seaIDs)
	}
	sfoIDs, err := col.FindByIndex("city", "sfo")
	if err != nil {
		t.Fatalf("find sfo city: %v", err)
	}
	if len(sfoIDs) != 1 || !bytes.Equal(sfoIDs[0], []byte("u1")) {
		t.Fatalf("sfo ids=%q want [u1]", sfoIDs)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered updates: %v", err)
	}
	flushedState := d.State()
	if flushedState.CommitSeq != afterSecondState.CommitSeq+1 {
		t.Fatalf("flush commit seq=%d want %d", flushedState.CommitSeq, afterSecondState.CommitSeq+1)
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesFlushesUnreadableBufferedInsert(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create users collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open users collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"a@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert buffered document: %v", err)
	}
	beforeSchemaChange := d.State()
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "audit"}); err != nil {
		t.Fatalf("create audit collection: %v", err)
	}
	afterSchemaChange := d.State()
	if afterSchemaChange.SystemRootPageID == beforeSchemaChange.SystemRootPageID {
		t.Fatalf("schema change did not advance system root: before=%d after=%d", beforeSchemaChange.SystemRootPageID, afterSchemaChange.SystemRootPageID)
	}
	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sea")},
	})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatalf("batched=%v results=%+v want batched", batched, results)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one modified row from flushed buffered insert", results)
	}
	afterUpdate := d.State()
	if afterUpdate.CommitSeq == afterSchemaChange.CommitSeq {
		t.Fatalf("unreadable buffered insert was not flushed before update: commit seq stayed %d", afterUpdate.CommitSeq)
	}
	seaIDs, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city: %v", err)
	}
	if len(seaIDs) != 1 || !bytes.Equal(seaIDs[0], []byte("u1")) {
		t.Fatalf("sea ids=%q want [u1]", seaIDs)
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesReadsBufferedInsert(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	before := d.State()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"a@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert buffered document: %v", err)
	}
	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sea")},
	})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatalf("batched=%v results=%+v want batched", batched, results)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one modified row", results)
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("buffered insert+update advanced commit seq by %d, want 0", after.CommitSeq-before.CommitSeq)
	}
	hnlIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find hnl city: %v", err)
	}
	if len(hnlIDs) != 0 {
		t.Fatalf("hnl ids=%q want none after buffered insert+update", hnlIDs)
	}
	seaIDs, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea city: %v", err)
	}
	if len(seaIDs) != 1 || !bytes.Equal(seaIDs[0], []byte("u1")) {
		t.Fatalf("sea ids=%q want [u1]", seaIDs)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered insert+update: %v", err)
	}
	flushed := d.State()
	if flushed.CommitSeq != before.CommitSeq+1 {
		t.Fatalf("flush advanced commit seq by %d, want 1", flushed.CommitSeq-before.CommitSeq)
	}
}

func TestCollectionUpdateBatchBuildsPrimaryRunIndexForBufferedPlanning(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl","score":0}`)},
	); err != nil {
		t.Fatalf("insert document: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush document: %v", err)
	}

	first, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONField("score", 1)},
	})
	if err != nil {
		t.Fatalf("first UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched || len(first) != 1 || !first[0].Matched || !first[0].Modified {
		t.Fatalf("first results=%+v batched=%v want one modified row", first, batched)
	}
	if !collectionHasBufferedPrimaryRunIndexForTest(t, col) {
		t.Fatal("primary run index was not maintained while buffering first update")
	}

	sawBufferedUpdate := false
	second, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			var doc map[string]any
			if err := json.Unmarshal(current, &doc); err != nil {
				return nil, false, err
			}
			if score, ok := int64ValueForTest(doc["score"]); !ok || score != 1 {
				return nil, false, fmt.Errorf("score=%v want buffered score 1 in %s", doc["score"], current)
			}
			sawBufferedUpdate = true
			doc["score"] = 2
			next, err := json.Marshal(doc)
			if err != nil {
				return nil, false, err
			}
			return next, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("second UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched || len(second) != 1 || !second[0].Matched || !second[0].Modified {
		t.Fatalf("second results=%+v batched=%v want one modified row", second, batched)
	}
	if !sawBufferedUpdate {
		t.Fatal("second update did not read the buffered first update")
	}
	if !collectionHasBufferedPrimaryRunIndexForTest(t, col) {
		t.Fatal("primary run index was not built for buffered update planning")
	}
}

func TestSnapshotUpdateBatchBufferedReadCachesEmptyPrimaryRunIndex(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}
	domain := &collectionWriteDomain{
		loaded:         true,
		meta:           meta,
		catalog:        &collectionCatalog{meta: meta},
		baseSystemRoot: 7,
		count:          1,
		rootRuns: map[string][]memtable.Table{
			collectionPrimaryRootName("users"): {newCollectionRunTable(0)},
		},
	}

	read, _, blocked, staleSnapshot, err := snapshotUpdateBatchBufferedRead(nil, domain, meta, 1, 7, []updateBatchItem{{
		UpdateBatchItem: UpdateBatchItem{DocumentID: []byte("missing")},
	}}, DocumentFormatJSON)
	if err != nil {
		t.Fatalf("snapshotUpdateBatchBufferedRead: %v", err)
	}
	defer putUpdateBatchBufferedEntries(read.primaryEntries, read.primaryBuffer)
	if blocked {
		t.Fatal("buffered read reported blocked")
	}
	if staleSnapshot {
		t.Fatal("buffered read reported stale snapshot")
	}
	if !read.enabled {
		t.Fatal("buffered read was not enabled")
	}
	domain.mu.RLock()
	built := domain.primaryRunIndex != nil
	domain.mu.RUnlock()
	if !built {
		t.Fatal("empty primary run index was not cached")
	}
}

func TestSnapshotUpdateBatchBufferedReadDetectsStalePublishedDomain(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}
	domain := &collectionWriteDomain{
		loaded:         true,
		meta:           meta,
		catalog:        &collectionCatalog{meta: meta},
		baseCommitSeq:  8,
		baseSystemRoot: 9,
	}

	read, _, blocked, staleSnapshot, err := snapshotUpdateBatchBufferedRead(nil, domain, meta, 7, 7, []updateBatchItem{{
		UpdateBatchItem: UpdateBatchItem{DocumentID: []byte("u1")},
	}}, DocumentFormatJSON)
	if err != nil {
		t.Fatalf("snapshotUpdateBatchBufferedRead: %v", err)
	}
	defer putUpdateBatchBufferedEntries(read.primaryEntries, read.primaryBuffer)
	if read.enabled || blocked || !staleSnapshot {
		t.Fatalf("enabled=%v blocked=%v stale=%v want false/false/true", read.enabled, blocked, staleSnapshot)
	}
}

func TestSnapshotUpdateBatchBufferedReadPrimaryRunIndexAvoidsCollectingPendingRuns(t *testing.T) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}
	primaryName := collectionPrimaryRootName("users")
	primaryTable := newCollectionRunTable(1)
	setCollectionRunValue(primaryTable, []byte("u1"), []byte(`{"city":"paris"}`))
	primaryTable.Freeze()
	defer resetCollectionRunTable(primaryTable)

	primaryIndex := newBufferedPrimaryRunIndex(1)
	if err := addBufferedPrimaryRunIndexEntries(primaryIndex, primaryTable); err != nil {
		t.Fatalf("add primary run index entries: %v", err)
	}
	domain := &collectionWriteDomain{
		loaded:         true,
		meta:           meta,
		catalog:        &collectionCatalog{meta: meta},
		baseSystemRoot: 7,
		count:          1,
		rootRuns: map[string][]memtable.Table{
			primaryName: {primaryTable},
		},
		primaryRunIndex: primaryIndex,
	}
	for i := 0; i < 256; i++ {
		domain.indexedFlushUnits = append(domain.indexedFlushUnits, indexedFlushUnit{
			rootRuns: map[string][]memtable.Table{
				primaryName: make([]memtable.Table, 8),
			},
			rootRunCount: 8,
		})
	}
	items := []updateBatchItem{{UpdateBatchItem: UpdateBatchItem{DocumentID: []byte("u1")}}}
	assertRead := func() {
		t.Helper()
		read, _, blocked, staleSnapshot, needPrimaryRunIndex, err := snapshotUpdateBatchBufferedReadLocked(nil, domain, meta, 1, 7, items, DocumentFormatJSON, false)
		if err != nil {
			t.Fatalf("snapshotUpdateBatchBufferedReadLocked: %v", err)
		}
		defer putUpdateBatchBufferedEntries(read.primaryEntries, read.primaryBuffer)
		if blocked || staleSnapshot || needPrimaryRunIndex || !read.enabled {
			t.Fatalf("read enabled=%v blocked=%v stale=%v needPrimaryRunIndex=%v", read.enabled, blocked, staleSnapshot, needPrimaryRunIndex)
		}
		if len(read.primaryEntries) != 1 || !read.primaryEntries[0].found || !bytes.Equal(read.primaryEntries[0].value, []byte(`{"city":"paris"}`)) {
			t.Fatalf("primary entries=%+v want buffered u1 document", read.primaryEntries)
		}
	}
	assertRead()

	if allocs := testing.AllocsPerRun(100, assertRead); allocs > 2 {
		t.Fatalf("buffered read allocations/run=%0.1f want <= 2; primary-run index path should not collect pending root runs", allocs)
	}
}

func TestSnapshotUpdateBatchBufferedPrimaryEntriesFromIndexUsesValueArena(t *testing.T) {
	primaryTable := newCollectionRunTable(3)
	setCollectionRunValue(primaryTable, []byte("u1"), []byte(`{"city":"paris"}`))
	setCollectionRunValue(primaryTable, []byte("u2"), []byte(`{"city":"rome"}`))
	setCollectionRunValue(primaryTable, []byte("u3"), []byte(`{"city":"oslo"}`))
	primaryTable.Freeze()
	defer resetCollectionRunTable(primaryTable)

	primaryIndex := newBufferedPrimaryRunIndex(3)
	if err := addBufferedPrimaryRunIndexEntries(primaryIndex, primaryTable); err != nil {
		t.Fatalf("add primary run index entries: %v", err)
	}
	items := []updateBatchItem{
		{UpdateBatchItem: UpdateBatchItem{DocumentID: []byte("u1")}},
		{UpdateBatchItem: UpdateBatchItem{DocumentID: []byte("u2")}},
		{UpdateBatchItem: UpdateBatchItem{DocumentID: []byte("u3")}},
	}
	assertRead := func() {
		t.Helper()
		entries, buffer, err := snapshotUpdateBatchBufferedPrimaryEntriesFromIndex(primaryIndex, items)
		if err != nil {
			t.Fatalf("snapshotUpdateBatchBufferedPrimaryEntriesFromIndex: %v", err)
		}
		defer putUpdateBatchBufferedEntries(entries, buffer)
		if buffer == nil || len(buffer.arena) == 0 {
			t.Fatal("buffered primary read did not use the pooled value arena")
		}
		arenaStart := uintptr(unsafe.Pointer(unsafe.SliceData(buffer.arena)))
		arenaEnd := arenaStart + uintptr(len(buffer.arena))
		want := [][]byte{
			[]byte(`{"city":"paris"}`),
			[]byte(`{"city":"rome"}`),
			[]byte(`{"city":"oslo"}`),
		}
		for i := range want {
			if !entries[i].found || !bytes.Equal(entries[i].value, want[i]) {
				t.Fatalf("entry %d found=%v value=%q want %q", i, entries[i].found, entries[i].value, want[i])
			}
			valueStart := uintptr(unsafe.Pointer(unsafe.SliceData(entries[i].value)))
			if valueStart < arenaStart || valueStart >= arenaEnd {
				t.Fatalf("entry %d value is not backed by buffered arena", i)
			}
		}
	}
	assertRead()
}

func TestUpdateBatchBufferedEntryBufferCopyValuePreservesEmptySlice(t *testing.T) {
	buffer := &updateBatchBufferedEntryBuffer{}
	buffer.ensureValueArenaCapacity(1)
	empty := []byte{}
	got := buffer.copyValue(empty)
	if got == nil {
		t.Fatal("copyValue(empty non-nil slice) returned nil")
	}
	if len(got) != 0 {
		t.Fatalf("copyValue(empty) len=%d want 0", len(got))
	}
	if nilValue := buffer.copyValue(nil); nilValue != nil {
		t.Fatalf("copyValue(nil)=%v want nil", nilValue)
	}
}

func BenchmarkSnapshotUpdateBatchBufferedPrimaryEntriesFromIndexValues(b *testing.B) {
	const entriesCount = 256
	primaryTable := newCollectionRunTable(entriesCount)
	items := make([]updateBatchItem, 0, entriesCount)
	value := []byte(strings.Repeat("x", 512))
	for i := 0; i < entriesCount; i++ {
		id := []byte(fmt.Sprintf("u%05d", i))
		setCollectionRunValue(primaryTable, id, value)
		items = append(items, updateBatchItem{UpdateBatchItem: UpdateBatchItem{DocumentID: id}})
	}
	primaryTable.Freeze()
	defer resetCollectionRunTable(primaryTable)

	primaryIndex := newBufferedPrimaryRunIndex(entriesCount)
	if err := addBufferedPrimaryRunIndexEntries(primaryIndex, primaryTable); err != nil {
		b.Fatalf("add primary run index entries: %v", err)
	}
	warmEntries, warmBuffer, err := snapshotUpdateBatchBufferedPrimaryEntriesFromIndex(primaryIndex, items)
	if err != nil {
		b.Fatalf("warm buffered read: %v", err)
	}
	putUpdateBatchBufferedEntries(warmEntries, warmBuffer)

	total := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries, buffer, err := snapshotUpdateBatchBufferedPrimaryEntriesFromIndex(primaryIndex, items)
		if err != nil {
			b.Fatalf("snapshotUpdateBatchBufferedPrimaryEntriesFromIndex: %v", err)
		}
		for j := range entries {
			total += len(entries[j].value)
		}
		putUpdateBatchBufferedEntries(entries, buffer)
	}
	b.StopTimer()
	if total == 0 {
		b.Fatal("benchmark did not consume buffered values")
	}
}

func TestBufferedRunLenHintSumsPendingRunLengths(t *testing.T) {
	first := newCollectionRunTable(2)
	setCollectionRunValue(first, []byte("u1"), []byte("one"))
	setCollectionRunValue(first, []byte("u2"), []byte("two"))
	first.Freeze()
	defer resetCollectionRunTable(first)

	second := newCollectionRunTable(1)
	setCollectionRunValue(second, []byte("u3"), []byte("three"))
	second.Freeze()
	defer resetCollectionRunTable(second)

	if got := bufferedRunLenHint([]memtable.Table{nil, first, second}); got != 3 {
		t.Fatalf("bufferedRunLenHint=%d want 3", got)
	}
	if got := boundedBufferedRunLenHint(bufferedRunLenHintMaxCapacity + 1); got != bufferedRunLenHintMaxCapacity {
		t.Fatalf("boundedBufferedRunLenHint over cap=%d want %d", got, bufferedRunLenHintMaxCapacity)
	}
}

func TestRebuildBufferedIndexesCoverMultiplePendingRuns(t *testing.T) {
	primaryName := collectionPrimaryRootName("users")
	primaryA := newCollectionRunTable(2)
	setCollectionRunValue(primaryA, []byte("u1"), []byte("one"))
	setCollectionRunValue(primaryA, []byte("u2"), []byte("two"))
	primaryA.Freeze()
	defer resetCollectionRunTable(primaryA)

	primaryB := newCollectionRunTable(1)
	setCollectionRunValue(primaryB, []byte("u3"), []byte("three"))
	primaryB.Freeze()
	defer resetCollectionRunTable(primaryB)

	uniqueA := newCollectionRunTable(1)
	setCollectionRunValue(uniqueA, []byte("city:boston"), nil)
	uniqueA.Freeze()
	defer resetCollectionRunTable(uniqueA)

	uniqueB := newCollectionRunTable(1)
	setCollectionRunValue(uniqueB, []byte("city:seattle"), nil)
	uniqueB.Freeze()
	defer resetCollectionRunTable(uniqueB)

	runs := map[string][]memtable.Table{
		primaryName: {primaryA, primaryB},
		"city_1":    {uniqueA, uniqueB},
	}
	primaryIDs := rebuildBufferedPrimaryIDIndex("users", runs)
	if primaryIDs == nil {
		t.Fatal("primary ID index is nil")
	}
	if got := primaryIDs.len(); got != 3 {
		t.Fatalf("primary ID index len=%d want 3", got)
	}
	for _, key := range [][]byte{[]byte("u1"), []byte("u2"), []byte("u3")} {
		if !primaryIDs.contains(key) {
			t.Fatalf("primary ID index missing %q", key)
		}
	}

	primaryRuns, err := rebuildBufferedPrimaryRunIndex("users", runs)
	if err != nil {
		t.Fatalf("rebuildBufferedPrimaryRunIndex: %v", err)
	}
	for _, tc := range []struct {
		key   []byte
		table memtable.Table
	}{
		{[]byte("u1"), primaryA},
		{[]byte("u2"), primaryA},
		{[]byte("u3"), primaryB},
	} {
		got, ok := primaryRuns.lookup(tc.key)
		if !ok || got != tc.table {
			t.Fatalf("primary run lookup %q table=%p ok=%v want %p", tc.key, got, ok, tc.table)
		}
	}

	uniqueIndexes := rebuildBufferedUniqueValueIndexes(map[string][]memtable.Table{
		"city_1": {uniqueA, uniqueB},
	})
	unique := uniqueIndexes["city_1"]
	if unique == nil {
		t.Fatal("unique index is nil")
	}
	if got := unique.len(); got != 2 {
		t.Fatalf("unique index len=%d want 2", got)
	}
	for _, key := range [][]byte{[]byte("city:boston"), []byte("city:seattle")} {
		if !unique.contains(key) {
			t.Fatalf("unique index missing %q", key)
		}
	}
}

func BenchmarkRebuildBufferedPendingIndexes(b *testing.B) {
	const entries = 4096
	primaryName := collectionPrimaryRootName("users")
	primary := newCollectionRunTable(entries)
	unique := newCollectionRunTable(entries)
	for i := 0; i < entries; i++ {
		id := fmt.Sprintf("u%05d", i)
		setCollectionRunValue(primary, []byte(id), []byte("doc"))
		setCollectionRunValue(unique, []byte("email:"+id), nil)
	}
	primary.Freeze()
	unique.Freeze()
	defer resetCollectionRunTable(primary)
	defer resetCollectionRunTable(unique)

	runs := map[string][]memtable.Table{
		primaryName: {primary},
		"email_1":   {unique},
	}
	uniqueRuns := map[string][]memtable.Table{"email_1": {unique}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if index := rebuildBufferedPrimaryIDIndex("users", runs); index == nil || index.len() != entries {
			got := 0
			if index != nil {
				got = index.len()
			}
			b.Fatalf("primary ID index len=%d want %d", got, entries)
		}
		if index, err := rebuildBufferedPrimaryRunIndex("users", runs); err != nil || index == nil || len(index.values) != entries {
			got := 0
			if index != nil {
				got = len(index.values)
			}
			b.Fatalf("primary run index len=%d err=%v want %d", got, err, entries)
		}
		if indexes := rebuildBufferedUniqueValueIndexes(uniqueRuns); indexes["email_1"] == nil || indexes["email_1"].len() != entries {
			got := 0
			if index := indexes["email_1"]; index != nil {
				got = index.len()
			}
			b.Fatalf("unique index len=%d want %d", got, entries)
		}
	}
}

func BenchmarkSnapshotUpdateBatchBufferedReadPrimaryRunIndexPendingUnits(b *testing.B) {
	meta := CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}
	primaryName := collectionPrimaryRootName("users")
	primaryTable := newCollectionRunTable(1)
	setCollectionRunValue(primaryTable, []byte("u1"), []byte(`{"city":"paris"}`))
	primaryTable.Freeze()
	defer resetCollectionRunTable(primaryTable)

	primaryIndex := newBufferedPrimaryRunIndex(1)
	if err := addBufferedPrimaryRunIndexEntries(primaryIndex, primaryTable); err != nil {
		b.Fatalf("add primary run index entries: %v", err)
	}
	domain := &collectionWriteDomain{
		loaded:         true,
		meta:           meta,
		catalog:        &collectionCatalog{meta: meta},
		baseSystemRoot: 7,
		count:          1,
		rootRuns: map[string][]memtable.Table{
			primaryName: {primaryTable},
		},
		primaryRunIndex: primaryIndex,
	}
	for i := 0; i < 256; i++ {
		domain.indexedFlushUnits = append(domain.indexedFlushUnits, indexedFlushUnit{
			rootRuns: map[string][]memtable.Table{
				primaryName: make([]memtable.Table, 8),
			},
			rootRunCount: 8,
		})
	}
	items := []updateBatchItem{{UpdateBatchItem: UpdateBatchItem{DocumentID: []byte("u1")}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		read, _, blocked, staleSnapshot, needPrimaryRunIndex, err := snapshotUpdateBatchBufferedReadLocked(nil, domain, meta, 1, 7, items, DocumentFormatJSON, false)
		if err != nil {
			b.Fatalf("snapshotUpdateBatchBufferedReadLocked: %v", err)
		}
		if blocked || staleSnapshot || needPrimaryRunIndex || !read.enabled || len(read.primaryEntries) != 1 || !read.primaryEntries[0].found {
			b.Fatalf("unexpected read enabled=%v entries=%d blocked=%v stale=%v needPrimaryRunIndex=%v", read.enabled, len(read.primaryEntries), blocked, staleSnapshot, needPrimaryRunIndex)
		}
		putUpdateBatchBufferedEntries(read.primaryEntries, read.primaryBuffer)
	}
}

func TestLockCollectionDomainMutationDoesNotAllocate(t *testing.T) {
	domain := &collectionWriteDomain{}
	allocs := testing.AllocsPerRun(1000, func() {
		unlock := lockCollectionDomainMutation(domain)
		unlock.Unlock()
	})
	if allocs != 0 {
		t.Fatalf("lockCollectionDomainMutation allocations/run=%0.1f want 0", allocs)
	}
	if got := domain.mutationLockCalls.Load(); got == 0 {
		t.Fatal("mutation lock stats were not recorded")
	}
}

func collectionHasBufferedPrimaryRunIndexForTest(t *testing.T, col *Collection) bool {
	t.Helper()
	col.writeDomain.mu.RLock()
	defer col.writeDomain.mu.RUnlock()
	return col.writeDomain.primaryRunIndex != nil
}

func TestCollectionUpdateBatchDoesNotBufferUnchangedUniqueValues(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"a@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert flushed document: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush document: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"email":"b@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert buffered document: %v", err)
	}
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sea")},
	}); err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatal("update batch was not buffered")
	}

	encodedA, err := encodeIndexScalar(IndexValueString, "a@example.com")
	if err != nil {
		t.Fatalf("encode email: %v", err)
	}
	_, prefixA, err := appendIndexValuePrefixSlice(nil, encodedA)
	if err != nil {
		t.Fatalf("email prefix: %v", err)
	}
	encodedB, err := encodeIndexScalar(IndexValueString, "b@example.com")
	if err != nil {
		t.Fatalf("encode buffered email: %v", err)
	}
	_, prefixB, err := appendIndexValuePrefixSlice(nil, encodedB)
	if err != nil {
		t.Fatalf("buffered email prefix: %v", err)
	}
	col.writeDomain.mu.RLock()
	pending := col.writeDomain.uniqueValueIndex["email"]
	containsA := pending != nil && pending.contains(prefixA)
	containsB := pending != nil && pending.contains(prefixB)
	uniqueRuns := len(col.writeDomain.uniqueValueRuns["email"])
	col.writeDomain.mu.RUnlock()
	if containsA {
		t.Fatal("buffered non-unique update added unchanged persisted unique email to pending unique-value index")
	}
	if !containsB {
		t.Fatal("pending buffered insert unique email missing from pending unique-value index")
	}
	if uniqueRuns != 1 {
		t.Fatalf("unique value runs=%d want only the pending insert run", uniqueRuns)
	}
	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	col.writeDomain.mu.RLock()
	pending = col.writeDomain.uniqueValueIndex["email"]
	containsA = pending != nil && pending.contains(prefixA)
	containsB = pending != nil && pending.contains(prefixB)
	publishingUniqueRuns := 0
	if len(col.writeDomain.indexedPublishingUnits) > 0 {
		publishingUniqueRuns = len(col.writeDomain.indexedPublishingUnits[0].uniqueValueRuns["email"])
	}
	col.writeDomain.mu.RUnlock()
	if containsA {
		t.Fatal("rotated non-unique update added unchanged persisted unique email to pending unique-value index")
	}
	if !containsB {
		t.Fatal("rotated pending insert unique email missing from pending unique-value index")
	}
	if publishingUniqueRuns != 1 {
		t.Fatalf("publishing unique value runs=%d want only the pending insert run", publishingUniqueRuns)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u3")},
		[][]byte{[]byte(`{"email":"a@example.com","city":"hnl"}`)},
	); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate persisted unique email err=%v want ErrUniqueIndexConflict", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u4")},
		[][]byte{[]byte(`{"email":"b@example.com","city":"hnl"}`)},
	); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate pending unique email err=%v want ErrUniqueIndexConflict", err)
	}
	ids, err := col.FindByIndex("email", "a@example.com")
	if err != nil {
		t.Fatalf("find buffered updated email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("email ids=%q want [u1]", ids)
	}
	if err := col.publishPreparedIndexedFlush(work); err != nil {
		t.Fatalf("publish prepared async flush: %v", err)
	}
}

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesDeclinesUniqueUpdates(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"email":"a@example.com"}`), []byte(`{"email":"b@example.com"}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := d.State()

	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONEmail("c@example.com")},
		{DocumentID: []byte("u2"), Update: setJSONEmail("d@example.com")},
	})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if batched {
		t.Fatalf("batched=%v results=%+v want declined", batched, results)
	}
	if len(results) != 2 || results[0].Matched || results[0].Modified || results[1].Matched || results[1].Modified {
		t.Fatalf("declined results=%+v want two zero-valued results", results)
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("declined unique update advanced commit seq by %d", after.CommitSeq-before.CommitSeq)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Contains(got, []byte(`"email":"a@example.com"`)) {
		t.Fatalf("u1 document=%s want unchanged email", got)
	}
}

func TestNormalizedEncodedIndexValuesEqualUsesCanonicalOrder(t *testing.T) {
	left := [][]byte{[]byte("s:a"), []byte("s:b")}
	right := [][]byte{[]byte("s:a"), []byte("s:b")}
	if !normalizedEncodedIndexValuesEqual(left, right) {
		t.Fatalf("normalizedEncodedIndexValuesEqual(%q, %q)=false want true", left, right)
	}
	if normalizedEncodedIndexValuesEqual([][]byte{[]byte("s:b"), []byte("s:a")}, right) {
		t.Fatal("normalizedEncodedIndexValuesEqual ignored canonical order")
	}
	if normalizedEncodedIndexValuesEqual(left, [][]byte{[]byte("s:a"), []byte("s:c")}) {
		t.Fatal("normalizedEncodedIndexValuesEqual matched different values")
	}
	if normalizedEncodedIndexValuesEqual([][]byte{[]byte("s:a"), []byte("s:a")}, right) {
		t.Fatal("normalizedEncodedIndexValuesEqual ignored duplicate cardinality")
	}
}

func TestCollectionUpdateBatchClonesAliasedReplacements(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"score":0}`), []byte(`{"score":0}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	scratch := make([]byte, 0, 32)
	setScore := func(score int) func([]byte) ([]byte, bool, error) {
		return func([]byte) ([]byte, bool, error) {
			scratch = append(scratch[:0], fmt.Sprintf(`{"score":%d}`, score)...)
			return scratch, true, nil
		}
	}
	if _, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setScore(1)},
		{DocumentID: []byte("u2"), Update: setScore(2)},
	}); err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	for _, tc := range []struct {
		id    []byte
		score string
	}{
		{id: []byte("u1"), score: `"score":1`},
		{id: []byte("u2"), score: `"score":2`},
	} {
		got, err := col.Get(tc.id)
		if err != nil {
			t.Fatalf("get %s: %v", tc.id, err)
		}
		if !bytes.Contains(got, []byte(tc.score)) {
			t.Fatalf("%s document=%s want %s", tc.id, got, tc.score)
		}
	}
}

func TestCollectionUpdateBatchReplansAfterConcurrentCollectionMutation(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	left, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open left collection: %v", err)
	}
	right, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open right collection: %v", err)
	}
	if _, err := left.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"score":0}`), []byte(`{"score":0}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	ready := make(chan struct{})
	stopRight := make(chan struct{})
	var readyOnce sync.Once
	rightDone := make(chan error, 1)
	rightFinished := make(chan struct{})
	go func() {
		defer close(rightFinished)
		select {
		case <-ready:
		case <-stopRight:
			return
		}
		_, err := right.UpdateBatch([]UpdateBatchItem{{
			DocumentID: []byte("u2"),
			Update: func([]byte) ([]byte, bool, error) {
				return []byte(`{"score":2}`), true, nil
			},
		}})
		rightDone <- err
	}()
	defer func() {
		close(stopRight)
		timer := time.NewTimer(collectionTestTimeout(t, 5*time.Second))
		defer timer.Stop()
		select {
		case <-rightFinished:
		case <-timer.C:
			t.Error("timed out waiting for concurrent update goroutine cleanup")
		}
	}()

	concurrentUpdateWait := collectionTestTimeout(t, 10*time.Second)
	var callbackCalls atomic.Int32
	results, err := left.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			if callbackCalls.Add(1) == 1 {
				readyOnce.Do(func() { close(ready) })
				timer := time.NewTimer(concurrentUpdateWait)
				defer timer.Stop()
				select {
				case err := <-rightDone:
					if err != nil {
						return nil, false, err
					}
				case <-timer.C:
					return nil, false, errors.New("timed out waiting for concurrent update")
				}
			}
			return []byte(`{"score":1}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("left UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want matched modified", results)
	}
	if got := callbackCalls.Load(); got < 2 {
		t.Fatalf("callbackCalls=%d want retry after concurrent collection mutation", got)
	}
	for _, tc := range []struct {
		id    []byte
		score float64
	}{
		{id: []byte("u1"), score: 1},
		{id: []byte("u2"), score: 2},
	} {
		got, err := left.Get(tc.id)
		if err != nil {
			t.Fatalf("get %s: %v", tc.id, err)
		}
		var doc map[string]any
		if err := json.Unmarshal(got, &doc); err != nil {
			t.Fatalf("parse %s document=%s: %v", tc.id, got, err)
		}
		if gotScore, ok := doc["score"].(float64); !ok || gotScore != tc.score {
			t.Fatalf("%s score=%v want %v document=%s", tc.id, doc["score"], tc.score, got)
		}
	}
}

func TestCollectionUpdateBatchClonesDocumentIDs(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"score":0}`), []byte(`{"score":0}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	id := []byte("u1")
	if _, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: id,
		Update: func([]byte) ([]byte, bool, error) {
			id[1] = '2'
			return []byte(`{"score":1}`), true, nil
		},
	}}); err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	gotU1, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Contains(gotU1, []byte(`"score":1`)) {
		t.Fatalf("u1 document=%s want score 1", gotU1)
	}
	gotU2, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if !bytes.Contains(gotU2, []byte(`"score":0`)) {
		t.Fatalf("u2 document=%s want score 0", gotU2)
	}
}

func TestCollectionUpdateBatchFlushesBufferedIndexedWritesBeforePublish(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	left, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open left collection: %v", err)
	}
	right, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open right collection: %v", err)
	}
	if _, err := left.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl","score":0}`)},
	); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if err := mgr.FlushAll(); err != nil {
		t.Fatalf("flush initial indexed write: %v", err)
	}

	var staged atomic.Bool
	results, err := left.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			if staged.CompareAndSwap(false, true) {
				if _, err := right.InsertBatch(
					[][]byte{[]byte("u2")},
					[][]byte{[]byte(`{"city":"sfo","score":2}`)},
				); err != nil {
					return nil, false, err
				}
			}
			return []byte(`{"city":"sea","score":1}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want matched modified", results)
	}
	if err := mgr.FlushAll(); err != nil {
		t.Fatalf("FlushAll after update with staged indexed write: %v", err)
	}
	seaIDs, err := left.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find sea: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, seaIDs, []byte("u1"))
	sfoIDs, err := left.FindByIndex("city", "sfo")
	if err != nil {
		t.Fatalf("find sfo: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, sfoIDs, []byte("u2"))
}

func TestCollectionUpdateBatchNoOpAllowsOtherCollectionRootDrift(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create users collection: %v", err)
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "teams"}); err != nil {
		t.Fatalf("create teams collection: %v", err)
	}
	users, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open users collection: %v", err)
	}
	teams, err := mgr.OpenCollection("teams")
	if err != nil {
		t.Fatalf("open teams collection: %v", err)
	}
	if _, err := users.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"city":"hnl","score":0}`)},
	); err != nil {
		t.Fatalf("insert user: %v", err)
	}

	var callbackCalls atomic.Int32
	results, err := users.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func(current []byte) ([]byte, bool, error) {
			call := callbackCalls.Add(1)
			if _, err := teams.InsertBatch(
				[][]byte{[]byte(fmt.Sprintf("team-%d", call))},
				[][]byte{[]byte(fmt.Sprintf(`{"name":"team-%d"}`, call))},
			); err != nil {
				return nil, false, err
			}
			return current, false, nil
		},
	}})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || results[0].Modified {
		t.Fatalf("results=%+v want matched and not modified", results)
	}
	if got := callbackCalls.Load(); got != 1 {
		t.Fatalf("callback calls=%d want 1", got)
	}
}

func TestCollectionRootDescriptorDeltaWrappersRejectNilReceiver(t *testing.T) {
	var col *Collection
	if _, err := col.buildRootDescriptorSystemDeltaIterator(0, 0, nil, nil, nil); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("buildRootDescriptorSystemDeltaIterator err=%v want ErrClosed", err)
	}
	if err := col.validateRootDescriptorSystemDelta(0, 0, nil, nil); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("validateRootDescriptorSystemDelta err=%v want ErrClosed", err)
	}
}

func TestCollectionUpdateBatchBSONRejectsIDMutation(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(0)}})
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	beforeState := d.State()
	beforeRoot := collectionPrimaryRootIDForTest(t, d, "users")
	replacement := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "score", Value: int32(1)}})
	_, err = col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: func([]byte) ([]byte, bool, error) {
			return replacement, true, nil
		}},
	})
	if !errors.Is(err, errBSONIDMutation) || !strings.Contains(err.Error(), "index 0") {
		t.Fatalf("UpdateBatch err=%v want indexed _id mutation error", err)
	}
	afterState := d.State()
	if afterState.CommitSeq != beforeState.CommitSeq {
		t.Fatalf("rejected _id UpdateBatch advanced commit seq by %d", afterState.CommitSeq-beforeState.CommitSeq)
	}
	afterRoot := collectionPrimaryRootIDForTest(t, d, "users")
	if afterRoot != beforeRoot {
		t.Fatalf("primary root changed from %d to %d after rejected _id UpdateBatch", beforeRoot, afterRoot)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("u1 after rejected _id UpdateBatch=%x want original %x", got, doc)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if got != nil {
		t.Fatalf("u2 after rejected _id UpdateBatch=%x want nil", got)
	}
}

func TestCollectionUpdateBatchBSONAllowsNoopNilReplacement(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "score", Value: int32(0)}})
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	results, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: func([]byte) ([]byte, bool, error) {
			return nil, false, nil
		}},
	})
	if err != nil {
		t.Fatalf("UpdateBatch noop: %v", err)
	}
	if got, want := len(results), 1; got != want {
		t.Fatalf("results len=%d want %d", got, want)
	}
	if !results[0].Matched || results[0].Modified {
		t.Fatalf("result=%+v want matched noop", results[0])
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("document changed got=%v want=%v", bson.Raw(got), bson.Raw(doc))
	}
}

func TestCollectionUpdateBatchTemplateV1MaterializesUpdatedDocuments(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustTemplateV1Document(t, []string{"name", "city", "score"}, []any{"ada", "hnl", int64(0)}),
			mustTemplateV1Document(t, []string{"name", "city", "score"}, []any{"grace", "hnl", int64(0)}),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	setScore := func(score int64, city string) func([]byte) ([]byte, bool, error) {
		return func([]byte) ([]byte, bool, error) {
			next, err := EncodeTemplateV1DocumentJSON([]byte(fmt.Sprintf(`{"name":"updated","city":%q,"score":%d}`, city, score)))
			if err != nil {
				return nil, false, err
			}
			return next, true, nil
		}
	}
	results, err := col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setScore(11, "sea")},
		{DocumentID: []byte("u2"), Update: setScore(12, "sfo")},
	})
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 2 || !results[0].Modified || !results[1].Modified {
		t.Fatalf("results=%+v want both modified", results)
	}
	for _, tc := range []struct {
		id    []byte
		score int64
		city  string
	}{
		{id: []byte("u1"), score: 11, city: "sea"},
		{id: []byte("u2"), score: 12, city: "sfo"},
	} {
		stored, err := col.Get(tc.id)
		if err != nil {
			t.Fatalf("get %s: %v", tc.id, err)
		}
		jsonDoc, err := col.StoredDocumentJSON(stored)
		if err != nil {
			t.Fatalf("materialize %s: %v", tc.id, err)
		}
		var doc map[string]any
		if err := json.Unmarshal(jsonDoc, &doc); err != nil {
			t.Fatalf("unmarshal %s: %v", tc.id, err)
		}
		if got, _ := int64ValueForTest(doc["score"]); got != tc.score {
			t.Fatalf("%s score=%d want %d", tc.id, got, tc.score)
		}
		if got, _ := doc["city"].(string); got != tc.city {
			t.Fatalf("%s city=%q want %q", tc.id, got, tc.city)
		}
	}
	ids, err := col.FindByIndex("city", "sea")
	if err != nil {
		t.Fatalf("find city index: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("city index ids=%q want [u1]", ids)
	}
}

func incrementJSONCount(current []byte) ([]byte, bool, error) {
	var doc map[string]any
	if err := json.Unmarshal(current, &doc); err != nil {
		return nil, false, err
	}
	count, _ := int64ValueForTest(doc["count"])
	doc["count"] = count + 1
	next, err := json.Marshal(doc)
	if err != nil {
		return nil, false, err
	}
	return next, true, nil
}

func setJSONEmail(email string) func([]byte) ([]byte, bool, error) {
	return setJSONField("email", email)
}

func setJSONCity(city string) func([]byte) ([]byte, bool, error) {
	return setJSONField("city", city)
}

func setBSONField(field string, value any) func([]byte) ([]byte, bool, error) {
	return func(current []byte) ([]byte, bool, error) {
		var doc bson.M
		if err := bson.Unmarshal(current, &doc); err != nil {
			return nil, false, err
		}
		doc[field] = value
		next, err := bson.Marshal(doc)
		if err != nil {
			return nil, false, err
		}
		return next, true, nil
	}
}

func mustBSONRawValue(t testing.TB, value any) bson.RawValue {
	t.Helper()
	typ, raw, err := bson.MarshalValue(value)
	if err != nil {
		t.Fatalf("marshal BSON value: %v", err)
	}
	return bson.RawValue{Type: typ, Value: raw}
}

func setTemplateV1JSON(t *testing.T, raw string) func([]byte) ([]byte, bool, error) {
	t.Helper()
	next, err := EncodeTemplateV1DocumentJSON([]byte(raw))
	if err != nil {
		t.Fatalf("encode template-v1 update document: %v", err)
	}
	return func([]byte) ([]byte, bool, error) {
		return next, true, nil
	}
}

func setJSONField(field string, value any) func([]byte) ([]byte, bool, error) {
	return func(current []byte) ([]byte, bool, error) {
		var doc map[string]any
		if err := json.Unmarshal(current, &doc); err != nil {
			return nil, false, err
		}
		doc[field] = value
		next, err := json.Marshal(doc)
		if err != nil {
			return nil, false, err
		}
		return next, true, nil
	}
}

func int64ValueForTest(value any) (int64, bool) {
	switch v := value.(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	default:
		return 0, false
	}
}

func TestNoIndexInsertAfterRawCommitDoesNotReenterWriteDomainLock(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := d.Set([]byte("raw/unrelated"), []byte("value")); err != nil {
		t.Fatalf("raw set: %v", err)
	}

	timeout := 5 * time.Second
	if deadline, ok := t.Deadline(); ok {
		if remaining := time.Until(deadline) / 2; remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	done := make(chan error, 1)
	go func() {
		_, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	case <-timer.C:
		_ = d.Close()
		select {
		case err := <-done:
			t.Fatalf("insert unblocked after timeout with err=%v", err)
		case <-time.After(time.Second):
		}
		t.Fatal("insert blocked while refreshing write-domain catalog after raw commit")
	}
}

func TestCollectionSingleInsertMatchesSingleItemBatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		indexes []IndexDefinition
	}{
		{name: "no_index"},
		{name: "indexed", indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = d.Close() }()

			mgr := NewCollectionManager(d)
			insertMeta := &CollectionMeta{Name: "insert_users", Indexes: tc.indexes}
			batchMeta := &CollectionMeta{Name: "batch_users", Indexes: tc.indexes}
			if _, err := mgr.CreateCollection(insertMeta); err != nil {
				t.Fatalf("create insert collection: %v", err)
			}
			if _, err := mgr.CreateCollection(batchMeta); err != nil {
				t.Fatalf("create batch collection: %v", err)
			}
			insertCol, err := mgr.OpenCollection("insert_users")
			if err != nil {
				t.Fatalf("open insert collection: %v", err)
			}
			batchCol, err := mgr.OpenCollection("batch_users")
			if err != nil {
				t.Fatalf("open batch collection: %v", err)
			}

			doc := []byte(`{"email":"ada@example.com","city":"hnl"}`)
			insertID, err := insertCol.Insert([]byte("u1"), doc)
			if err != nil {
				t.Fatalf("single insert: %v", err)
			}
			batchIDs, err := batchCol.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc})
			if err != nil {
				t.Fatalf("single-item batch insert: %v", err)
			}
			if len(batchIDs) != 1 || !bytes.Equal(insertID, batchIDs[0]) {
				t.Fatalf("ids insert=%q batch=%q", insertID, batchIDs)
			}

			insertDoc, err := insertCol.Get([]byte("u1"))
			if err != nil {
				t.Fatalf("single get: %v", err)
			}
			batchDoc, err := batchCol.Get([]byte("u1"))
			if err != nil {
				t.Fatalf("batch get: %v", err)
			}
			if !bytes.Equal(insertDoc, batchDoc) {
				t.Fatalf("documents differ: insert=%q batch=%q", insertDoc, batchDoc)
			}
			if len(tc.indexes) == 0 {
				return
			}
			insertIDs, err := insertCol.FindByIndex("email", "ada@example.com")
			if err != nil {
				t.Fatalf("single find email: %v", err)
			}
			batchIndexIDs, err := batchCol.FindByIndex("email", "ada@example.com")
			if err != nil {
				t.Fatalf("batch find email: %v", err)
			}
			if !reflect.DeepEqual(insertIDs, batchIndexIDs) {
				t.Fatalf("email ids differ: insert=%q batch=%q", insertIDs, batchIndexIDs)
			}
		})
	}
}

func TestCollectionSingleInsertRejectsUniqueConflictAtomically(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("duplicate insert err=%v want unique index conflict", err)
	}
	got, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get failed insert: %v", err)
	}
	if got != nil {
		t.Fatalf("failed insert left primary doc=%q", got)
	}
	ids, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("email ids=%q want [u1]", ids)
	}
}

func TestCollectionSingleDocumentReopenUsesPersistedRootDescriptors(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"grace@example.com"}`)); err != nil {
		t.Fatalf("insert u2: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	got, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get deleted u1 after reopen: %v", err)
	}
	if got != nil {
		t.Fatalf("deleted u1 visible after reopen: %q", got)
	}
	got, err = reopenedCol.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2 after reopen: %v", err)
	}
	if want := []byte(`{"email":"grace@example.com"}`); !bytes.Equal(got, want) {
		t.Fatalf("u2 after reopen=%q want %q", got, want)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find deleted email after reopen: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("deleted email ids after reopen=%q want none", ids)
	}
	ids, err = reopenedCol.FindByIndex("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find remaining email after reopen: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("remaining email ids after reopen=%q want [u2]", ids)
	}
}

func TestCollectionInsertBatchBridge_AppendsWithoutDroppingExistingRoots(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert first batch: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"email":"grace@example.com","city":"hnl"}`)},
	); err != nil {
		t.Fatalf("insert second batch: %v", err)
	}

	for id, want := range map[string][]byte{
		"u1": []byte(`{"email":"ada@example.com","city":"hnl"}`),
		"u2": []byte(`{"email":"grace@example.com","city":"hnl"}`),
	} {
		got, err := col.Get([]byte(id))
		if err != nil {
			t.Fatalf("get %q: %v", id, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%q=%q want %q", id, got, want)
		}
	}

	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, cityIDs, []byte("u1"), []byte("u2"))
}

func TestCollectionCreateIndexBackfill_BuildsSecondaryAndIndexState(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2"), []byte("u1")},
		[][]byte{
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	meta, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city", ValueType: IndexValueString})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, ok := findIndex(meta.Indexes, "city"); !ok {
		t.Fatalf("created meta missing city index: %+v", meta.Indexes)
	}
	if _, ok := findIndex(col.Meta().Indexes, "city"); !ok {
		t.Fatalf("collection meta missing city index after create: %+v", col.Meta().Indexes)
	}

	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	collectionMaintenanceRequireUnorderedIDs(t, cityIDs, []byte("u1"), []byte("u2"))

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "users")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if got := catalog.rootID(rootName); got == 0 {
			t.Fatalf("root %q was not persisted", rootName)
		}
	}

	if _, err := col.Insert([]byte("u3"), []byte(`{"email":"katherine@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("insert after create index: %v", err)
	}
	cityIDs, err = col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city after insert: %v", err)
	}
	if len(cityIDs) != 3 ||
		!bytes.Equal(cityIDs[0], []byte("u1")) ||
		!bytes.Equal(cityIDs[1], []byte("u2")) ||
		!bytes.Equal(cityIDs[2], []byte("u3")) {
		t.Fatalf("city ids after insert=%q want [u1 u2 u3]", cityIDs)
	}
}

func TestCollectionVectorIndexMetadataCreateDropReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	meta, err := col.CreateVectorIndex(VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 64,
	})
	if err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	created, ok := findVectorIndex(meta.VectorIndexes, "embedding")
	if !ok {
		t.Fatalf("created meta missing vector index: %+v", meta.VectorIndexes)
	}
	if created.M != defaultVectorIndexM || created.EfConstruction != defaultVectorIndexEfConstruction || created.EfSearch != defaultVectorIndexEfSearch {
		t.Fatalf("vector defaults=%+v", created)
	}
	if _, ok := findVectorIndex(col.Meta().VectorIndexes, "embedding"); !ok {
		t.Fatalf("collection meta missing vector index: %+v", col.Meta().VectorIndexes)
	}
	if len(col.Meta().Indexes) != 0 {
		t.Fatalf("vector create added scalar indexes: %+v", col.Meta().Indexes)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	d = nil

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	if _, ok := findVectorIndex(reopenedCol.Meta().VectorIndexes, "embedding"); !ok {
		t.Fatalf("reopened meta missing vector index: %+v", reopenedCol.Meta().VectorIndexes)
	}

	meta, err = reopenedCol.DropVectorIndex("embedding")
	if err != nil {
		t.Fatalf("drop vector index: %v", err)
	}
	if _, ok := findVectorIndex(meta.VectorIndexes, "embedding"); ok {
		t.Fatalf("dropped meta still has vector index: %+v", meta.VectorIndexes)
	}
	if _, err := reopenedCol.DropVectorIndex("embedding"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("drop missing vector index err=%v want ErrIndexNotFound", err)
	}
}

func TestCollectionVectorIndexMetadataJSONUsesStableStrings(t *testing.T) {
	meta, err := normalizeCollectionMeta(CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Metric:     VectorMetricInnerProduct,
			Dimensions: 64,
			Encoding:   VectorIndexEncodingInt8,
		}},
	})
	if err != nil {
		t.Fatalf("normalize meta: %v", err)
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"metric":"inner_product"`)) || !bytes.Contains(raw, []byte(`"encoding":"int8"`)) {
		t.Fatalf("vector metadata JSON=%s want string metric and encoding", raw)
	}
	var decoded CollectionMeta
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal string meta: %v", err)
	}
	decoded, err = normalizeCollectionMeta(decoded)
	if err != nil {
		t.Fatalf("normalize decoded meta: %v", err)
	}
	if !sameCollectionMeta(meta, decoded) {
		t.Fatalf("decoded meta=%+v want %+v", decoded, meta)
	}
	var numericDecoded CollectionMeta
	if err := json.Unmarshal([]byte(`{"name":"docs","vector_indexes":[{"name":"embedding","field":"embedding","metric":0,"dimensions":64,"encoding":1}]}`), &numericDecoded); err != nil {
		t.Fatalf("unmarshal numeric compatibility meta: %v", err)
	}
	numericDecoded, err = normalizeCollectionMeta(numericDecoded)
	if err != nil {
		t.Fatalf("normalize numeric meta: %v", err)
	}
	got, ok := findVectorIndex(numericDecoded.VectorIndexes, "embedding")
	if !ok || got.Metric != VectorMetricCosine || got.Encoding != VectorIndexEncodingInt8 {
		t.Fatalf("numeric decoded vector index=%+v ok=%v", got, ok)
	}
}

func TestCollectionVectorIndexMetadataValidation(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	_, err = mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Dimensions: 0,
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "dimensions must be positive") {
		t.Fatalf("zero dimensions err=%v want dimensions error", err)
	}
	_, err = mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Indexes: []IndexDefinition{{
			Name:      "embedding",
			Field:     "city",
			ValueType: IndexValueString,
		}},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Dimensions: 64,
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate index") {
		t.Fatalf("duplicate scalar/vector err=%v want duplicate index", err)
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create docs: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open docs: %v", err)
	}
	if _, err := col.CreateVectorIndex(VectorIndexDefinition{Field: "embedding", Dimensions: 64}); err != nil {
		t.Fatalf("create default-named vector index: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "embedding", Field: "city", ValueType: IndexValueString}); err == nil || !strings.Contains(err.Error(), "duplicate index") {
		t.Fatalf("duplicate scalar create err=%v want duplicate index", err)
	}
	if _, err := col.CreateVectorIndex(VectorIndexDefinition{Name: "other", Field: "embedding", Dimensions: -1}); err == nil || !strings.Contains(err.Error(), "dimensions must be positive") {
		t.Fatalf("negative dimensions err=%v want dimensions error", err)
	}
	if _, err := col.CreateVectorIndex(VectorIndexDefinition{Name: "bad", Field: ".embedding", Dimensions: 64}); err == nil || !strings.Contains(err.Error(), "field") {
		t.Fatalf("bad field err=%v want field error", err)
	}
}

func TestCollectionDropScalarIndexPreservesVectorMetadata(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Indexes: []IndexDefinition{{
			Name:      "city",
			Field:     "city",
			ValueType: IndexValueString,
		}},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Dimensions: 64,
		}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	meta, err := col.DropIndex("city")
	if err != nil {
		t.Fatalf("drop scalar index: %v", err)
	}
	if _, ok := findIndex(meta.Indexes, "city"); ok {
		t.Fatalf("dropped scalar index still present: %+v", meta.Indexes)
	}
	if _, ok := findVectorIndex(meta.VectorIndexes, "embedding"); !ok {
		t.Fatalf("drop scalar index lost vector metadata: %+v", meta.VectorIndexes)
	}
}

func TestCollectionDropVectorIndexPreservesScalarMetadata(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Indexes: []IndexDefinition{{
			Name:      "city",
			Field:     "city",
			ValueType: IndexValueString,
		}},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Dimensions: 64,
		}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	meta, err := col.DropVectorIndex("embedding")
	if err != nil {
		t.Fatalf("drop vector index: %v", err)
	}
	if _, ok := findVectorIndex(meta.VectorIndexes, "embedding"); ok {
		t.Fatalf("dropped vector index still present: %+v", meta.VectorIndexes)
	}
	if _, ok := findIndex(meta.Indexes, "city"); !ok {
		t.Fatalf("drop vector index lost scalar metadata: %+v", meta.Indexes)
	}
}

func TestCollectionDropIndexUpdatesSchema(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	meta, err := col.DropIndex("city")
	if err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if _, ok := findIndex(meta.Indexes, "city"); ok {
		t.Fatalf("dropped meta still has city index: %+v", meta.Indexes)
	}
	if _, ok := findIndex(meta.Indexes, "email"); !ok {
		t.Fatalf("dropped meta lost email index: %+v", meta.Indexes)
	}

	reopened, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("reopen collection: %v", err)
	}
	if _, ok := findIndex(reopened.Meta().Indexes, "city"); ok {
		t.Fatalf("reopened meta still has city index: %+v", reopened.Meta().Indexes)
	}
	if ids, err := reopened.FindByIndex("city", "hnl"); err != nil {
		t.Fatalf("find dropped city: %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("find dropped city ids=%q want none", ids)
	}
	if ids, err := reopened.FindByIndex("email", "ada@example.com"); err != nil {
		t.Fatalf("find retained email: %v", err)
	} else if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("find retained email ids=%q want u1", ids)
	}
	if err := reopened.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete while city index dropped: %v", err)
	}
	if _, err := reopened.Insert([]byte("u2"), []byte(`{"email":"grace@example.com","city":"sfo"}`)); err != nil {
		t.Fatalf("insert while city index dropped: %v", err)
	}
	if _, err := reopened.CreateIndex(IndexDefinition{Name: "city", Field: "city", ValueType: IndexValueString}); err != nil {
		t.Fatalf("recreate city index: %v", err)
	}
	if ids, err := reopened.FindByIndex("city", "hnl"); err != nil {
		t.Fatalf("find recreated city hnl: %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("recreated city hnl ids=%q want none", ids)
	}
	if ids, err := reopened.FindByIndex("city", "sfo"); err != nil {
		t.Fatalf("find recreated city sfo: %v", err)
	} else if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("recreated city sfo ids=%q want u2", ids)
	}
	if _, err := reopened.DropIndex("missing"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("drop missing err=%v want ErrIndexNotFound", err)
	}
	if _, err := reopened.DropIndexes([]string{"city", "missing"}); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("bulk drop with missing err=%v want ErrIndexNotFound", err)
	}
	if _, ok := findIndex(reopened.Meta().Indexes, "city"); !ok {
		t.Fatalf("bulk drop with missing removed city index: %+v", reopened.Meta().Indexes)
	}
	if _, ok := findIndex(reopened.Meta().Indexes, "email"); !ok {
		t.Fatalf("bulk drop with missing removed email index: %+v", reopened.Meta().Indexes)
	}
	meta, err = reopened.DropIndexes([]string{"city", "email"})
	if err != nil {
		t.Fatalf("bulk drop indexes: %v", err)
	}
	if len(meta.Indexes) != 0 {
		t.Fatalf("bulk drop meta indexes=%+v want none", meta.Indexes)
	}
}

func TestCollectionScanDocumentsAndFindByIndexValue(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2"), []byte("u3")},
		[][]byte{
			[]byte(`{"city":"hnl","name":"ada"}`),
			[]byte(`{"city":"sfo","name":"grace"}`),
			[]byte(`{"city":"hnl","name":"katherine"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	ids, err := col.FindByIndexValue("city", "hnl")
	if err != nil {
		t.Fatalf("find by index value: %v", err)
	}
	if len(ids) != 2 || !bytes.Equal(ids[0], []byte("u1")) || !bytes.Equal(ids[1], []byte("u3")) {
		t.Fatalf("ids=%q want u1,u3", ids)
	}

	ids, truncated, err := col.FindByIndexValueLimit("city", "hnl", 1)
	if err != nil {
		t.Fatalf("find by index value limit: %v", err)
	}
	if !truncated || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("limited ids=%q truncated=%v want u1/true", ids, truncated)
	}

	records, truncated, err := col.ScanDocuments(10)
	if err != nil {
		t.Fatalf("scan documents: %v", err)
	}
	if truncated {
		t.Fatal("scan unexpectedly truncated")
	}
	if len(records) != 3 {
		t.Fatalf("records len=%d want 3", len(records))
	}
	if !bytes.Equal(records[0].ID, []byte("u1")) || !bytes.Contains(records[0].Document, []byte(`"ada"`)) {
		t.Fatalf("first record=%+v", records[0])
	}
	records, truncated, err = col.ScanDocuments(1)
	if err != nil {
		t.Fatalf("scan limited documents: %v", err)
	}
	if !truncated || len(records) != 1 {
		t.Fatalf("limited scan truncated=%v len=%d want true/1", truncated, len(records))
	}
	var callbackIDs [][]byte
	truncated, err = col.ScanDocumentsFunc(1, func(record DocumentRecord) (bool, error) {
		callbackIDs = append(callbackIDs, record.ID)
		return false, nil
	})
	if err != nil {
		t.Fatalf("scan documents func: %v", err)
	}
	if truncated || len(callbackIDs) != 1 || !bytes.Equal(callbackIDs[0], []byte("u1")) {
		t.Fatalf("callback scan truncated=%v ids=%q want false/[u1]", truncated, callbackIDs)
	}
}

func TestCollectionIndexValueTypeIsRequired(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	_, err = mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "city", Field: "city"}},
	})
	if err == nil || !strings.Contains(err.Error(), "value_type is required") {
		t.Fatalf("missing value type err=%v want value_type is required", err)
	}
	_, err = mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueType("dynamic")}},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported value_type") {
		t.Fatalf("unknown value type err=%v want unsupported value_type", err)
	}
}

func TestCollectionFindByMissingIndexReturnsNil(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	ids, err := col.FindByIndexValue("missing", "hnl")
	if err != nil {
		t.Fatalf("find by missing index: %v", err)
	}
	if ids != nil {
		t.Fatalf("missing index ids=%q want nil", ids)
	}
	ids, truncated, err := col.FindByIndexValueLimit("missing", "hnl", 1)
	if err != nil {
		t.Fatalf("find by missing index limit: %v", err)
	}
	if ids != nil || truncated {
		t.Fatalf("missing index limit ids=%q truncated=%v want nil/false", ids, truncated)
	}
	ids, truncated, err = col.FindByIndexRange("missing", IndexRangeOptions{
		Lower: IndexRangeBound{Value: "hnl", Inclusive: true},
		Upper: IndexRangeBound{Value: "hnl", Inclusive: true},
	})
	if err != nil {
		t.Fatalf("find missing index range: %v", err)
	}
	if ids != nil || truncated {
		t.Fatalf("missing index range ids=%q truncated=%v want nil/false", ids, truncated)
	}
	records, truncated, err := col.FindDocumentsByIndexRange("missing", IndexRangeOptions{
		Lower: IndexRangeBound{Value: "hnl", Inclusive: true},
		Upper: IndexRangeBound{Value: "hnl", Inclusive: true},
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("find documents missing index range: %v", err)
	}
	if records != nil || truncated {
		t.Fatalf("missing index document range records=%v truncated=%v want nil/false", records, truncated)
	}
}

func TestCollectionFindByIndexValueMatchesLargeJSONInteger(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "big", Field: "big", ValueType: IndexValueInt64}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"big":9007199254740993}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	ids, err := col.FindByIndexValue("big", int64(9007199254740993))
	if err != nil {
		t.Fatalf("find by index value: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("ids=%q want u1", ids)
	}
}

func TestCollectionIndexesCanonicalExtendedJSONNumbers(t *testing.T) {
	cases := []struct {
		name   string
		format DocumentFormat
		encode func([]byte) ([]byte, error)
	}{
		{
			name:   "json",
			format: DocumentFormatJSON,
			encode: func(doc []byte) ([]byte, error) {
				return doc, nil
			},
		},
		{
			name:   "template-v1",
			format: DocumentFormatTemplateV1,
			encode: EncodeTemplateV1DocumentJSON,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = d.Close() }()

			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(&CollectionMeta{
				Name: "users",
				Options: CollectionOptions{
					DocumentFormat: tc.format,
				},
				Indexes: []IndexDefinition{
					{Name: "age", Field: "age", ValueType: IndexValueInt64},
					{Name: "score", Field: "score", ValueType: IndexValueDouble},
				},
			}); err != nil {
				t.Fatalf("create collection: %v", err)
			}
			col, err := mgr.OpenCollection("users")
			if err != nil {
				t.Fatalf("open collection: %v", err)
			}
			doc, err := tc.encode([]byte(`{"age":{"$numberLong":"42"},"score":{"$numberDouble":"2.5"}}`))
			if err != nil {
				t.Fatalf("encode document: %v", err)
			}
			if _, err := col.Insert([]byte("u1"), doc); err != nil {
				t.Fatalf("insert: %v", err)
			}

			ageIDs, err := col.FindByIndexValue("age", int64(42))
			if err != nil {
				t.Fatalf("find age: %v", err)
			}
			if len(ageIDs) != 1 || !bytes.Equal(ageIDs[0], []byte("u1")) {
				t.Fatalf("age ids=%q want u1", ageIDs)
			}
			scoreIDs, truncated, err := col.FindByIndexRange("score", IndexRangeOptions{
				Lower: IndexRangeBound{Value: 2.0, Inclusive: false},
				Upper: IndexRangeBound{Value: 3.0, Inclusive: true},
			})
			if err != nil {
				t.Fatalf("find score range: %v", err)
			}
			if truncated || len(scoreIDs) != 1 || !bytes.Equal(scoreIDs[0], []byte("u1")) {
				t.Fatalf("score ids=%q truncated=%v want u1 false", scoreIDs, truncated)
			}
		})
	}
}

func TestCollectionFindByIndexRangeTypedInt64(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "score", Field: "score", ValueType: IndexValueInt64}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2"), []byte("u3"), []byte("u4")},
		[][]byte{
			[]byte(`{"score":-10}`),
			[]byte(`{"score":0}`),
			[]byte(`{"score":2}`),
			[]byte(`{"score":10}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	// Use a non-equality range so this exercises non-exact range materialization,
	// not the exact-prefix helper that already used pending root runs.
	ids, truncated, err := col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(0), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(10), Inclusive: false},
	})
	if err != nil {
		t.Fatalf("find range: %v", err)
	}
	if truncated || len(ids) != 2 || !bytes.Equal(ids[0], []byte("u2")) || !bytes.Equal(ids[1], []byte("u3")) {
		t.Fatalf("ids=%q truncated=%v want u2,u3 false", ids, truncated)
	}

	ids, truncated, err = col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(0), Inclusive: true},
		Upper: IndexRangeBound{Unbounded: true},
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("find limited range: %v", err)
	}
	if !truncated || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("limited ids=%q truncated=%v want u2 true", ids, truncated)
	}
	ids, truncated, err = col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Unbounded: true},
		Upper: IndexRangeBound{Value: int64(0), Inclusive: false},
	})
	if err != nil {
		t.Fatalf("find upper-only range: %v", err)
	}
	if truncated || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("upper-only ids=%q truncated=%v want u1 false", ids, truncated)
	}
	ids, truncated, err = col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(10), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(10), Inclusive: true},
	})
	if err != nil {
		t.Fatalf("find equality range: %v", err)
	}
	if truncated || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u4")) {
		t.Fatalf("equality ids=%q truncated=%v want u4 false", ids, truncated)
	}
	ids, truncated, err = col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(10), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(10), Inclusive: false},
	})
	if err != nil {
		t.Fatalf("find empty half-open range: %v", err)
	}
	if truncated || len(ids) != 0 {
		t.Fatalf("empty half-open ids=%q truncated=%v want none false", ids, truncated)
	}
	if _, _, err := col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Unbounded: true},
		Upper: IndexRangeBound{Unbounded: true},
		Desc:  true,
	}); err == nil || !strings.Contains(err.Error(), "descending index range scans are not supported") {
		t.Fatalf("descending range err=%v want unsupported descending", err)
	}
	if _, _, err := col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: "0", Inclusive: true},
		Upper: IndexRangeBound{Unbounded: true},
	}); err == nil || !strings.Contains(err.Error(), "int64-compatible") {
		t.Fatalf("wrong-type range err=%v want int64-compatible", err)
	}
}

func requireJSONFieldValue(t *testing.T, document []byte, field string, want any) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	var decoded map[string]any
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("decode document %s: %v", document, err)
	}
	got, ok := decoded[field]
	if !ok {
		t.Fatalf("document %s missing field %q", document, field)
	}
	switch want := want.(type) {
	case string:
		gotString, ok := got.(string)
		if !ok || gotString != want {
			t.Fatalf("document %s field %q=%v want %q", document, field, got, want)
		}
	case int64:
		gotNumber, ok := got.(json.Number)
		if !ok {
			t.Fatalf("document %s field %q=%v want JSON number %d", document, field, got, want)
		}
		gotInt, err := gotNumber.Int64()
		if err != nil || gotInt != want {
			t.Fatalf("document %s field %q=%v want %d", document, field, got, want)
		}
	default:
		t.Fatalf("unsupported expected JSON field type %T", want)
	}
}

func TestCollectionFindDocumentsByIndexRangeTypedInt64(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "score", Field: "score", ValueType: IndexValueInt64}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2"), []byte("u3")},
		[][]byte{
			[]byte(`{"score":-10,"name":"low"}`),
			[]byte(`{"score":0,"name":"zero"}`),
			[]byte(`{"score":2,"name":"two"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	records, truncated, err := col.FindDocumentsByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(0), Inclusive: true},
		Upper: IndexRangeBound{Unbounded: true},
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("find documents range: %v", err)
	}
	if !truncated || len(records) != 1 {
		t.Fatalf("records len=%d truncated=%v want 1,true", len(records), truncated)
	}
	if !bytes.Equal(records[0].ID, []byte("u2")) {
		t.Fatalf("record id=%q want u2", records[0].ID)
	}
	requireJSONFieldValue(t, records[0].Document, "name", "zero")

	records, truncated, err = col.FindDocumentsByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(100), Inclusive: true},
		Upper: IndexRangeBound{Unbounded: true},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("find empty documents range: %v", err)
	}
	if truncated || len(records) != 0 {
		t.Fatalf("empty records len=%d truncated=%v want 0,false", len(records), truncated)
	}

	if _, truncated, err = col.FindDocumentsByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: "not-an-int", Inclusive: true},
		Upper: IndexRangeBound{Unbounded: true},
		Limit: 1,
	}); err == nil || truncated {
		t.Fatalf("bad bound err=%v truncated=%v want error,false", err, truncated)
	}
	if _, _, err := col.FindDocumentsByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(0), Inclusive: true},
		Upper: IndexRangeBound{Unbounded: true},
	}); err == nil || !strings.Contains(err.Error(), "positive limit") {
		t.Fatalf("unlimited document range err=%v want positive limit", err)
	}
}

func TestCollectionDocumentIndexRangeRejectsOrderedBSONV2Indexes(t *testing.T) {
	valueDocument, err := bson.Marshal(bson.D{{Key: "createdAt", Value: int32(7)}})
	if err != nil {
		t.Fatalf("marshal valid BSON range value: %v", err)
	}
	value := bson.Raw(valueDocument).Lookup("createdAt")
	opts := IndexRangeOptions{
		Lower: IndexRangeBound{Value: value, Inclusive: true},
		Upper: IndexRangeBound{Value: value, Inclusive: true},
		Limit: 1,
	}
	for _, index := range []IndexDefinition{
		{
			Name:       "created_desc",
			ValueType:  IndexValueBSONOrderedV2,
			Components: []IndexComponent{{Field: "createdAt", Direction: IndexDirectionDescending}},
		},
		{
			Name:      "tenant_created",
			ValueType: IndexValueBSONOrderedV2,
			Components: []IndexComponent{
				{Field: "tenant", Direction: IndexDirectionAscending},
				{Field: "createdAt", Direction: IndexDirectionDescending},
			},
		},
	} {
		t.Run(index.Name, func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			mgr := NewCollectionManager(db)
			if _, err := mgr.CreateCollection(&CollectionMeta{
				Name:    "events",
				Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
				Indexes: []IndexDefinition{index},
			}); err != nil {
				t.Fatalf("CreateCollection: %v", err)
			}
			col, err := mgr.OpenCollection("events")
			if err != nil {
				t.Fatalf("OpenCollection: %v", err)
			}

			records, truncated, err := col.FindDocumentsByIndexRange(index.Name, opts)
			if err == nil || !strings.Contains(err.Error(), "require FindByCompoundIndexRange") || records != nil || truncated {
				t.Fatalf("FindDocumentsByIndexRange records=%v truncated=%v err=%v want ordered BSON v2 rejection", records, truncated, err)
			}
			called := false
			truncated, err = col.ScanBorrowedDocumentsByIndexRange(index.Name, opts, func(BorrowedDocumentRecord) (bool, error) {
				called = true
				return true, nil
			})
			if err == nil || !strings.Contains(err.Error(), "require FindByCompoundIndexRange") || called || truncated {
				t.Fatalf("ScanBorrowedDocumentsByIndexRange called=%v truncated=%v err=%v want ordered BSON v2 rejection", called, truncated, err)
			}
		})
	}
}

func TestCollectionScanBorrowedDocumentsByIndexRangeContracts(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "score", Field: "score", ValueType: IndexValueInt64}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"score":1,"name":"one"}`),
			[]byte(`{"score":2,"name":"two"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush rows: %v", err)
	}
	opts := IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(1), Inclusive: true},
		Upper: IndexRangeBound{Unbounded: true},
		Limit: 2,
	}
	if _, err := col.ScanBorrowedDocumentsByIndexRange("score", opts, nil); err == nil || !strings.Contains(err.Error(), "nil borrowed") {
		t.Fatalf("nil borrowed callback err=%v want nil-callback error", err)
	}
	called := false
	truncated, err := col.ScanBorrowedDocumentsByIndexRange("missing", opts, func(BorrowedDocumentRecord) (bool, error) {
		called = true
		return true, nil
	})
	if err != nil || truncated || called {
		t.Fatalf("missing index truncated=%v called=%v err=%v want false,false,nil", truncated, called, err)
	}
	if _, err := col.ScanBorrowedDocumentsByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(1), Inclusive: true},
		Upper: IndexRangeBound{Unbounded: true},
	}, func(BorrowedDocumentRecord) (bool, error) { return true, nil }); err == nil || !strings.Contains(err.Error(), "positive limit") {
		t.Fatalf("zero limit borrowed err=%v want positive limit", err)
	}
	if _, err := col.ScanBorrowedDocumentsByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(1), Inclusive: true},
		Upper: IndexRangeBound{Unbounded: true},
		Limit: 1,
		Desc:  true,
	}, func(BorrowedDocumentRecord) (bool, error) { return true, nil }); err == nil || !strings.Contains(err.Error(), "descending") {
		t.Fatalf("descending borrowed err=%v want descending error", err)
	}
	var ids [][]byte
	truncated, err = col.ScanBorrowedDocumentsByIndexRange("score", opts, func(record BorrowedDocumentRecord) (bool, error) {
		ids = append(ids, bytes.Clone(record.ID))
		return false, nil
	})
	if err != nil || truncated || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("early stop ids=%q truncated=%v err=%v want u1,false,nil", ids, truncated, err)
	}
}

func TestCollectionFindDocumentsByIndexRangeUsesBufferedPrimaryView(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "score", Field: "score", ValueType: IndexValueInt64}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"score":5,"name":"old-indexed"}`),
			[]byte(`{"score":8,"name":"old-primary"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush persisted rows: %v", err)
	}
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONField("score", int64(7))},
		{DocumentID: []byte("u2"), Update: setJSONField("name", "fresh-primary")},
	}); err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("batched=%v want buffered update batch", batched)
	}

	records, truncated, err := col.FindDocumentsByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(7), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(9), Inclusive: false},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("find documents range: %v", err)
	}
	if truncated || len(records) != 2 {
		t.Fatalf("records len=%d truncated=%v want 2,false", len(records), truncated)
	}
	if !bytes.Equal(records[0].ID, []byte("u1")) {
		t.Fatalf("first record id=%q want u1", records[0].ID)
	}
	requireJSONFieldValue(t, records[0].Document, "score", int64(7))
	if !bytes.Equal(records[1].ID, []byte("u2")) {
		t.Fatalf("second record id=%q want u2", records[1].ID)
	}
	requireJSONFieldValue(t, records[1].Document, "name", "fresh-primary")
}

func TestCollectionFindByIndexRangeDedupesPersistedOnlyMultiKeyRange(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Indexes: []IndexDefinition{
			{Name: "tag", Field: "tags", ValueType: IndexValueString, MultiKey: true},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1"), []byte("d2")},
		[][]byte{
			[]byte(`{"tags":["a","b"]}`),
			[]byte(`{"tags":["c"]}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	ids, truncated, err := reopenedCol.FindByIndexRange("tag", IndexRangeOptions{
		Lower: IndexRangeBound{Value: "a", Inclusive: true},
		Upper: IndexRangeBound{Value: "d", Inclusive: false},
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("find range: %v", err)
	}
	if truncated || len(ids) != 2 || !bytes.Equal(ids[0], []byte("d1")) || !bytes.Equal(ids[1], []byte("d2")) {
		t.Fatalf("ids=%q truncated=%v want d1,d2 false", ids, truncated)
	}
}

func TestCollectionFindByIndexRangeDedupesAllowedArrayScalarIndex(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			AllowArrayValuesInIndex: true,
		},
		Indexes: []IndexDefinition{
			{Name: "tag", Field: "tags", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1"), []byte("d2")},
		[][]byte{
			[]byte(`{"tags":["a","b"]}`),
			[]byte(`{"tags":["c"]}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	ids, truncated, err := col.FindByIndexRange("tag", IndexRangeOptions{
		Lower: IndexRangeBound{Value: "a", Inclusive: true},
		Upper: IndexRangeBound{Value: "d", Inclusive: false},
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("find range: %v", err)
	}
	if truncated || len(ids) != 2 || !bytes.Equal(ids[0], []byte("d1")) || !bytes.Equal(ids[1], []byte("d2")) {
		t.Fatalf("ids=%q truncated=%v want d1,d2 false", ids, truncated)
	}
}

func TestCollectionFindByIndexRangePersistedOnlyFastPathLimitAndClones(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "score", Field: "score", ValueType: IndexValueInt64}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2"), []byte("u3")},
		[][]byte{
			[]byte(`{"score":1}`),
			[]byte(`{"score":2}`),
			[]byte(`{"score":3}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	ids, truncated, err := col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(1), Inclusive: true},
		Upper: IndexRangeBound{Unbounded: true},
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("find range: %v", err)
	}
	if !truncated || len(ids) != 2 || !bytes.Equal(ids[0], []byte("u1")) || !bytes.Equal(ids[1], []byte("u2")) {
		t.Fatalf("ids=%q truncated=%v want u1,u2 true", ids, truncated)
	}
	ids[0][0] = 'x'
	if !bytes.Equal(ids[1], []byte("u2")) {
		t.Fatalf("ids[1]=%q changed after mutating ids[0]", ids[1])
	}
	again, truncated, err := col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(1), Inclusive: true},
		Upper: IndexRangeBound{Unbounded: true},
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("find range again: %v", err)
	}
	if !truncated || len(again) != 1 || !bytes.Equal(again[0], []byte("u1")) {
		t.Fatalf("again ids=%q truncated=%v want u1 true", again, truncated)
	}
}

func TestCollectionFindDocumentsByIndexRangeUniqueExactBufferedTombstone(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","name":"ada"}`)},
	); err != nil {
		t.Fatalf("insert persisted row: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush persisted row: %v", err)
	}
	primaryTable := newCollectionRunTable(1)
	setCollectionRunValue(primaryTable, []byte("u1"), []byte(`{"email":"grace@example.com","name":"ada"}`))
	primaryTable.Freeze()
	defer resetCollectionRunTable(primaryTable)

	oldEmail, err := encodeIndexScalar(IndexValueString, "ada@example.com")
	if err != nil {
		t.Fatalf("encode old email: %v", err)
	}
	newEmail, err := encodeIndexScalar(IndexValueString, "grace@example.com")
	if err != nil {
		t.Fatalf("encode new email: %v", err)
	}
	emailTable := newCollectionRunTable(2)
	if _, err := deleteCollectionSecondaryIndexEntry(emailTable, oldEmail, []byte("u1")); err != nil {
		t.Fatalf("delete old email index entry: %v", err)
	}
	if _, err := setCollectionSecondaryIndexEntry(emailTable, newEmail, []byte("u1")); err != nil {
		t.Fatalf("set new email index entry: %v", err)
	}
	emailTable.Freeze()
	defer resetCollectionRunTable(emailTable)

	domain := col.writeDomain
	domain.mu.Lock()
	domain.count = 1
	domain.meta = col.Meta()
	domain.rootRuns = map[string][]memtable.Table{
		collectionPrimaryRootName("users"):            {primaryTable},
		collectionSecondaryRootName("users", "email"): {emailTable},
	}
	domain.rootRunCount = 2
	domain.mu.Unlock()

	records, truncated, err := col.FindDocumentsByIndexRange("email", IndexRangeOptions{
		Lower: IndexRangeBound{Value: "ada@example.com", Inclusive: true},
		Upper: IndexRangeBound{Value: "ada@example.com", Inclusive: true},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("find old email exact range: %v", err)
	}
	if truncated || len(records) != 0 {
		t.Fatalf("old email records=%+v truncated=%v want none false", records, truncated)
	}

	records, truncated, err = col.FindDocumentsByIndexRange("email", IndexRangeOptions{
		Lower: IndexRangeBound{Value: "grace@example.com", Inclusive: true},
		Upper: IndexRangeBound{Value: "grace@example.com", Inclusive: true},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("find new email exact range: %v", err)
	}
	if truncated || len(records) != 1 || !bytes.Equal(records[0].ID, []byte("u1")) {
		t.Fatalf("new email records=%+v truncated=%v want u1 false", records, truncated)
	}
	requireJSONFieldValue(t, records[0].Document, "email", "grace@example.com")
}

func TestCollectionFindDocumentsByIndexRangeExactBufferedTombstoneAfterLimit(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"email":"ada@example.com","name":"stale"}`)},
	); err != nil {
		t.Fatalf("insert persisted row: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush persisted row: %v", err)
	}

	email, err := encodeIndexScalar(IndexValueString, "ada@example.com")
	if err != nil {
		t.Fatalf("encode email: %v", err)
	}
	primaryTable := newCollectionRunTable(1)
	setCollectionRunValue(primaryTable, []byte("u0"), []byte(`{"email":"ada@example.com","name":"live"}`))
	primaryTable.Freeze()
	defer resetCollectionRunTable(primaryTable)

	emailTable := newCollectionRunTable(2)
	if _, err := setCollectionSecondaryIndexEntry(emailTable, email, []byte("u0")); err != nil {
		t.Fatalf("set live email index entry: %v", err)
	}
	if _, err := deleteCollectionSecondaryIndexEntry(emailTable, email, []byte("u2")); err != nil {
		t.Fatalf("delete stale email index entry: %v", err)
	}
	emailTable.Freeze()
	defer resetCollectionRunTable(emailTable)

	domain := col.writeDomain
	domain.mu.Lock()
	domain.count = 1
	domain.meta = col.Meta()
	domain.rootRuns = map[string][]memtable.Table{
		collectionPrimaryRootName("users"):            {primaryTable},
		collectionSecondaryRootName("users", "email"): {emailTable},
	}
	domain.rootRunCount = 2
	domain.mu.Unlock()

	records, truncated, err := col.FindDocumentsByIndexRange("email", IndexRangeOptions{
		Lower: IndexRangeBound{Value: "ada@example.com", Inclusive: true},
		Upper: IndexRangeBound{Value: "ada@example.com", Inclusive: true},
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("find exact email range: %v", err)
	}
	if truncated || len(records) != 1 || !bytes.Equal(records[0].ID, []byte("u0")) {
		t.Fatalf("records=%+v truncated=%v want u0 false", records, truncated)
	}
	requireJSONFieldValue(t, records[0].Document, "name", "live")
}

func TestCollectionFindByIndexRangeMergesBufferedUpdates(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "score", Field: "score", ValueType: IndexValueInt64}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2"), []byte("u3")},
		[][]byte{
			[]byte(`{"score":5}`),
			[]byte(`{"score":6}`),
			[]byte(`{"score":9}`),
		},
	); err != nil {
		t.Fatalf("insert persisted rows: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush persisted rows: %v", err)
	}
	before := d.State()
	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONField("score", int64(7))},
		{DocumentID: []byte("u2"), Update: setJSONField("score", int64(8))},
	})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatalf("batched=%v results=%+v want buffered update batch", batched, results)
	}
	if after := d.State(); after.CommitSeq != before.CommitSeq {
		t.Fatalf("buffered update advanced commit seq by %d, want 0", after.CommitSeq-before.CommitSeq)
	}

	// Use a non-equality range so this exercises non-exact range materialization,
	// not the exact-prefix helper that already used pending root runs.
	ids, truncated, err := col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(5), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(6), Inclusive: true},
	})
	if err != nil {
		t.Fatalf("find old score range: %v", err)
	}
	if truncated || len(ids) != 0 {
		t.Fatalf("old score ids=%q truncated=%v want none false", ids, truncated)
	}
	ids, truncated, err = col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(7), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(9), Inclusive: false},
	})
	if err != nil {
		t.Fatalf("find buffered score range: %v", err)
	}
	if truncated || len(ids) != 2 || !bytes.Equal(ids[0], []byte("u1")) || !bytes.Equal(ids[1], []byte("u2")) {
		t.Fatalf("buffered score ids=%q truncated=%v want u1,u2 false", ids, truncated)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush buffered updates: %v", err)
	}
	ids, truncated, err = col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(7), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(9), Inclusive: false},
	})
	if err != nil {
		t.Fatalf("find flushed score range: %v", err)
	}
	if truncated || len(ids) != 2 || !bytes.Equal(ids[0], []byte("u1")) || !bytes.Equal(ids[1], []byte("u2")) {
		t.Fatalf("flushed score ids=%q truncated=%v want u1,u2 false", ids, truncated)
	}
}

func TestCollectionFindByIndexRangeSeesQueuedIndexedFlushUnits(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "score", Field: "score", ValueType: IndexValueInt64}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2"), []byte("u3")},
		[][]byte{
			[]byte(`{"score":5}`),
			[]byte(`{"score":6}`),
			[]byte(`{"score":9}`),
		},
	); err != nil {
		t.Fatalf("insert persisted rows: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush persisted rows: %v", err)
	}
	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONField("score", int64(7))},
		{DocumentID: []byte("u2"), Update: setJSONField("score", int64(8))},
	})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatalf("batched=%v results=%+v want buffered update batch", batched, results)
	}
	col.writeDomain.mu.Lock()
	if !rotateIndexedMutableToFlushUnitLocked(col.writeDomain) {
		col.writeDomain.mu.Unlock()
		t.Fatal("rotate indexed mutable state returned false")
	}
	if got := len(col.writeDomain.indexedFlushUnits); got != 1 {
		col.writeDomain.mu.Unlock()
		t.Fatalf("queued flush units=%d want 1", got)
	}
	col.writeDomain.mu.Unlock()

	ids, truncated, err := col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(7), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(9), Inclusive: false},
	})
	if err != nil {
		t.Fatalf("find queued new score range: %v", err)
	}
	if truncated || len(ids) != 2 || !bytes.Equal(ids[0], []byte("u1")) || !bytes.Equal(ids[1], []byte("u2")) {
		t.Fatalf("queued new score ids=%q truncated=%v want u1,u2 false", ids, truncated)
	}
	ids, truncated, err = col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(5), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(7), Inclusive: false},
	})
	if err != nil {
		t.Fatalf("find queued old score range: %v", err)
	}
	if truncated || len(ids) != 0 {
		t.Fatalf("queued old score ids=%q truncated=%v want none false", ids, truncated)
	}
}

func TestCollectionFindByIndexRangeSeesPublishingIndexedFlushUnits(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "score", Field: "score", ValueType: IndexValueInt64}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2"), []byte("u3")},
		[][]byte{
			[]byte(`{"score":5}`),
			[]byte(`{"score":6}`),
			[]byte(`{"score":9}`),
		},
	); err != nil {
		t.Fatalf("insert persisted rows: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush persisted rows: %v", err)
	}
	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONField("score", int64(7))},
		{DocumentID: []byte("u2"), Update: setJSONField("score", int64(8))},
	})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatalf("batched=%v results=%+v want buffered update batch", batched, results)
	}
	work, err := col.prepareIndexedAsyncPublish()
	if err != nil {
		t.Fatalf("prepare async publish: %v", err)
	}
	if work == nil {
		t.Fatal("prepare async publish returned nil work")
	}
	defer collectionTestCloseIndexedFlushWork(work)

	ids, truncated, err := col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(7), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(9), Inclusive: false},
	})
	if err != nil {
		t.Fatalf("find publishing new score range: %v", err)
	}
	if truncated || len(ids) != 2 || !bytes.Equal(ids[0], []byte("u1")) || !bytes.Equal(ids[1], []byte("u2")) {
		t.Fatalf("publishing new score ids=%q truncated=%v want u1,u2 false", ids, truncated)
	}
	ids, truncated, err = col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(5), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(7), Inclusive: false},
	})
	if err != nil {
		t.Fatalf("find publishing old score range: %v", err)
	}
	if truncated || len(ids) != 0 {
		t.Fatalf("publishing old score ids=%q truncated=%v want none false", ids, truncated)
	}
	if err := col.publishPreparedIndexedFlush(work); err != nil {
		t.Fatalf("publish prepared async flush: %v", err)
	}
}

func TestCollectionFindByIndexRangeSkipsBufferedTombstone(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{{Name: "score", Field: "score", ValueType: IndexValueInt64}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"score":5}`), []byte(`{"score":5}`)},
	); err != nil {
		t.Fatalf("insert persisted rows: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush persisted rows: %v", err)
	}

	encoded, err := encodeIndexScalar(IndexValueInt64, int64(5))
	if err != nil {
		t.Fatalf("encode score: %v", err)
	}
	key, err := indexEntryKey(encoded, []byte("u1"))
	if err != nil {
		t.Fatalf("index key: %v", err)
	}
	table := newCollectionRunTable(1)
	table.DeleteSteal(key)
	table.Freeze()
	domain := col.writeDomain
	domain.mu.Lock()
	domain.count = 1
	domain.meta = col.Meta()
	domain.rootRuns = map[string][]memtable.Table{
		collectionSecondaryRootName("users", "score"): {table},
	}
	domain.mu.Unlock()

	// Use a non-equality range so this exercises bufferedIndexRangeTableLocked,
	// not the exact-prefix helper.
	ids, truncated, err := col.FindByIndexRange("score", IndexRangeOptions{
		Lower: IndexRangeBound{Value: int64(5), Inclusive: true},
		Upper: IndexRangeBound{Value: int64(6), Inclusive: false},
	})
	if err != nil {
		t.Fatalf("find range: %v", err)
	}
	if truncated || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("ids=%q truncated=%v want u2 false", ids, truncated)
	}
}

func TestCollectionCreateIndexBackfill_EmptyCollectionUpdatesSchema(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city", ValueType: IndexValueString}); err != nil {
		t.Fatalf("create index on empty collection: %v", err)
	}
	if _, ok := findIndex(col.Meta().Indexes, "city"); !ok {
		t.Fatalf("collection meta missing city index after empty create: %+v", col.Meta().Indexes)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
		t.Fatalf("insert after empty create index: %v", err)
	}
	ids, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city after empty create: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("city ids after empty create=%q want [u1]", ids)
	}
}

func TestCollectionCreateIndexBackfill_PreservesExistingIndexState(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"sfo"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city", ValueType: IndexValueString}); err != nil {
		t.Fatalf("create city index: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
	}

	emailIDs, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find deleted email: %v", err)
	}
	if len(emailIDs) != 0 {
		t.Fatalf("deleted email ids=%q want none", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find deleted city: %v", err)
	}
	if len(cityIDs) != 0 {
		t.Fatalf("deleted city ids=%q want none", cityIDs)
	}
	if _, err := col.Insert([]byte("u3"), []byte(`{"email":"ada@example.com","city":"hnl"}`)); err != nil {
		t.Fatalf("reuse unique email after delete: %v", err)
	}
}

func TestCollectionCreateIndexBackfill_ReopenUsesPersistedSchemaAndRoots(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}); err != nil {
		t.Fatalf("create unique index: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find email after reopen: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("email ids after reopen=%q want [u1]", ids)
	}
	if _, err := reopenedCol.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("duplicate insert err=%v want unique index conflict", err)
	}
}

func TestCollectionCreateIndexBackfill_RangeMatchesPersistedIndexAfterReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2"), []byte("u3")},
		[][]byte{
			[]byte(`{"score":5}`),
			[]byte(`{"score":7}`),
			[]byte(`{"score":9}`),
		},
	); err != nil {
		t.Fatalf("insert before index backfill: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "score", Field: "score", ValueType: IndexValueInt64}); err != nil {
		t.Fatalf("create score index: %v", err)
	}
	assertScoreRange := func(label string, c *Collection) {
		t.Helper()
		ids, truncated, err := c.FindByIndexRange("score", IndexRangeOptions{
			Lower: IndexRangeBound{Value: int64(6), Inclusive: true},
			Upper: IndexRangeBound{Value: int64(9), Inclusive: false},
		})
		if err != nil {
			t.Fatalf("%s find range: %v", label, err)
		}
		if truncated || len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
			t.Fatalf("%s ids=%q truncated=%v want u2 false", label, ids, truncated)
		}
	}
	assertScoreRange("backfilled", col)
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	assertScoreRange("reopened", reopenedCol)
}

func TestCollectionCreateIndexBackfill_RejectsUniqueConflictAtomically(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"ada@example.com"}`),
		},
	); err != nil {
		t.Fatalf("insert duplicate documents before unique index: %v", err)
	}

	_, err = col.CreateIndex(IndexDefinition{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true})
	if err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("create unique index err=%v want unique index conflict", err)
	}
	if _, ok := findIndex(col.Meta().Indexes, "email"); ok {
		t.Fatalf("collection meta gained failed email index: %+v", col.Meta().Indexes)
	}
	ids, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find failed index: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("failed index visible ids=%q want none", ids)
	}
}

func TestCollectionReplaceRejectsUniqueConflictAtomically(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"sfo"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	replaced, err := col.Replace([]byte("u1"), []byte(`{"email":"grace@example.com","city":"hnl"}`))
	if !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("replace err=%v want ErrUniqueIndexConflict", err)
	}
	if replaced {
		t.Fatal("replace reported success on unique conflict")
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1 after failed replace: %v", err)
	}
	if want := []byte(`{"email":"ada@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("u1 after failed replace=%q want %q", got, want)
	}

	replaced, err = col.Replace([]byte("u1"), []byte(`{"email":"ada2@example.com","city":"hnl"}`))
	if err != nil {
		t.Fatalf("replace valid: %v", err)
	}
	if !replaced {
		t.Fatal("replace valid reported false")
	}
	ids, err := col.FindByIndex("email", "ada2@example.com")
	if err != nil {
		t.Fatalf("find replacement email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("replacement email ids=%q want u1", ids)
	}
	ids, err = col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find old email: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("old email ids=%q want none", ids)
	}

	beforeState := d.State()
	if beforeState == nil {
		t.Fatal("missing db state before identical replace")
	}
	replaced, err = col.Replace([]byte("u1"), []byte(`{"email":"ada2@example.com","city":"hnl"}`))
	if err != nil {
		t.Fatalf("replace identical: %v", err)
	}
	if !replaced {
		t.Fatal("replace identical reported false")
	}
	afterState := d.State()
	if afterState == nil {
		t.Fatal("missing db state after identical replace")
	}
	if afterState.SystemRootPageID != beforeState.SystemRootPageID {
		t.Fatalf("identical replace changed system root from %d to %d", beforeState.SystemRootPageID, afterState.SystemRootPageID)
	}
}

func TestCollectionUpdateConcurrentCounterNoLostUpdates(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"count":0}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	const (
		workers    = 8
		increments = 5
	)
	workerCols := make([]*Collection, workers)
	for i := range workerCols {
		workerCol, err := mgr.OpenCollection("users")
		if err != nil {
			t.Fatalf("open worker collection: %v", err)
		}
		workerCols[i] = workerCol
	}
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerCol *Collection) {
			defer wg.Done()
			<-start
			for j := 0; j < increments; j++ {
				matched, modified, err := workerCol.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
					var doc struct {
						Count int `json:"count"`
					}
					if err := json.Unmarshal(current, &doc); err != nil {
						return nil, false, err
					}
					doc.Count++
					next, err := json.Marshal(doc)
					if err != nil {
						return nil, false, err
					}
					return next, true, nil
				})
				if err != nil {
					errs <- err
					return
				}
				if !matched || !modified {
					errs <- fmt.Errorf("update matched=%v modified=%v", matched, modified)
					return
				}
			}
			errs <- nil
		}(workerCols[i])
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("update: %v", err)
		}
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get counter: %v", err)
	}
	var doc struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatalf("unmarshal counter: %v", err)
	}
	if want := workers * increments; doc.Count != want {
		t.Fatalf("count=%d want %d", doc.Count, want)
	}
}

func TestCollectionUpdateConcurrentIndexedNoRetryExhaustion(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	const documents = 32
	ids := make([][]byte, documents)
	docs := make([][]byte, documents)
	for i := 0; i < documents; i++ {
		ids[i] = []byte(fmt.Sprintf("u%02d", i))
		docs[i] = []byte(fmt.Sprintf(`{"email":"user%02d@example.test","city":"hnl","count":0}`, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	const (
		workers = 8
		updates = 100
	)
	workerCols := make([]*Collection, workers)
	for i := range workerCols {
		workerCol, err := mgr.OpenCollection("users")
		if err != nil {
			t.Fatalf("open worker collection: %v", err)
		}
		workerCols[i] = workerCol
	}
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int, workerCol *Collection) {
			defer wg.Done()
			<-start
			for update := 0; update < updates; update++ {
				id := ids[(worker*updates+update*37)%documents]
				matched, modified, err := workerCol.Update(id, func(current []byte) ([]byte, bool, error) {
					var doc struct {
						Email string `json:"email"`
						City  string `json:"city"`
						Count int    `json:"count"`
					}
					if err := json.Unmarshal(current, &doc); err != nil {
						return nil, false, err
					}
					doc.Count++
					next, err := json.Marshal(doc)
					if err != nil {
						return nil, false, err
					}
					return next, true, nil
				})
				if err != nil {
					errs <- err
					return
				}
				if !matched || !modified {
					errs <- fmt.Errorf("update matched=%v modified=%v", matched, modified)
					return
				}
			}
			errs <- nil
		}(worker, workerCols[worker])
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("update: %v", err)
		}
	}

	total := 0
	for _, id := range ids {
		got, err := col.Get(id)
		if err != nil {
			t.Fatalf("get %q: %v", id, err)
		}
		var doc struct {
			Count int `json:"count"`
		}
		if err := json.Unmarshal(got, &doc); err != nil {
			t.Fatalf("unmarshal %q: %v", id, err)
		}
		total += doc.Count
	}
	if want := workers * updates; total != want {
		t.Fatalf("total count=%d want %d", total, want)
	}
}

func TestCollectionUpdateAllowsUnrelatedUserRootCommit(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"count":0}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	calls := 0
	matched, modified, err := col.Update([]byte("u1"), func(current []byte) ([]byte, bool, error) {
		calls++
		if err := d.Set([]byte(fmt.Sprintf("raw/unrelated/%02d", calls)), []byte("value")); err != nil {
			return nil, false, err
		}
		var doc struct {
			Count int `json:"count"`
		}
		if err := json.Unmarshal(current, &doc); err != nil {
			return nil, false, err
		}
		doc.Count++
		next, err := json.Marshal(doc)
		if err != nil {
			return nil, false, err
		}
		return next, true, nil
	})
	if err != nil {
		t.Fatalf("update with unrelated user-root commit: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("update matched=%t modified=%t want true true", matched, modified)
	}
	if calls != 1 {
		t.Fatalf("update callback calls=%d want 1", calls)
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get updated doc: %v", err)
	}
	var doc struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatalf("unmarshal updated doc: %v", err)
	}
	if doc.Count != 1 {
		t.Fatalf("count=%d want 1", doc.Count)
	}
	raw, err := d.Get([]byte("raw/unrelated/01"))
	if err != nil {
		t.Fatalf("get unrelated raw key: %v", err)
	}
	if !bytes.Equal(raw, []byte("value")) {
		t.Fatalf("raw value=%q want value", raw)
	}
}

func TestCollectionUpdateSkipsIndexRootsWhenIndexedValuesUnchanged(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl","seen":false}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush indexed memtables: %v", err)
	}

	loadCatalog := func() *collectionCatalog {
		t.Helper()
		snap := d.AcquireSnapshot()
		if snap == nil {
			t.Fatal("expected snapshot")
		}
		defer func() { _ = snap.Close() }()
		catalog, err := loadCollectionCatalog(snap, "users")
		if err != nil {
			t.Fatalf("load catalog: %v", err)
		}
		if catalog == nil {
			t.Fatal("missing catalog")
		}
		return catalog
	}
	roots := func(catalog *collectionCatalog) map[string]uint64 {
		t.Helper()
		names := []string{
			collectionPrimaryRootName("users"),
			collectionIndexStateRootName("users"),
			collectionSecondaryRootName("users", "email"),
			collectionSecondaryRootName("users", "city"),
		}
		out := make(map[string]uint64, len(names))
		for _, name := range names {
			rootID := catalog.rootID(name)
			if rootID == 0 {
				t.Fatalf("root %q was not persisted", name)
			}
			out[name] = rootID
		}
		return out
	}

	before := roots(loadCatalog())
	matched, modified, err := col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"email":"ada@example.com","city":"hnl","seen":true}`), true, nil
	})
	if err != nil {
		t.Fatalf("update non-indexed field: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("update matched=%v modified=%v want true/true", matched, modified)
	}
	after := roots(loadCatalog())
	for _, rootName := range []string{
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	} {
		if after[rootName] != before[rootName] {
			t.Fatalf("root %q changed from %d to %d for non-indexed update", rootName, before[rootName], after[rootName])
		}
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get updated document: %v", err)
	}
	if !bytes.Contains(got, []byte(`"seen":true`)) {
		t.Fatalf("updated document=%q want seen=true", got)
	}

	matched, modified, err = col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"email":"ada2@example.com","city":"hnl","seen":true}`), true, nil
	})
	if err != nil {
		t.Fatalf("update indexed field: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("indexed update matched=%v modified=%v want true/true", matched, modified)
	}
	afterIndexed := roots(loadCatalog())
	for _, rootName := range []string{
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "email"),
	} {
		if afterIndexed[rootName] == after[rootName] {
			t.Fatalf("root %q did not change for indexed update", rootName)
		}
	}
	if rootName := collectionSecondaryRootName("users", "city"); afterIndexed[rootName] != after[rootName] {
		t.Fatalf("root %q changed from %d to %d for email-only update", rootName, after[rootName], afterIndexed[rootName])
	}
	ids, err := col.FindByIndexValue("email", "ada2@example.com")
	if err != nil {
		t.Fatalf("find new email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("new email ids=%q want u1", ids)
	}
	ids, err = col.FindByIndexValue("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find old email: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("old email ids=%q want none", ids)
	}
}

func TestCollectionUpdateBatchSkipsUnchangedSecondaryIndexes(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DisableIndexedWriteMemtables: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
			{Name: "active", Field: "active", ValueType: IndexValueBool},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":"hnl","active":true,"seen":false}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	loadCatalog := func() *collectionCatalog {
		t.Helper()
		snap := d.AcquireSnapshot()
		if snap == nil {
			t.Fatal("expected snapshot")
		}
		defer func() { _ = snap.Close() }()
		catalog, err := loadCollectionCatalog(snap, "users")
		if err != nil {
			t.Fatalf("load catalog: %v", err)
		}
		if catalog == nil {
			t.Fatal("missing catalog")
		}
		return catalog
	}
	roots := func(catalog *collectionCatalog) map[string]uint64 {
		t.Helper()
		names := []string{
			collectionPrimaryRootName("users"),
			collectionIndexStateRootName("users"),
			collectionSecondaryRootName("users", "email"),
			collectionSecondaryRootName("users", "city"),
			collectionSecondaryRootName("users", "active"),
		}
		out := make(map[string]uint64, len(names))
		for _, name := range names {
			rootID := catalog.rootID(name)
			if rootID == 0 {
				t.Fatalf("root %q was not persisted", name)
			}
			out[name] = rootID
		}
		return out
	}

	before := roots(loadCatalog())
	results, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"email":"ada@example.com","city":"sea","active":true,"seen":false}`), true, nil
		},
	}})
	if err != nil {
		t.Fatalf("update city: %v", err)
	}
	if got := results; len(got) != 1 || !got[0].Matched || !got[0].Modified {
		t.Fatalf("results=%+v want one matched modified row", got)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.SecondaryDeleteEntries, 1; got != want {
		t.Fatalf("secondary deletes=%d want %d", got, want)
	}
	if got, want := stats.SecondarySetEntries, 1; got != want {
		t.Fatalf("secondary sets=%d want %d", got, want)
	}
	if got, want := len(stats.SecondaryRuns), 1; got != want {
		t.Fatalf("secondary runs=%d want %d: %+v", got, want, stats.SecondaryRuns)
	}
	if run := stats.SecondaryRuns[0]; run.IndexName != "city" || run.Deletes != 1 || run.Sets != 1 || run.KeyBytes == 0 {
		t.Fatalf("city secondary run stats=%+v want city delete+set with key bytes", run)
	}
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("index value changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 2; got != want {
		t.Fatalf("index value unchanged=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexChecks, 0; got != want {
		t.Fatalf("unique index checks=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexCheckSkips, 1; got != want {
		t.Fatalf("unique index check skips=%d want %d", got, want)
	}
	indexStats := stats.IndexStats[:stats.IndexStatsCount]
	if got, want := len(indexStats), 3; got != want {
		t.Fatalf("index decision stats=%d want %d: %+v", got, want, stats.IndexStats)
	}
	indexStatByName := func(stats []CollectionUpdateIndexStats, name string) CollectionUpdateIndexStats {
		t.Helper()
		for _, stat := range stats {
			if stat.IndexName == name {
				return stat
			}
		}
		t.Fatalf("missing index stat %q in %+v", name, stats)
		return CollectionUpdateIndexStats{}
	}
	if idx := indexStatByName(indexStats, "email"); !idx.Unique || idx.Changed != 0 || idx.Unchanged != 1 || idx.UniqueChecks != 0 || idx.UniqueCheckSkips != 1 || idx.SecondaryRuns != 0 {
		t.Fatalf("email index decision stats=%+v want unchanged unique skip", idx)
	}
	if idx := indexStatByName(indexStats, "city"); idx.Unique || idx.Changed != 1 || idx.Unchanged != 0 || idx.SecondaryRuns != 1 || idx.SecondaryDeletes != 1 || idx.SecondarySets != 1 || idx.SecondaryKeyBytes == 0 {
		t.Fatalf("city index decision stats=%+v want changed secondary run", idx)
	}
	if idx := indexStatByName(indexStats, "active"); idx.Unique || idx.Changed != 0 || idx.Unchanged != 1 || idx.SecondaryRuns != 0 || idx.SecondaryDeletes != 0 || idx.SecondarySets != 0 || idx.SecondaryKeyBytes != 0 {
		t.Fatalf("active index decision stats=%+v want unchanged/no secondary work", idx)
	}
	stats.SecondaryRuns[0].IndexName = "mutated"
	if got := col.LastUpdateStats().SecondaryRuns[0].IndexName; got != "city" {
		t.Fatalf("LastUpdateStats did not return owned secondary-run stats, got %q", got)
	}
	stats.IndexStats[0].IndexName = "mutated"
	again := col.LastUpdateStats()
	if got := indexStatByName(again.IndexStats[:again.IndexStatsCount], "email").IndexName; got != "email" {
		t.Fatalf("LastUpdateStats did not return owned index decision stats, got %q", got)
	}

	afterCity := roots(loadCatalog())
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "city"),
	} {
		if afterCity[rootName] == before[rootName] {
			t.Fatalf("root %q did not change for city update", rootName)
		}
	}
	if rootName := collectionSecondaryRootName("users", "email"); afterCity[rootName] != before[rootName] {
		t.Fatalf("root %q changed from %d to %d for city-only update", rootName, before[rootName], afterCity[rootName])
	}
	if rootName := collectionSecondaryRootName("users", "active"); afterCity[rootName] != before[rootName] {
		t.Fatalf("root %q changed from %d to %d for city-only update", rootName, before[rootName], afterCity[rootName])
	}
	ids, err := col.FindByIndexValue("city", "sea")
	if err != nil {
		t.Fatalf("find city sea: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("city sea ids=%q want u1", ids)
	}
	ids, err = col.FindByIndexValue("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("email ids=%q want u1", ids)
	}

	beforeSame := roots(loadCatalog())
	if _, err := col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"email":"ada@example.com","city":"sea","active":true,"seen":true}`), true, nil
		},
	}}); err != nil {
		t.Fatalf("update same indexed values: %v", err)
	}
	stats = col.LastUpdateStats()
	if stats.SecondaryDeleteEntries != 0 || stats.SecondarySetEntries != 0 || stats.SecondaryKeyBytes != 0 || len(stats.SecondaryRuns) != 0 {
		t.Fatalf("same-index update secondary stats deletes=%d sets=%d bytes=%d runs=%+v, want no secondary work",
			stats.SecondaryDeleteEntries, stats.SecondarySetEntries, stats.SecondaryKeyBytes, stats.SecondaryRuns)
	}
	if got, want := stats.IndexValueChanges, 0; got != want {
		t.Fatalf("same-index update changes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 3; got != want {
		t.Fatalf("same-index update unchanged=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexChecks, 0; got != want {
		t.Fatalf("same-index update unique checks=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexCheckSkips, 1; got != want {
		t.Fatalf("same-index update unique check skips=%d want %d", got, want)
	}
	indexStats = stats.IndexStats[:stats.IndexStatsCount]
	if got, want := len(indexStats), 3; got != want {
		t.Fatalf("same-index update index decision stats=%d want %d: %+v", got, want, stats.IndexStats)
	}
	for _, idx := range indexStats {
		if idx.Changed != 0 || idx.Unchanged != 1 || idx.SecondaryRuns != 0 || idx.SecondaryDeletes != 0 || idx.SecondarySets != 0 || idx.SecondaryKeyBytes != 0 {
			t.Fatalf("same-index update per-index stats=%+v want unchanged/no secondary work", idx)
		}
	}
	afterSame := roots(loadCatalog())
	for _, rootName := range []string{
		collectionIndexStateRootName("users"),
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
		collectionSecondaryRootName("users", "active"),
	} {
		if afterSame[rootName] != beforeSame[rootName] {
			t.Fatalf("root %q changed from %d to %d for same-index update", rootName, beforeSame[rootName], afterSame[rootName])
		}
	}
	if rootName := collectionPrimaryRootName("users"); afterSame[rootName] == beforeSame[rootName] {
		t.Fatalf("primary root %q did not change for document replacement", rootName)
	}
}

func TestCollectionUpdateBatchIndexChangedMaskFallbackForRuntimeAtOrAbove64(t *testing.T) {
	for _, targetOrdinal := range []int{63, 64, 71} {
		t.Run(fmt.Sprintf("ordinal_%d", targetOrdinal), func(t *testing.T) {
			const indexCount = 72
			d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = d.Close() }()

			mgr := NewCollectionManager(d)
			meta := CollectionMeta{
				Name: "users",
				Options: CollectionOptions{
					DisableIndexedWriteMemtables: true,
				},
				Indexes: collectionManyStringIndexesForTest(indexCount, nil),
			}
			if _, err := mgr.CreateCollection(&meta); err != nil {
				t.Fatalf("create collection: %v", err)
			}
			col, err := mgr.OpenCollection("users")
			if err != nil {
				t.Fatalf("open collection: %v", err)
			}
			if _, err := col.InsertBatch(
				[][]byte{[]byte("u1")},
				[][]byte{collectionManyIndexJSONDocumentForTest(indexCount, nil, nil)},
			); err != nil {
				t.Fatalf("insert: %v", err)
			}

			rootNames := make([]string, 0, indexCount)
			for i := 0; i < indexCount; i++ {
				rootNames = append(rootNames, collectionSecondaryRootName("users", fmt.Sprintf("idx%02d", i)))
			}
			before := collectionRootIDsForTest(t, d, "users", rootNames)
			replacement := collectionManyIndexJSONDocumentForTest(indexCount, map[int]string{
				targetOrdinal: fmt.Sprintf("new%02d", targetOrdinal),
			}, map[string]string{"note": "replacement"})
			results, err := col.UpdateBatch([]UpdateBatchItem{{
				DocumentID: []byte("u1"),
				Update: func([]byte) ([]byte, bool, error) {
					return replacement, true, nil
				},
			}})
			if err != nil {
				t.Fatalf("update target ordinal %d: %v", targetOrdinal, err)
			}
			if len(results) != 1 || !results[0].Matched || !results[0].Modified {
				t.Fatalf("results=%+v want one matched modified row", results)
			}
			stats := col.LastUpdateStats()
			if got, want := stats.IndexValueChanges, 1; got != want {
				t.Fatalf("changed indexes=%d want %d", got, want)
			}
			if got, want := stats.IndexValueUnchanged, indexCount-1; got != want {
				t.Fatalf("unchanged indexes=%d want %d", got, want)
			}

			after := collectionRootIDsForTest(t, d, "users", rootNames)
			targetRoot := collectionSecondaryRootName("users", fmt.Sprintf("idx%02d", targetOrdinal))
			changedRoots := 0
			for _, rootName := range rootNames {
				changed := after[rootName] != before[rootName]
				if rootName == targetRoot {
					if !changed {
						t.Fatalf("target root %q did not change", rootName)
					}
					changedRoots++
					continue
				}
				if changed {
					t.Fatalf("unrelated root %q changed from %d to %d for target ordinal %d", rootName, before[rootName], after[rootName], targetOrdinal)
				}
			}
			if changedRoots != 1 {
				t.Fatalf("changed secondary roots=%d want 1", changedRoots)
			}

			newIDs, err := col.FindByIndexValue(fmt.Sprintf("idx%02d", targetOrdinal), fmt.Sprintf("new%02d", targetOrdinal))
			if err != nil {
				t.Fatalf("find new target value: %v", err)
			}
			if len(newIDs) != 1 || !bytes.Equal(newIDs[0], []byte("u1")) {
				t.Fatalf("new target ids=%q want [u1]", newIDs)
			}
			oldIDs, err := col.FindByIndexValue(fmt.Sprintf("idx%02d", targetOrdinal), fmt.Sprintf("v%02d", targetOrdinal))
			if err != nil {
				t.Fatalf("find old target value: %v", err)
			}
			if len(oldIDs) != 0 {
				t.Fatalf("old target ids=%q want none", oldIDs)
			}
		})
	}
}

func TestCollectionUpdateBatchUniqueOrdinal64Fallback(t *testing.T) {
	const indexCount = 65
	const uniqueOrdinal = 64
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: collectionManyStringIndexesForTest(indexCount, map[int]bool{uniqueOrdinal: true}),
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	u1Doc := collectionManyIndexJSONDocumentForTest(indexCount, map[int]string{
		uniqueOrdinal: "u1-unique",
	}, nil)
	u2Doc := collectionManyIndexJSONDocumentForTest(indexCount, map[int]string{
		uniqueOrdinal: "u2-unique",
	}, nil)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{u1Doc, u2Doc},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	unchangedUniqueReplacement := collectionManyIndexJSONDocumentForTest(indexCount, map[int]string{
		0:             "new-nonunique",
		uniqueOrdinal: "u1-unique",
	}, nil)
	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return unchangedUniqueReplacement, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("unchanged unique ordinal %d update: %v", uniqueOrdinal, err)
	}
	if !batched {
		t.Fatalf("unchanged unique ordinal %d update declined", uniqueOrdinal)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("unchanged unique results=%+v want one matched modified row", results)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.UniqueIndexChecks, 0; got != want {
		t.Fatalf("unchanged unique checks=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexCheckSkips, 1; got != want {
		t.Fatalf("unchanged unique skips=%d want %d", got, want)
	}
	if got, want := stats.IndexValueChanges, 1; got != want {
		t.Fatalf("unchanged unique changed indexes=%d want %d", got, want)
	}
	if got, want := stats.IndexValueUnchanged, indexCount-1; got != want {
		t.Fatalf("unchanged unique unchanged indexes=%d want %d", got, want)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush unchanged unique update: %v", err)
	}

	changedUniqueReplacement := collectionManyIndexJSONDocumentForTest(indexCount, map[int]string{
		0:             "new-nonunique",
		uniqueOrdinal: "fresh-unique",
	}, nil)
	results, batched, err = col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return changedUniqueReplacement, true, nil
		},
	}})
	if err != nil {
		t.Fatalf("changed unique ordinal %d declined update returned err: %v", uniqueOrdinal, err)
	}
	if batched {
		t.Fatalf("changed unique ordinal %d update batched with results=%+v, want declined", uniqueOrdinal, results)
	}

	conflictingUniqueReplacement := collectionManyIndexJSONDocumentForTest(indexCount, map[int]string{
		0:             "new-nonunique",
		uniqueOrdinal: "u2-unique",
	}, nil)
	_, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return conflictingUniqueReplacement, true, nil
		},
	}})
	if !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("conflicting unique ordinal %d update err=%v want ErrUniqueIndexConflict", uniqueOrdinal, err)
	}
}

func collectionManyStringIndexesForTest(n int, unique map[int]bool) []IndexDefinition {
	indexes := make([]IndexDefinition, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("idx%02d", i)
		indexes[i] = IndexDefinition{
			Name:      name,
			Field:     fmt.Sprintf("f%02d", i),
			ValueType: IndexValueString,
			Unique:    unique[i],
		}
	}
	return indexes
}

func collectionManyIndexJSONDocumentForTest(indexCount int, overrides map[int]string, extra map[string]string) []byte {
	var builder strings.Builder
	builder.WriteByte('{')
	first := true
	writeStringField := func(name, value string) {
		if !first {
			builder.WriteByte(',')
		}
		first = false
		fmt.Fprintf(&builder, "%q:%q", name, value)
	}
	for i := 0; i < indexCount; i++ {
		value := fmt.Sprintf("v%02d", i)
		if overrides != nil {
			if override, ok := overrides[i]; ok {
				value = override
			}
		}
		writeStringField(fmt.Sprintf("f%02d", i), value)
	}
	extraNames := make([]string, 0, len(extra))
	for name := range extra {
		extraNames = append(extraNames, name)
	}
	sort.Strings(extraNames)
	for _, name := range extraNames {
		writeStringField(name, extra[name])
	}
	builder.WriteByte('}')
	return []byte(builder.String())
}

func collectionRootIDsForTest(t *testing.T, d *backenddb.DB, collectionName string, rootNames []string) map[string]uint64 {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collectionName)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	out := make(map[string]uint64, len(rootNames))
	for _, rootName := range rootNames {
		rootID := catalog.rootID(rootName)
		if rootID == 0 {
			t.Fatalf("root %q was not persisted", rootName)
		}
		out[rootName] = rootID
	}
	return out
}

func TestCollectionInsertBatchBridge_RejectsPersistedUniqueConflictAtomically(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("seed"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	_, err = col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	)
	if err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("err=%v want unique index conflict", err)
	}
	for _, id := range [][]byte{[]byte("u1"), []byte("u2")} {
		got, err := col.Get(id)
		if err != nil {
			t.Fatalf("get %q: %v", id, err)
		}
		if got != nil {
			t.Fatalf("unexpected doc %q=%q", id, got)
		}
	}
}

func TestCollectionInsertBatchBridge_RejectsPersistedDocumentIDAtomically(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"seed"}`)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	_, err = col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"name":"dup"}`), []byte(`{"name":"new"}`)},
	)
	if err == nil || !strings.Contains(err.Error(), "document already exists") {
		t.Fatalf("err=%v want persisted document id conflict", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if want := []byte(`{"name":"seed"}`); !bytes.Equal(got, want) {
		t.Fatalf("u1=%q want %q", got, want)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if got != nil {
		t.Fatalf("unexpected u2 doc=%q", got)
	}
}

func TestCollectionInsertBatchBridge_RejectsDuplicateIDBeforePublish(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "users"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	_, err = col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u1")},
		[][]byte{[]byte(`{"name":"first"}`), []byte(`{"name":"second"}`)},
	)
	if err == nil || !strings.Contains(err.Error(), "duplicate document id") {
		t.Fatalf("err=%v want duplicate document id", err)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if got != nil {
		t.Fatalf("unexpected u1 doc=%q", got)
	}
}

func TestCollectionDeleteBridge_RemovesPrimaryAndSecondaryEntries(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	deleted, err := col.DeleteDocument([]byte("missing"))
	if err != nil {
		t.Fatalf("delete missing: %v", err)
	}
	if deleted {
		t.Fatal("delete missing reported deleted")
	}
	deleted, err = col.DeleteDocument([]byte("u1"))
	if err != nil {
		t.Fatalf("delete u1: %v", err)
	}
	if !deleted {
		t.Fatal("delete u1 reported not deleted")
	}
	deleted, err = col.DeleteDocument([]byte("u1"))
	if err != nil {
		t.Fatalf("delete u1 again: %v", err)
	}
	if deleted {
		t.Fatal("delete u1 again reported deleted")
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get deleted u1: %v", err)
	}
	if got != nil {
		t.Fatalf("deleted u1 still visible: %q", got)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if want := []byte(`{"email":"grace@example.com","city":"hnl"}`); !bytes.Equal(got, want) {
		t.Fatalf("u2=%q want %q", got, want)
	}

	emailIDs, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find deleted email: %v", err)
	}
	if len(emailIDs) != 0 {
		t.Fatalf("deleted email ids=%q want none", emailIDs)
	}
	cityIDs, err := col.FindByIndex("city", "hnl")
	if err != nil {
		t.Fatalf("find city: %v", err)
	}
	if len(cityIDs) != 1 || !bytes.Equal(cityIDs[0], []byte("u2")) {
		t.Fatalf("city ids=%q want [u2]", cityIDs)
	}
}

func TestCollectionDeleteBridge_AllowsUniqueValueReuse(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert u1: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
	}
	if _, err := col.Insert([]byte("u2"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("reuse unique email: %v", err)
	}
	ids, err := col.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("email ids=%q want [u2]", ids)
	}
}

func TestCollectionDeleteBridge_ReopenUsesDeletedRootDescriptors(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com"}`),
			[]byte(`{"email":"grace@example.com"}`),
		},
	); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if err := col.Delete([]byte("u1")); err != nil {
		t.Fatalf("delete u1: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	got, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get deleted u1 after reopen: %v", err)
	}
	if got != nil {
		t.Fatalf("deleted u1 visible after reopen: %q", got)
	}
	ids, err := reopenedCol.FindByIndex("email", "ada@example.com")
	if err != nil {
		t.Fatalf("find deleted email after reopen: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("deleted email ids after reopen=%q want none", ids)
	}
	ids, err = reopenedCol.FindByIndex("email", "grace@example.com")
	if err != nil {
		t.Fatalf("find remaining email after reopen: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u2")) {
		t.Fatalf("remaining email ids=%q want [u2]", ids)
	}
}
