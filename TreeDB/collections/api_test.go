package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
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
	opts := treedb.Options{
		Dir:                        t.TempDir(),
		Durability:                 treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
	}
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
	if testing.Short() {
		t.Skip("loads enough documents to exercise collection leaf generation pack/GC")
	}
	opts := treedb.Options{
		Dir:                        t.TempDir(),
		Durability:                 treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
	}
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
		documents                              = 4_000
		batchSize                              = 500
		minExpectedCollectionLiveBytesForSmoke = 512
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
	if got := packStats.SourceBytesLive; got <= minExpectedCollectionLiveBytesForSmoke {
		t.Fatalf("SourceBytesLive=%d, want real collection live bytes copied (stats=%+v)", got, packStats)
	}
	requireCollectionMaintenanceTemplateReads(t, col)

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
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
			[]byte(`{"email":"grace@example.com","city":"sfo"}`),
		},
	); err != nil {
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
	if stats.CurrentRead != 0 || stats.Callback != 0 || stats.BufferStage != 0 ||
		stats.BufferStagePrecheck != 0 ||
		stats.BufferStageLockWait != 0 || stats.BufferStageLockHold != 0 ||
		stats.BufferStageValidation != 0 || stats.BufferStageRootScan != 0 ||
		stats.BufferStageDomainPrepare != 0 ||
		stats.BufferStagePrimaryIdx != 0 || stats.BufferStageUniqueIdx != 0 ||
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
		"treedb.collections.write_domain.update_batch.unique_checks_total",
		"treedb.collections.write_domain.update_batch.unique_check_skips_total",
	} {
		if _, ok := exported[key]; !ok {
			t.Fatalf("manager stats missing %s: keys=%v", key, exported)
		}
	}
	for _, key := range []string{
		"treedb.collections.write_domain.update_batch.buffer_stage_precheck_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_lock_wait_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_lock_hold_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_validation_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_root_scan_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_domain_prepare_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_primary_index_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_unique_index_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_root_append_ns_total",
		"treedb.collections.write_domain.update_batch.buffer_stage_flush_ns_total",
	} {
		if _, ok := exported[key]; !ok {
			t.Fatalf("manager stats missing %s: keys=%v", key, exported)
		}
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
		{"primary_index", "treedb.collections.write_domain.update_batch.buffer_stage_primary_index_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStagePrimaryIdx = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferPrimaryIdx }},
		{"unique_index", "treedb.collections.write_domain.update_batch.buffer_stage_unique_index_ns_total", func(s *CollectionUpdateStats, d time.Duration) { s.BufferStageUniqueIdx = d }, func(s CollectionManagerStats) time.Duration { return s.UpdateBatchBufferUniqueIdx }},
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
	if stats.IndexedFlushDuration <= 0 || stats.IndexedFlushMaterialize <= 0 || stats.IndexedFlushPublish <= 0 {
		t.Fatalf("stats indexed flush duration/materialize/publish=%s/%s/%s want positive", stats.IndexedFlushDuration, stats.IndexedFlushMaterialize, stats.IndexedFlushPublish)
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
	if got := meta.Options.BufferedIndexedWriteMaxDocuments; got != DefaultIndexedWriteMemtableMaxDocuments {
		t.Fatalf("default max docs=%d want %d", got, DefaultIndexedWriteMemtableMaxDocuments)
	}
	if got := meta.Options.BufferedIndexedWriteMaxBytes; got != 0 {
		t.Fatalf("default max bytes=%d want 0", got)
	}
	if got := meta.Options.BufferedIndexedWriteMaxRootRuns; got != DefaultIndexedWriteMemtableMaxRootRuns {
		t.Fatalf("default max root runs=%d want %d", got, DefaultIndexedWriteMemtableMaxRootRuns)
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
	if got := meta.Options.BufferedIndexedWriteMaxDocuments; got != DefaultIndexedWriteMemtableMaxDocuments {
		t.Fatalf("max documents=%d want default %d", got, DefaultIndexedWriteMemtableMaxDocuments)
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
		wantUnlock              bool
	}{
		{
			name:           "json-no-indexed-memtables",
			documentFormat: DocumentFormatJSON,
			wantUnlock:     true,
		},
		{
			name:                    "json-buffered-indexed-memtables",
			documentFormat:          DocumentFormatJSON,
			indexedMemtablesEnabled: true,
			bufferIndexedInserts:    true,
			wantUnlock:              true,
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
			)
			if got != tt.wantUnlock {
				t.Fatalf("shouldUnlockInsertPlanning()=%v want %v", got, tt.wantUnlock)
			}
		})
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

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- col.flushBufferedWrites()
	}()
	select {
	case err := <-flushDone:
		t.Fatalf("flush returned before in-flight async publish drained: %v", err)
	case <-time.After(50 * time.Millisecond):
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
	domain.mu.Lock()
	scanDone := make(chan error, 1)
	go func() {
		_, err := col.ScanDocumentsFunc(16, func(DocumentRecord) (bool, error) {
			return true, nil
		})
		scanDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
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
	case err := <-scanDone:
		t.Fatalf("scan returned before scheduled async publish drained: %v", err)
	case <-time.After(50 * time.Millisecond):
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

	createDone := make(chan error, 1)
	go func() {
		_, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city", ValueType: IndexValueString})
		createDone <- err
	}()
	select {
	case err := <-createDone:
		t.Fatalf("CreateIndex returned before in-flight async publish drained: %v", err)
	case <-time.After(50 * time.Millisecond):
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
			BufferedIndexedWrites:        true,
			BufferedIndexedWriteMaxBytes: 1,
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
			BufferedIndexedWrites:           true,
			BufferedIndexedWriteMaxRootRuns: 2,
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

	index := newBufferedPrimaryRunIndex(0)
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

func TestCollectionCachedCatalogUsesCommitSeq(t *testing.T) {
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
	staleCatalog := cloneCatalogWithRootUpdates(catalog, catalog.meta, []string{rootName}, []uint64{^uint64(0)})

	col.catalogMu.Lock()
	col.catalog = staleCatalog
	col.catalogSystemRoot = snapshotSystemRoot(snap)
	col.catalogCommitSeq = snapshotCommitSeq(snap) + 1
	col.catalogMu.Unlock()

	refreshed, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	if got := refreshed.rootID(rootName); got == ^uint64(0) {
		t.Fatalf("catalog cache ignored commit sequence and returned stale root %d", got)
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
	staleRootIDs := map[string]uint64{rootName: baseRootIDs[rootName] + 1}
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
	validation.baseRootIDs = map[string]uint64{}
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
	oldPrimaryRoot := baseRootIDs[collectionPrimaryRootName("users")]

	if _, err := col.InsertBatch([][]byte{[]byte("u3")}, [][]byte{[]byte(`{"city":"c"}`)}); err != nil {
		t.Fatalf("insert disjoint concurrent row: %v", err)
	}

	mutationLocked := false
	var unlockMutation collectionMutationUnlock
	pin, currentCatalog, _, _, err := col.lockAndValidateInsertBatchPlan(&mutationLocked, &unlockMutation, nil, catalog, meta, rootNames, baseRootIDs, plan)
	if err != nil {
		t.Fatalf("validate disjoint root drift: %v", err)
	}
	defer unlockMutation.Unlock()
	defer func() { _ = pin.Close() }()
	currentPrimaryRoot := currentCatalog.rootID(collectionPrimaryRootName("users"))
	if currentPrimaryRoot == oldPrimaryRoot {
		t.Fatal("test did not create primary root drift")
	}
	if got := baseRootIDs[collectionPrimaryRootName("users")]; got != currentPrimaryRoot {
		t.Fatalf("rebased primary root=%d want %d", got, currentPrimaryRoot)
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
	})
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
	})
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
	}, make(chan collectionUpdateCombineResult, 1))
	if got := req.documentIDBytes(); !bytes.Equal(got, id) {
		t.Fatalf("inline document id=%q want %q", got, id)
	}
	id[0] = 'X'
	if got := req.documentIDBytes(); !bytes.Equal(got, []byte("user-123")) {
		t.Fatalf("inline document id changed after caller mutation: %q", got)
	}

	allocID := []byte("user-123")
	if allocs := testing.AllocsPerRun(1000, func() {
		req := newCollectionUpdateCombineRequest(nil, allocID, nil, nil)
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
	}, make(chan collectionUpdateCombineResult, 1))
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

func TestDirectBufferedRootEntriesOwnKeysAndRetainDocumentArena(t *testing.T) {
	scratch := getUpdateBatchPlanScratch(1, 0)
	documentID := []byte("u1")
	document := appendUpdateBatchPlanScratchDocument(scratch, []byte(`{"city":"hnl"}`))
	primaryEntries := buildDirectBufferedPrimaryRootEntries([]preparedBatchUpdate{{
		documentID: documentID,
		document:   document,
	}})
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
	if got := len(domain.rootValueArenas); got != 1 {
		t.Fatalf("retained document arenas=%d want 1", got)
	}
	plan.close()
	reused := getUpdateBatchPlanScratch(1, 0)
	_ = appendUpdateBatchPlanScratchDocument(reused, []byte(`{"city":"koa"}`))
	putUpdateBatchPlanScratch(reused)
	got, deleted, ok := table.Get([]byte("u1"))
	if !ok || deleted || !bytes.Equal(got, []byte(`{"city":"hnl"}`)) {
		t.Fatalf("staged primary value=%q ok=%v deleted=%v, want retained original", got, ok, deleted)
	}

	templateRecords := []templateV1Record{{
		id:  [32]byte{1, 2, 3},
		raw: []byte("template-record"),
	}}
	templateEntries := buildDirectBufferedTemplateRootEntries(templateRecords)
	if len(templateEntries) != 1 {
		t.Fatalf("template entries=%d want 1", len(templateEntries))
	}
	templateRecords[0].id[0] = 9
	templateRecords[0].raw[0] = 'X'
	if templateEntries[0].key[0] != 1 || !bytes.Equal(templateEntries[0].value, []byte("template-record")) {
		t.Fatalf("template entry key[0]=%d value=%q, want owned original bytes", templateEntries[0].key[0], templateEntries[0].value)
	}
}

func newBufferedUsersUpdateCollection(t *testing.T) (*backenddb.DB, *Collection) {
	t.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

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
	return d, col
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

	plan, err := col.buildUpdateBatchPlan([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sfo")},
	}, updateBatchModeNoSecondaryUniqueIndexChanges, true)
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

func TestCollectionUpdateBatchIfNoSecondaryUniqueIndexChangesRejectsStaleZeroDeltaPlan(t *testing.T) {
	_, col := newBufferedUsersUpdateCollection(t)
	if _, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setJSONCity("sea")},
	}); err != nil {
		t.Fatalf("first UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched {
		t.Fatalf("first batch was declined")
	}

	plan, err := col.buildUpdateBatchPlan([]UpdateBatchItem{
		{
			DocumentID: []byte("u1"),
			Update: func(current []byte) ([]byte, bool, error) {
				if !bytes.Contains(current, []byte(`"city":"sea"`)) {
					return nil, false, fmt.Errorf("current document %s did not include buffered city", current)
				}
				return current, false, nil
			},
		},
	}, updateBatchModeNoSecondaryUniqueIndexChanges, true)
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
	if collectionHasBufferedPrimaryRunIndexForTest(t, col) {
		t.Fatal("primary run index was built before buffered read planning")
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

	read, _, blocked, err := snapshotUpdateBatchBufferedRead(domain, meta, 7, []UpdateBatchItem{{DocumentID: []byte("missing")}}, DocumentFormatJSON)
	if err != nil {
		t.Fatalf("snapshotUpdateBatchBufferedRead: %v", err)
	}
	defer putUpdateBatchBufferedEntries(read.primaryEntries, read.primaryBuffer)
	if blocked {
		t.Fatal("buffered read reported blocked")
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
	items := []UpdateBatchItem{{DocumentID: []byte("u1")}}
	assertRead := func() {
		t.Helper()
		read, _, blocked, needPrimaryRunIndex, err := snapshotUpdateBatchBufferedReadLocked(domain, meta, 7, items, DocumentFormatJSON, false)
		if err != nil {
			t.Fatalf("snapshotUpdateBatchBufferedReadLocked: %v", err)
		}
		defer putUpdateBatchBufferedEntries(read.primaryEntries, read.primaryBuffer)
		if blocked || needPrimaryRunIndex || !read.enabled {
			t.Fatalf("read enabled=%v blocked=%v needPrimaryRunIndex=%v", read.enabled, blocked, needPrimaryRunIndex)
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
	items := []UpdateBatchItem{
		{DocumentID: []byte("u1")},
		{DocumentID: []byte("u2")},
		{DocumentID: []byte("u3")},
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
	items := make([]UpdateBatchItem, 0, entriesCount)
	value := []byte(strings.Repeat("x", 512))
	for i := 0; i < entriesCount; i++ {
		id := []byte(fmt.Sprintf("u%05d", i))
		setCollectionRunValue(primaryTable, id, value)
		items = append(items, UpdateBatchItem{DocumentID: id})
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
	items := []UpdateBatchItem{{DocumentID: []byte("u1")}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		read, _, blocked, needPrimaryRunIndex, err := snapshotUpdateBatchBufferedReadLocked(domain, meta, 7, items, DocumentFormatJSON, false)
		if err != nil {
			b.Fatalf("snapshotUpdateBatchBufferedReadLocked: %v", err)
		}
		if blocked || needPrimaryRunIndex || !read.enabled || len(read.primaryEntries) != 1 || !read.primaryEntries[0].found {
			b.Fatalf("unexpected read enabled=%v entries=%d blocked=%v needPrimaryRunIndex=%v", read.enabled, len(read.primaryEntries), blocked, needPrimaryRunIndex)
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
	replacement := mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u2"}, {Key: "score", Value: int32(1)}})
	_, err = col.UpdateBatch([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: func([]byte) ([]byte, bool, error) {
			return replacement, true, nil
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "index 0") || !strings.Contains(err.Error(), "cannot modify _id") {
		t.Fatalf("UpdateBatch err=%v want indexed _id mutation error", err)
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
