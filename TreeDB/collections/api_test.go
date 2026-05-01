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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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
			{Name: "email", Field: "email", Unique: true, StoragePolicy: RootStorageCompressed},
			{Name: "city", Field: "city", StoragePolicy: RootStorageCompressed},
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
			{Name: "email", Field: "email", Unique: true, StoragePolicy: RootStorageCompressed},
			{Name: "city", Field: "city", StoragePolicy: RootStorageCompressed},
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
			{Name: "email", Field: "email", Unique: true, StoragePolicy: RootStorageCompressed},
			{Name: "city", Field: "city", StoragePolicy: RootStorageCompressed},
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
		Indexes: []IndexDefinition{{Name: "user_id", Field: "user_id"}},
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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

func TestCollectionIndexedWriteMemtablesDefaultForIndexedSchemas(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "city", Field: "city"}},
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
	if meta.Options.BufferedIndexedWriteMaxDocuments != 0 || meta.Options.BufferedIndexedWriteMaxBytes != 0 {
		t.Fatalf("no-index buffered limits docs=%d bytes=%d want zero",
			meta.Options.BufferedIndexedWriteMaxDocuments, meta.Options.BufferedIndexedWriteMaxBytes)
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
		Indexes: []IndexDefinition{{Name: "city", Field: "city"}},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if meta.Options.BufferedIndexedWrites {
		t.Fatal("disabled indexed write memtables reported enabled")
	}
	if meta.Options.BufferedIndexedWriteMaxDocuments != 0 || meta.Options.BufferedIndexedWriteMaxBytes != 0 {
		t.Fatalf("disabled buffered limits docs=%d bytes=%d want zero",
			meta.Options.BufferedIndexedWriteMaxDocuments, meta.Options.BufferedIndexedWriteMaxBytes)
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
		Indexes: []IndexDefinition{{Name: "city", Field: "city"}},
	}
	if !col.shouldBufferIndexedInsertBatch(meta, DefaultIndexedWriteMemtableDirectBatchDocuments-1) {
		t.Fatal("default indexed memtable path bypassed a below-threshold batch")
	}
	if col.shouldBufferIndexedInsertBatch(meta, DefaultIndexedWriteMemtableDirectBatchDocuments) {
		t.Fatal("default indexed memtable path buffered a large direct-publish batch")
	}
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
		Indexes: []IndexDefinition{{Name: "city", Field: "city"}},
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
		Indexes: []IndexDefinition{{Name: "city", Field: "city"}},
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
		Indexes: []IndexDefinition{{Name: "city", Field: "city"}},
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

func TestCollectionFindByIndexValueLimitMaxIntDoesNotOverflow(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "city", Field: "city"}},
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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

func TestBufferedPrimaryIDArenaCapAvoidsOverflow(t *testing.T) {
	if got := bufferedPrimaryIDArenaCap(2); got != 32 {
		t.Fatalf("small arena cap=%d want 32", got)
	}
	maxInt := int(^uint(0) >> 1)
	if got := bufferedPrimaryIDArenaCap(maxInt/16 + 1); got != 0 {
		t.Fatalf("overflow arena cap=%d want 0", got)
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
	if _, err := indexer.CreateIndex(IndexDefinition{Name: "email", Field: "email", Unique: true}); err != nil {
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
	if _, err := indexer.CreateIndex(IndexDefinition{Name: "email", Field: "email"}); err != nil {
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
	newMeta, normalizedDef, err := addIndexToCollectionMeta(baseMeta, IndexDefinition{Name: "email", Field: "email"})
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
	if _, err := col.CreateIndex(IndexDefinition{Name: "email", Field: "email", Unique: true}); err != nil {
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
	combiner := &collectionUpdateCombiner{maxBatch: 8}
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

	directCol := *col
	directCol.writeDomain = nil
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

func TestCompleteUpdateCombineRequestDoesNotBlockWhenDoneIsFull(t *testing.T) {
	done := make(chan collectionUpdateCombineResult, 1)
	original := collectionUpdateCombineResult{matched: true}
	done <- original
	completeUpdateCombineRequest(
		collectionUpdateCombineRequest{done: done},
		collectionUpdateCombineResult{modified: true},
	)
	got := <-done
	if got != original {
		t.Fatalf("done result=%+v want original %+v", got, original)
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

func TestCollectionUpdateCombinerStopDoesNotWaitForActiveWorker(t *testing.T) {
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
	go func() {
		combiner.stop()
		close(stopReturned)
	}()
	select {
	case <-stopReturned:
	case <-time.After(time.Second):
		t.Fatal("stop waited for active combiner worker")
	}

	close(release)
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
	if _, err := col.CreateIndex(IndexDefinition{Name: "email", Field: "email", Unique: true}); err != nil {
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
	if _, err := fresh.CreateIndex(IndexDefinition{Name: "email", Field: "email", Unique: true}); err != nil {
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
		select {
		case <-rightFinished:
		case <-time.After(time.Second):
			t.Error("timed out waiting for concurrent update goroutine cleanup")
		}
	}()

	concurrentUpdateWait := 10 * time.Second
	if deadline, ok := t.Deadline(); ok {
		if remaining := time.Until(deadline) - 500*time.Millisecond; remaining > 0 && remaining < concurrentUpdateWait {
			concurrentUpdateWait = remaining
		}
	}
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
		Indexes: []IndexDefinition{{Name: "city", Field: "city"}},
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
		Indexes: []IndexDefinition{{Name: "city", Field: "city"}},
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
	return func(current []byte) ([]byte, bool, error) {
		var doc map[string]any
		if err := json.Unmarshal(current, &doc); err != nil {
			return nil, false, err
		}
		doc["email"] = email
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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

	meta, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city"})
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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
	if _, err := reopened.CreateIndex(IndexDefinition{Name: "city", Field: "city"}); err != nil {
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
		Indexes: []IndexDefinition{{Name: "city", Field: "city"}},
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

func TestCollectionFindByIndexValueMatchesLargeJSONInteger(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "big", Field: "big"}},
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
	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city"}); err != nil {
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city"}); err != nil {
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
	if _, err := col.CreateIndex(IndexDefinition{Name: "email", Field: "email", Unique: true}); err != nil {
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

	_, err = col.CreateIndex(IndexDefinition{Name: "email", Field: "email", Unique: true})
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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

func TestCollectionInsertBatchBridge_RejectsPersistedUniqueConflictAtomically(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "users",
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
			{Name: "email", Field: "email", Unique: true},
			{Name: "city", Field: "city"},
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
		Indexes: []IndexDefinition{{Name: "email", Field: "email", Unique: true}},
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
