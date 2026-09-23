package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/page"
)

const preparedInsertTestRequestLimit = 2 << 30

func TestPreparedInsertOverlapsOrderedCommit(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	sibling := createColumnRetainedSemanticStreamCollection(t, d, "sibling")
	docs := [][]byte{[]byte(`{"row_id":1,"kind":"one"}`), []byte(`{"row_id":2,"kind":"two"}`)}
	first, err := col.PrepareInsertBatchOwned([][]byte{[]byte("b")}, [][]byte{docs[0]}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var firstPublication sync.Once
	restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error {
		firstPublication.Do(func() { close(entered); <-release })
		return nil
	})
	defer restore()
	committed := make(chan error, 1)
	go func() { _, committedErr := first.Commit(); committed <- committedErr }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not reach publication")
	}
	second, err := col.PrepareInsertBatchOwned([][]byte{[]byte("a")}, [][]byte{docs[1]}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-committed:
		t.Fatalf("commit acknowledged before release: %v", err)
	default:
	}
	if got, err := col.Get([]byte("b")); err != nil || got != nil {
		t.Fatalf("first batch visible before ordered publication: %s, %v", got, err)
	}
	if got, err := col.Get([]byte("a")); err != nil || got != nil {
		t.Fatalf("second prepared batch visible before its commit: %s, %v", got, err)
	}
	// Launch an ordinary sibling writer while the prepared commit is held.
	// The start signal is before its InsertBatch call; this checks liveness
	// after release, not where inside the write path it was blocked.
	siblingStarted, siblingDone := make(chan struct{}), make(chan error, 1)
	go func() {
		close(siblingStarted)
		_, err := sibling.InsertBatch([][]byte{[]byte("s")}, [][]byte{[]byte(`{"row_id":3,"kind":"sibling"}`)})
		siblingDone <- err
	}()
	<-siblingStarted
	close(release)
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-siblingDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sibling ordinary writer stalled behind prepared commit")
	}
	ids, err := second.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("a")) {
		t.Fatalf("result IDs=%q", ids)
	}
	for _, id := range [][]byte{[]byte("a"), []byte("b")} {
		if _, err := col.Get(id); err != nil {
			t.Fatalf("Get %q: %v", id, err)
		}
	}
	if got, err := sibling.Get([]byte("s")); err != nil || !bytes.Contains(got, []byte(`"sibling"`)) {
		t.Fatalf("sibling ordinary row=%s, %v", got, err)
	}
}

func TestPreparedInsertColdCatalogRejectsBeforeReadOrWAL(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	col.catalogMu.Lock()
	col.catalog = nil
	col.catalogMu.Unlock()
	col.writeDomain.mu.Lock()
	col.writeDomain.catalog = nil
	col.writeDomain.loaded = false
	col.writeDomain.mu.Unlock()
	var catalogLoads atomic.Int32
	restore := setTestCollectionCatalogLoadHookForTest(func(ctx collectionCatalogLoadFaultContext) error {
		if ctx.Collection == "events" && ctx.Stage == collectionCatalogLoadFaultMeta {
			catalogLoads.Add(1)
		}
		return nil
	})
	defer restore()
	ids, docs := [][]byte{[]byte("a")}, [][]byte{[]byte(`{"row_id":1,"kind":"one"}`)}
	beforeNext, beforeApplied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN
	prepared, err := col.PrepareInsertBatchOwned(ids, docs, preparedInsertTestRequestLimit)
	if prepared != nil || !errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("cold catalog prepare=(%v,%v), want resource limit", prepared, err)
	}
	if got := catalogLoads.Load(); got != 0 {
		t.Fatalf("prepared request loaded cold catalog %d times", got)
	}
	if next, applied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN; next != beforeNext || applied != beforeApplied {
		t.Fatalf("cold catalog rejection advanced command WAL next/applied LSN to %d/%d from %d/%d", next, applied, beforeNext, beforeApplied)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("ordinary cold-catalog fallback: %v", err)
	}
	if got := catalogLoads.Load(); got == 0 {
		t.Fatal("ordinary insert did not load the cold catalog")
	}
}

func TestPreparedInsertRejectsBufferedFlushBeforeCommitLSN(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	prepared, err := col.PrepareInsertBatchOwned([][]byte{[]byte("pending")}, [][]byte{[]byte(`{"row_id":1,"kind":"pending"}`)}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	beforeNext, beforeApplied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN
	domain := col.writeDomain
	domain.mu.Lock()
	domain.count = 1
	domain.mu.Unlock()
	defer func() {
		domain.mu.Lock()
		domain.count = 0
		domain.mu.Unlock()
	}()
	if _, err := prepared.Commit(); !errors.Is(err, ErrPreparedInsertResourceLimit) || !strings.Contains(err.Error(), "buffered collection writes") {
		t.Fatalf("prepared commit with pending buffered work: %v", err)
	}
	if next, applied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN; next != beforeNext || applied != beforeApplied {
		t.Fatalf("buffered-work rejection advanced command WAL next/applied LSN to %d/%d from %d/%d", next, applied, beforeNext, beforeApplied)
	}
}

func TestPreparedInsertManifestBudgetBeforeCommit(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	if err := col.checkPreparedInsertManifestBudget(0, preparedInsertMaxManifestRecords, preparedInsertMaxManifestBytes); err != nil {
		t.Fatalf("first publication without manifest: %v", err)
	}
	prepared, err := col.PrepareInsertBatchOwned([][]byte{[]byte("a")}, [][]byte{[]byte(`{"row_id":1,"kind":"one"}`)}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot unavailable")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	rootID := catalog.rootID(collectionColumnManifestRootName("events"))
	if rootID == 0 {
		t.Fatal("committed manifest root missing")
	}
	if err := col.checkPreparedInsertManifestBudget(rootID, preparedInsertMaxManifestRecords, preparedInsertMaxManifestBytes); err != nil {
		t.Fatalf("admitted manifest: %v", err)
	}
	if err := col.checkPreparedInsertManifestPurePointClosure(rootID, catalog.meta); err != nil {
		t.Fatalf("append-only manifest closure: %v", err)
	}
	for _, limit := range []struct {
		records int
		bytes   int64
	}{{1, preparedInsertMaxManifestBytes}, {preparedInsertMaxManifestRecords, 1}} {
		if err := col.checkPreparedInsertManifestBudget(rootID, limit.records, limit.bytes); !errors.Is(err, ErrPreparedInsertResourceLimit) {
			t.Fatalf("records=%d bytes=%d err=%v, want resource limit", limit.records, limit.bytes, err)
		}
	}
	second, err := col.PrepareInsertBatchOwned([][]byte{[]byte("b")}, [][]byte{[]byte(`{"row_id":2,"kind":"two"}`)}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Commit(); err != nil {
		t.Fatalf("warm prepared manifest publish: %v", err)
	}
}

func TestPreparedInsertLateRootPointProfilesColdAndWarm(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	primaryName := collectionPrimaryRootName("events")
	streamName := collectionRetainedSemanticStreamRootName("events")
	manifestName := collectionColumnManifestRootName("events")
	locatorName := collectionColumnRowLocatorRootName("events")
	rootNames := []string{primaryName, streamName, manifestName, locatorName}
	profile := func(id string) {
		t.Helper()
		col.catalogMu.RLock()
		catalog := col.catalog
		col.catalogMu.RUnlock()
		if catalog == nil {
			t.Fatal("missing catalog")
		}
		baseRootIDs := make(map[string]uint64, len(rootNames))
		for _, name := range rootNames {
			baseRootIDs[name] = catalog.rootID(name)
		}
		input := columnWritePublishInput{
			meta: catalog.meta, operation: ColumnPublishOperationInsert,
			rootNames: rootNames[:2], documents: []columnWriteDocument{{ID: []byte(id)}},
			baseSystemRoot: d.State().SystemRootPageID,
		}
		context, system, err := col.profilePreparedColumnLateRoots(input, rootNames, baseRootIDs)
		if err != nil {
			t.Fatal(err)
		}
		if len(context) != 2 || context[0].BaseRoot != baseRootIDs[manifestName] ||
			context[1].BaseRoot != baseRootIDs[locatorName] ||
			context[0].OutputPages == 0 || context[1].OutputPages == 0 ||
			system.BaseRoot != input.baseSystemRoot || system.OutputPages == 0 {
			t.Fatalf("context=%+v system=%+v", context, system)
		}
	}
	profile("a")
	prepared, err := col.PrepareInsertBatchOwned([][]byte{[]byte("a")}, [][]byte{[]byte(`{"row_id":1,"kind":"one"}`)}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	profile("b")
}

func TestPreparedInsertLateRootPointProfileAdmitsSequential16K(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	const rows = preparedInsertMaxRows
	ids, documents := make([][]byte, rows), make([][]byte, rows)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("%08d", i))
		documents[i] = []byte(fmt.Sprintf(`{"row_id":%d,"kind":"one"}`, i))
	}
	prepared, err := col.PrepareInsertBatchOwned(ids, documents, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	primaryName := collectionPrimaryRootName("events")
	streamName := collectionRetainedSemanticStreamRootName("events")
	manifestName := collectionColumnManifestRootName("events")
	locatorName := collectionColumnRowLocatorRootName("events")
	rootNames := []string{primaryName, streamName, manifestName, locatorName}
	col.catalogMu.RLock()
	catalog := col.catalog
	col.catalogMu.RUnlock()
	if catalog == nil {
		t.Fatal("missing catalog")
	}
	baseRootIDs := make(map[string]uint64, len(rootNames))
	for _, name := range rootNames {
		baseRootIDs[name] = catalog.rootID(name)
	}
	nextIDs := make([]columnWriteDocument, rows)
	for i := range nextIDs {
		nextIDs[i].ID = []byte(fmt.Sprintf("%08d", rows+i))
	}
	input := columnWritePublishInput{
		meta: catalog.meta, operation: ColumnPublishOperationInsert,
		rootNames: rootNames[:2], documents: nextIDs,
		baseSystemRoot: d.State().SystemRootPageID,
	}
	context, system, err := col.profilePreparedColumnLateRoots(input, rootNames, baseRootIDs)
	if err != nil {
		t.Fatal(err)
	}
	if len(context) != 2 || context[0].OutputPages == 0 || context[1].OutputPages == 0 || system.OutputPages == 0 {
		t.Fatalf("late root profiles context=%+v system=%+v", context, system)
	}
	primaryKeys := make([][]byte, rows)
	for i := range nextIDs {
		primaryKeys[i] = nextIDs[i].ID
	}
	primaryPolicy, err := collectionRootStoragePolicyForDB(d, catalog.meta, primaryName)
	if err != nil {
		t.Fatal(err)
	}
	primary, err := d.ProfilePreparedRootPointKeys(baseRootIDs[primaryName], primaryPolicy, primaryKeys, preparedInsertRootPointCensusLimit)
	if err != nil {
		t.Fatal(err)
	}
	for i := range documents {
		documents[i] = []byte(fmt.Sprintf(`{"row_id":%d,"kind":"one"}`, rows+i))
	}
	next, err := col.PrepareInsertBatchOwned(primaryKeys, documents, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Abandon()
	if next.retained.semanticStreamBlocks == nil {
		t.Fatal("missing semantic stream blocks")
	}
	streamKeys := make([][]byte, 0, next.retained.semanticStreamBlocks.Len())
	iter := next.retained.semanticStreamBlocks.NewIterator(nil, nil)
	for ; iter.Valid(); iter.Next() {
		streamKeys = append(streamKeys, bytes.Clone(iter.UnsafeKey()))
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	_ = iter.Close()
	streamPolicy, err := collectionRootStoragePolicyForDB(d, catalog.meta, streamName)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := d.ProfilePreparedRootPointKeys(baseRootIDs[streamName], streamPolicy, streamKeys, preparedInsertRootPointCensusLimit)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("16K warm root page ceilings: primary=%d stream=%d manifest=%d locator=%d system=%d", primary.OutputPages, stream.OutputPages, context[0].OutputPages, context[1].OutputPages, system.OutputPages)
	t.Logf("16K warm publisher base census: %+v", d.PreparedRootPublicationBaseProfile())
}

func TestPreparedInsertManifestPurePointClosureRejectsOrphanMarker(t *testing.T) {
	ref := ColumnAssetRef{
		Kind: ColumnAssetKindTCS1PartImage, Namespace: "assets", Generation: 1,
		PartID: 1, FileID: 2, Offset: 0, Length: 32, Checksum: 1,
	}
	partValue, err := encodeColumnManifestPartRecord(ColumnPreparedAsset{
		Ref: ref, Rows: 1, Bytes: ref.Length, PublishID: 1,
		GenerationID: ref.Generation, Reason: string(ColumnPublishOperationInsert),
	})
	if err != nil {
		t.Fatal(err)
	}
	markerValue, err := encodeColumnManifestSegmentOwnership(columnManifestSegmentOwnership{Ref: ref, Frontier: 32})
	if err != nil {
		t.Fatal(err)
	}
	part := columnManifestRecord{key: columnManifestPartRecordKey(1, 1), value: partValue}
	marker := columnManifestRecord{key: columnManifestSegmentOwnershipRecordKey(2), value: markerValue}
	if err := preparedInsertManifestPurePointClosure([]columnManifestRecord{part, marker}, 2, "assets"); err != nil {
		t.Fatalf("live marker rejected: %v", err)
	}
	if err := preparedInsertManifestPurePointClosure([]columnManifestRecord{marker}, 2, "assets"); !errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("orphan marker err=%v, want prepared resource limit", err)
	}
}

func TestPreparedInsertRejectsTypedPrebuildWithoutCredit(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	const rows = 4096
	ids, documents := make([][]byte, rows), make([][]byte, rows)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("%08d", i))
		documents[i] = []byte(fmt.Sprintf(`{"row_id":%d,"kind":"one"}`, i))
	}
	before, applied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN
	prepared, err := col.PrepareInsertBatchOwned(ids, documents, 32<<20)
	if prepared != nil || !errors.Is(err, ErrPreparedInsertResourceLimit) || !strings.Contains(err.Error(), "typed preparation") {
		t.Fatalf("prepare=(%v,%v), want pre-typed resource rejection", prepared, err)
	}
	if got := d.CommandWALNextLSN(); got != before {
		t.Fatalf("typed prebuild rejection advanced next LSN from %d to %d", before, got)
	}
	if got := d.State().AppliedCommandLSN; got != applied {
		t.Fatalf("typed prebuild rejection advanced applied LSN from %d to %d", applied, got)
	}
	if err := d.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("typed prebuild rejection poisoned command WAL: %v", err)
	}
}

func TestPreparedInsertLongIDsRejectCommitReserveBeforeWAL(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	const rows = 512
	ids, documents := make([][]byte, rows), make([][]byte, rows)
	for i := range ids {
		ids[i] = append([]byte(fmt.Sprintf("%08d", i)), bytes.Repeat([]byte("x"), preparedInsertMaxIDBytes-8)...)
		documents[i] = []byte(fmt.Sprintf(`{"row_id":%d,"kind":"long-id"}`, i))
	}
	before, applied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN
	prepared, err := col.PrepareInsertBatchOwned(ids, documents, 96<<20)
	if prepared != nil || !errors.Is(err, ErrPreparedInsertResourceLimit) || !strings.Contains(err.Error(), "commit reserve") {
		t.Fatalf("prepare=(%v,%v), want pre-WAL commit-reserve rejection", prepared, err)
	}
	if got := d.CommandWALNextLSN(); got != before {
		t.Fatalf("commit-reserve rejection advanced next LSN from %d to %d", before, got)
	}
	if got := d.State().AppliedCommandLSN; got != applied {
		t.Fatalf("commit-reserve rejection advanced applied LSN from %d to %d", applied, got)
	}
	if err := d.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("commit-reserve rejection poisoned command WAL: %v", err)
	}
}

func TestPreparedInsertRepeatedStringsChargeCommitBeforeWAL(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	meta := CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "group", Path: "group", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerRowAsset, Dictionary: true},
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
		},
		AggregateMetadata: []ColumnAggregateMetadata{{Name: "by_group", GroupColumn: "group", Kind: ColumnAggregateCount}},
		RetainedPayload:   ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction: ColumnReconstructionRetainedPayloadAndColumns,
	}}}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col := openColumnRetainedPlacementCollection(t, d, meta.Name)
	const rows = 128
	group := strings.Repeat("g", 8192)
	ids, documents := make([][]byte, rows), make([][]byte, rows)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("%08d", i))
		documents[i] = []byte(fmt.Sprintf(`{"group":"%s","time_us":%d}`, group, i))
	}
	prepared, err := col.PrepareInsertBatchOwned(ids, documents, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	rowStringBytes, groupBytes := preparedInsertRepeatedStringBytes(*meta.Options.ColumnStore, prepared.retained.declaredRows)
	distinctBytes := prepared.retained.declaredStringBackingBytes
	if rowStringBytes != rows*int64(len(group)) || groupBytes != rowStringBytes || distinctBytes >= rowStringBytes {
		t.Fatalf("source bytes row=%d group=%d distinct=%d", rowStringBytes, groupBytes, distinctBytes)
	}
	// The previous reserve used distinct backing for both outputs. This limit
	// lies above that old reserve but below the per-row serialization charge.
	delta := 3*(rowStringBytes-distinctBytes) + 6*groupBytes
	reserved := prepared.ReservedBytes()
	if delta <= 0 || reserved <= delta {
		t.Fatalf("reserve=%d repeated-string delta=%d", reserved, delta)
	}
	prepared.Abandon()
	before, applied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN
	tightLimit := reserved - delta/2
	if tightLimit <= reserved-delta {
		t.Fatalf("tight limit=%d did not separate old/new reserve", tightLimit)
	}
	if token, err := col.PrepareInsertBatchOwned(ids, documents, tightLimit); token != nil ||
		!errors.Is(err, ErrPreparedInsertResourceLimit) || !strings.Contains(err.Error(), "commit reserve") {
		t.Fatalf("tight prepare=(%v,%v), want pre-WAL commit reserve rejection", token, err)
	}
	if got := d.CommandWALNextLSN(); got != before {
		t.Fatalf("resource rejection advanced next LSN from %d to %d", before, got)
	}
	if got := d.State().AppliedCommandLSN; got != applied {
		t.Fatalf("resource rejection advanced applied LSN from %d to %d", applied, got)
	}
	if err := d.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("resource rejection poisoned command WAL: %v", err)
	}
	adequate, err := col.PrepareInsertBatchOwned(ids, documents, reserved)
	if err != nil {
		t.Fatalf("prepare with derived credit: %v", err)
	}
	if _, err := adequate.Commit(); err != nil {
		t.Fatalf("commit with derived credit: %v", err)
	}
}

func TestPreparedInsertSharedStringAcrossTypedDictionariesChargedBeforeWAL(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	columns := make([]ColumnStoreColumn, 5)
	for i := range columns {
		name := fmt.Sprintf("field_%d", i)
		columns[i] = ColumnStoreColumn{Name: name, Path: name, ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true}
	}
	meta := CollectionMeta{Name: "shared", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: &ColumnStoreConfig{
		Enabled: true, Columns: columns,
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction: ColumnReconstructionRetainedPayloadAndColumns,
	}}}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col := openColumnRetainedPlacementCollection(t, d, meta.Name)
	value := strings.Repeat("v", 8192)
	id := []byte("shared-row")
	doc := []byte(fmt.Sprintf(`{"field_0":%q,"field_1":%q,"field_2":%q,"field_3":%q,"field_4":%q}`, value, value, value, value, value))
	prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	dictionaryBytes := preparedTypedDictionaryStringBytes(prepared.typedBatch)
	backingBytes := prepared.retained.declaredStringBackingBytes
	if dictionaryBytes != 5*int64(len(value)) || backingBytes >= dictionaryBytes {
		t.Fatalf("typed dictionary bytes=%d interned backing=%d", dictionaryBytes, backingBytes)
	}
	reserved := prepared.ReservedBytes()
	prepared.Abandon()
	// A single global interner charges one copy, while the image encodes five
	// dictionary entries. The intervening limit must fail before any WAL LSN.
	delta := 12 * (dictionaryBytes - backingBytes)
	if delta <= 0 || reserved <= delta {
		t.Fatalf("reserve=%d dictionary delta=%d", reserved, delta)
	}
	before, applied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN
	tightLimit := reserved - delta/2
	if token, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, tightLimit); token != nil ||
		!errors.Is(err, ErrPreparedInsertResourceLimit) || !strings.Contains(err.Error(), "commit reserve") {
		t.Fatalf("tight prepare=(%v,%v), want pre-WAL dictionary reserve rejection", token, err)
	}
	if got := d.CommandWALNextLSN(); got != before {
		t.Fatalf("resource rejection advanced next LSN from %d to %d", before, got)
	}
	if got := d.State().AppliedCommandLSN; got != applied {
		t.Fatalf("resource rejection advanced applied LSN from %d to %d", applied, got)
	}
	if err := d.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("resource rejection poisoned command WAL: %v", err)
	}
}

func TestPreparedInsertRejectsUnfittablePrimaryKeyBeforeCommandWAL(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	id := bytes.Repeat([]byte("k"), 5000)
	before := d.CommandWALNextLSN()
	beforeApplied := d.State().AppliedCommandLSN
	prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{[]byte(`{"row_id":1,"kind":"one"}`)}, 16<<20)
	if !errors.Is(err, ErrPreparedInsertResourceLimit) || prepared != nil {
		t.Fatalf("prepare=(%v,%v), want resource limit before token creation", prepared, err)
	}
	if got := d.CommandWALNextLSN(); got != before {
		t.Fatalf("unfittable primary key advanced command WAL next LSN from %d to %d", before, got)
	}
	if got := d.State().AppliedCommandLSN; got != beforeApplied {
		t.Fatalf("unfittable primary key advanced applied LSN from %d to %d", beforeApplied, got)
	}
	if err := d.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("unfittable primary key poisoned command WAL: %v", err)
	}
	if got, err := col.Get(id); err != nil || got != nil {
		t.Fatalf("unfittable primary key published row=%s, err=%v", got, err)
	}
	allowedID := bytes.Repeat([]byte("a"), preparedInsertMaxIDBytes)
	allowed, err := col.PrepareInsertBatchOwned([][]byte{allowedID}, [][]byte{[]byte(`{"row_id":2,"kind":"allowed"}`)}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatalf("prepare maximum-length ID: %v", err)
	}
	if _, err := allowed.Commit(); err != nil {
		t.Fatalf("commit maximum-length ID: %v", err)
	}
	if got, err := col.Get(allowedID); err != nil || !bytes.Contains(got, []byte(`"allowed"`)) {
		t.Fatalf("maximum-length ID row=%s, err=%v", got, err)
	}
}

func TestPreparedInsertRejectsOversizedSchemaReferencesBeforeTokenCopy(t *testing.T) {
	for _, tc := range []struct {
		name      string
		path      string
		namespace string
	}{
		{name: "column_path", path: strings.Repeat("p", preparedInsertMaxSchemaPathBytes+1)},
		{name: "asset_namespace", path: "kind", namespace: strings.Repeat("n", 200) + "/" + strings.Repeat("n", 200) + "/" + strings.Repeat("n", preparedInsertMaxAssetNamespaceBytes-401)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			enableColumnRetainedPlacementCommandWAL(t, dir)
			d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
			defer func() { _ = d.Close() }()
			cfg := &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{
					{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
					{Name: "kind", Path: tc.path, ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Nullable: true},
				},
				RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
				Reconstruction: ColumnReconstructionRetainedPayloadAndColumns,
			}
			if tc.namespace != "" {
				cfg.AssetManager = &ColumnAssetManagerConfig{Namespace: tc.namespace}
			}
			meta := CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: cfg}}
			if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
				t.Fatalf("create otherwise valid collection: %v", err)
			}
			col := openColumnRetainedPlacementCollection(t, d, meta.Name)
			id, doc := []byte("a"), []byte(`{"row_id":1,"kind":"one"}`)
			before := d.CommandWALNextLSN()
			if prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 16<<20); prepared != nil ||
				!errors.Is(err, ErrPreparedInsertIneligible) || errors.Is(err, ErrPreparedInsertResourceLimit) {
				t.Fatalf("oversized schema prepare=(%v,%v), want configuration ineligibility", prepared, err)
			}
			if got := d.CommandWALNextLSN(); got != before {
				t.Fatalf("rejected schema advanced command WAL next LSN from %d to %d", before, got)
			}
			if _, err := col.InsertBatch([][]byte{id}, [][]byte{doc}); err != nil {
				t.Fatalf("ordinary path for valid schema: %v", err)
			}
		})
	}
}

func TestPreparedInsertSortedValuesAndReopen(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids := [][]byte{[]byte("z"), []byte("a"), []byte("m")}
	docs := [][]byte{
		[]byte(`{"row_id":3,"kind":"z","extra":null}`),
		[]byte(`{"row_id":1,"kind":"a"}`),
		[]byte(`{"row_id":2,"kind":"m","extra":{"nested":true}}`),
	}
	prepared, err := col.PrepareInsertBatchOwned(ids, docs, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.OwnedBytes() <= 0 || prepared.OwnedBytes() > 16<<20 {
		t.Fatalf("owned bytes=%d", prepared.OwnedBytes())
	}
	if prepared.ReservedBytes() <= prepared.OwnedBytes() || prepared.ReservedBytes() > preparedInsertTestRequestLimit {
		t.Fatalf("reservation=%d owned=%d", prepared.ReservedBytes(), prepared.OwnedBytes())
	}
	if publisher := preparedInsertPublisherReserveBytes(); prepared.ReservedBytes()-prepared.OwnedBytes() < publisher {
		t.Fatalf("reservation=%d owned=%d omits fixed publisher tranche %d", prepared.ReservedBytes(), prepared.OwnedBytes(), publisher)
	}
	resultIDs, err := prepared.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if prepared.collection != nil || prepared.meta.Name != "" || prepared.retained.semanticStreamBlocks != nil {
		t.Fatal("consumed batch still retains collection or prepared buffers")
	}
	for i := range ids {
		if !bytes.Equal(resultIDs[i], ids[i]) {
			t.Fatalf("result ID %d=%q want %q", i, resultIDs[i], ids[i])
		}
		got, err := col.Get(ids[i])
		if err != nil || !bytes.Equal(got, docs[i]) {
			t.Fatalf("Get %q=%s, %v want %s", ids[i], got, err, docs[i])
		}
	}
	if _, err := prepared.Commit(); err == nil {
		t.Fatal("second commit succeeded")
	}
	// Ownership returns after Commit. Persistent values and returned IDs must
	// remain intact when the caller reuses the handed-off input buffers.
	wantIDs, wantDocs := make([][]byte, len(ids)), make([][]byte, len(docs))
	for i := range ids {
		wantIDs[i] = bytes.Clone(ids[i])
		wantDocs[i] = bytes.Clone(docs[i])
		for j := range ids[i] {
			ids[i][j] = 'x'
		}
		for j := range docs[i] {
			docs[i][j] = 'x'
		}
		if !bytes.Equal(resultIDs[i], wantIDs[i]) {
			t.Fatalf("returned ID %d changed after caller reuse: %q", i, resultIDs[i])
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col = openColumnRetainedPlacementCollection(t, d, "events")
	for i := range ids {
		got, err := col.Get(wantIDs[i])
		if err != nil || !bytes.Equal(got, wantDocs[i]) {
			t.Fatalf("reopen Get %q=%s, %v want %s", wantIDs[i], got, err, wantDocs[i])
		}
	}
}

func TestPreparedInsertRejectsHiddenOuterSliceOwners(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	id, doc := []byte("a"), []byte(`{"row_id":1,"kind":"one"}`)
	for _, tc := range []struct {
		name      string
		ids, docs [][]byte
	}{
		{name: "id_tail", ids: [][]byte{id, make([]byte, 1<<20)}[:1], docs: [][]byte{doc}},
		{name: "document_tail", ids: [][]byte{id}, docs: [][]byte{doc, make([]byte, 1<<20)}[:1]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := d.CommandWALNextLSN()
			prepared, err := col.PrepareInsertBatchOwned(tc.ids, tc.docs, 16<<20)
			if prepared != nil || !errors.Is(err, ErrPreparedInsertResourceLimit) {
				t.Fatalf("prepare=(%v,%v), want resource limit for hidden outer owner", prepared, err)
			}
			if got := d.CommandWALNextLSN(); got != before {
				t.Fatalf("rejected outer owner advanced command WAL next LSN from %d to %d", before, got)
			}
		})
	}
	// A partially filled outer allocation is eligible when the unused slots
	// cannot retain any backing. The final JSONBench batch has this shape.
	ids, docs := make([][]byte, 1, 2), make([][]byte, 1, 2)
	ids[0], docs[0] = id, doc
	prepared, err := col.PrepareInsertBatchOwned(ids, docs, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatalf("nil-tail prepare: %v", err)
	}
	prepared.Abandon()
}

func TestPreparedInsertRejectsBatchHeaderCreditBeforePreparation(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids := [][]byte{[]byte("a")}
	docs := [][]byte{[]byte(`{"row_id":1,"kind":"one"}`)}
	limit := preparedInsertInputBytes(ids, docs) + 128*int64(len(ids)) + preparedSemanticStreamBatchReserveBytes - 1
	beforeNext, beforeApplied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN
	prepared, err := col.PrepareInsertBatchOwned(ids, docs, limit)
	if prepared != nil || !errors.Is(err, ErrPreparedInsertResourceLimit) || !strings.Contains(err.Error(), "batch header credit") {
		t.Fatalf("prepare=(%v,%v), want pre-allocation batch header limit", prepared, err)
	}
	if next, applied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN; next != beforeNext || applied != beforeApplied {
		t.Fatalf("rejected batch advanced command WAL next/applied LSN to %d/%d from %d/%d", next, applied, beforeNext, beforeApplied)
	}
}

func TestPreparedInsertChargesInternedDeclaredStringOnce(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids := [][]byte{[]byte("a"), []byte("b")}
	docs := [][]byte{
		[]byte(`{"row_id":1,"kind":"repeated"}`),
		[]byte(`{"row_id":2,"kind":"repeated"}`),
	}
	prepared, err := col.PrepareInsertBatchOwned(ids, docs, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Abandon()
	if got, want := prepared.retained.declaredStringBackingBytes, int64(len("repeated")); got != want {
		t.Fatalf("declared string backing=%d want one interned copy %d", got, want)
	}
}

func TestPreparedInsertAbandonBoundsAndLateConflict(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	id := []byte("id")
	doc := []byte(`{"row_id":1,"kind":"first"}`)
	if _, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 1); !errors.Is(err, ErrPreparedInsertResourceLimit) || errors.Is(err, ErrPreparedInsertIneligible) {
		t.Fatalf("oversized prepare error=%v", err)
	}
	prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.typedBatch == nil || prepared.typedBatch.Options.PartID != 0 {
		t.Fatalf("typed preparation retained a publication identity: %+v", prepared.typedBatch)
	}
	prepared.Abandon()
	if prepared.collection != nil || prepared.meta.Name != "" || prepared.retained.semanticStreamBlocks != nil {
		t.Fatal("abandoned batch still retains collection or prepared buffers")
	}
	if _, err := prepared.Commit(); err == nil {
		t.Fatal("abandoned commit succeeded")
	}
	if got, err := col.Get(id); err != nil || got != nil {
		t.Fatalf("abandoned prepare published a row: %s, %v", got, err)
	}
	prepared, err = col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Commit(); !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("late conflict error=%v, want document exists", err)
	}
	beforeNext, beforeApplied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN
	prepared, err = col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatalf("preparing a pre-existing ID for late validation: %v", err)
	}
	if _, err := prepared.Commit(); !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("pre-existing ID commit error=%v, want document exists", err)
	}
	if next, applied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN; next != beforeNext || applied != beforeApplied {
		t.Fatalf("pre-existing ID rejection advanced command WAL next/applied LSN to %d/%d from %d/%d", next, applied, beforeNext, beforeApplied)
	}
	if _, err := col.PrepareInsertBatchOwned([][]byte{id, id}, [][]byte{doc, doc}, 16<<20); !errors.Is(err, ErrDuplicateDocumentID) {
		t.Fatalf("within-batch duplicate error=%v", err)
	}
}

func TestPreparedInsertMalformedExistingIDErrorPrecedence(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	id := []byte("id")
	if _, err := col.InsertBatch([][]byte{id}, [][]byte{[]byte(`{"row_id":1,"kind":"first"}`)}); err != nil {
		t.Fatal(err)
	}

	malformed := []byte(`{"row_id":`)
	beforeNext, beforeApplied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN
	if prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{malformed}, preparedInsertTestRequestLimit); prepared != nil || err == nil || errors.Is(err, ErrDocumentExists) {
		t.Fatalf("prepared malformed existing-ID result=(%v, %v), want source-validation error before late conflict", prepared, err)
	}
	if next, applied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN; next != beforeNext || applied != beforeApplied {
		t.Fatalf("prepared source rejection advanced command WAL next/applied LSN to %d/%d from %d/%d", next, applied, beforeNext, beforeApplied)
	}
	if _, err := col.InsertBatch([][]byte{id}, [][]byte{malformed}); !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("ordinary malformed existing-ID error=%v, want document exists", err)
	}
	if next, applied := d.CommandWALNextLSN(), d.State().AppliedCommandLSN; next != beforeNext || applied != beforeApplied {
		t.Fatalf("ordinary conflict rejection advanced command WAL next/applied LSN to %d/%d from %d/%d", next, applied, beforeNext, beforeApplied)
	}
}

func TestPreparedInsertCheckpointBeforeCommitAndReopen(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	sibling := createColumnRetainedSemanticStreamCollection(t, d, "sibling")
	id, doc := []byte("checkpoint-row"), []byte(`{"row_id":41,"kind":"checkpoint"}`)
	siblingID, siblingDoc := []byte("sibling-row"), []byte(`{"row_id":42,"kind":"ordinary"}`)
	beforeLSN := d.State().AppliedCommandLSN
	prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	if got := d.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("prepare advanced command LSN from %d to %d", beforeLSN, got)
	}
	commitGate := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-commitGate:
		default:
			close(commitGate)
		}
	})
	commitQueued := make(chan struct{})
	commitDone := make(chan error, 1)
	go func() {
		close(commitQueued)
		<-commitGate
		_, commitErr := prepared.Commit()
		commitDone <- commitErr
	}()
	<-commitQueued
	if _, err := sibling.InsertBatch([][]byte{siblingID}, [][]byte{siblingDoc}); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	interleavedLSN := d.State().AppliedCommandLSN
	if interleavedLSN <= beforeLSN {
		t.Fatalf("sibling write did not advance command LSN: %d <= %d", interleavedLSN, beforeLSN)
	}
	// Force the late commit through its bounded cold-catalog reader after the
	// sibling write and checkpoint changed the system root.
	col.catalogMu.Lock()
	col.catalog = nil
	col.catalogMu.Unlock()
	col.writeDomain.mu.Lock()
	col.writeDomain.catalog = nil
	col.writeDomain.mu.Unlock()
	select {
	case err := <-commitDone:
		t.Fatalf("queued prepared commit acknowledged before release: %v", err)
	default:
	}
	if got, err := col.Get(id); err != nil || got != nil {
		t.Fatalf("prepared row visible before commit: %s, %v", got, err)
	}
	close(commitGate)
	if err := <-commitDone; err != nil {
		t.Fatal(err)
	}
	if got := d.State().AppliedCommandLSN; got <= interleavedLSN {
		t.Fatalf("late commit did not advance command LSN: %d <= %d", got, interleavedLSN)
	}
	typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(typedRefs) != 1 || typedRefs[0].PartID != typedColumnPartAssetPartID || typedRefs[0].Generation == 0 {
		t.Fatalf("late-bound typed refs=%+v", typedRefs)
	}
	typedRaw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), typedRefs[0])
	if err != nil {
		t.Fatal(err)
	}
	typedImage, err := typedcolumn.ParseColumnPartImage(typedRaw)
	if err != nil {
		t.Fatal(err)
	}
	if typedImage.PartID != typedRefs[0].PartID || typedImage.Rows != 1 {
		t.Fatalf("late-bound typed image part=%d rows=%d ref=%+v", typedImage.PartID, typedImage.Rows, typedRefs[0])
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col = openColumnRetainedPlacementCollection(t, d, "events")
	if got := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col)); !reflect.DeepEqual(got, typedRefs) {
		t.Fatalf("reopened typed refs=%+v want %+v", got, typedRefs)
	}
	if got, err := col.Get(id); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("reopened committed row=%s, %v want %s", got, err, doc)
	}
	sibling = openColumnRetainedPlacementCollection(t, d, "sibling")
	if got, err := sibling.Get(siblingID); err != nil || !bytes.Equal(got, siblingDoc) {
		t.Fatalf("reopened sibling row=%s, %v want %s", got, err, siblingDoc)
	}
}

func TestPreparedInsertValueLogBlockPointerSurvivesReopenAndGC(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids, docs := retainedSemanticStreamDocuments(96)
	prepared, err := col.PrepareInsertBatchOwned(ids, docs, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	blockKey, row, ptr := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[17])
	if row != 17 || !page.IsValueLogFileID(ptr.FileID) {
		t.Fatalf("prepared locator row=%d pointer=%+v", row, ptr)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col = openColumnRetainedPlacementCollection(t, d, "events")
	if _, err := d.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
		t.Fatal(err)
	}
	reopenedKey, reopenedRow, reopenedPtr := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[17])
	if !bytes.Equal(reopenedKey, blockKey) || reopenedRow != row || reopenedPtr != ptr {
		t.Fatalf("prepared block changed after reopen and GC: row %d/%d pointer %+v/%+v", reopenedRow, row, reopenedPtr, ptr)
	}
	got, err := col.Get(ids[17])
	if err != nil {
		t.Fatal(err)
	}
	assertRetainedSemanticStreamDocument(t, got, 17)
}

func TestPreparedInsertCrashRecoveryCuts(t *testing.T) {
	const id = "prepared-cut-row"
	doc := []byte(`{"row_id":51,"kind":"crash-cut"}`)
	opts := backenddb.Options{CommandWAL: true, ResolvedProfile: backenddb.ProfileCommandWALDurable}
	if dir := os.Getenv("GOMAP_PREPARED_INSERT_CRASH_DIR"); dir != "" {
		d := openColumnRetainedPlacementDB(t, dir, opts)
		col := openColumnRetainedPlacementCollection(t, d, "events")
		prepared, err := col.PrepareInsertBatchOwned([][]byte{[]byte(id)}, [][]byte{doc}, preparedInsertTestRequestLimit)
		if err != nil {
			t.Fatal(err)
		}
		mode := os.Getenv("GOMAP_PREPARED_INSERT_CRASH_MODE")
		injected := errors.New("prepared commit crash cut")
		var fired atomic.Bool
		if mode == "ack" {
			durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyFileSync {
					fired.Store(true)
				}
				return nil
			})
		} else {
			resource, point := durabilitycut.ResourceCommandWAL, durabilitycut.BeforeDependencyAppend
			switch mode {
			case "wal_after_lsn_assignment":
				point = durabilitycut.AfterDependencyAppend
			case "wal_after_sync":
				point = durabilitycut.AfterDependencyFileSync
			case "asset_before_sync":
				resource, point = durabilitycut.ResourceAuxiliary, durabilitycut.BeforeDependencyFileSync
			case "before_applied_lsn_candidate":
				resource, point = durabilitycut.ResourceMeta, durabilitycut.BeforeAppliedLSNAdvance
			case "after_applied_lsn_candidate":
				resource, point = durabilitycut.ResourceMeta, durabilitycut.AfterAppliedLSNAdvance
			case "before_seal_write":
				resource, point = durabilitycut.ResourceSeal, durabilitycut.BeforePublicationSealWrite
			case "before_meta_write":
				resource, point = durabilitycut.ResourceMeta, durabilitycut.BeforeMetaWrite
			case "wal_before_append":
			default:
				t.Fatalf("unknown crash mode %q", mode)
			}
			durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource == resource && event.Point == point && fired.CompareAndSwap(false, true) {
					return injected
				}
				return nil
			})
		}
		_, err = prepared.Commit()
		if (mode == "before_seal_write" || mode == "before_meta_write") && err == nil {
			// Command-WAL acknowledgment can precede queued root installation.
			// Checkpoint waits for that installation while the cut is active.
			err = d.Checkpoint()
		}
		if mode == "ack" {
			if err != nil || !fired.Load() {
				t.Fatalf("durable prepared acknowledgment err=%v WAL sync observed=%t", err, fired.Load())
			}
		} else if !fired.Load() || !errors.Is(err, injected) {
			t.Fatalf("cut %s fired=%t err=%v", mode, fired.Load(), err)
		}
		os.Exit(0) // Simulate process loss without Close or deferred sync.
	}
	for _, mode := range []string{"ack", "wal_before_append", "wal_after_lsn_assignment", "wal_after_sync", "asset_before_sync", "before_applied_lsn_candidate", "after_applied_lsn_candidate", "before_seal_write", "before_meta_write"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
				t.Fatal(err)
			}
			d := openColumnRetainedPlacementDB(t, dir, opts)
			createColumnRetainedSemanticStreamCollection(t, d, "events")
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestPreparedInsertCrashRecoveryCuts$")
			cmd.Env = append(os.Environ(), "GOMAP_PREPARED_INSERT_CRASH_DIR="+dir, "GOMAP_PREPARED_INSERT_CRASH_MODE="+mode)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child %s: %v\n%s", mode, err, output)
			}
			d = openColumnRetainedPlacementDB(t, dir, opts)
			defer d.Close()
			col := openColumnRetainedPlacementCollection(t, d, "events")
			got, err := col.Get([]byte(id))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "wal_before_append" {
				if got != nil {
					t.Fatalf("pre-WAL cut recovered uncommitted row: %s", got)
				}
			} else if mode == "wal_after_lsn_assignment" {
				// An append without a successful sync is ambiguous. Recovery may
				// select the complete frame or discard its unsynced suffix.
				if got != nil && !bytes.Equal(got, doc) {
					t.Fatalf("post-append cut recovered wrong row: %s", got)
				}
			} else if !bytes.Equal(got, doc) {
				t.Fatalf("%s recovered row=%s, want %s", mode, got, doc)
			}
		})
	}
}

func TestPreparedInsertRejectsMismatchedCapturedSchema(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	id := []byte("schema-row")
	prepared, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{[]byte(`{"row_id":42,"kind":"one","payload":"value"}`)}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	// The command-WAL profile rejects catalog index DDL. Inject a mismatch in
	// the captured prepared metadata to exercise commit's catalog comparison.
	prepared.meta.Options.ColumnStore.Columns[1].Name = "changed_kind"
	beforeLSN := d.State().AppliedCommandLSN
	if _, err := prepared.Commit(); err == nil || !strings.Contains(err.Error(), "concurrent schema modification") {
		t.Fatalf("stale prepared commit error=%v", err)
	}
	if got := d.State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("rejected commit advanced command LSN from %d to %d", beforeLSN, got)
	}
	if got, err := openColumnRetainedPlacementCollection(t, d, "events").Get(id); err != nil || got != nil {
		t.Fatalf("rejected row visible: %s, %v", got, err)
	}
}

func TestPreparedInsertRarePathsStayWithinBudget(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	var document strings.Builder
	document.WriteString(`{"row_id":1,"kind":"many"`)
	for i := 0; i < 512; i++ {
		fmt.Fprintf(&document, `,"field_%d":%d`, i, i)
	}
	document.WriteByte('}')
	prepared, err := col.PrepareInsertBatchOwned([][]byte{[]byte("wide")}, [][]byte{[]byte(document.String())}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.OwnedBytes() > 16<<20 {
		t.Fatalf("owned bytes=%d exceed budget", prepared.OwnedBytes())
	}
	if _, err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := col.Get([]byte("wide"))
	if err != nil {
		t.Fatal(err)
	}
	var gotObject, wantObject map[string]any
	if err := json.Unmarshal(got, &gotObject); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(document.String()), &wantObject); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotObject, wantObject) {
		t.Fatal("wide row changed after commit")
	}
}

func TestPreparedInsertDenseHighEntropyBlock(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	const rows, fields = 512, 24
	ids, documents := make([][]byte, rows), make([][]byte, rows)
	for row := range documents {
		ids[row] = []byte(fmt.Sprintf("entropy-%04d", row))
		var document strings.Builder
		fmt.Fprintf(&document, `{"row_id":%d,"kind":"entropy"`, row)
		for field := 0; field < fields; field++ {
			digest := sha256.Sum256([]byte(fmt.Sprintf("row-%d-field-%d", row, field)))
			fmt.Fprintf(&document, `,"field_%02d":"%x"`, field, digest)
		}
		document.WriteByte('}')
		documents[row] = []byte(document.String())
	}
	if input := preparedInsertInputBytes(ids, documents); input >= 10<<20 {
		t.Fatalf("adversarial source input %d exceeds target source slot", input)
	}
	prepared, err := col.PrepareInsertBatchOwned(ids, documents, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.OwnedBytes() > preparedInsertTestRequestLimit || prepared.ReservedBytes() > preparedInsertTestRequestLimit {
		t.Fatalf("prepared charges owned=%d reserved=%d exceed test request limit", prepared.OwnedBytes(), prepared.ReservedBytes())
	}
	if _, err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, row := range []int{0, rows / 2, rows - 1} {
		got, err := col.Get(ids[row])
		if err != nil || !bytes.Equal(got, documents[row]) {
			t.Fatalf("dense row %d got=%d bytes err=%v", row, len(got), err)
		}
	}
}

func TestPreparedInsertNearRowLimitHighEntropy(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids, documents := make([][]byte, preparedInsertMaxRows), make([][]byte, preparedInsertMaxRows)
	for row := range documents {
		ids[row] = []byte(fmt.Sprintf("entropy-%05d", row))
		var document strings.Builder
		fmt.Fprintf(&document, `{"row_id":%d,"kind":"entropy"`, row)
		for field := 0; field < 4; field++ {
			digest := sha256.Sum256([]byte(fmt.Sprintf("row-%d-field-%d", row, field)))
			fmt.Fprintf(&document, `,"field_%02d":"%x"`, (row+field)%64, digest)
		}
		document.WriteByte('}')
		documents[row] = []byte(document.String())
	}
	if input := preparedInsertInputBytes(ids, documents); input >= 10<<20 {
		t.Fatalf("near-limit source input %d exceeds target source slot", input)
	}
	prepared, err := col.PrepareInsertBatchOwned(ids, documents, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.OwnedBytes() > preparedInsertTestRequestLimit || prepared.ReservedBytes() > preparedInsertTestRequestLimit {
		t.Fatalf("prepared charges owned=%d reserved=%d exceed test request limit", prepared.OwnedBytes(), prepared.ReservedBytes())
	}
	t.Logf("near-limit input=%d charged_owned=%d estimated_reserved=%d", preparedInsertInputBytes(ids, documents), prepared.OwnedBytes(), prepared.ReservedBytes())
	if _, err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, row := range []int{0, preparedInsertMaxRows / 2, preparedInsertMaxRows - 1} {
		got, err := col.Get(ids[row])
		if err != nil {
			t.Fatalf("near-limit row %d Get: %v", row, err)
		}
		var gotObject, wantObject map[string]any
		if err := json.Unmarshal(got, &gotObject); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(documents[row], &wantObject); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(gotObject, wantObject) {
			t.Fatalf("near-limit row %d got=%s want=%s", row, got, documents[row])
		}
	}
}

func TestPreparedInsertRejectsBlockPathGrowthBeforePublication(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids := [][]byte{[]byte("wide-a"), []byte("wide-b")}
	documents := make([][]byte, len(ids))
	for row := range documents {
		var document strings.Builder
		fmt.Fprintf(&document, `{"row_id":%d,"kind":"wide"`, row+1)
		for field := 0; field < 600; field++ {
			fmt.Fprintf(&document, `,"field_%d_%d":%d`, row, field, field)
		}
		document.WriteByte('}')
		documents[row] = []byte(document.String())
	}
	if _, err := col.PrepareInsertBatchOwned(ids, documents, 64<<20); !errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("block path limit: %v", err)
	}
	for _, id := range ids {
		if got, err := col.Get(id); err != nil || got != nil {
			t.Fatalf("rejected row %q became visible: %s, %v", id, got, err)
		}
	}
}

func TestPreparedInsertRejectsOversizedPathKeyBeforePublication(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	id := []byte("long-path")
	document := []byte(`{"row_id":1,"kind":"wide","` + strings.Repeat("a", preparedSemanticStreamMaxKeyBytes+1) + `":1}`)
	if _, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{document}, 32<<20); !errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("path-key limit: %v", err)
	}
	if got, err := col.Get(id); err != nil || got != nil {
		t.Fatalf("rejected row became visible: %s, %v", got, err)
	}
}

func TestPreparedInsertRejectsUnsupportedStructuralShapesBeforeCommit(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")

	deep := `{"row_id":1,"kind":"deep","extra":`
	for range preparedSemanticStreamMaxCursorDepth + 1 {
		deep += `{"nested":`
	}
	deep += `true` + strings.Repeat("}", preparedSemanticStreamMaxCursorDepth+1) + `}`
	wide := `{"row_id":2,"kind":"wide"`
	for i := 0; i < preparedSemanticStreamMaxCursorDescriptors+1; i++ {
		wide += fmt.Sprintf(`,"field_%d":%d`, i, i)
	}
	wide += `}`
	oversize := `{"row_id":3,"kind":"oversize","extra":"` + strings.Repeat("x", preparedInsertMaxDocumentBytes) + `"}`
	for i, document := range []string{deep, wide, oversize} {
		id := []byte(fmt.Sprintf("shape-%d", i))
		if _, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{[]byte(document)}, 32<<20); !errors.Is(err, ErrPreparedInsertResourceLimit) || errors.Is(err, ErrPreparedInsertIneligible) {
			t.Errorf("shape %d prepared error=%v, want resource limit", i, err)
		}
		if got, err := col.Get(id); err != nil || got != nil {
			t.Errorf("shape %d visible after rejected prepare: %s, %v", i, got, err)
		}
		if _, err := col.InsertBatch([][]byte{id}, [][]byte{[]byte(document)}); err != nil {
			t.Errorf("shape %d ordinary fallback: %v", i, err)
		}
	}
}

func TestPreparedInsertFallsBackBeforeUnboundedDeclaredRowExtraction(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer d.Close()
	meta := CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
			{Name: "active", Path: "active", ValueType: ColumnStoreValueBool, Owner: TypedStorageOwnerColumnPart}},
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction: ColumnReconstructionRetainedPayloadAndColumns,
	}}}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col := openColumnRetainedPlacementCollection(t, d, "events")
	id, doc := []byte("bool-row"), []byte(`{"row_id":7,"active":true}`)
	if _, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 16<<20); !errors.Is(err, ErrPreparedInsertIneligible) || errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("unsupported declared-row parser shape prepared: %v", err)
	}
	if _, err := prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDsBudget(*meta.Options.ColumnStore, [][]byte{id}, [][]byte{doc}, 16<<20); !errors.Is(err, ErrPreparedInsertIneligible) {
		t.Fatalf("budgeted helper entered unbounded declared-row extraction: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatalf("ordinary fallback: %v", err)
	}
	if got, err := col.Get(id); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("ordinary fallback row=%s, err=%v", got, err)
	}
}

func TestPreparedInsertSixScalarColumnsUseOrdinaryPath(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer d.Close()
	columns := make([]ColumnStoreColumn, 6)
	for i := range columns {
		name := fmt.Sprintf("field_%d", i)
		columns[i] = ColumnStoreColumn{Name: name, Path: name, ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart}
	}
	meta := CollectionMeta{Name: "wide", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: &ColumnStoreConfig{
		Enabled: true, Columns: columns,
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction: ColumnReconstructionRetainedPayloadAndColumns,
	}}}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col := openColumnRetainedPlacementCollection(t, d, "wide")
	id, doc := []byte("wide-row"), []byte(`{"field_0":"a","field_1":"b","field_2":"c","field_3":"d","field_4":"e","field_5":"f"}`)
	if _, err := col.PrepareInsertBatchOwned([][]byte{id}, [][]byte{doc}, 16<<20); !errors.Is(err, ErrPreparedInsertIneligible) || errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("six-column prepared error=%v, want configuration ineligibility", err)
	}
	if _, err := col.InsertBatch([][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	if got, err := col.Get(id); err != nil || !bytes.Equal(got, doc) {
		t.Fatalf("ordinary six-column row=%s, err=%v", got, err)
	}
}

func TestPreparedInsertAggregateMetadataFanoutRejectedBeforeCommandWAL(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer d.Close()
	meta := CollectionMeta{Name: "metadata", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		AggregateMetadata: []ColumnAggregateMetadata{
			{Name: "by_kind_1", GroupColumn: "kind", Kind: ColumnAggregateCount},
			{Name: "by_kind_2", GroupColumn: "kind", Kind: ColumnAggregateCount},
			{Name: "by_kind_3", GroupColumn: "kind", Kind: ColumnAggregateCount},
			{Name: "by_kind_4", GroupColumn: "kind", Kind: ColumnAggregateCount},
		},
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction: ColumnReconstructionRetainedPayloadAndColumns,
	}}}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col := openColumnRetainedPlacementCollection(t, d, meta.Name)
	before := d.CommandWALNextLSN()
	_, err := col.PrepareInsertBatchOwned([][]byte{[]byte("a")}, [][]byte{[]byte(`{"row_id":1,"kind":"a"}`)}, 16<<20)
	if !errors.Is(err, ErrPreparedInsertIneligible) {
		t.Fatalf("aggregate metadata prepared err=%v, want configuration ineligibility", err)
	}
	if got := d.CommandWALNextLSN(); got != before {
		t.Fatalf("command WAL next LSN=%d, want unchanged %d", got, before)
	}
	if err := d.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("rejected preparation poisoned command WAL: %v", err)
	}
}

func TestPreparedInsertThreeAggregateSpecsRemainEligible(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer d.Close()
	postPredicates := []ColumnPhysicalQueryPredicate{
		{Column: "kind", Value: "commit"},
		{Column: "operation", Value: "create"},
		{Column: "event", Value: "app.bsky.feed.post"},
	}
	q3Predicates := append([]ColumnPhysicalQueryPredicate(nil), postPredicates...)
	q3Predicates[2] = ColumnPhysicalQueryPredicate{Column: "event", Kind: ColumnPhysicalQueryPredicateInList,
		Values: []string{"app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"}}
	meta := CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "event", Path: "commit.collection", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
			{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
			{Name: "operation", Path: "commit.operation", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
		},
		SortKey: []ColumnSortKey{{Column: "time_us"}},
		AggregateMetadata: []ColumnAggregateMetadata{
			{Name: "q1", GroupColumn: "event", Kind: ColumnAggregateCount},
			{Name: "q3", Column: "time_us", GroupColumn: "event", Kind: ColumnAggregateGroupHourCount, Predicates: q3Predicates},
			{Name: "q5", Column: "time_us", GroupColumn: "did", Kind: ColumnAggregateMin, Predicates: postPredicates},
		},
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction: ColumnReconstructionRetainedPayloadAndColumns,
	}}}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col := openColumnRetainedPlacementCollection(t, d, meta.Name)
	before := d.CommandWALNextLSN()
	prepared, err := col.PrepareInsertBatchOwned([][]byte{[]byte("a")},
		[][]byte{[]byte(`{"commit":{"collection":"app.bsky.feed.post","operation":"create"},"did":"did:one","kind":"commit","time_us":123}`)}, preparedInsertTestRequestLimit)
	if err != nil {
		t.Fatalf("three-spec target preparation: %v", err)
	}
	if prepared.typedBatch == nil || prepared.typedBatch.Options.PartID != 0 {
		t.Fatalf("three-spec target missing identity-free typed batch: %+v", prepared.typedBatch)
	}
	if got := prepared.typedBatch.Options.DefaultCompression; got != typedcolumn.CompressionLZ4 {
		t.Fatalf("target typed granule compression=%s, want LZ4", got)
	}
	if got := prepared.typedBatch.Options.SectionCompression; got != typedcolumn.CompressionZSTD {
		t.Fatalf("target typed section compression=%s, want ZSTD", got)
	}
	if got := d.CommandWALNextLSN(); got != before {
		t.Fatalf("preparation advanced command WAL next LSN from %d to %d", before, got)
	}
	if _, err := prepared.Commit(); err != nil {
		t.Fatalf("three-spec target commit: %v", err)
	}
}

func TestPreparedInsertRejectsZSTDGranuleOverrideBeforeCommandWAL(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	meta := CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		ProfileSupport:  ColumnStoreProfileBenchmarkRelaxed,
		RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction: ColumnReconstructionRetainedPayloadAndColumns,
	}}}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col := openColumnRetainedPlacementCollection(t, d, meta.Name)
	t.Setenv(typedColumnBenchmarkCompressionEnv, "zstd")
	before := d.CommandWALNextLSN()
	prepared, err := col.PrepareInsertBatchOwned([][]byte{[]byte("a")}, [][]byte{[]byte(`{"row_id":1,"kind":"one"}`)}, 32<<20)
	if prepared != nil || !errors.Is(err, ErrPreparedInsertIneligible) {
		t.Fatalf("ZSTD-granule override prepare=(%v,%v), want configuration ineligibility", prepared, err)
	}
	if got := d.CommandWALNextLSN(); got != before {
		t.Fatalf("rejected override advanced command WAL next LSN from %d to %d", before, got)
	}
	if err := d.CheckCommandWALPublishReady(); err != nil {
		t.Fatalf("rejected override poisoned command WAL: %v", err)
	}
}
