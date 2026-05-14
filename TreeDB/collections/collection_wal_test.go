package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/collectionwal"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionWALNoIndexInsertBatchWALOnEmitsCommittedFramesAndWatermark(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"name":"ada"}`), []byte(`{"name":"grace"}`)},
	); err != nil {
		t.Fatalf("first InsertBatch: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u3")},
		[][]byte{[]byte(`{"name":"katherine"}`)},
	); err != nil {
		t.Fatalf("second InsertBatch: %v", err)
	}

	path := collectionwal.SegmentPath(dir, 0, 1)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read collection WAL segment: %v", err)
	}
	header, frames, err := collectionwal.ScanSegment(data, true)
	if err != nil {
		t.Fatalf("scan collection WAL segment: %v", err)
	}
	if header.Lane != 0 || header.SegmentSeq != 1 || header.FirstWALLSN != 1 {
		t.Fatalf("segment header=%+v", header)
	}
	if len(frames) != 2 {
		t.Fatalf("frame count=%d want 2", len(frames))
	}
	cfg, ok, err := backenddb.LoadFormatConfig(dir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if !ok {
		t.Fatalf("expected format.json after collection WAL write")
	}
	if !cfg.HasRequiredFeature(backenddb.FormatFeatureCollectionWALV1) {
		t.Fatalf("format.json required_features=%v, want %q", cfg.RequiredFeatures, backenddb.FormatFeatureCollectionWALV1)
	}
	uid, err := parseCollectionUID(meta.CollectionUID)
	if err != nil {
		t.Fatalf("parse collection uid: %v", err)
	}
	for i, frame := range frames {
		wantSeq := uint64(i + 1)
		if frame.Outcome != collectionwal.OutcomeCompleteValid {
			t.Fatalf("frame %d outcome=%s err=%v", i, frame.Outcome, frame.Err)
		}
		txn := frame.Transaction
		if frame.Header.WALLSN != wantSeq || txn.WALLSN != wantSeq {
			t.Fatalf("frame %d WALLSN header=%d txn=%d want %d", i, frame.Header.WALLSN, txn.WALLSN, wantSeq)
		}
		if txn.CollectionUID != uid {
			t.Fatalf("frame %d collection uid=%x want %x", i, txn.CollectionUID, uid)
		}
		if txn.CollectionSeq != wantSeq || txn.DependsOnCollectionSeq != wantSeq-1 {
			t.Fatalf("frame %d collection seq=%d depends=%d want %d/%d", i, txn.CollectionSeq, txn.DependsOnCollectionSeq, wantSeq, wantSeq-1)
		}
		if txn.MutationClass != collectionWALMutationClassNoIndexRowInsert || txn.RootDeltaCount != 1 || txn.SideRefCount != 0 || txn.DescriptorOpCount != 2 {
			t.Fatalf("frame %d txn header mutation=%d roots=%d sideRefs=%d descriptorOps=%d", i, txn.MutationClass, txn.RootDeltaCount, txn.SideRefCount, txn.DescriptorOpCount)
		}
		if !collectionWALSectionTypesPresent(txn, collectionwal.SectionTypeRootDeltaTable, collectionwal.SectionTypeSideRefTable, collectionwal.SectionTypeSystemDeltaTemplate, collectionwal.SectionTypeDescriptorOps) {
			t.Fatalf("frame %d missing required replay sections: %+v", i, txn.Sections)
		}
		if txn.BaseCatalogDigest == ([32]byte{}) || txn.CatalogDigest == ([32]byte{}) || txn.LocalReplayCatalogDigest == ([32]byte{}) {
			t.Fatalf("frame %d catalog digest missing: %+v", i, txn)
		}
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	applied, err := loadCollectionWALAppliedSeq(snap, meta.CollectionUID)
	if err != nil {
		t.Fatalf("load collection WAL applied seq: %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied seq=%d want 2", applied)
	}
	featureValue, ok, err := getSystemValue(snap, "treedb/storage-format/required-features/"+backenddb.FormatFeatureCollectionWALV1)
	if err != nil {
		t.Fatalf("load collection WAL system feature: %v", err)
	}
	if !ok || string(featureValue) != "true" {
		t.Fatalf("system feature value=%q ok=%t, want true", featureValue, ok)
	}
	stats := d.CollectionWALStatsSnapshot()
	if stats.AppendSuccess != 2 || stats.AppendFailure != 0 {
		t.Fatalf("collection WAL append stats success=%d failure=%d want 2/0", stats.AppendSuccess, stats.AppendFailure)
	}
	if stats.RetainedSegments != 1 || stats.RetainedBytes == 0 {
		t.Fatalf("collection WAL retained debt segments=%d bytes=%d want one non-empty segment", stats.RetainedSegments, stats.RetainedBytes)
	}
	if stats.ValueLogGCBlockerSegments != 0 || stats.ValueLogGCBlockerBytes != 0 {
		t.Fatalf("collection WAL value-log GC blocker stats segments=%d bytes=%d want 0/0", stats.ValueLogGCBlockerSegments, stats.ValueLogGCBlockerBytes)
	}
}

func TestCollectionWALNoIndexPrivatePlanningUsesGlobalPublisher(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	for _, name := range []string{"users", "orders"} {
		if _, err := mgr.CreateCollection(&CollectionMeta{
			Name: name,
			Options: CollectionOptions{
				CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
			},
		}); err != nil {
			t.Fatalf("create collection %s: %v", name, err)
		}
	}
	users, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open users collection: %v", err)
	}
	orders, err := mgr.OpenCollection("orders")
	if err != nil {
		t.Fatalf("open orders collection: %v", err)
	}

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	var firstOnce sync.Once
	var secondOnce sync.Once
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseFirst) })
	}
	defer release()
	users.testCollectionWALPrivatePlanHook = func() {
		firstOnce.Do(func() { close(firstEntered) })
		<-releaseFirst
	}
	orders.testCollectionWALPrivatePlanHook = func() {
		secondOnce.Do(func() { close(secondEntered) })
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := users.InsertBatch(
			[][]byte{[]byte("u1")},
			[][]byte{[]byte(`{"name":"ada"}`)},
		)
		firstDone <- err
	}()
	waitCollectionWALSignal(t, firstEntered, "first private plan")

	secondDone := make(chan error, 1)
	go func() {
		_, err := orders.InsertBatch(
			[][]byte{[]byte("o1")},
			[][]byte{[]byte(`{"sku":"book"}`)},
		)
		secondDone <- err
	}()
	assertCollectionWALStillBlocked(t, secondEntered, secondDone, "second private plan")

	release()
	if err := waitCollectionWALErr(t, firstDone, "first InsertBatch"); err != nil {
		t.Fatalf("first InsertBatch: %v", err)
	}
	waitCollectionWALSignal(t, secondEntered, "second private plan")
	if err := waitCollectionWALErr(t, secondDone, "second InsertBatch"); err != nil {
		t.Fatalf("second InsertBatch: %v", err)
	}

	data, err := os.ReadFile(collectionwal.SegmentPath(dir, 0, 1))
	if err != nil {
		t.Fatalf("read collection WAL segment: %v", err)
	}
	_, frames, err := collectionwal.ScanSegment(data, true)
	if err != nil {
		t.Fatalf("scan collection WAL segment: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("frame count=%d want 2", len(frames))
	}
}

func TestCollectionWALNoIndexInsertWALOnEmitsCommittedFrameAndWatermark(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	gotID, err := col.Insert([]byte("u1"), []byte(`{"name":"ada"}`))
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if string(gotID) != "u1" {
		t.Fatalf("Insert id=%q want u1", gotID)
	}

	data, err := os.ReadFile(collectionwal.SegmentPath(dir, 0, 1))
	if err != nil {
		t.Fatalf("read collection WAL segment: %v", err)
	}
	_, frames, err := collectionwal.ScanSegment(data, true)
	if err != nil {
		t.Fatalf("scan collection WAL segment: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("frame count=%d want 1", len(frames))
	}
	if frames[0].Outcome != collectionwal.OutcomeCompleteValid {
		t.Fatalf("frame outcome=%s err=%v", frames[0].Outcome, frames[0].Err)
	}
	if frames[0].Transaction.CollectionSeq != 1 || frames[0].Transaction.DependsOnCollectionSeq != 0 {
		t.Fatalf("collection seq=%d depends=%d want 1/0", frames[0].Transaction.CollectionSeq, frames[0].Transaction.DependsOnCollectionSeq)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	applied, err := loadCollectionWALAppliedSeq(snap, meta.CollectionUID)
	if err != nil {
		t.Fatalf("load collection WAL applied seq: %v", err)
	}
	if applied != 1 {
		t.Fatalf("applied seq=%d want 1", applied)
	}
}

func TestCollectionWALStatsAppendSuccess(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
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
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
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
		[][]byte{[]byte(`{"name":"ada"}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	stats := d.CollectionWALStatsSnapshot()
	if stats.AppendSuccess != 1 || stats.AppendFailure != 0 {
		t.Fatalf("collection WAL append stats success=%d failure=%d want 1/0", stats.AppendSuccess, stats.AppendFailure)
	}
	if stats.RetainedSegments != 1 || stats.RetainedBytes == 0 {
		t.Fatalf("collection WAL retained debt segments=%d bytes=%d want one non-empty segment", stats.RetainedSegments, stats.RetainedBytes)
	}
	if stats.RetainedDebtScanFailure != 0 {
		t.Fatalf("collection WAL retained debt scan failures=%d want 0", stats.RetainedDebtScanFailure)
	}
	nativeStats := d.Stats()
	if got := nativeStats["treedb.collection_wal.append.txns_total"]; got != "1" {
		t.Fatalf("native collection WAL append.txns_total=%q want 1", got)
	}
	if got := nativeStats["treedb.collection_wal.append.docs_total"]; got != "1" {
		t.Fatalf("native collection WAL append.docs_total=%q want 1", got)
	}
	if got := nativeStats["treedb.collection_wal.append.bytes_total"]; got == "" || got == "0" {
		t.Fatalf("native collection WAL append.bytes_total=%q want non-zero", got)
	}
	if got := nativeStats["treedb.collection_wal.cleanup.debt.segments_current"]; got != "1" {
		t.Fatalf("native collection WAL cleanup debt segments=%q want 1", got)
	}
}

func TestCollectionWALStatsAppendFailureBeforeCommit(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
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
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	segmentPath := collectionwal.SegmentPath(dir, 0, 1)
	if err := os.MkdirAll(filepath.Dir(segmentPath), 0o700); err != nil {
		t.Fatalf("mkdir collection WAL dir: %v", err)
	}
	if err := os.WriteFile(segmentPath, []byte("not a collection WAL segment"), 0o600); err != nil {
		t.Fatalf("write corrupt collection WAL segment: %v", err)
	}

	_, err = col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"ada"}`)},
	)
	if err == nil {
		t.Fatal("InsertBatch succeeded with corrupt collection WAL segment, want append/open failure")
	}
	doc, getErr := col.Get([]byte("u1"))
	if getErr != nil {
		t.Fatalf("Get after failed WAL append: %v", getErr)
	}
	if doc != nil {
		t.Fatalf("document became visible after failed WAL append: %s", doc)
	}
	stats := d.CollectionWALStatsSnapshot()
	if stats.AppendSuccess != 0 || stats.AppendFailure == 0 {
		t.Fatalf("collection WAL append stats success=%d failure=%d want 0/non-zero", stats.AppendSuccess, stats.AppendFailure)
	}
	nativeStats := d.Stats()
	if got := nativeStats["treedb.collection_wal.append.failures_total"]; got == "" || got == "0" {
		t.Fatalf("native collection WAL append.failures_total=%q want non-zero", got)
	}
}

func TestCollectionWALNoIndexInsertBatchWALOffDoesNotCreateSegment(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
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
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
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
		[][]byte{[]byte(`{"name":"ada"}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, err := os.Stat(collectionwal.SegmentPath(dir, 0, 1)); !os.IsNotExist(err) {
		t.Fatalf("collection WAL segment stat err=%v want not-exist", err)
	}
}

func TestCollectionWALNoIndexInsertBatchReopenContinuesSegment(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	})
	if err != nil {
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
		t.Fatalf("first InsertBatch: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	d, err = backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr = NewCollectionManager(d)
	col, err = mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("reopen collection: %v", err)
	}
	reopenStats := d.CollectionWALStatsSnapshot()
	if reopenStats.RecoverySkip != 1 || reopenStats.RecoveryReplay != 0 {
		t.Fatalf("reopen recovery stats skip=%d replay=%d want 1/0", reopenStats.RecoverySkip, reopenStats.RecoveryReplay)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"name":"grace"}`)},
	); err != nil {
		t.Fatalf("second InsertBatch: %v", err)
	}

	data, err := os.ReadFile(collectionwal.SegmentPath(dir, 0, 1))
	if err != nil {
		t.Fatalf("read collection WAL segment: %v", err)
	}
	_, frames, err := collectionwal.ScanSegment(data, true)
	if err != nil {
		t.Fatalf("scan collection WAL segment: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("frame count=%d want 2", len(frames))
	}
	if frames[0].Transaction.CollectionSeq != 1 || frames[1].Transaction.CollectionSeq != 2 {
		t.Fatalf("collection seqs=%d/%d want 1/2", frames[0].Transaction.CollectionSeq, frames[1].Transaction.CollectionSeq)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	applied, err := loadCollectionWALAppliedSeq(snap, meta.CollectionUID)
	if err != nil {
		t.Fatalf("load collection WAL applied seq: %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied seq=%d want 2", applied)
	}
}

func TestCollectionWALAppendFailureRejectsWriteBeforeVisibility(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	segmentPath := collectionwal.SegmentPath(dir, 0, 1)
	if err := os.MkdirAll(filepath.Dir(segmentPath), 0o700); err != nil {
		t.Fatalf("mkdir collection WAL dir: %v", err)
	}
	if err := os.WriteFile(segmentPath, []byte("not a collection WAL segment"), 0o600); err != nil {
		t.Fatalf("write corrupt collection WAL segment: %v", err)
	}

	_, err = col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"ada"}`)},
	)
	if err == nil {
		t.Fatal("InsertBatch succeeded with corrupt collection WAL segment, want append/open failure")
	}
	doc, getErr := col.Get([]byte("u1"))
	if getErr != nil {
		t.Fatalf("Get after failed WAL append: %v", getErr)
	}
	if doc != nil {
		t.Fatalf("document became visible after failed WAL append: %s", doc)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	applied, err := loadCollectionWALAppliedSeq(snap, meta.CollectionUID)
	if err != nil {
		t.Fatalf("load collection WAL applied seq: %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied seq=%d want 0 after failed WAL append", applied)
	}
	stats := d.CollectionWALStatsSnapshot()
	if stats.AppendFailure == 0 {
		t.Fatalf("collection WAL append failure stat=%d want non-zero", stats.AppendFailure)
	}
	if stats.AppendSuccess != 0 {
		t.Fatalf("collection WAL append success stat=%d want 0 after failed append", stats.AppendSuccess)
	}
}

func TestCollectionWALPostAppendFailureIsCommitAmbiguousAndRecovers(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	}); err != nil {
		_ = d.Close()
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		_ = d.Close()
		t.Fatalf("open collection: %v", err)
	}
	postAppendErr := errors.New("post collection WAL append failpoint")
	col.testCollectionWALPostAppendErr = postAppendErr

	_, err = col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"ada"}`)},
	)
	if !errors.Is(err, ErrCommitAmbiguous) {
		_ = d.Close()
		t.Fatalf("InsertBatch error=%v want ErrCommitAmbiguous", err)
	}
	if !errors.Is(err, postAppendErr) {
		_ = d.Close()
		t.Fatalf("InsertBatch error=%v want wrapped post-append failpoint", err)
	}
	doc, getErr := col.Get([]byte("u1"))
	if getErr != nil {
		_ = d.Close()
		t.Fatalf("Get after ambiguous error: %v", getErr)
	}
	if doc != nil {
		_ = d.Close()
		t.Fatalf("document became visible before recovery after ambiguous error: %s", doc)
	}
	stats := d.CollectionWALStatsSnapshot()
	if stats.AppendSuccess != 1 || stats.AppendFailure != 0 {
		_ = d.Close()
		t.Fatalf("collection WAL append stats success=%d failure=%d want 1/0", stats.AppendSuccess, stats.AppendFailure)
	}
	col.testCollectionWALPostAppendErr = nil
	_, err = col.InsertBatch(
		[][]byte{[]byte("u2")},
		[][]byte{[]byte(`{"name":"grace"}`)},
	)
	if !errors.Is(err, backenddb.ErrRecoveryRequired) {
		_ = d.Close()
		t.Fatalf("InsertBatch after ambiguous error=%v want ErrRecoveryRequired", err)
	}
	data, readErr := os.ReadFile(collectionwal.SegmentPath(dir, 0, 1))
	if readErr != nil {
		_ = d.Close()
		t.Fatalf("read collection WAL segment after blocked retry: %v", readErr)
	}
	_, frames, scanErr := collectionwal.ScanSegment(data, true)
	if scanErr != nil {
		_ = d.Close()
		t.Fatalf("scan collection WAL segment after blocked retry: %v", scanErr)
	}
	if len(frames) != 1 {
		_ = d.Close()
		t.Fatalf("frame count after blocked retry=%d want 1", len(frames))
	}
	if err := d.Checkpoint(); !errors.Is(err, backenddb.ErrRecoveryRequired) {
		_ = d.Close()
		t.Fatalf("Checkpoint after ambiguous error=%v want ErrRecoveryRequired", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	readOnly, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, backenddb.ErrRecoveryRequired) {
		if readOnly != nil {
			_ = readOnly.Close()
		}
		t.Fatalf("read-only open err=%v want ErrRecoveryRequired", err)
	}

	recovered, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("read-write recover open: %v", err)
	}
	defer func() { _ = recovered.Close() }()
	recoveredCol, err := NewCollectionManager(recovered).OpenCollection("users")
	if err != nil {
		t.Fatalf("open recovered collection: %v", err)
	}
	doc, err = recoveredCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get recovered document: %v", err)
	}
	if string(doc) != `{"name":"ada"}` {
		t.Fatalf("recovered document=%s want original JSON", doc)
	}
	recoveryStats := recovered.CollectionWALStatsSnapshot()
	if recoveryStats.RecoveryReplay != 1 || recoveryStats.RecoverySkip != 0 || recoveryStats.RecoveryHardFailure != 0 {
		t.Fatalf("collection WAL recovery stats replay=%d skip=%d hardFailure=%d want 1/0/0", recoveryStats.RecoveryReplay, recoveryStats.RecoverySkip, recoveryStats.RecoveryHardFailure)
	}
}

func TestCollectionWALOversizedInlineDocumentRejectsBeforeFrame(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
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
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc := append([]byte(`{"blob":"`), bytes.Repeat([]byte("x"), collectionwal.MaxInlineDeltaValueBytes)...)
	doc = append(doc, []byte(`"}`)...)

	_, err = col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc})
	if !errors.Is(err, collectionwal.ErrCollectionWALResourceLimit) {
		t.Fatalf("InsertBatch error=%v want ErrCollectionWALResourceLimit", err)
	}
	if _, statErr := os.Stat(collectionwal.SegmentPath(dir, 0, 1)); !os.IsNotExist(statErr) {
		t.Fatalf("collection WAL segment stat err=%v want not-exist", statErr)
	}
	got, getErr := col.Get([]byte("u1"))
	if getErr != nil {
		t.Fatalf("Get after oversized rejected insert: %v", getErr)
	}
	if got != nil {
		t.Fatalf("oversized document became visible after rejected insert: len=%d", len(got))
	}
}

func TestCollectionWALValueLogStoragePolicyRejectsBeforeFrame(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	d.SetValueLogAppender(collectionWALTestValueLogAppender{})

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	_, err = col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"ada"}`)},
	)
	if !errors.Is(err, collectionwal.ErrCollectionWALUnsupportedMode) {
		t.Fatalf("InsertBatch error=%v want ErrCollectionWALUnsupportedMode", err)
	}
	if _, statErr := os.Stat(collectionwal.SegmentPath(dir, 0, 1)); !os.IsNotExist(statErr) {
		t.Fatalf("collection WAL segment stat err=%v want not-exist", statErr)
	}
	got, getErr := col.Get([]byte("u1"))
	if getErr != nil {
		t.Fatalf("Get after value-log policy rejected insert: %v", getErr)
	}
	if got != nil {
		t.Fatalf("document became visible after value-log policy rejected insert: %s", got)
	}
}

type collectionWALTestValueLogAppender struct{}

func (collectionWALTestValueLogAppender) AppendValues([][]byte) ([]page.ValuePtr, error) {
	return nil, errors.New("unexpected value-log append")
}

func (collectionWALTestValueLogAppender) Flush() error { return nil }

func (collectionWALTestValueLogAppender) Sync() error { return nil }

func (collectionWALTestValueLogAppender) CurrentValueLogSegment() (string, uint32, bool) {
	return "", 0, false
}

func TestCollectionWALNoIndexInsertOnlyRejectsUnsupportedMutationsBeforeVisibility(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
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
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"name":"ada","city":"hnl"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	_, _, err = col.Update([]byte("u1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"name":"ada","city":"sea"}`), true, nil
	})
	if !errors.Is(err, collectionwal.ErrCollectionWALUnsupportedMode) {
		t.Fatalf("Update error=%v want ErrCollectionWALUnsupportedMode", err)
	}
	_, err = col.UpdateBatch([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return []byte(`{"name":"ada","city":"sfo"}`), true, nil
		},
	}})
	if !errors.Is(err, collectionwal.ErrCollectionWALUnsupportedMode) {
		t.Fatalf("UpdateBatch error=%v want ErrCollectionWALUnsupportedMode", err)
	}
	if _, err := col.DeleteDocument([]byte("u1")); !errors.Is(err, collectionwal.ErrCollectionWALUnsupportedMode) {
		t.Fatalf("DeleteDocument error=%v want ErrCollectionWALUnsupportedMode", err)
	}
	if _, err := col.DeleteBatch([][]byte{[]byte("u1")}); !errors.Is(err, collectionwal.ErrCollectionWALUnsupportedMode) {
		t.Fatalf("DeleteBatch error=%v want ErrCollectionWALUnsupportedMode", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "by_name", Field: "name", ValueType: IndexValueString}); !errors.Is(err, collectionwal.ErrCollectionWALUnsupportedMode) {
		t.Fatalf("CreateIndex error=%v want ErrCollectionWALUnsupportedMode", err)
	}

	doc, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get after rejected mutations: %v", err)
	}
	if string(doc) != `{"name":"ada","city":"hnl"}` {
		t.Fatalf("document after rejected mutations=%s want original JSON", doc)
	}
	if got := col.Meta(); len(got.Indexes) != 0 {
		t.Fatalf("indexes after rejected CreateIndex=%v want none", got.Indexes)
	}
}

func TestCollectionWALNoIndexInsertOnlyRejectsBSONBeforeWAL(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
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
			DocumentFormat:                    DocumentFormatBSON,
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	_, err = col.InsertBatchValidatedBSON(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{{Key: "_id", Value: "u1"}, {Key: "name", Value: "ada"}})},
	)
	if !errors.Is(err, collectionwal.ErrCollectionWALUnsupportedMode) {
		t.Fatalf("InsertBatchValidatedBSON error=%v want ErrCollectionWALUnsupportedMode", err)
	}
	if _, statErr := os.Stat(collectionwal.SegmentPath(dir, 0, 1)); !os.IsNotExist(statErr) {
		t.Fatalf("collection WAL segment stat err=%v want not-exist", statErr)
	}
	doc, getErr := col.Get([]byte("u1"))
	if getErr != nil {
		t.Fatalf("Get after rejected BSON insert: %v", getErr)
	}
	if doc != nil {
		t.Fatalf("BSON document became visible after rejected insert: %x", doc)
	}
}

func TestCollectionWALReadOnlyOpenAllowsRetainedAppliedSegment(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
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
		[][]byte{[]byte(`{"name":"ada"}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	readOnly, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("read-only open with retained applied collection WAL: %v", err)
	}
	defer func() { _ = readOnly.Close() }()
}

func TestCollectionWALCheckpointRetainsAppliedSegment(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
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
		[][]byte{[]byte(`{"name":"ada"}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if _, err := os.Stat(collectionwal.SegmentPath(dir, 0, 1)); err != nil {
		t.Fatalf("collection WAL segment after checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	readOnly, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("read-only open after checkpoint with retained applied collection WAL: %v", err)
	}
	defer func() { _ = readOnly.Close() }()
	reopenedCol, err := NewCollectionManager(readOnly).OpenCollection("users")
	if err != nil {
		t.Fatalf("open read-only collection: %v", err)
	}
	doc, err := reopenedCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get read-only document: %v", err)
	}
	if string(doc) != `{"name":"ada"}` {
		t.Fatalf("read-only document=%s want original JSON", doc)
	}
}

func TestCollectionWALCompactStorageAbortsOnCheckpointDebt(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
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
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
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
		[][]byte{[]byte(`{"name":"ada"}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	_, err = d.CompactStorage(context.Background(), backenddb.CompactStorageOptions{})
	if !errors.Is(err, backenddb.ErrRecoveryRequired) {
		t.Fatalf("CompactStorage error=%v want ErrRecoveryRequired", err)
	}
	if _, err := os.Stat(collectionwal.SegmentPath(dir, 0, 1)); err != nil {
		t.Fatalf("collection WAL segment after blocked CompactStorage: %v", err)
	}
}

func TestCollectionWALMetricsUseUIDAndHashesNotNames(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	})
	if err != nil {
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
		t.Fatalf("InsertBatch: %v", err)
	}

	stats := d.Stats()
	rawUID := meta.CollectionUID
	byCollectionMetricSeen := false
	for key, value := range stats {
		if !strings.HasPrefix(key, "treedb.collection_wal.") {
			continue
		}
		if strings.Contains(key, "users") || strings.Contains(value, "users") {
			t.Fatalf("collection WAL metric leaked collection name: %s=%q", key, value)
		}
		if rawUID != "" && (strings.Contains(key, rawUID) || strings.Contains(value, rawUID)) {
			t.Fatalf("collection WAL metric leaked raw collection UID: %s=%q", key, value)
		}
		if strings.HasPrefix(key, "treedb.collection_wal.by_collection.") && strings.HasSuffix(key, ".applied_seq_current") {
			if value != "1" {
				t.Fatalf("by_collection applied seq metric %s=%q want 1", key, value)
			}
			byCollectionMetricSeen = true
		}
	}
	if !byCollectionMetricSeen {
		t.Fatalf("missing hashed by_collection applied seq metric in stats: %v", stats)
	}
}

func TestCollectionWALMissingRetainedSegmentFailsOpen(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
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
		[][]byte{[]byte(`{"name":"ada"}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	segmentPath := collectionwal.SegmentPath(dir, 0, 1)
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	if err := os.Remove(segmentPath); err != nil {
		t.Fatalf("remove retained collection WAL segment: %v", err)
	}

	readOnly, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, backenddb.ErrRecoveryRequired) {
		if readOnly != nil {
			_ = readOnly.Close()
		}
		t.Fatalf("read-only open err=%v want ErrRecoveryRequired", err)
	}
	readWrite, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOnRelaxed})
	if !errors.Is(err, collectionwal.ErrCollectionWALCorruptMiddle) {
		if readWrite != nil {
			_ = readWrite.Close()
		}
		t.Fatalf("read-write open err=%v want ErrCollectionWALCorruptMiddle", err)
	}
}

func TestCollectionWALReadWriteOpenReplaysUnappliedWatermark(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	})
	if err != nil {
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
		t.Fatalf("InsertBatch: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot returned nil")
	}
	rootName := collectionPrimaryRootName("users")
	rawRoot, ok, err := getSystemValue(snap, systemCollectionRootKey(rootName))
	if err != nil {
		t.Fatalf("get root descriptor: %v", err)
	}
	if !ok {
		t.Fatal("missing root descriptor after insert")
	}
	originalRootID, err := decodeRootID(rawRoot)
	if err != nil {
		t.Fatalf("decode root descriptor: %v", err)
	}
	_ = snap.Close()
	_, _, err = d.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return buildSystemDeltaIterator(map[string][]byte{
			systemCollectionRootKey(rootName):                    encodeRootID(0),
			systemCollectionWALAppliedSeqKey(meta.CollectionUID): encodeCollectionWALAppliedSeq(0),
		})
	})
	if err != nil {
		t.Fatalf("reset descriptor/watermark to simulate crash before visibility commit: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	readOnly, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, backenddb.ErrRecoveryRequired) {
		if readOnly != nil {
			_ = readOnly.Close()
		}
		t.Fatalf("read-only open err=%v want ErrRecoveryRequired", err)
	}

	recovered, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("read-write recover open: %v", err)
	}
	defer func() { _ = recovered.Close() }()
	snap = recovered.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	applied, err := loadCollectionWALAppliedSeq(snap, meta.CollectionUID)
	if err != nil {
		t.Fatalf("load collection WAL applied seq: %v", err)
	}
	if applied != 1 {
		t.Fatalf("applied seq=%d want 1 after recovery", applied)
	}
	rawRoot, ok, err = getSystemValue(snap, systemCollectionRootKey(rootName))
	if err != nil {
		t.Fatalf("get recovered root descriptor: %v", err)
	}
	if !ok {
		t.Fatal("missing recovered root descriptor")
	}
	recoveredRootID, err := decodeRootID(rawRoot)
	if err != nil {
		t.Fatalf("decode recovered root descriptor: %v", err)
	}
	if recoveredRootID == 0 {
		t.Fatal("recovered root descriptor stayed at base root 0")
	}
	if recoveredRootID == originalRootID {
		t.Fatalf("recovered root id=%d matched original planned root id; want rematerialized root", recoveredRootID)
	}
	recoveredMgr := NewCollectionManager(recovered)
	recoveredCol, err := recoveredMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open recovered collection: %v", err)
	}
	doc, err := recoveredCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get recovered document: %v", err)
	}
	if string(doc) != `{"name":"ada"}` {
		t.Fatalf("recovered document=%s want original JSON", doc)
	}
}

func TestCollectionWALRecoveryRejectsCatalogGenerationMismatch(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	})
	if err != nil {
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
		t.Fatalf("InsertBatch: %v", err)
	}

	mutatedMeta := *meta
	mutatedMeta.CollectionGeneration++
	encodedMeta, err := encodeCollectionMeta(mutatedMeta)
	if err != nil {
		t.Fatalf("encode mutated collection metadata: %v", err)
	}
	rootName := collectionPrimaryRootName("users")
	_, _, err = d.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return buildSystemDeltaIterator(map[string][]byte{
			systemCollectionMetaKey(meta.Name):                   encodedMeta,
			systemCollectionRootKey(rootName):                    encodeRootID(0),
			systemCollectionWALAppliedSeqKey(meta.CollectionUID): encodeCollectionWALAppliedSeq(0),
		})
	})
	if err != nil {
		t.Fatalf("mutate metadata and reset descriptor/watermark: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	recovered, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if !errors.Is(err, collectionwal.ErrCollectionWALIdentityMismatch) {
		if recovered != nil {
			_ = recovered.Close()
		}
		t.Fatalf("read-write recover open err=%v want ErrCollectionWALIdentityMismatch", err)
	}
}

func TestCollectionWALRecoveryRejectsBaseCatalogDigestMismatch(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			CollectionWALDurableAckCapability: CollectionWALDurableAckNoIndexRowInsertOnly,
		},
	})
	if err != nil {
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
		t.Fatalf("InsertBatch: %v", err)
	}

	encodedMeta, err := encodeCollectionMeta(*meta)
	if err != nil {
		t.Fatalf("encode collection metadata: %v", err)
	}
	var drifted map[string]any
	if err := json.Unmarshal(encodedMeta, &drifted); err != nil {
		t.Fatalf("decode metadata JSON: %v", err)
	}
	drifted["digest_probe_unknown_field"] = true
	driftedMeta, err := json.Marshal(drifted)
	if err != nil {
		t.Fatalf("encode drifted metadata JSON: %v", err)
	}
	rootName := collectionPrimaryRootName("users")
	_, _, err = d.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return buildSystemDeltaIterator(map[string][]byte{
			systemCollectionMetaKey(meta.Name):                   driftedMeta,
			systemCollectionRootKey(rootName):                    encodeRootID(0),
			systemCollectionWALAppliedSeqKey(meta.CollectionUID): encodeCollectionWALAppliedSeq(0),
		})
	})
	if err != nil {
		t.Fatalf("drift metadata and reset descriptor/watermark: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	recovered, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
	})
	if !errors.Is(err, collectionwal.ErrCollectionWALIdentityMismatch) {
		if recovered != nil {
			_ = recovered.Close()
		}
		t.Fatalf("read-write recover open err=%v want ErrCollectionWALIdentityMismatch", err)
	}
}

func TestCollectionWALNoIndexInsertBatchCapabilityDefaultsOff(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:        dir,
		Durability: backenddb.DurabilityWALOnRelaxed,
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
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"ada"}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, err := os.Stat(collectionwal.SegmentPath(dir, 0, 1)); !os.IsNotExist(err) {
		t.Fatalf("collection WAL segment stat err=%v want not-exist", err)
	}
}

func collectionWALSectionTypesPresent(txn collectionwal.Transaction, types ...collectionwal.SectionType) bool {
	seen := make(map[collectionwal.SectionType]struct{}, len(txn.Sections))
	for _, section := range txn.Sections {
		seen[section.Type] = struct{}{}
	}
	for _, typ := range types {
		if _, ok := seen[typ]; !ok {
			return false
		}
	}
	return true
}

func waitCollectionWALSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatalf("timeout waiting for %s", what)
	}
}

func waitCollectionWALErr(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(collectionTestTimeout(t, 5*time.Second)):
		t.Fatalf("timeout waiting for %s", what)
		return nil
	}
}

func assertCollectionWALStillBlocked(t *testing.T, entered <-chan struct{}, done <-chan error, what string) {
	t.Helper()
	select {
	case <-entered:
		t.Fatalf("%s entered while the first collection WAL publisher section was still active", what)
	case err := <-done:
		t.Fatalf("%s returned while the first collection WAL publisher section was still active: %v", what, err)
	case <-time.After(collectionTestTimeout(t, 100*time.Millisecond)):
	}
}
