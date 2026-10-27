//go:build !windows

package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"unsafe"

	"github.com/snissn/compress/zstd"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Normal cached leaf writes intentionally supply dictionary ID zero. This
// fixture changes only their encoding, retaining the real appender, resource
// capture, segment registration and backend publication paths. Restrict the
// promoted methods so prepared appends cannot bypass the dictionary writer.
type cowDictionaryLeafLifecycle interface {
	backenddb.LeafPageLog
	backenddb.LeafPageLogCreatedSegmentProvider
	backenddb.LeafPageLogCurrentSegmentProvider
	backenddb.LeafPageLogSegmentRegistrationObserver
	backenddb.LeafPageLogCachedWrapperOwner
	backenddb.LeafPageLogSequenceReserver
	CurrentValueLogSegment() (string, uint32, bool)
}

type cowDictionaryLeafLog struct {
	cowDictionaryLeafLifecycle
	db         *DB
	dictID     uint64
	dictionary []byte
}

func (l *cowDictionaryLeafLog) AppendLeafPage(leaf []byte) (page.LeafLogPtr, error) {
	ptr, resources, err := l.AppendLeafPageWithStableResources(leaf)
	resources.Release()
	return ptr, err
}

func (l *cowDictionaryLeafLog) AppendLeafPages(leaves [][]byte) ([]page.LeafLogPtr, error) {
	ptrs, resources, err := l.AppendLeafPagesWithStableResources(leaves)
	resources.Release()
	return ptrs, err
}

func (l *cowDictionaryLeafLog) AppendLeafPageWithStableResources(leaf []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	ptrs, resources, err := l.AppendLeafPagesWithStableResources([][]byte{leaf})
	if err != nil {
		return page.LeafLogPtr{}, nil, err
	}
	return ptrs[0], resources, nil
}

func (l *cowDictionaryLeafLog) AppendLeafPagesWithStableResources(leaves [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if len(leaves) == 0 {
		return nil, nil, nil
	}
	start, err := l.db.ReserveValueLogRIDs(len(leaves))
	if err != nil {
		return nil, nil, err
	}
	records := make([]valuelog.Record, len(leaves))
	for i, leaf := range leaves {
		payload, _, err := valuelog.MaybeCompactLeafLogPayload(leaf)
		if err != nil {
			return nil, nil, err
		}
		records[i] = valuelog.Record{RID: start + uint64(i), Value: payload}
	}
	ptrs, resources, err := l.db.appendValueLogWithStableResources(&l.db.leafLog, l.dictID, l.dictionary, records, journalDurabilityNone)
	if err != nil {
		return nil, nil, err
	}
	defer putValueLogPtrs(ptrs)
	if len(ptrs) != len(leaves) {
		resources.Release()
		return nil, nil, fmt.Errorf("dictionary leaf append returned %d pointers for %d pages", len(ptrs), len(leaves))
	}
	refs := make([]page.LeafLogPtr, len(ptrs))
	for i, ptr := range ptrs {
		refs[i], err = page.LeafLogPtrFromValuePtr(ptr)
		if err != nil {
			resources.Release()
			return nil, nil, err
		}
		l.db.noteLeafGenerationRecordLength(ptr)
	}
	return refs, resources, nil
}

func TestCOWOrderedIteratorDictionaryLeafFallback(t *testing.T) {
	dir := t.TempDir()
	var store *dictdb.Store
	backend, err := backenddb.Open(backenddb.Options{
		Dir: dir, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true,
		ValueLog: backenddb.ValueLogOptions{DictLookup: func(id uint64) ([]byte, error) {
			if store == nil {
				return nil, valuelog.ErrMissingDict
			}
			return store.GetDictBytes(context.Background(), id)
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered", FlushThreshold: 1 << 30,
		IndexOuterLeavesInValueLog: true, ValueLogPointerThreshold: 256,
		ValueLogCompression: uint8(vlogCompressionDict), ValueLogAutoPolicy: uint8(vlogAutoSize),
		ValueLogCompressionAutotune:         valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium},
		ValueLogDictIncompressibleHoldBytes: -1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	want := bytes.Repeat([]byte("old-inline-dictionary-value|"), 4)
	samples := make([][]byte, 4)
	for sample := range samples {
		leaf := make([]byte, page.PageSize)
		builder := node.NewBuilderWithOptions(leaf, page.PageTypeLeaf, node.BuilderOptions{})
		for i := 0; i < 16; i++ {
			key := []byte(fmt.Sprintf("a%03d", sample*16+i))
			if err := builder.AddLeafEntry(key, want, node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatal(err)
			}
		}
		builder.FinishNoNode()
		samples[sample], _, err = valuelog.MaybeCompactLeafLogPayload(leaf)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Retain just one training page as dictionary history. Concatenating every
	// tiny sample into History lets the trainer match the entire corpus without
	// literals, which the vendored trainer cannot build a Huffman table for.
	dictionary, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID: 73, Contents: samples, History: append([]byte(nil), samples[0]...),
		Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err = dictdb.Open(dir+"/dictdb", backenddb.Options{ChunkSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	dictID, err := store.PutDictBytes(context.Background(), dictionary)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	db.SetDictStore(store) // DB.Close owns the real store after attachment.
	backend.SetLeafPageLog(&cowDictionaryLeafLog{
		cowDictionaryLeafLifecycle: &cachingLeafPageLog{db: db, lane: &db.leafLog},
		db:                         db, dictID: dictID, dictionary: dictionary,
	})
	for i := 0; i < 64; i++ {
		if err := db.Set([]byte(fmt.Sprintf("a%03d", i)), want); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	basis := old.cowCut.basis
	var keyScratch, leafScratch [page.PageSize]byte
	var pointer page.LeafLogPtr
	stop := errors.New("capture actual dictionary leaf pointer")
	_, err = basis.snapshot.GetEntryExactWithFixedScratch([]byte("a000"), keyScratch[:], leafScratch[:], func(ptr page.LeafLogPtr, _ []byte) ([]byte, error) {
		pointer = ptr
		return nil, stop
	})
	if !errors.Is(err, stop) || pointer.FileID == 0 {
		t.Fatalf("real dictionary leaf pointer: %+v %v", pointer, err)
	}
	file, _, err := basis.snapshot.PinnedValueLogFile(pointer.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	shape, err := valuelog.InspectCOWLeafRecord(file, pointer.ValuePtr(), db.cow.readLimits())
	if err != nil || !shape.Compressed || shape.DictID == 0 || shape.DictID != dictID {
		t.Fatalf("persisted dictionary leaf shape: %+v %v; want dictionary %d", shape, err, dictID)
	}

	// Capable definitions finish inline points in the first retained basis
	// traversal; no transient Snapshot is required even at the view frontier.
	releaseViews := cowPinViewFrontier(t, db.cow)
	for i := 0; i < 2; i++ {
		if got, err := db.Get([]byte("a000")); err != nil || !bytes.Equal(got, want) {
			releaseViews()
			t.Fatalf("public dictionary point at view frontier: %q %v", got, err)
		}
	}
	releaseViews()
	// Successor selection retains the same complete cut even when it finishes
	// in reusable basis traversal rather than constructing a private decoder.
	successor, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	key, value, found, seekErr := successor.SeekGEVersionRange([]byte("a000"), []byte("a001"))
	if seekErr != nil || !found || string(key) != "a000" || !bytes.Equal(value, want) || successor.cowCut.basis != basis {
		_ = successor.Close()
		t.Fatalf("dictionary successor lost its exact caller cut: key=%q found=%t err=%v", key, found, seekErr)
	}
	if err := successor.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, want) {
		t.Fatal("dictionary successor output did not survive caller-cut close")
	}

	it, err := old.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	child := it.(*snapshotBoundIterator).inner.(*cowMergeIterator)
	check := func() {
		t.Helper()
		if child.snapshot != old || old.cowCut.basis != basis || child.workspace == nil || child.workspace.snapshot != old {
			t.Fatal("dictionary fallback lost its original caller cut")
		}
		var decoder *cowReadCodec
		for _, codec := range child.workspace.codecs {
			if codec != nil {
				t.Fatal("capable dictionary iterator retained a private decoder")
			}
		}
		db.cow.physicalMu.Lock()
		shared := db.cow.physicalReader
		for _, codec := range shared.codecs {
			if codec != nil && codec.id == dictID && codec.decoder != nil {
				decoder = codec
			}
		}
		independent := decoder != nil && decoder.definition != nil && shared.snapshot == nil && shared.basis == nil
		db.cow.physicalMu.Unlock()
		if !independent {
			t.Fatal("real dictionary leaf did not populate an independently owned shared decoder")
		}
		for i := 0; i < 64; i++ {
			key := []byte(fmt.Sprintf("a%03d", i))
			it.Seek(key)
			if !it.Valid() || !bytes.Equal(it.Key(), key) || !bytes.Equal(it.Value(), want) || it.Error() != nil {
				t.Fatalf("dictionary leaf seek %s: key=%q value=%q error=%v", key, it.Key(), it.Value(), it.Error())
			}
		}
		db.cow.physicalMu.Lock()
		reused := shared.codecs[0] == decoder
		db.cow.physicalMu.Unlock()
		if !reused {
			t.Fatal("warm dictionary reads replaced the same producer's shared decoder")
		}
	}
	check() // First public construction and reads use the original cache owner.
	check() // Repeated seeks reuse it, including already visited physical leaves.
	if err := db.Set([]byte("a000"), []byte("new-current-value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if db.cow.cut.basis == basis {
		t.Fatal("checkpoint did not replace the current backend basis")
	}
	check() // The public iterator still reads its original dictionary leaf cut.
	// Closing this iterator releases only its own cursor/output and exact cut;
	// the independently owned shared decoder survives until its cache retires.
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("a000")); err != nil || string(got) != "new-current-value" {
		t.Fatalf("new cut value: %q %v", got, err)
	}
}

// Use the maintained dictionary leaf producer and training recipe, rather than
// inventing a read hook or an unsupported cached leaf serialization.
func cowSuccessorDictionaryLeafFixture(t testing.TB) (*DB, []byte, page.LeafLogPtr) {
	t.Helper()
	dir := t.TempDir()
	var store *dictdb.Store
	backend, err := backenddb.Open(backenddb.Options{
		Dir: dir, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true,
		ValueLog: backenddb.ValueLogOptions{DictLookup: func(id uint64) ([]byte, error) {
			if store == nil {
				return nil, valuelog.ErrMissingDict
			}
			return store.GetDictBytes(context.Background(), id)
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered", FlushThreshold: 1 << 30,
		IndexOuterLeavesInValueLog: true, ValueLogPointerThreshold: 256,
		ValueLogCompression: uint8(vlogCompressionDict), ValueLogAutoPolicy: uint8(vlogAutoSize),
		ValueLogCompressionAutotune: valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium}, ValueLogDictIncompressibleHoldBytes: -1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !db.closing.Load() {
			_ = db.Close()
		}
	})
	want := bytes.Repeat([]byte("old-inline-dictionary-value|"), 4)
	samples := make([][]byte, 4)
	for sample := range samples {
		leaf := make([]byte, page.PageSize)
		builder := node.NewBuilderWithOptions(leaf, page.PageTypeLeaf, node.BuilderOptions{})
		for i := 0; i < 16; i++ {
			if err := builder.AddLeafEntry([]byte(fmt.Sprintf("a%03d", sample*16+i)), want, node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatal(err)
			}
		}
		builder.FinishNoNode()
		samples[sample], _, err = valuelog.MaybeCompactLeafLogPayload(leaf)
		if err != nil {
			t.Fatal(err)
		}
	}
	dictionary, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID: 73, Contents: samples, History: append([]byte(nil), samples[0]...), Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err = dictdb.Open(dir+"/dictdb", backenddb.Options{ChunkSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	dictID, err := store.PutDictBytes(context.Background(), dictionary)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	db.SetDictStore(store)
	backend.SetLeafPageLog(&cowDictionaryLeafLog{
		cowDictionaryLeafLifecycle: &cachingLeafPageLog{db: db, lane: &db.leafLog}, db: db, dictID: dictID, dictionary: dictionary,
	})
	for i := 0; i < 64; i++ {
		if err := db.Set([]byte(fmt.Sprintf("a%03d", i)), want); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	basis := db.cow.cut.basis
	var key, leaf [page.PageSize]byte
	var pointer page.LeafLogPtr
	stop := errors.New("capture real dictionary leaf")
	_, err = basis.snapshot.GetEntryExactWithFixedScratch([]byte("a000"), key[:], leaf[:], func(ptr page.LeafLogPtr, _ []byte) ([]byte, error) {
		pointer = ptr
		return nil, stop
	})
	if !errors.Is(err, stop) || pointer.FileID == 0 {
		t.Fatalf("real dictionary leaf pointer: %+v err=%v", pointer, err)
	}
	return db, want, pointer
}

func TestCOWSuccessorFreshDictionaryLaneRefundsBeforeLocalTraversal(t *testing.T) {
	for _, busyPrimary := range []bool{false, true} {
		t.Run(fmt.Sprintf("busy-primary=%t", busyPrimary), func(t *testing.T) {
			db, want, pointer := cowSuccessorDictionaryLeafFixture(t)
			s, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			// Warm the real original-cut dictionary decoder without point scratch.
			// The subsequent public successor still needs to admit its own traversal.
			s.cowReadMu.Lock()
			w, err := s.cowWorkspaceLocked()
			var pageScratch [page.PageSize]byte
			if err == nil {
				// This fixture specifically exercises a pre-existing local owner,
				// including legacy captured-only readers. Warm the maintained local
				// kernel explicitly; public capable reads now share generic scratch.
				var f *os.File
				f, err = w.file(pointer.ValuePtr())
				if err == nil {
					var shape valuelog.COWRecordShape
					shape, err = valuelog.InspectCOWLeafRecord(f, pointer.ValuePtr(), w.limits)
					if err == nil {
						_, err = w.readInspected(f, pointer.ValuePtr(), shape, true, pageScratch[:], nil)
					}
				}
			}
			s.cowReadMu.Unlock()
			if err != nil || w.point != nil || len(w.codecs) == 0 || w.codecs[0].id == 0 {
				t.Fatalf("warm actual dictionary decoder without traversal: err=%v", err)
			}
			basis, budget := s.cowCut.basis, db.cow.budget
			leaseCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			workspaceCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) + cowReadWorkspaceInspectionCharge() + leaseCharge
			pointCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPointScratch{}))) + leaseCharge
			// Exactly one fresh lane fits, but retaining it during the dictionary
			// retry would prevent admission of the original local point scratch.
			before := budget.Stats()
			pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - workspaceCharge - pointCharge - leaseCharge)
			if err != nil {
				t.Fatal(err)
			}
			defer pressure.Close()
			pressured := budget.Stats()
			if busyPrimary {
				basis.readMu.Lock()
				defer basis.readMu.Unlock()
			}
			key, got, found, err := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
			after := budget.Stats()
			if err != nil || !found || string(key) != "a000" || !bytes.Equal(got, want) {
				t.Fatalf("dictionary handoff stranded original local traversal: found=%t err=%v", found, err)
			}
			if basis.reader != nil || basis.readSlots[0].reader != nil || s.cowReader != w || w.point == nil ||
				after.ExternalBytes != pressured.ExternalBytes+pointCharge || after.ReservedBytes != pressured.ReservedBytes+pointCharge ||
				after.ExternalLeases != pressured.ExternalLeases+1 {
				t.Fatalf("fresh dictionary lane must refund before exactly one local point: before=%+v after=%+v", pressured, after)
			}
		})
	}
}

func TestCOWPublicPointFreshDictionaryRefundsBeforeLocalAdmission(t *testing.T) {
	db, want, pointer := cowSuccessorDictionaryLeafFixture(t)
	cowCapturePendingLeafDictionary(t, db, pointer, want)
	// Keep the captured-only provider handoff/refund contract explicit.
	store := db.dictStore
	defer func() { db.dictStore = store }()
	db.dictStore = cowCapturedDictionaryOnlyStore{store}
	cowAssertFreshPointHandoffRefundsBeforeLocalAdmission(t, db, []byte("a000"), want, false)
}

func TestCOWSuccessorDictionaryHandoffKeepsWarmTraversal(t *testing.T) {
	for _, busy := range []bool{false, true} {
		t.Run(fmt.Sprintf("busy-primary=%t", busy), func(t *testing.T) {
			db, want, pointer := cowSuccessorDictionaryLeafFixture(t)
			cowCapturePendingLeafDictionary(t, db, pointer, want)
			// Keep the captured-only provider handoff/refund contract explicit.
			store := db.dictStore
			defer func() { db.dictStore = store }()
			db.dictStore = cowCapturedDictionaryOnlyStore{store}
			cowAssertSuccessorHandoffKeepsWarmOwner(t, db, []byte("a000"), []byte("a001"), want, busy)
		})
	}
}

// Materialize a real pending dictionary resource before exercising a provider
// that exposes only captured definitions, as legacy cuts do. No synthetic
// dictionary owner or decoded leaf is installed by these handoff controls.
func cowCapturePendingLeafDictionary(t *testing.T, db *DB, pointer page.LeafLogPtr, want []byte) {
	t.Helper()
	f, _, err := db.cow.cut.basis.snapshot.PinnedValueLogFile(pointer.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	shape, err := valuelog.InspectCOWLeafRecord(f, pointer.ValuePtr(), db.cow.readLimits())
	if err != nil || shape.DictID == 0 {
		t.Fatalf("pending dictionary shape: %+v %v", shape, err)
	}
	store := db.dictStore.(*dictdb.Store)
	if err := store.SetCurrent(context.Background(), shape.DictID); err != nil {
		t.Fatal(err)
	}
	db.SetDictStore(store)
	if err := db.Set([]byte("z-pending-dictionary"), bytes.Repeat(want, 8)); err != nil {
		t.Fatal(err)
	}
}
