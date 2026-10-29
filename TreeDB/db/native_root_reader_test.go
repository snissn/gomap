package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type nativeRootReadTestStorage struct {
	readers   [3]SnapshotRootReader
	key, leaf [page.PageSize]byte
	successor tree.OwnedSuccessorScratch
}

func TestNativeRootReaderCapturedRootsWithoutCacheGrowth(t *testing.T) {
	d := openOrderedRootSpanNativeTestDB(t, t.TempDir(), false, 0)
	defer d.Close()
	if err := d.Set([]byte("raw"), []byte("old-raw")); err != nil {
		t.Fatal(err)
	}
	delta := newOrderedRootSpanNativeBatch(t, 600, "old")
	oldRoot := publishOrderedRootSpanNativeBatch(t, d, 0, delta, OrderedRootStoragePagerLeaves)
	delta.Close()
	s := d.AcquireSnapshot()
	defer s.Close()
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := budget.AdmitNativeBytes(uint64(unsafe.Sizeof(nativeRootReadTestStorage{})))
	if err != nil {
		t.Fatal(err)
	}
	storage := new(nativeRootReadTestStorage)
	defer func() {
		*storage = nativeRootReadTestStorage{}
		storage = nil
		lease.Close()
		budget.Close()
		if got := budget.Stats(); got.TotalBytes != 0 {
			t.Error("reader storage refund", got)
		}
	}()
	roots := [3]uint64{s.state.RootPageID, s.state.SystemRootPageID, oldRoot}
	for i, root := range roots {
		storage.readers[i], err = s.NativeReaderAtRoot(root)
		if err != nil {
			t.Fatal(root, err)
		}
	}
	if len(s.rootTrees) != 0 || cap(s.rootTrees) != 0 {
		t.Fatal("native root capture populated root cache")
	}
	if allocs := testing.AllocsPerRun(100, func() {
		for i, root := range roots {
			storage.readers[i], err = s.NativeReaderAtRoot(root)
			if err != nil {
				panic(err)
			}
		}
	}); allocs != 0 {
		t.Fatalf("by-value root capture allocated %g", allocs)
	}
	if err := d.Set([]byte("raw"), []byte("new-raw")); err != nil {
		t.Fatal(err)
	}
	delta = newOrderedRootSpanNativeBatch(t, 600, "new")
	newRoot := publishOrderedRootSpanNativeBatch(t, d, oldRoot, delta, OrderedRootStoragePagerLeaves)
	delta.Close()
	if newRoot == oldRoot {
		t.Fatal("fixture did not replace physical root")
	}
	for i, probe := range []struct{ key, value string }{
		{"raw", "old-raw"},
		{collectionRootDescriptorPrefix + "ordered-root-span-native", string(encodeMaintenanceRootID(oldRoot))},
		{"key-000599", "old-000599"},
	} {
		entry, err := storage.readers[i].GetEntryExactWithFixedScratch([]byte(probe.key), storage.key[:], storage.leaf[:], nil)
		if err != nil || string(entry.Value) != probe.value {
			t.Fatalf("captured root %d: %q %v", i, entry.Value, err)
		}
	}
	entry, ok, err := storage.readers[2].SeekGEEntryWithFixedScratch([]byte("key-000299x"), []byte("key-000302"), storage.key[:], storage.leaf[:], &storage.successor, nil)
	if err != nil || !ok || string(entry.Key) != "key-000300" || string(entry.Value) != "old-000300" {
		t.Fatal(entry, ok, err)
	}
	if _, ok, err := storage.readers[2].SeekGEEntryWithFixedScratch([]byte("key-000300"), []byte("key-000300"), storage.key[:], storage.leaf[:], &storage.successor, nil); err != nil || ok {
		t.Fatal("empty domain", ok, err)
	}
	if empty, err := storage.readers[2].OwnedRootEmpty(); err != nil || empty {
		t.Fatal(empty, err)
	}
	if err := lease.Resize(uint64(unsafe.Sizeof(nativeRootReadTestStorage{})) + memtable.COWAllocationCharge(tree.OwnedPointerProjectionAllocationSize())); err != nil {
		t.Fatal(err)
	}
	projection, err := storage.readers[2].OwnedPointerProjectionIterator([]byte("key-000298"), []byte("key-000303"), nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 298; i < 303; i++ {
		if !projection.Valid() || string(projection.UnsafeValue()) != fmt.Sprintf("old-%06d", i) {
			t.Fatalf("captured projection %d: key=%q value=%q err=%v", i, projection.UnsafeKey(), projection.UnsafeValue(), projection.Error())
		}
		projection.Next()
	}
	if projection.Valid() || projection.Error() != nil {
		t.Fatal("projection bounds", projection.Error())
	}
	if err := projection.Close(); err != nil {
		t.Fatal(err)
	}
	if len(s.rootTrees) != 0 || cap(s.rootTrees) != 0 {
		t.Fatal("fixed scratch reads populated root cache")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for i := range storage.readers {
		if _, err := storage.readers[i].GetEntryExactWithFixedScratch(nil, storage.key[:], storage.leaf[:], nil); !errors.Is(err, ErrClosed) {
			t.Fatal("closed root read", err)
		}
		if _, _, err := storage.readers[i].SeekGEEntryWithFixedScratch(nil, nil, storage.key[:], storage.leaf[:], &storage.successor, nil); !errors.Is(err, ErrClosed) {
			t.Fatal("closed root seek", err)
		}
		if _, err := storage.readers[i].OwnedRootEmpty(); !errors.Is(err, ErrClosed) {
			t.Fatal("closed root emptiness", err)
		}
		if _, err := storage.readers[i].OwnedPointerProjectionIterator(nil, nil, nil); !errors.Is(err, ErrClosed) {
			t.Fatal("closed root projection", err)
		}
	}
}

func TestNativeRootReaderRevisionTombstoneAndCloseOverlap(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	group, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	delta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	defer delta.Close()
	if err := delta.SetWithRevision([]byte("a"), []byte("value"), 37); err != nil {
		t.Fatal(err)
	}
	if err := delta.DeleteWithRevision([]byte("b"), 41); err != nil {
		t.Fatal(err)
	}
	root, err := group.BuildNativeOrderedRoot(OrderedRootDeltaBatchPublishInput{Delta: delta, IncludeDeletedOnColdBuild: true})
	if err != nil {
		t.Fatal(err)
	}
	meta := batch.New(nil, orderedRootDeltaBatchInlineThreshold)
	defer meta.Close()
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], root)
	if err := meta.Set([]byte(collectionRootDescriptorPrefix+"native-reader"), encoded[:]); err != nil {
		t.Fatal(err)
	}
	if err := group.BuildNativeSystemDelta(meta); err != nil {
		t.Fatal(err)
	}
	final := d.NewPhysicalBatch().(*Batch)
	defer final.Close()
	if err := final.SetRootPublicationBuildGroup(group, true); err != nil {
		t.Fatal(err)
	}
	if err := final.Write(); err != nil {
		t.Fatal(err)
	}
	s := d.AcquireSnapshot()
	defer s.Close()
	reader, err := s.NativeReaderAtRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	var key, leaf [page.PageSize]byte
	var scratch tree.OwnedSuccessorScratch
	entry, err := reader.GetEntryExactWithFixedScratch([]byte("a"), key[:], leaf[:], nil)
	if err != nil || entry.Revision != 37 || string(entry.Value) != "value" {
		t.Fatal(entry, err)
	}
	entry, ok, err := reader.SeekGEEntryWithFixedScratch([]byte("b"), nil, key[:], leaf[:], &scratch, nil)
	if err != nil || !ok || entry.Revision != 41 || entry.Flags&node.FlagTombstone == 0 {
		t.Fatal("lost physical tombstone", entry, ok, err)
	}
	projection, err := reader.OwnedPointerProjectionIterator(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, revision := iterator.UnsafeEntryWithRevision(projection)
	if revision != 37 {
		t.Fatal("projection value revision", revision)
	}
	projection.Next()
	_, _, flags, revision := iterator.UnsafeEntryWithRevision(projection)
	if !projection.Valid() || flags&node.FlagTombstone == 0 || revision != 41 || projection.Error() != nil {
		t.Fatal("projection tombstone revision", flags, revision, projection.Error())
	}
	if err := projection.Close(); err != nil {
		t.Fatal(err)
	}
	owned, ok, err := d.SeekGEOwnedAtRoot(root, []byte("b"), nil)
	if err != nil || !ok || owned.Revision != 41 || owned.Flags&node.FlagTombstone == 0 || string(owned.Key) != "b" {
		t.Fatal("synchronous successor lost physical tombstone", owned, ok, err)
	}
	if _, err := reader.GetEntryExactWithFixedScratch([]byte("a"), nil, leaf[:], nil); !errors.Is(err, node.ErrCorruptedNode) {
		t.Fatal("short scratch", err)
	}
	if _, err := s.NativeReaderAtRoot(0); !errors.Is(err, tree.ErrKeyNotFound) {
		t.Fatal("zero root", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var localKey, localLeaf [page.PageSize]byte
		for range 1000 {
			_, err := reader.GetEntryExactWithFixedScratch([]byte("a"), localKey[:], localLeaf[:], nil)
			if err != nil && !errors.Is(err, ErrClosed) {
				t.Errorf("read overlapping Close: %v", err)
				return
			}
		}
	}()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if _, err := s.NativeReaderAtRoot(0); !errors.Is(err, ErrClosed) {
		t.Fatal("closed zero root", err)
	}
	var nilSnapshot *Snapshot
	if _, err := nilSnapshot.NativeReaderAtRoot(root); !errors.Is(err, ErrClosed) {
		t.Fatal("nil snapshot", err)
	}
	var zero SnapshotRootReader
	if _, err := zero.OwnedRootEmpty(); !errors.Is(err, ErrClosed) {
		t.Fatal("uninitialized reader", err)
	}
}

func TestNativeRootReaderCapturedPrimaryDirectory(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.Set([]byte("a"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := d.Set([]byte("gone"), []byte("prior")); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	if entry, found := primaryDBDirectory(t, d).Search([]byte("gone")); !found || !entry.InlineAbsence() {
		t.Fatal("fixture lacks PRIMARY inline absence")
	}
	s := d.AcquireSnapshot()
	defer s.Close()
	r, err := s.NativeReaderAtRoot(s.state.RootPageID)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Set([]byte("a"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := d.Set([]byte("gone"), []byte("restored")); err != nil {
		t.Fatal(err)
	}
	var key, leaf [page.PageSize]byte
	var scratch tree.OwnedSuccessorScratch
	entry, err := r.GetEntryExactWithFixedScratch([]byte("a"), key[:], leaf[:], nil)
	if err != nil || string(entry.Value) != "old" {
		t.Fatal("captured PRIMARY", entry, err)
	}
	entry, ok, err := r.SeekGEEntryWithFixedScratch([]byte("gone"), nil, key[:], leaf[:], &scratch, nil)
	if err != nil || !ok || entry.Flags&node.FlagTombstone == 0 {
		t.Fatal("captured inline absence", entry, ok, err)
	}
	if len(s.rootTrees) != 0 {
		t.Fatal("PRIMARY reader entered root cache")
	}
	r = SnapshotRootReader{}
}

func TestNativeRootReaderExternalLeavesRequireCapturedResolver(t *testing.T) {
	dir := t.TempDir()
	d := openOrderedRootSpanNativeTestDB(t, dir, true, 0)
	defer d.Close()
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionBlock, BlockCodec: ValueLogBlockLZ4})
	if err != nil {
		t.Fatal(err)
	}
	defer leafLog.Close()
	d.SetLeafPageLog(leafLog)
	delta := newOrderedRootSpanNativeBatch(t, 600, "external")
	defer delta.Close()
	root := publishOrderedRootSpanNativeBatch(t, d, 0, delta, OrderedRootStorageValueLogLeaves)
	s := d.AcquireSnapshot()
	defer s.Close()
	r, err := s.NativeReaderAtRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	var key, leaf [page.PageSize]byte
	var scratch tree.OwnedSuccessorScratch
	if _, err := r.GetEntryExactWithFixedScratch([]byte("key-000599"), key[:], leaf[:], nil); !errors.Is(err, tree.ErrOwnedIteratorLeafReader) {
		t.Fatal("external point used allocating/current resolver", err)
	}
	if _, _, err := r.SeekGEEntryWithFixedScratch(nil, nil, key[:], leaf[:], &scratch, nil); !errors.Is(err, tree.ErrOwnedIteratorLeafReader) {
		t.Fatal("external successor used allocating/current resolver", err)
	}
	r = SnapshotRootReader{}
}
