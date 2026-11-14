package collections

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Observe synchronous calls without replacing the physical builder/appender.
// All returned pointers and private roots remain on the original group.
type nativeStripedBuildRecorder struct {
	*backenddb.RootPublicationBuildGroup
	sizes                      []int
	files                      map[uint32]bool
	placements, placementBytes int
}

func (r *nativeStripedBuildRecorder) BuildNativeOrderedRoot(input backenddb.OrderedRootDeltaBatchPublishInput) (uint64, error) {
	r.sizes = append(r.sizes, input.Delta.Len())
	return r.RootPublicationBuildGroup.BuildNativeOrderedRoot(input)
}

func (r *nativeStripedBuildRecorder) AppendNativeValueLogValues(values [][]byte) ([]page.ValuePtr, error) {
	if len(values) != 1 {
		return nil, fmt.Errorf("native source placement width=%d, want singleton", len(values))
	}
	ptrs, err := r.RootPublicationBuildGroup.AppendNativeValueLogValues(values)
	if err == nil {
		r.placements++
		r.placementBytes += len(values[0])
		for _, ptr := range ptrs {
			r.files[ptr.FileID] = true
		}
	}
	return ptrs, err
}

func TestNativeDomainBuildStripedPointerBatching(t *testing.T) {
	// Deterministic mechanism control: the maintained native appender chooses
	// eight writers at this parallelism. Public timing keeps host defaults and
	// independently proves natural activation.
	previous := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(previous)
	for _, count := range []int{3, 257} {
		t.Run(fmt.Sprintf("rows-%d", count), func(t *testing.T) {
			testNativeDomainBuildStripedPointers(t, count)
		})
	}
}

func testNativeDomainBuildStripedPointers(t *testing.T, count int) {
	t.Helper()
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	var owner *treedb.DB
	var manager *CollectionManager
	var collection *Collection
	open := func(create bool) {
		t.Helper()
		var err error
		owner, err = treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err := owner.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = NewCollectionManager(backend)
		if create {
			_, err = manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}})
			if err != nil {
				t.Fatal(err)
			}
		}
		collection, err = manager.OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
	}
	closeOwner := func() {
		t.Helper()
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			manager = nil
		}
		if owner != nil {
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			owner = nil
		}
	}
	defer func() {
		if manager != nil {
			_ = manager.Close()
		}
		if owner != nil {
			_ = owner.Close()
		}
	}()
	open(true)
	seed, err := bson.Marshal(bson.D{{Key: "payload", Value: "physical basis"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Insert([]byte("basis"), seed); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	ids, docs := make([][]byte, count), make([][]byte, count)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("row-%04d", i))
		docs[i], err = bson.Marshal(bson.D{{Key: "row", Value: i}, {Key: "payload", Value: string(bytes.Repeat([]byte("p"), 8192))}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := collection.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	read, err := collection.nativeJSON.Capture(collection.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	source, ok := read.collection.pending.Get("primary")
	if !ok || source.baseRoot == 0 || source.pending == nil || source.captured != nil {
		t.Fatalf("need one real retained warm primary source: domain=%+v", source)
	}
	// Only the test copies expected bytes. Production borrows this retained
	// source through synchronous apply/reset; no fixture manufactures COW rows.
	type expectedRow struct {
		value    []byte
		revision page.EntryRevision
	}
	want := make(map[string]expectedRow)
	wantBytes := 0
	source.pending.ScanOwned(func(row memtable.COWRecord) bool {
		want[row.Key] = expectedRow{[]byte(row.Value), row.Revision}
		wantBytes += len(row.Value)
		return true
	})
	if len(want) != count {
		t.Fatalf("source rows=%d want=%d", len(want), count)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) {
		return read.capture.NativeAllocator().AdmitNativeBytes(n)
	}
	build, err := newNativeDomainBuild(source, 1, admit)
	if err != nil {
		t.Fatal(err)
	}
	finish := func() {
		t.Helper()
		if build == nil {
			return
		}
		done, err := build.finish()
		if err != nil {
			t.Error(err)
			return
		}
		done.Complete()
		build = nil
	}
	defer finish()
	group, err := read.database.BeginRootPublicationBuildGroupFromSnapshot(read.capture.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	recorder := &nativeStripedBuildRecorder{RootPublicationBuildGroup: group, files: make(map[uint32]bool)}
	root, err := build.materialize(recorder)
	if err != nil {
		t.Fatal(err)
	}
	if recorder.placements != count || recorder.placementBytes != wantBytes || (count == 257 && len(recorder.files) <= 4) || (count == 3 && len(recorder.files) > 4) {
		t.Fatalf("striped fixture: real files=%d singleton placements=%d bytes=%d want rows=%d bytes=%d", len(recorder.files), recorder.placements, recorder.placementBytes, count, wantBytes)
	}
	if err := group.WithNativeConstructionRoots([]uint64{root}, func(readers []iterator.UnsafeIterator) error {
		it := readers[0]
		seen, basisRows := 0, 0
		for ; it.Valid(); it.Next() {
			key := string(it.UnsafeKey())
			row, exists := want[key]
			if !exists {
				basisRows++
				continue
			}
			if !bytes.Equal(it.UnsafeValue(), row.value) {
				return fmt.Errorf("private row %q differs from retained source", key)
			}
			_, _, _, revision := iterator.UnsafeEntryWithRevision(it)
			if revision != row.revision {
				return fmt.Errorf("private revision %q=%d want=%d", key, revision, row.revision)
			}
			seen++
		}
		if err := it.Error(); err != nil {
			return err
		}
		if seen != count || basisRows != 1 {
			return fmt.Errorf("private rows=%d basis=%d want=%d+1", seen, basisRows, count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	wantSizes := []int{count}
	if count == 257 {
		wantSizes = []int{256, 1}
	}
	t.Logf("warm basis=%d real pointer files=%d singleton placements=%d bytes=%d apply sizes=%v", source.baseRoot, len(recorder.files), recorder.placements, recorder.placementBytes, recorder.sizes)
	if !reflect.DeepEqual(recorder.sizes, wantSizes) {
		t.Fatalf("fifth-file flushing fragmented admitted native batch: sizes=%v want=%v", recorder.sizes, wantSizes)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	finish()
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	// The private group is abandoned. Ordinary public maintenance separately
	// persists the same acknowledged collection contents.
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	closeOwner()
	open(false)
	for i, id := range ids {
		got, err := collection.Get(id)
		if err != nil || !bytes.Equal(got, docs[i]) {
			t.Fatalf("reopen %q differs: err=%v", id, err)
		}
	}
	closeOwner()
}
