package treedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeBSONConditionalUpdateBypassesIndependentValueRead(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	opts.ValueLog.Compression = ValueLogCompressionBlock
	opts.ValueLog.BlockCodec = ValueLogBlockSnappy
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	backend, err := db.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	t.Cleanup(func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	const name = "docs"
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: name, Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
	}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	pad := string(bytes.Repeat([]byte("abcxyz"), 189))
	documents, ids := make([][]byte, 8), make([][]byte, 8)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc%d", i))
		documents[i], err = bson.Marshal(bson.D{{Key: "_id", Value: string(ids[i])}, {Key: "n", Value: int32(1)}, {Key: "pad", Value: pad}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := col.InsertBatch(ids, documents); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if capture != nil {
			if err := capture.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	snap := capture.Snapshot()
	token, ok := snap.StateToken()
	if !ok {
		t.Fatal("missing opening state")
	}
	root, err := snap.GetAtRoot(token.SystemRootPageID, []byte("collections/root/"+name+"/primary"))
	if err != nil || len(root) != 8 {
		t.Fatalf("BSON primary root: %x %v", root, err)
	}
	type original struct {
		ptr  page.ValuePtr
		file rootpublication.StableIdentity
	}
	pointers := make([]original, len(ids))
	for i := range ids {
		entry, err := snap.GetEntryAtRoot(binary.BigEndian.Uint64(root), ids[i])
		if err != nil || entry.Flags&node.FlagPointer == 0 {
			t.Fatalf("missing physical pointer %s: %+v %v", ids[i], entry, err)
		}
		f, file, err := snap.PinnedValueLogFile(entry.ValuePtr.FileID)
		if err != nil {
			t.Fatal(err)
		}
		const max = page.PageSize / 2
		shape, err := valuelog.InspectCOWRecord(f, entry.ValuePtr, valuelog.COWReadLimits{MaxRecordBytes: max, MaxRawBytes: max, MaxValueBytes: max})
		if err != nil || !shape.Compressed || shape.DictID != 0 || shape.Codec != valuelog.BlockCodecSnappy || shape.RawBytes != uint64(len(documents[i])) || page.ValuePtrSubIndex(entry.ValuePtr) != 0 {
			t.Fatalf("actual small Snappy singleton %s: %+v %v", ids[i], shape, err)
		}
		pointers[i] = original{entry.ValuePtr, file}
	}
	victim := 1
	for ; victim < len(pointers) && pointers[0].file.ObjectID == pointers[victim].file.ObjectID; victim++ {
	}
	if victim == len(pointers) {
		t.Fatalf("need different registered physical files: %+v", pointers)
	}
	t.Logf("semantic holder %s file=%+v ptr=%+v; conditional UPDATE %s file=%+v ptr=%+v",
		ids[0], pointers[0].file, pointers[0].ptr, ids[victim], pointers[victim].file, pointers[victim].ptr)

	// Warm the existing verified leaf cache for the victim's opening pointer.
	// Otherwise a cold external tree leaf has its own shared-decoder wait
	// before the UPDATE ever reaches value decoding.
	base, err := snap.NativeReaderAtRoot(binary.BigEndian.Uint64(root))
	if err != nil {
		t.Fatal(err)
	}
	var keyScratch, leafScratch [page.PageSize]byte
	var valueLeaf page.LeafLogPtr
	readLeaf := func(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
		valueLeaf = ptr
		return capture.(backenddb.NativePhysicalLeafReader).NativeLeafPage(ptr, dst)
	}
	for range 2 {
		entry, err := base.GetEntryExactWithFixedScratch(ids[victim], keyScratch[:], leafScratch[:], readLeaf)
		if err != nil || entry.ValuePtr != pointers[victim].ptr || valueLeaf.FileID == 0 {
			t.Fatalf("verified opening leaf pointer: %+v %+v %v", entry, valueLeaf, err)
		}
	}
	f, identity, err := snap.PinnedValueLogFile(valueLeaf.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	if _, hit, err := snap.CopyVerifiedCachedLeafPage(valueLeaf, f, identity, leafScratch[:0]); err != nil || !hit {
		t.Fatalf("expected warm verified opening leaf: hit=%t err=%v", hit, err)
	}

	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	held := make(chan error, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		value, err := capture.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(pointers[0].ptr, func(uint64) error {
			close(entered)
			<-release
			return nil
		})
		if err == nil && !bytes.Equal(value, documents[0]) {
			err = errors.New("held reader returned changed bytes")
		}
		held <- err
	}()
	defer func() {
		unblock()
		<-readerDone
	}()
	select {
	case <-entered:
	case err := <-held:
		t.Fatalf("semantic holder did not reach copy-out inside the shared reader: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("semantic holder did not enter")
	}
	typ, value, err := bson.MarshalValue(int32(2))
	if err != nil {
		t.Fatal(err)
	}
	nextLSN := backend.CommandWALNextLSN()
	updated := make(chan error, 1)
	go func() {
		matched, modified, err := col.UpdateBSONSet(ids[victim], []collections.BSONSetField{{Key: "n", Value: bson.RawValue{Type: typ, Value: value}}})
		if err == nil && !(matched && modified) {
			err = fmt.Errorf("BSON UPDATE matched=%t modified=%t", matched, modified)
		}
		updated <- err
	}()
	blocked := false
	select {
	case err = <-updated:
	case <-time.After(5 * time.Second):
		blocked = true
	}
	unblock()
	if readErr := <-held; readErr != nil {
		t.Fatal(readErr)
	}
	if blocked {
		select {
		case err := <-updated:
			if err != nil {
				t.Fatalf("BSON UPDATE after releasing independent read: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("BSON UPDATE did not finish after releasing independent read")
		}
		t.Fatal("BSON UPDATE waited for the independent semantic value read")
	}
	if err != nil {
		t.Fatal(err)
	}
	if got := backend.CommandWALNextLSN(); got != nextLSN+1 {
		t.Fatalf("BSON UPDATE command frontier %d, want %d", got, nextLSN+1)
	}
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	capture = nil
	closingManager := manager
	manager = nil
	if err := closingManager.Close(); err != nil {
		t.Fatal(err)
	}
	closingDB := db
	db = nil
	if err := closingDB.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err = db.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	reopenedManager := collections.NewCollectionManager(backend)
	defer func() {
		if err := reopenedManager.Close(); err != nil {
			t.Error(err)
		}
	}()
	reopened, err := reopenedManager.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{0, victim} {
		doc, err := reopened.Get(ids[index])
		if err != nil {
			t.Fatal(err)
		}
		want := int32(1)
		if index == victim {
			want = 2
		}
		raw := bson.Raw(doc)
		if raw.Lookup("n").Int32() != want || raw.Lookup("pad").StringValue() != pad {
			t.Fatalf("reopened BSON %s changed: %v", ids[index], raw)
		}
	}
}
