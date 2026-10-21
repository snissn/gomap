//go:build !windows

package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/compress/zstd"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
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
			if codec != nil && codec.id == dictID && codec.decoder != nil {
				decoder = codec
			}
		}
		if decoder == nil {
			t.Fatal("real dictionary leaf did not populate the caller-local decoder")
		}
		for i := 0; i < 64; i++ {
			key := []byte(fmt.Sprintf("a%03d", i))
			it.Seek(key)
			if !it.Valid() || !bytes.Equal(it.Key(), key) || !bytes.Equal(it.Value(), want) || it.Error() != nil {
				t.Fatalf("dictionary leaf seek %s: key=%q value=%q error=%v", key, it.Key(), it.Value(), it.Error())
			}
		}
		if child.workspace.codecs[0] != decoder {
			t.Fatal("warm dictionary reads replaced the caller-local decoder")
		}
	}
	check() // First public construction and reads require a cold local decoder.
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
	// Each dictionary decoder uses its original admitted workspace. Finish
	// that operation before admitting an independent current-cut point decoder;
	// this fixture does not require two full decoder envelopes to fit at once.
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("a000")); err != nil || string(got) != "new-current-value" {
		t.Fatalf("new cut value: %q %v", got, err)
	}
}
