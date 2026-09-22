package collections

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type typedGraphCopyTestIterator struct {
	*systemTargetIterator
	closes int
	err    error
}

func TestTypedGraphCaptureDefaultRawWorkEnvelope(t *testing.T) {
	// Finite policy arithmetic, not a corpus fixture or capacity measurement.
	// Each current/old primary, locator and two scalar roots includes raw
	// tombstones/history; the independently checked latest locator is ninth.
	const rawRows uint64 = 2_600_000
	const rowStreams = 2*(1+1+2) + 1
	const manifestRecords uint64 = 65_537
	const rowRecords = rowStreams * rawRows
	const records = rowRecords + 2*manifestRecords + 4
	const bytes = rowRecords*128 + 2*(64<<20+9*manifestRecords) + 17_486
	if records != 23_531_078 || bytes != 3_130_614_880 {
		t.Fatal("raw envelope arithmetic changed")
	}
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine}
	const namespace = "minima/column-assets"
	if bound := typedGraphCaptureGraphRecordBound(def, namespace, 33); bound != 13_369 {
		t.Fatalf("graph metadata bound=%d; revisit declared envelope", bound)
	}
	copy := typedGraphBaseCopy{
		catalog:   &collectionCatalog{meta: CollectionMeta{Name: "minima", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{AssetManager: &ColumnAssetManagerConfig{Namespace: namespace}}}}},
		remaining: typedGraphCaptureBudget{records: typedGraphCaptureMaxRecords, bytes: typedGraphCaptureMaxBytes},
	}
	initial := copy.remaining
	// Leave graph-control growth to the real producer, including header,
	// identity, record keys/framing and the complete inline vector state.
	if err := copy.remaining.charge(records-4, bytes-17_486); err != nil {
		t.Fatalf("default raw capture ceiling cannot admit declared envelope: budget=%+v need_records=%d need_bytes=%d: %v", copy.remaining, records, bytes, err)
	}
	adjacency := make([]uint32, 35) // 33 empty layers, maximal admitted level 32.
	adjacency[0], adjacency[1] = columnVectorGraphLayeredAdjacencyMagic, 32
	if err := copy.reserveManifest(def, []columnVectorGraphAssetRow{{Adjacency: adjacency}}); err != nil {
		t.Fatal(err)
	}
	if copy.manifestLimit != 17_486 || copy.remaining.records != initial.records-records || copy.remaining.bytes != initial.bytes-bytes {
		t.Fatalf("remaining/growth=%+v manifest=%d", copy.remaining, copy.manifestLimit)
	}
	before := copy.remaining
	for _, extra := range []typedGraphCaptureBudget{{records: before.records + 1}, {bytes: before.bytes + 1}} {
		if err := copy.remaining.charge(extra.records, extra.bytes); !errors.Is(err, errTypedGraphCaptureBudget) || copy.remaining != before {
			t.Fatalf("over-limit charge changed remaining budget: %+v err=%v", copy.remaining, err)
		}
	}
}

func TestTypedGraphBaseRecaptureDeletesAndEmpty(t *testing.T) {
	col, reader, ids, _, _, _ := openTypedGraphQualityFixture(t, 512)
	defer reader.Close()
	for round := 0; round < 2; round++ {
		if _, err := col.DeleteBatch(ids[round*256 : (round+1)*256]); err != nil {
			t.Fatal(err)
		}
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			t.Fatal(err)
		}
		if err := col.db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		snap := col.db.AcquireSnapshot()
		catalog, err := loadCollectionCatalog(snap, col.Name())
		if err != nil {
			t.Fatal(err)
		}
		open := func(root uint64) iterator.UnsafeIterator {
			if root == 0 {
				return &systemTargetIterator{}
			}
			it, err := snap.IteratorAtRoot(root, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			return it
		}
		for name, root := range catalog.typedGraphBase.roots {
			a, b := open(catalog.rootID(name)), open(root)
			for a.Valid() || b.Valid() {
				for a.Valid() && a.IsDeleted() {
					a.Next()
				}
				for b.Valid() && b.IsDeleted() {
					b.Next()
				}
				if !a.Valid() && !b.Valid() {
					break
				}
				if a.Valid() != b.Valid() || !bytes.Equal(a.UnsafeKey(), b.UnsafeKey()) {
					t.Fatalf("round%d %s retained deleted/missing key", round, name)
				}
				av, ap, af, ar := iterator.UnsafeEntryWithRevision(a)
				bv, bp, bf, br := iterator.UnsafeEntryWithRevision(b)
				if !bytes.Equal(av, bv) || ap != bp || af != bf || ar != br {
					t.Fatalf("round%d %s raw entry changed", round, name)
				}
				a.Next()
				b.Next()
			}
			if err := errors.Join(a.Error(), b.Error(), a.Close(), b.Close()); err != nil {
				t.Fatal(err)
			}
		}
		_ = snap.Close()
	}
}

func TestTypedGraphBaseCopyRejectsStaleAlias(t *testing.T) {
	col, reader, _, _, _, _ := openTypedGraphQualityFixture(t, 32)
	defer reader.Close()
	snap := col.db.AcquireSnapshot()
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, col.Name())
	if err != nil {
		t.Fatal(err)
	}
	state := snap.State()
	name := collectionPrimaryRootName(col.Name())
	intent, err := col.newCollectionUpdateCommandWALIntent(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Fixture-only descriptor cutover; current collection roots stay unchanged.
	_, _, err = col.db.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(nil, nil, intent, func(_ backenddb.CommandWALPublishContext, _ []uint64) (iterator.UnsafeIterator, error) {
		return buildSystemDeltaIterator(map[string][]byte{systemCollectionRootKey(typedGraphBaseAliasRootName(col.Name(), name)): encodeRootID(catalog.rootID(name))})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := col.validateColumnGraphRebuildSource(catalog, state.CommitSeq, state.SystemRootPageID); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("stale alias accepted: %v", err)
	}
}

func TestTypedGraphCaptureGraphMetadataBound(t *testing.T) {
	col, reader, _, _, _, _ := openTypedGraphQualityFixture(t, 32)
	defer reader.Close()
	snap := col.db.AcquireSnapshot()
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, col.Name())
	if err != nil {
		t.Fatal(err)
	}
	_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotViewAtCatalog("embedding_graph", snap, catalog)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeColumnVectorGraphManifestRecord(graph)
	if err != nil {
		t.Fatal(err)
	}
	def := catalog.meta.VectorIndexes[0]
	if uint64(len(raw)) > typedGraphCaptureGraphRecordBound(def, catalog.meta.Options.ColumnStore.AssetManager.Namespace, graph.AdjacencyLayerCount) {
		t.Fatal("actual graph exceeds bound")
	}
	// Exercise maximal-width numeric fields and long producer strings without
	// asserting these synthetic metadata values describe a valid physical asset.
	namespace := strings.Repeat("n", 4096)
	for _, layers := range []int{1, 12, 128} {
		var encoded bytes.Buffer
		for layer := 0; layer < layers; layer++ {
			source := columnVectorGraphAdjacencySourceSnapshot{Schema: columnVectorGraphAdjacencySourceSchema(layer), ColumnName: columnVectorGraphAdjacencySourceColumnName(layer), ValueType: string(ColumnStoreValueAdjacencyList), Encoding: columnVectorIndexStateEncodingRawUint32List, Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: namespace, Generation: ^uint64(0), PartID: ^uint64(0)}, BaseManifestGeneration: ^uint64(0), BaseManifestChecksum: ^uint64(0), GraphSchemaHash: ^uint64(0)}
			encodeColumnVectorGraphLayer0AdjacencySource(&encoded, source)
		}
		layerBound := typedGraphCaptureGraphRecordBound(def, namespace, layers) - typedGraphCaptureGraphRecordBound(def, namespace, 0)
		if uint64(encoded.Len()) != layerBound {
			t.Fatalf("layer codec bytes=%d bound=%d", encoded.Len(), layerBound)
		}
	}
}

func (it *typedGraphCopyTestIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	return nil, page.ValuePtr{FileID: 7, Offset: uint64(it.idx + 10), Length: 123}, node.FlagPointer, page.EntryRevision(100 + it.idx)
}
func (it *typedGraphCopyTestIterator) UnsafeValue() []byte { panic("copy resolved persistent pointer") }
func (it *typedGraphCopyTestIterator) Close() error        { it.closes++; return it.err }
func (it *typedGraphCopyTestIterator) Error() error        { return it.err }

func TestTypedGraphCaptureReplacementRawEntries(t *testing.T) {
	for _, test := range []struct{ old, next, want string }{{"ac", "bc", "abc"}, {"ac", "", "ac"}, {"", "bc", "bc"}, {"", "", ""}} {
		makeIter := func(keys string) *typedGraphCopyTestIterator {
			it := &typedGraphCopyTestIterator{systemTargetIterator: &systemTargetIterator{}}
			for _, key := range []byte(keys) {
				it.entries = append(it.entries, systemTargetEntry{key: []byte{key}})
			}
			return it
		}
		old, next := makeIter(test.old), makeIter(test.next)
		it := newTypedGraphCaptureReplacementIterator(old, next)
		var got []byte
		for ; it.Valid(); it.Next() {
			key := it.UnsafeKey()[0]
			got = append(got, key)
			value, ptr, flags, revision := iterator.UnsafeEntryWithRevision(it)
			index := bytes.IndexByte([]byte(test.next), key)
			if index < 0 {
				if flags != node.FlagTombstone || ptr != (page.ValuePtr{}) || len(value) != 0 {
					t.Fatal("old-only entry was not deleted")
				}
			} else if flags != node.FlagPointer || ptr.Offset != uint64(index+10) || revision != page.EntryRevision(100+index) {
				t.Fatal("raw pointer/revision changed")
			}
		}
		if string(got) != test.want {
			t.Fatalf("keys=%s want=%s", got, test.want)
		}
		sentinel := errors.New("iterator failure")
		old.err = sentinel
		if !errors.Is(it.Error(), sentinel) || !errors.Is(it.Close(), sentinel) {
			t.Fatal("iterator error lost")
		}
		_ = it.Close()
		if old.closes != 1 || next.closes != 1 {
			t.Fatal("iterators not closed exactly once")
		}
	}
}

func TestTypedGraphCaptureCopyBoundsBeforeEffects(t *testing.T) {
	col, reader, _, _, _, _ := openTypedGraphQualityFixture(t, 32)
	defer reader.Close()
	snap := col.db.AcquireSnapshot()
	defer snap.Close()
	catalog, err := loadCollectionCatalog(snap, col.Name())
	if err != nil {
		t.Fatal(err)
	}
	frames := countCollectionCommandWALFrames(t, col.db.Dir())
	before := snap.State()
	for _, limit := range []typedGraphCaptureBudget{{records: 1, bytes: typedGraphCaptureMaxBytes}, {records: typedGraphCaptureMaxRecords, bytes: 1}} {
		if copy, err := prepareTypedGraphBaseCopy(snap, catalog, limit); copy != nil || !errors.Is(err, errTypedGraphCaptureBudget) {
			t.Fatalf("tiny bound: %v", err)
		}
	}
	current := col.db.AcquireSnapshot()
	defer current.Close()
	if current.State().CommitSeq != before.CommitSeq || countCollectionCommandWALFrames(t, col.db.Dir()) != frames {
		t.Fatal("rejected copy changed roots or WAL")
	}
}
