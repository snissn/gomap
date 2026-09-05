package collections

import (
	"bytes"
	"errors"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Initial capture safety ceilings, not qualified workload capacity or a total
// heap/disk limit. The publisher additionally owns batch and tree-build scratch.
const typedGraphCaptureMaxRecords = 8_000_000
const typedGraphCaptureMaxBytes = 512 << 20

var errTypedGraphCaptureBudget = errors.New("collections: typed graph captured index budget exceeded")

type typedGraphCaptureBudget struct{ records, bytes uint64 }

func (b *typedGraphCaptureBudget) charge(records, bytes uint64) error {
	if records > b.records || bytes > b.bytes {
		return errTypedGraphCaptureBudget
	}
	b.records -= records
	b.bytes -= bytes
	return nil
}

// Checks raw entries without resolving persistent value pointers. Both old and
// new streams are charged because recapture must remove old-only keys.
func scanTypedGraphCaptureRoot(snap *backenddb.Snapshot, root uint64, budget *typedGraphCaptureBudget) (payload uint64, err error) {
	if root == 0 {
		return 0, nil
	}
	it, err := snap.IteratorAtRoot(root, nil, nil)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	for ; it.Valid(); it.Next() {
		value, _, flags, _ := iterator.UnsafeEntryWithRevision(it)
		n := uint64(len(it.UnsafeKey())) + uint64(len(value)) + 9 // flags + revision
		if flags&node.FlagPointer != 0 {
			n = uint64(len(it.UnsafeKey())) + page.ValuePtrSize + 9
		}
		if err := budget.charge(1, n); err != nil {
			return 0, err
		}
		payload += n
	}
	return payload, it.Error()
}

type typedGraphBaseCopy struct {
	snap          *backenddb.Snapshot // borrowed through synchronous publication
	catalog       *collectionCatalog
	names         []string
	manifestBytes uint64
	remaining     typedGraphCaptureBudget
	manifestLimit uint64
}

func prepareTypedGraphBaseCopy(snap *backenddb.Snapshot, catalog *collectionCatalog, limits typedGraphCaptureBudget) (*typedGraphBaseCopy, error) {
	names, err := typedGraphBaseRootNames(catalog.meta)
	if err != nil {
		return nil, err
	}
	copy := &typedGraphBaseCopy{snap: snap, catalog: catalog, names: names}
	for _, name := range names {
		n, err := scanTypedGraphCaptureRoot(snap, catalog.rootID(name), &limits)
		if err != nil {
			return nil, err
		}
		if name == collectionColumnManifestRootName(catalog.meta.Name) {
			copy.manifestBytes = n
		}
		if catalog.typedGraphBase != nil {
			if _, err := scanTypedGraphCaptureRoot(snap, catalog.typedGraphBase.roots[name], &limits); err != nil {
				return nil, err
			}
		}
	}
	// Reserve growth separately before the WAL frame: graph control sources
	// are charged once the built adjacency layer count is known.
	copy.remaining = limits
	return copy, nil
}

func (copy *typedGraphBaseCopy) reserveManifest(def VectorIndexDefinition, rows []columnVectorGraphAssetRow) error {
	layers := 1
	for _, row := range rows {
		layer, err := columnVectorGraphAdjacencyMaxLayer(row.Adjacency)
		if err != nil {
			return err
		}
		if layer+1 > layers {
			layers = layer + 1
		}
	}
	namespace := copy.catalog.meta.Options.ColumnStore.AssetManager.Namespace
	graphBytes := typedGraphCaptureGraphRecordBound(def, namespace, layers)
	// Existing header/identity bytes are already counted. Reserving one entire
	// additional header also covers the initial empty catalog: 6-byte tag,
	// two strings and 8 uint64s. All admitted operation names are six bytes.
	headerBytes := uint64(6 + 2*8 + 8*8 + len(copy.catalog.meta.Name) + len(ColumnPublishOperationInsert))
	growth := graphBytes + uint64(columnVectorIndexStateMaxInlineRecordBytes) + headerBytes + columnManifestIdentityRecordSize
	growth += uint64(len(columnManifestIdentityRecordKey) + len(columnManifestHeaderRecordKey) + len(columnVectorGraphManifestRecordKey(def.Name)) + len(columnVectorIndexStateRecordKey(def.Name)) + 4*9)
	if err := copy.remaining.charge(4, growth); err != nil {
		return err
	}
	copy.manifestLimit = copy.manifestBytes + growth
	return nil
}

func typedGraphCaptureGraphRecordBound(def VectorIndexDefinition, namespace string, layers int) uint64 {
	// encodeColumnVectorGraphManifestRecord: 6-byte tag, 6 string lengths,
	// 16 uint64s; all-layer envelope: 6-byte tag and 2 uint64s.
	n := uint64(6 + 6*8 + 16*8 + 6 + 2*8 + len(def.Name) + len(def.Field) + len(def.Metric.String()) + len(def.Encoding.String()) + len(string(ColumnAssetKindTCS1TypedColumnPart)) + len(namespace))
	for layer := 0; layer < layers; layer++ {
		// encodeColumnVectorGraphLayer0AdjacencySource: 6-byte tag,
		// 6 string lengths, 24 fixed-width numeric fields.
		n += uint64(6 + 6*8 + 24*8 + len(columnVectorGraphAdjacencySourceSchema(layer)) + len(columnVectorGraphAdjacencySourceColumnName(layer)) + len(string(ColumnStoreValueAdjacencyList)) + len(columnVectorIndexStateEncodingRawUint32List) + len(string(ColumnAssetKindTCS1TypedColumnPart)) + len(namespace))
	}
	return n
}

// A complete replacement expressed as a sorted delta, retaining raw pointer
// and revision ownership. Only old-only keys become tombstones. No ID map or
// payload copies are needed; both borrowed iterators stay pinned by the caller.
type typedGraphCaptureReplacementIterator struct {
	iterator.UnsafeIterator // new stream; overridden accessors select old-only
	old                     iterator.UnsafeIterator
	oldOnly, equal          bool
	closed                  bool
}

func newTypedGraphCaptureReplacementIterator(old, next iterator.UnsafeIterator) *typedGraphCaptureReplacementIterator {
	it := &typedGraphCaptureReplacementIterator{UnsafeIterator: next, old: old}
	it.selectEntry()
	return it
}
func (it *typedGraphCaptureReplacementIterator) selectEntry() {
	it.equal = false
	it.oldOnly = it.old.Valid() && !it.UnsafeIterator.Valid()
	if it.old.Valid() && it.UnsafeIterator.Valid() {
		cmp := bytes.Compare(it.old.UnsafeKey(), it.UnsafeIterator.UnsafeKey())
		it.oldOnly, it.equal = cmp < 0, cmp == 0
	}
}
func (it *typedGraphCaptureReplacementIterator) Valid() bool {
	return !it.closed && (it.old.Valid() || it.UnsafeIterator.Valid())
}
func (it *typedGraphCaptureReplacementIterator) Next() {
	if it.oldOnly || it.equal {
		it.old.Next()
	}
	if !it.oldOnly {
		it.UnsafeIterator.Next()
	}
	it.selectEntry()
}
func (it *typedGraphCaptureReplacementIterator) Seek(key []byte) {
	it.old.Seek(key)
	it.UnsafeIterator.Seek(key)
	it.selectEntry()
}
func (it *typedGraphCaptureReplacementIterator) UnsafeKey() []byte {
	if it.oldOnly {
		return it.old.UnsafeKey()
	}
	return it.UnsafeIterator.UnsafeKey()
}
func (it *typedGraphCaptureReplacementIterator) Key() []byte { return it.UnsafeKey() }
func (it *typedGraphCaptureReplacementIterator) KeyCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeKey()...)
}
func (it *typedGraphCaptureReplacementIterator) UnsafeValue() []byte {
	if it.oldOnly {
		return nil
	}
	return it.UnsafeIterator.UnsafeValue()
}
func (it *typedGraphCaptureReplacementIterator) Value() []byte { return it.UnsafeValue() }
func (it *typedGraphCaptureReplacementIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeValue()...)
}
func (it *typedGraphCaptureReplacementIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if it.oldOnly {
		return nil, page.ValuePtr{}, node.FlagTombstone, page.LegacyEntryRevision
	}
	return iterator.UnsafeEntryWithRevision(it.UnsafeIterator)
}
func (it *typedGraphCaptureReplacementIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	v, p, f, _ := it.UnsafeEntryWithRevision()
	return v, p, f
}
func (it *typedGraphCaptureReplacementIterator) IsDeleted() bool {
	return it.oldOnly || it.UnsafeIterator.IsDeleted()
}
func (it *typedGraphCaptureReplacementIterator) Error() error {
	return errors.Join(it.old.Error(), it.UnsafeIterator.Error())
}
func (it *typedGraphCaptureReplacementIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	return errors.Join(it.old.Close(), it.UnsafeIterator.Close())
}

func (copy *typedGraphBaseCopy) inputs(identity [columnManifestIdentityRecordSize]byte, records []columnManifestRecord) (_ []backenddb.OrderedRootDeltaPublishInput, err error) {
	actual := uint64(len(columnManifestIdentityRecordKey) + len(identity) + 9)
	for _, record := range records {
		actual += uint64(len(record.key) + len(record.value) + 9)
	}
	if copy.manifestLimit == 0 || actual > copy.manifestLimit {
		return nil, fmt.Errorf("%w: prepared manifest exceeds reserved metadata", errTypedGraphCaptureBudget)
	}
	inputs := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(copy.names))
	defer func() {
		if err != nil {
			for _, input := range inputs {
				err = errors.Join(err, input.Iter.Close())
			}
		}
	}()
	for _, name := range copy.names {
		var next iterator.UnsafeIterator
		if name == collectionColumnManifestRootName(copy.catalog.meta.Name) {
			next = columnManifestRootRecordIteratorOwned(identity, records)
		} else if root := copy.catalog.rootID(name); root != 0 {
			next, err = copy.snap.IteratorAtRoot(root, nil, nil)
			if err != nil {
				return nil, err
			}
		} else {
			next = &systemTargetIterator{}
		}
		var base uint64
		if copy.catalog.typedGraphBase != nil {
			base = copy.catalog.typedGraphBase.roots[name]
		}
		if base != 0 {
			old, openErr := copy.snap.IteratorAtRoot(base, nil, nil)
			if openErr != nil {
				return nil, errors.Join(openErr, next.Close())
			}
			next = newTypedGraphCaptureReplacementIterator(old, next)
		}
		inputs = append(inputs, backenddb.OrderedRootDeltaPublishInput{BaseRoot: base, Iter: next, StoragePolicy: backenddb.OrderedRootStoragePagerLeaves})
	}
	return inputs, nil
}

func (copy *typedGraphBaseCopy) captured(meta CollectionMeta, roots []uint64) (*typedGraphBaseAlias, error) {
	if len(roots) != len(copy.names) {
		return nil, fmt.Errorf("collections: captured root count %d want %d", len(roots), len(copy.names))
	}
	base := &typedGraphBaseAlias{meta: meta, roots: make(map[string]uint64, len(roots))}
	for i, name := range copy.names {
		base.roots[name] = roots[i]
	}
	return base, nil
}
