package collections

import (
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// ordinaryColumnGraphValuesEqual borrows the original complete-cut read and
// the operation-local asset view already preadmitted by typedRowEqual. Full
// retains the document verbatim, so public document-reconstruction eligibility
// is intentionally irrelevant to this declared-value comparison. Resolve the
// authoritative locator at the same captured cut; never parse the Full JSON
// back into vectors or obtain a second snapshot.
func (r *NativeJSONRead) ordinaryColumnGraphValuesEqual(view *CollectionReadView, id []byte, wanted []columnDeclaredValue) (bool, error) {
	if !nativeOrdinaryColumnGraphMeta(r.meta) || view == nil || view.snapshot != r.capture.Snapshot() {
		return false, backenddb.ErrNativeSemanticUnsupported
	}
	if err := view.validateOpen(); err != nil {
		return false, err
	}
	domain, err := r.borrowedNamedDomain("column/row-locator")
	if err != nil {
		return false, err
	}
	entry, resolved, err := domain.memoryEntry(id)
	if !resolved {
		scratch, e := r.physicalScratch()
		if e != nil {
			return false, e
		}
		entry, err = domain.baseEntry(id, scratch, r.readLeaf)
	}
	if errors.Is(err, tree.ErrKeyNotFound) || (err == nil && entry.Flags&node.FlagTombstone != 0) {
		return false, errors.New("collections: typed replacement old row missing")
	}
	if err != nil {
		return false, err
	}
	value := entry.Value
	var valueLease *memtable.COWExternalLease
	defer func() {
		value = nil
		entry = node.LeafEntry{}
		domain = nativeDomainRead{}
		if valueLease != nil {
			valueLease.Close()
		}
	}()
	if entry.Flags&node.FlagPointer != 0 {
		value, err = r.readValue(entry.ValuePtr, func(n uint64) error {
			var e error
			valueLease, e = r.capture.NativeAllocator().AdmitNativeBytes(n)
			return e
		})
		if err != nil {
			return false, err
		}
	}
	ref, err := decodeColumnPrimaryRowLocatorBorrowedID(id, value)
	if err != nil {
		return false, err
	}
	// The decoder retains only caller-owned id and scalar coordinates. Neither
	// the inline leaf nor the independently admitted pointer output is exposed.
	value = nil
	entry = node.LeafEntry{}
	domain = nativeDomainRead{}
	cfg := *r.meta.Options.ColumnStore
	if err = view.ensureAssetReadCaches(cfg, ColumnAssetReadIntegrityVerify); err != nil {
		return false, err
	}
	physical, err := view.materializerColumnSnapshotView(cfg)
	if err != nil {
		return false, err
	}
	projection, err := view.pointRowScanProjection(physical, nil)
	if err != nil {
		return false, err
	}
	var scratch columnPhysicalRowReaderScratch
	row, err := view.fetchDocumentPointRow(physical, ref, projection, &scratch, nil)
	if err != nil {
		return false, err
	}
	cache := view.typedColumnReconstructionCacheForConfig(cfg)
	typed, err := view.collection.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache(view.snapshot, view.catalog.rootID(collectionColumnManifestRootName(r.meta.Name)), cfg, row, cache, nil)
	if err != nil {
		return false, err
	}
	values, err := mergeColumnReconstructionValues(cfg, row.Values, typed.Values)
	if err != nil {
		return false, err
	}
	return typedDeclaredValuesEqual(values, wanted)
}
