package collections

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Partition preparation consumes the original source rows, not the published
// graph's adjacency. Both adapters retain their original source until Close.
type vectorPartitionRowSourceV1 interface {
	Close() error
	partitionSourceIdentityV1() VectorPartitionSourceIdentityV1
	partitionSourceAvailabilityV1() (typed, ids bool)
	partitionSourceVectorV1(int) ([]float32, bool)
	documentIDForOrdinal(int) ([]byte, bool)
	partitionSourceRowV1(int, *columnPhysicalRowReaderScratch) (columnVectorGraphAssetRow, error)
}

func (r *columnVectorGraphPhysicalRowReader) partitionSourceAvailabilityV1() (bool, bool) {
	return r.typedVectorSource != nil, r.documentIDSource != nil
}

func (r *columnVectorGraphPhysicalRowReader) partitionSourceVectorV1(ordinal int) ([]float32, bool) {
	if r.typedVectorSource == nil {
		return nil, false
	}
	values, _, _, ok := r.typedVectorSource.vectorForOrdinal(ordinal)
	return values, ok
}

func (r *columnVectorGraphPhysicalRowReader) partitionSourceIdentityV1() VectorPartitionSourceIdentityV1 {
	return VectorPartitionSourceIdentityV1{Generation: r.graph.BaseManifestGeneration, Checksum: r.graph.BaseManifestChecksum, SchemaHash: r.graph.BaseSchemaHash, RowCount: uint64(r.graph.RowCount)}
}

func (r *columnVectorGraphPhysicalRowReader) partitionSourceRowV1(ordinal int, scratch *columnPhysicalRowReaderScratch) (columnVectorGraphAssetRow, error) {
	row, fetchErr := r.FetchRow(ordinal, scratch)
	if fetchErr != nil {
		row = columnVectorGraphPhysicalRow{}
	}
	if len(row.Vector) != r.def.Dimensions {
		row.Vector, _, _, _ = r.typedVectorForOrdinal(ordinal)
	}
	if len(row.Vector) != r.def.Dimensions {
		return columnVectorGraphAssetRow{}, fmt.Errorf("%w: authoritative typed row", ErrVectorPartitionSearchUnavailable)
	}
	ref, ok := r.rowRefForOrdinal(ordinal)
	if !ok {
		if fetchErr != nil || row.RowIndex < 0 {
			return columnVectorGraphAssetRow{}, fmt.Errorf("%w: authoritative source row ref", ErrVectorPartitionSearchUnavailable)
		}
		// Preserve the original legacy row-asset fallback. Native captured
		// sources below always supply their actual row locator and source LSN.
		ref = DocumentRowRef{Generation: r.graph.BaseManifestGeneration, PartID: 1, RowIndex: row.RowIndex, AppliedCommandLSN: 1}
	}
	if invNorm, _, _, ok := r.invNormForOrdinal(ordinal); ok {
		row.InvNorm = invNorm
	}
	id, ok := r.documentIDForOrdinal(ordinal)
	if !ok || len(id) == 0 {
		return columnVectorGraphAssetRow{}, fmt.Errorf("%w: authoritative stable ID", ErrVectorPartitionSearchUnavailable)
	}
	return columnVectorGraphAssetRow{ID: id, Vector: row.Vector, InvNorm: row.InvNorm, BaseRowRef: ref}, nil
}

type nativePartitionRowSourceV1 struct {
	snapshot *backenddb.Snapshot
	read     *NativeJSONRead
	lease    *memtable.COWExternalLease
	source   *columnVectorGraphTypedColumnVectorSource
	identity VectorPartitionSourceIdentityV1
	rows     []columnVectorGraphAssetRow
}

func (r *nativePartitionRowSourceV1) Close() error {
	if r == nil {
		return nil
	}
	r.rows = nil
	source, read, lease, snapshot := r.source, r.read, r.lease, r.snapshot
	r.source, r.read, r.lease, r.snapshot = nil, nil, nil, nil
	var err error
	if source != nil {
		err = source.Close()
	}
	if read != nil {
		err = errors.Join(err, read.Close())
	}
	if snapshot != nil {
		err = errors.Join(err, snapshot.Close())
	}
	if lease != nil {
		lease.Close()
	}
	return err
}

func (r *nativePartitionRowSourceV1) partitionSourceIdentityV1() VectorPartitionSourceIdentityV1 {
	return r.identity
}

func (r *nativePartitionRowSourceV1) partitionSourceAvailabilityV1() (bool, bool) {
	return r.source != nil, r.rows != nil
}

func (r *nativePartitionRowSourceV1) partitionSourceVectorV1(ordinal int) ([]float32, bool) {
	if ordinal < 0 || ordinal >= len(r.rows) {
		return nil, false
	}
	return r.rows[ordinal].Vector, true
}

func (r *nativePartitionRowSourceV1) documentIDForOrdinal(ordinal int) ([]byte, bool) {
	if ordinal < 0 || ordinal >= len(r.rows) {
		return nil, false
	}
	return r.rows[ordinal].ID, true
}

func (r *nativePartitionRowSourceV1) partitionSourceRowV1(ordinal int, _ *columnPhysicalRowReaderScratch) (columnVectorGraphAssetRow, error) {
	if ordinal < 0 || ordinal >= len(r.rows) {
		return columnVectorGraphAssetRow{}, fmt.Errorf("%w: authoritative source ordinal", ErrVectorPartitionSearchUnavailable)
	}
	return r.rows[ordinal], nil
}

// vectorPartitionStorageDBV1 selects only the existing backend for namespace
// authority and assets. It never equips a native collection with a legacy
// mutation facade or changes its logical publication route.
func (c *Collection) vectorPartitionStorageDBV1() *backenddb.DB {
	if c == nil {
		return nil
	}
	if c.nativeJSON != nil {
		return c.nativeJSON.db
	}
	return c.db
}

// Only partition-live uses this native fallback. Generic schema, typed graph
// and ad-hoc vector dispatch retain their existing coordinator selection.
func (c *Collection) partitionLiveCoordinatorV1() *collectionSchemaCoordinator {
	if c == nil {
		return nil
	}
	if c.nativeJSON != nil {
		return collectionSchemaCoordinatorForDBCollection(c.nativeJSON.db, c.name)
	}
	return c.collectionSchemaCoordinator()
}

func (c *Collection) openVectorPartitionRowSourceV1(index string) (vectorPartitionRowSourceV1, error) {
	if c != nil && c.nativeJSON != nil {
		return c.openNativePartitionRowSourceV1(index)
	}
	if c != nil && nativeSourceImportMetaV2(c.meta) {
		return c.openPhysicalPartitionSourceV2(index)
	}
	return c.openColumnVectorGraphPhysicalRowReader(index, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
}

func (c *Collection) nativeVectorPartitionSourceIdentityV1(index string) (identity VectorPartitionSourceIdentityV1, resultErr error) {
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return identity, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	status, err := c.nativeVectorGraphStatusAtRead(read, index)
	if err != nil {
		return identity, err
	}
	if !status.Loaded || status.State != VectorIndexStateColumnGraphLoaded {
		return identity, fmt.Errorf("%w: immutable graph source is not loaded", ErrVectorPartitionSearchUnavailable)
	}
	manifest, ok := read.collection.domains.Get("column/manifest")
	if !ok {
		return identity, backenddb.ErrNativeSemanticUnsupported
	}
	h, err := nativeVectorGraphHeader(read.meta, read.capture.Snapshot(), manifest.baseRoot)
	if err != nil {
		return identity, err
	}
	cfg := read.meta.Options.ColumnStore
	return VectorPartitionSourceIdentityV1{Generation: h.Generation, Checksum: cfg.ActiveManifest.Checksum, SchemaHash: cfg.SchemaHash, RowCount: h.Rows}, nil
}

func (c *Collection) openNativePartitionRowSourceV1(index string) (out *nativePartitionRowSourceV1, resultErr error) {
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return nil, err
	}
	r := &nativePartitionRowSourceV1{read: read}
	defer func() {
		if out == nil {
			resultErr = errors.Join(resultErr, r.Close())
		}
	}()
	def, ok := findVectorIndex(read.meta.VectorIndexes, index)
	if !ok {
		return nil, ErrIndexNotFound
	}
	primary, _ := read.collection.domains.Get("primary")
	manifest, ok := read.collection.domains.Get("column/manifest")
	if !ok || primary != nil && primary.typed != nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	snap := read.capture.Snapshot()
	h, err := nativeVectorGraphHeader(read.meta, snap, manifest.baseRoot)
	if err != nil {
		return nil, err
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativePartitionRowSourceV1{})))
	r.lease, err = read.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, err
	}
	admit := func(n uint64) error {
		if err := nativeTypedCheckedAdd(&charge, n); err != nil {
			return err
		}
		return r.lease.Resize(charge)
	}
	capacity, err := nativeVectorGraphSourceCapacity(read.meta, snap, manifest.baseRoot, read.database.ColumnAssetRootDir(), def, h, admit)
	if err != nil {
		return nil, err
	}
	if err = admit(capacity); err != nil {
		return nil, err
	}
	// Status validates the actual TVIS against this same captured directory.
	status, err := c.nativeVectorGraphStatusAtRead(read, index)
	if err != nil {
		return nil, err
	}
	if !status.Loaded || status.State != VectorIndexStateColumnGraphLoaded {
		return nil, fmt.Errorf("%w: immutable graph source is not loaded", ErrVectorPartitionSearchUnavailable)
	}
	records, err := nativeVectorGraphRecords(snap, manifest.baseRoot)
	if err != nil {
		return nil, err
	}
	roots := make(map[string]uint64, read.collection.domains.Len())
	read.collection.domains.Scan(func(name string, d *nativeDomainState) bool {
		roots[read.meta.Name+"/"+name] = d.baseRoot
		return true
	})
	catalog := newCollectionCatalog(read.meta, roots)
	cfg := *read.meta.Options.ColumnStore
	var used bool
	r.rows, r.source, used, err = columnVectorGraphRowsFromTypedColumnSnapshot(nil, read.database.ColumnAssetRootDir(), snap, catalog, cfg, records, columnManifestSnapshot{Collection: read.meta.Name, Generation: h.Generation, RowCount: int(h.Rows), AppliedCommandLSN: h.AppliedCommandLSN}, def)
	if err != nil {
		return nil, err
	}
	if !used || uint64(len(r.rows)) != h.Rows {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	r.identity = VectorPartitionSourceIdentityV1{Generation: h.Generation, Checksum: cfg.ActiveManifest.Checksum, SchemaHash: cfg.SchemaHash, RowCount: h.Rows}
	return r, nil
}

// Startup replay owns a physical predecessor, not a foreground native capture.
// Reuse its original Snapshot and the exact persisted cdir2 descriptor, as the
// original assigned103 replay does. No engine or second allocator is created.
func (c *Collection) openPhysicalPartitionSourceV2(index string) (out *nativePartitionRowSourceV1, resultErr error) {
	if c == nil || c.db == nil {
		return nil, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	r := &nativePartitionRowSourceV1{snapshot: snap}
	defer func() {
		if out == nil {
			resultErr = errors.Join(resultErr, r.Close())
		}
	}()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	meta := catalog.meta
	def, ok := findVectorIndex(meta.VectorIndexes, index)
	if !ok {
		return nil, ErrIndexNotFound
	}
	root := catalog.rootID(collectionColumnManifestRootName(meta.Name))
	h, err := nativeVectorGraphHeader(meta, snap, root)
	if err != nil {
		return nil, err
	}
	records, err := nativeVectorGraphRecords(snap, root)
	if err != nil {
		return nil, err
	}
	status, err := columnGraphVectorIndexStatusForRecords(c.db.ColumnAssetRootDir(), meta.Name, meta.Options.ColumnStore, def, records, binary.BigEndian.Uint64(h.Digest[:8]), true)
	if err != nil {
		return nil, err
	}
	if !status.Loaded || status.State != VectorIndexStateColumnGraphLoaded {
		return nil, ErrVectorPartitionSearchUnavailable
	}
	var used bool
	r.rows, r.source, used, err = columnVectorGraphRowsFromTypedColumnSnapshot(nil, c.db.ColumnAssetRootDir(), snap, catalog, *meta.Options.ColumnStore, records, columnManifestSnapshot{Collection: meta.Name, Generation: h.Generation, RowCount: int(h.Rows), AppliedCommandLSN: h.AppliedCommandLSN}, def)
	if err != nil {
		return nil, err
	}
	if !used || uint64(len(r.rows)) != h.Rows {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	r.identity = VectorPartitionSourceIdentityV1{Generation: h.Generation, Checksum: meta.Options.ColumnStore.ActiveManifest.Checksum, SchemaHash: meta.Options.ColumnStore.SchemaHash, RowCount: h.Rows}
	return r, nil
}
