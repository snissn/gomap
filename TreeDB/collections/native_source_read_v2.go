package collections

import (
	"bytes"
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

func nativeTypedReadManifestCapacity(snapshot *backenddb.Snapshot, root uint64, selected []columnManifestRecord) (uint64, error) {
	if selected == nil {
		return nativeTypedManifestCapacity(snapshot, root)
	}
	var total uint64
	for _, r := range selected {
		record := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(columnManifestRecord{}))) + memtable.COWAllocationCharge(uint64(len(r.key))) + memtable.COWAllocationCharge(uint64(len(r.value)))
		n, e := nativeTypedCheckedMul(record+preparedSemanticStreamMapEntryReserveBytes, 16)
		if e != nil {
			return 0, e
		}
		if e = nativeTypedCheckedAdd(&total, n); e != nil {
			return 0, e
		}
	}
	return total, nil
}

// Both ordinary full-manifest and source point reads use the SAME physical
// parser/capacity body. Source cdir2 needs only this row's generation, not every
// preceding immutable chunk. No retained map or second decoder is introduced.
func walkNativeTypedReadRecords(snapshot *backenddb.Snapshot, root uint64, selected []columnManifestRecord, visit func([]byte, byte) error) error {
	if selected != nil {
		for _, r := range selected {
			if e := visit(r.value, 0); e != nil {
				return e
			}
		}
		return nil
	}
	it, e := snapshot.IteratorAtRoot(root, columnManifestPartRecordPrefixBytes, nil)
	if e != nil {
		return e
	}
	defer it.Close()
	for it.Valid() && bytes.HasPrefix(it.UnsafeKey(), columnManifestPartRecordPrefixBytes) {
		if !it.IsDeleted() {
			raw, _, flags := it.UnsafeEntry()
			if e = visit(raw, flags); e != nil {
				return e
			}
		}
		it.Next()
	}
	return it.Error()
}

func (r *NativeJSONRead) sourceReadRecordsV2(id []byte, manifestRoot uint64) ([]columnManifestRecord, error) {
	locator, ok := r.collection.domains.Get("column/row-locator")
	if !ok {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	domain, e := newNativeDomainRead(locator, r.capture.Snapshot())
	if e != nil {
		return nil, e
	}
	scratch, e := r.physicalScratch()
	if e != nil {
		return nil, e
	}
	entry, e := domain.getEntry(id, scratch, r.readLeaf)
	if e != nil {
		return nil, e
	}
	if entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 {
		return nil, errors.New("collections: source row locator must be inline")
	}
	ref, e := decodeColumnPrimaryRowLocator(id, entry.Value)
	if e != nil {
		return nil, e
	}
	reader, e := r.capture.Snapshot().NativeReaderAtRoot(manifestRoot)
	if e != nil {
		return nil, e
	}
	records := make([]columnManifestRecord, 0, 2)
	for _, partID := range [...]uint64{ref.PartID, typedColumnPartAssetPartID} {
		key := columnManifestPartRecordKey(ref.Generation, partID)
		entry, e = reader.GetEntryExactWithOwnedScratch(key, scratch.key[:], scratch.leaf[:], &scratch.point, r.readLeaf)
		if e != nil {
			return nil, e
		}
		if entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 || len(entry.Value) > 3000 {
			return nil, errors.New("collections: source part must be bounded inline")
		}
		records = append(records, columnManifestRecord{key: key, value: bytes.Clone(entry.Value)})
	}
	return records, nil
}

func (c *Collection) openNativeSourceReaderV2(expected VectorPartitionSourceSnapshotV2, index string) (out *VectorPartitionSourceReaderV2, resultErr error) {
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	read, e := c.nativeJSON.Capture(c.name)
	if e != nil {
		return nil, e
	}
	var lease *memtable.COWExternalLease
	defer func() {
		if out == nil {
			if lease != nil {
				lease.Close()
			}
			resultErr = errors.Join(resultErr, read.Close())
		}
	}()
	if !nativeSourceImportMetaV2(read.meta) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	// Binding/progress/header are bounded original inline records. Sixteen
	// original decode/copy/control envelopes plus the finite catalog/root names.
	manifest, ok := read.collection.domains.Get("column/manifest")
	if !ok {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	charge, e := nativeSourceRequirementBasisCapacityV2(read.capture.Snapshot(), manifest.baseRoot)
	if e != nil {
		return nil, e
	}
	charge += 16 * (memtable.COWAllocationCharge(maxSourceImportBindingBytesV2) + memtable.COWAllocationCharge(columnSourceDirectoryHeaderBytesV2) + preparedSemanticStreamMapEntryReserveBytes)
	charge += memtable.COWAllocationCharge(uint64(unsafe.Sizeof(VectorPartitionSourceReaderV2{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(collectionCatalog{})))
	read.collection.domains.Scan(func(name string, _ *nativeDomainState) bool {
		charge += preparedSemanticStreamMapEntryReserveBytes + memtable.COWAllocationCharge(uint64(len(read.meta.Name)+len(name)+1))
		return true
	})
	lease, e = read.capture.NativeAllocator().AdmitNativeBytes(charge)
	if e != nil {
		return nil, e
	}
	roots := make(map[string]uint64, read.collection.domains.Len())
	read.collection.domains.Scan(func(name string, d *nativeDomainState) bool { roots[read.meta.Name+"/"+name] = d.baseRoot; return true })
	catalog := newCollectionCatalog(read.meta, roots)
	out, e = c.openVectorPartitionSourceCatalogV2(expected, index, read.capture.Snapshot(), catalog)
	if e != nil {
		return nil, e
	}
	out.nativeRead, out.nativeLease = read, lease
	return out, nil
}
