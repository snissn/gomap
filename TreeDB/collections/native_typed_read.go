package collections

import (
	"bytes"
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// The maintained reconstruction owns no retained codec or cache. One quota
// unit per input byte covers the original UseNumber object/array/scalar tree,
// decoder/encoder stacks and simultaneous old/replacement backing, including
// nested residuals. Encoded strings need at most six bytes per source byte.
func nativeTypedJSONReadCharge(bytes uint64) (uint64, error) {
	if bytes == ^uint64(0) {
		return 0, memtable.ErrCOWCapacity
	}
	maps, err := nativeTypedCheckedMul(bytes+1, preparedSemanticStreamMapEntryReserveBytes)
	if err != nil {
		return 0, err
	}
	output, err := nativeTypedCheckedMul(bytes+1, 24)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&maps, memtable.COWAllocationCharge(output)); err != nil {
		return 0, err
	}
	return maps, nil
}

// A declared string/vector adds one object member, not one map entry per
// value byte. Path bytes bound every split descriptor and newly inserted nested
// ancestor/map, even if that path was absent from the residual. Keep that tree
// bound separate from typed JSON escaping and old/replacement marshal backing.
func nativeTypedReconstructionCharge(meta CollectionMeta, residual, typedJSON uint64) (uint64, error) {
	var scalarJSON uint64
	for _, col := range meta.Options.ColumnStore.Columns {
		if err := nativeTypedCheckedAdd(&residual, uint64(len(col.Path))); err != nil {
			return 0, err
		}
		if err := nativeTypedCheckedAdd(&residual, 20); err != nil {
			return 0, err
		}
		if err := nativeTypedCheckedAdd(&scalarJSON, 20); err != nil {
			return 0, err
		}
	}
	total, err := nativeTypedJSONReadCharge(residual)
	if err != nil {
		return 0, err
	}
	if typedJSON > scalarJSON {
		output, e := nativeTypedCheckedMul(typedJSON-scalarJSON, 24)
		if e != nil {
			return 0, e
		}
		err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(output))
	}
	return total, err
}

func (r *NativeJSONRead) reconstructPendingTyped(row *columnDeclaredRow, residual []byte) (result []byte, resultErr error) {
	if row == nil {
		return nil, tree.ErrKeyNotFound
	}
	if r.meta.Options.ColumnStore.RetainedPayload == ColumnRetainedPayloadFull {
		// The original delta already owns the complete document. Like the
		// maintained physical Full reader, output never reconstructs declared
		// fields or borrows their asset backing. This copy belongs to the caller.
		return bytes.Clone(residual), nil
	}
	if len(row.Values) != len(r.meta.Options.ColumnStore.Columns) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	var typedJSON uint64
	for i, col := range r.meta.Options.ColumnStore.Columns {
		_, n, err := nativeTypedValueCapacity(col, row.Values[i])
		if err != nil {
			return nil, err
		}
		if err = nativeTypedCheckedAdd(&typedJSON, n); err != nil {
			return nil, err
		}
	}
	charge, err := nativeTypedReconstructionCharge(r.meta, uint64(len(residual)), typedJSON)
	if err != nil {
		return nil, err
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, err
	}
	defer lease.Close()
	return reconstructColumnJSONDocument(*r.meta.Options.ColumnStore, residual, row.Values)
}

// The original captured domains are the only physical root authority. Callers
// admit this construction before allocating the catalog, map or qualified names.
func (r *NativeJSONRead) nativeTypedReadViewCharge() (uint64, error) {
	total := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(collectionCatalog{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(Collection{})))
	var resultErr error
	r.collection.domains.Scan(func(name string, _ *nativeDomainState) bool {
		n := uint64(len(r.meta.Name))
		if resultErr = nativeTypedCheckedAdd(&n, 1); resultErr == nil {
			resultErr = nativeTypedCheckedAdd(&n, uint64(len(name)))
		}
		if resultErr == nil {
			resultErr = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(n))
		}
		if resultErr == nil {
			resultErr = nativeTypedCheckedAdd(&total, preparedSemanticStreamMapEntryReserveBytes)
		}
		return resultErr == nil
	})
	if resultErr != nil {
		return 0, resultErr
	}
	indexes, err := nativeJSONIndexedPlanCharge(r.meta, nil, nil)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, indexes); err != nil {
		return 0, err
	}
	// newCollectionCatalog independently qualifies its four well-known roots.
	names := uint64(len(r.meta.Name))
	if err := nativeTypedCheckedAdd(&names, 32); err != nil {
		return 0, err
	}
	names, err = nativeTypedCheckedMul(memtable.COWAllocationCharge(names), 4)
	if err != nil {
		return 0, err
	}
	if err = nativeTypedCheckedAdd(&total, names); err != nil {
		return 0, err
	}
	return total, nil
}

func (r *NativeJSONRead) nativeTypedReadCollection() *Collection {
	roots := make(map[string]uint64)
	r.collection.domains.Scan(func(name string, d *nativeDomainState) bool { roots[r.meta.Name+"/"+name] = d.baseRoot; return true })
	return &Collection{db: r.database, meta: r.meta, name: r.meta.Name, catalog: newCollectionCatalog(r.meta, roots), nativeOperationLocalReads: true}
}

// reconstructTyped borrows only the original complete-cut catalog and basis.
// All decoder/vector/cache backing is operation-local and released after its
// final aliases. The final independent JSON result belongs to the API caller.
func (r *NativeJSONRead) reconstructTyped(id, value []byte) (result []byte, resultErr error) {
	if !nativeTypedMeta(r.meta) {
		return value, nil
	}
	if r.database == nil || r.collection == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	manifest, ok := r.collection.domains.Get("column/manifest")
	if !ok {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	physical, err := nativeTypedReadCapacity(r.meta, r.capture.Snapshot(), manifest.baseRoot)
	if err != nil {
		return nil, err
	}
	charge, err := r.nativeTypedReadViewCharge()
	if err != nil {
		return nil, err
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, err
	}
	// Inspect compressed string dictionary lengths under this original read
	// receipt before decoder/output allocation. Preflight owns no retained cache
	// or file alias, and releases its scratch before semantic decoding starts.
	extra, typedJSON, err := nativeTypedFamilyReadCapacity(r.meta, r.capture.Snapshot(), manifest.baseRoot, r.database.ColumnAssetRootDir(), nil, func(n uint64) error {
		if e := nativeTypedCheckedAdd(&n, charge); e != nil {
			return e
		}
		return lease.Resize(n)
	})
	if err == nil {
		err = lease.Resize(charge)
	}
	if err != nil {
		lease.Close()
		return nil, err
	}
	if err = nativeTypedCheckedAdd(&physical, extra); err != nil {
		lease.Close()
		return nil, err
	}
	jsonCharge, err := nativeTypedReconstructionCharge(r.meta, uint64(len(value)), typedJSON)
	if err != nil {
		lease.Close()
		return nil, err
	}
	jsonLease, err := r.capture.NativeAllocator().AdmitNativeBytes(jsonCharge)
	if err != nil {
		lease.Close()
		return nil, err
	}
	var wrapper *Collection
	var catalog *collectionCatalog
	var residual, block, blockKey []byte
	var entry node.LeafEntry
	var domain nativeDomainRead
	var scratch *nativeDomainReadScratch
	var outputLease, decodeLease, decoderLease, physicalLease *memtable.COWExternalLease
	defer func() {
		block, blockKey = nil, nil
		entry = node.LeafEntry{}
		domain = nativeDomainRead{}
		scratch = nil
		catalog = nil
		wrapper = nil
		residual = nil
		value = nil
		if decoderLease != nil {
			decoderLease.Close()
		}
		if physicalLease != nil {
			physicalLease.Close()
		}
		if decodeLease != nil {
			decodeLease.Close()
		}
		if outputLease != nil {
			outputLease.Close()
		}
		jsonLease.Close()
		lease.Close()
	}()
	wrapper = r.nativeTypedReadCollection()
	catalog = wrapper.catalog
	residual = value
	var row uint64
	var locator bool
	if columnStoreRetainedPayloadUsesSemanticStreamV1(r.meta.Options.ColumnStore) {
		blockKey, row, locator, err = parseColumnRetainedSemanticStreamV1Locator(value)
		if err != nil {
			return nil, err
		}
	}
	if locator {
		source, ok := r.collection.domains.Get("retained/semantic-stream-v1")
		if !ok {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		domain, err = newNativeDomainRead(source, r.capture.Snapshot())
		if err != nil {
			return nil, err
		}
		scratch, err = r.physicalScratch()
		if err != nil {
			return nil, err
		}
		entry, err = domain.getEntry(blockKey, scratch, r.readLeaf)
		if err != nil {
			return nil, err
		}
		if entry.Flags&node.FlagTombstone != 0 {
			return nil, tree.ErrKeyNotFound
		}
		block = entry.Value
		if entry.Flags&node.FlagPointer != 0 {
			block, err = r.readValue(entry.ValuePtr, func(n uint64) error {
				var e error
				outputLease, e = r.capture.NativeAllocator().AdmitNativeBytes(n)
				return e
			})
			if err != nil {
				return nil, err
			}
		}
		raw := uint64(len(block))
		compressed := bytes.HasPrefix(block, columnRetainedSemanticStreamV1BlockZSTDMagic)
		if compressed {
			raw, _, err = readColumnRetainedSemanticStreamV1Uvarint(block, len(columnRetainedSemanticStreamV1BlockZSTDMagic), "zstd raw block length")
			if err != nil {
				return nil, err
			}
			if raw == 0 || raw > maxColumnRetainedSemanticStreamV1CompressedRawBlockBytes {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
		}
		amount, err := nativeTypedJSONReadCharge(raw)
		if err != nil {
			return nil, err
		}
		decodeLease, err = r.capture.NativeAllocator().AdmitNativeBytes(amount)
		if err != nil {
			return nil, err
		}
		if compressed {
			sizes, e := valuelog.COWDecoderRetentionSizes(0, raw, uint64(len(block)), max(raw, 1<<20))
			if e != nil {
				return nil, e
			}
			var decoderCharge uint64
			for _, n := range []uint64{sizes.OwnerWrapper, sizes.DefinitionCopy, sizes.RawBacking, sizes.InputBacking, sizes.SequenceBacking, raw} {
				if err = nativeTypedCheckedAdd(&decoderCharge, memtable.COWAllocationCharge(n)); err != nil {
					return nil, err
				}
			}
			for i := 0; i < sizes.FixedAllocationCount; i++ {
				if err = nativeTypedCheckedAdd(&decoderCharge, memtable.COWAllocationCharge(sizes.FixedAllocationBytes)); err != nil {
					return nil, err
				}
			}
			decoderLease, err = r.capture.NativeAllocator().AdmitNativeBytes(decoderCharge)
			if err != nil {
				return nil, err
			}
		}
		residual, err = decodeColumnRetainedSemanticStreamV1BlockRowJSON(block, row)
		block = nil
		entry = node.LeafEntry{}
		domain = nativeDomainRead{}
		scratch = nil
		blockKey = nil
		if decoderLease != nil {
			decoderLease.Close()
			decoderLease = nil
		}
		if err != nil {
			return nil, err
		}
	}
	physicalLease, err = r.capture.NativeAllocator().AdmitNativeBytes(physical)
	if err != nil {
		return nil, err
	}
	// Resolver sees an already resolved residual JSON object, so it cannot
	// allocate/decode the stream a second time through an unadmitted snapshot Get.
	result, err = (&Collection{db: r.database, nativeOperationLocalReads: true}).reconstructColumnDocumentAtSnapshot(r.capture.Snapshot(), catalog, id, residual)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("collections: missing typed reconstruction")
	}
	return result, nil
}

// Compare exact retained bytes and authoritative declared values at this SAME
// captured cut. Never round-trip typed values through reconstructed JSON.
func (r *NativeJSONRead) typedRowEqual(id, retained []byte, wanted []columnDeclaredValue) (found, equal bool, resultErr error) {
	if r.domain.domain == nil {
		return false, false, nil
	}
	if row, old, ok := r.domain.domain.typed.find(id); ok {
		if row == nil {
			return false, false, nil
		}
		if !bytes.Equal(bytes.TrimSpace(old), retained) {
			return true, false, nil
		}
		equal, err := typedDeclaredValuesEqual(row.Values, wanted)
		return true, equal, err
	}
	entry, _, err := r.lookupEntry(id, true)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return false, false, nil
	}
	value := entry.Value
	var valueLease *memtable.COWExternalLease
	defer func() {
		value = nil
		entry = node.LeafEntry{}
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
			return false, false, err
		}
	}
	if !bytes.Equal(bytes.TrimSpace(value), retained) {
		return true, false, nil
	}
	manifest, ok := r.collection.domains.Get("column/manifest")
	if !ok {
		return false, false, backenddb.ErrNativeSemanticUnsupported
	}
	physical, err := nativeTypedReadCapacity(r.meta, r.capture.Snapshot(), manifest.baseRoot)
	if err != nil {
		return false, false, err
	}
	header, err := r.nativeTypedReadViewCharge()
	if err != nil {
		return false, false, err
	}
	if err = nativeTypedCheckedAdd(&physical, header); err != nil {
		return false, false, err
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(physical)
	if err != nil {
		return false, false, err
	}
	var wrapper *Collection
	var catalog *collectionCatalog
	var view *CollectionReadView
	defer func() {
		if view != nil {
			resultErr = errors.Join(resultErr, view.Close())
		}
		view = nil
		wrapper = nil
		catalog = nil
		lease.Close()
	}()
	extra, _, err := nativeTypedFamilyReadCapacity(r.meta, r.capture.Snapshot(), manifest.baseRoot, r.database.ColumnAssetRootDir(), nil, func(n uint64) error {
		if e := nativeTypedCheckedAdd(&n, header); e != nil {
			return e
		}
		return lease.Resize(max(physical, n))
	})
	if err != nil {
		return false, false, err
	}
	if err = nativeTypedCheckedAdd(&physical, extra); err != nil {
		return false, false, err
	}
	if err = lease.Resize(physical); err != nil {
		return false, false, err
	}
	wrapper = r.nativeTypedReadCollection()
	catalog = wrapper.catalog
	view = newCollectionReadViewAtSnapshot(wrapper, r.capture.Snapshot(), catalog, false, "")
	equal, err = view.typedReplacementValuesEqual(id, wanted)
	return true, equal, err
}
