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

// The maintained top-level RawMessage reconstruction owns no retained codec or
// cache. One quota unit per input byte bounds map entries/control/growth even
// for the shortest valid member; encoded strings need at most six bytes/input.
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
// value byte. Keep the original residual/map bound and separately cover typed
// JSON output, escaping and old/replacement marshal backing.
func nativeTypedReconstructionCharge(residual, path, typedJSON uint64) (uint64, error) {
	if err := nativeTypedCheckedAdd(&residual, path); err != nil {
		return 0, err
	}
	if err := nativeTypedCheckedAdd(&residual, 20); err != nil {
		return 0, err
	}
	total, err := nativeTypedJSONReadCharge(residual)
	if err != nil {
		return 0, err
	}
	if typedJSON > 20 {
		output, e := nativeTypedCheckedMul(typedJSON-20, 24)
		if e != nil {
			return 0, e
		}
		err = nativeTypedCheckedAdd(&total, memtable.COWAllocationCharge(output))
	}
	return total, err
}

func (r *NativeJSONRead) reconstructPendingTyped(row *columnDeclaredRow, residual []byte) (result []byte, resultErr error) {
	col := r.meta.Options.ColumnStore.Columns[0]
	if row == nil {
		return nil, tree.ErrKeyNotFound
	}
	if len(row.Values) != 1 {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	_, typedJSON, err := nativeTypedValueCapacity(col, row.Values[0])
	if err != nil {
		return nil, err
	}
	charge, err := nativeTypedReconstructionCharge(uint64(len(residual)), uint64(len(col.Path)), typedJSON)
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
	physical, err := nativeTypedReadCapacity(r.capture.Snapshot(), manifest.baseRoot)
	if err != nil {
		return nil, err
	}
	// At most four root entries use the maintained per-entry quota. Catalog wrapper,
	// Collection wrapper and independently qualified root strings are distinct.
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(collectionCatalog{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(Collection{}))) + 4*preparedSemanticStreamMapEntryReserveBytes
	r.collection.domains.Scan(func(name string, _ *nativeDomainState) bool {
		charge += memtable.COWAllocationCharge(uint64(len(r.meta.Name) + 1 + len(name)))
		return true
	})
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, err
	}
	// Inspect compressed string dictionary lengths under this original read
	// receipt before decoder/output allocation. Preflight owns no retained cache
	// or file alias, and releases its scratch before semantic decoding starts.
	extra, typedJSON, err := nativeTypedFamilyReadCapacity(r.meta, r.capture.Snapshot(), manifest.baseRoot, r.database.ColumnAssetRootDir(), func(n uint64) error {
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
	jsonCharge, err := nativeTypedReconstructionCharge(uint64(len(value)), uint64(len(r.meta.Options.ColumnStore.Columns[0].Path)), typedJSON)
	if err != nil {
		lease.Close()
		return nil, err
	}
	jsonLease, err := r.capture.NativeAllocator().AdmitNativeBytes(jsonCharge)
	if err != nil {
		lease.Close()
		return nil, err
	}
	var roots map[string]uint64
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
		roots = nil
		catalog = nil
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
	roots = make(map[string]uint64, 4)
	r.collection.domains.Scan(func(name string, d *nativeDomainState) bool { roots[r.meta.Name+"/"+name] = d.baseRoot; return true })
	catalog = newCollectionCatalog(r.meta, roots)
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
	physical, err := nativeTypedReadCapacity(r.capture.Snapshot(), manifest.baseRoot)
	if err != nil {
		return false, false, err
	}
	header := uint64(8 * preparedSemanticStreamMapEntryReserveBytes)
	r.collection.domains.Scan(func(name string, _ *nativeDomainState) bool {
		n := uint64(len(r.meta.Name))
		if err = nativeTypedCheckedAdd(&n, 1); err == nil {
			err = nativeTypedCheckedAdd(&n, uint64(len(name)))
		}
		if err == nil {
			err = nativeTypedCheckedAdd(&header, memtable.COWAllocationCharge(n))
		}
		return err == nil
	})
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
	var roots map[string]uint64
	var catalog *collectionCatalog
	var view *CollectionReadView
	defer func() {
		if view != nil {
			resultErr = errors.Join(resultErr, view.Close())
		}
		view = nil
		catalog = nil
		roots = nil
		lease.Close()
	}()
	extra, _, err := nativeTypedFamilyReadCapacity(r.meta, r.capture.Snapshot(), manifest.baseRoot, r.database.ColumnAssetRootDir(), func(n uint64) error {
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
	roots = make(map[string]uint64, 4)
	r.collection.domains.Scan(func(name string, d *nativeDomainState) bool { roots[r.meta.Name+"/"+name] = d.baseRoot; return true })
	catalog = newCollectionCatalog(r.meta, roots)
	view = newCollectionReadViewAtSnapshot(&Collection{db: r.database, nativeOperationLocalReads: true}, r.capture.Snapshot(), catalog, false, "")
	equal, err = view.typedReplacementValuesEqual(id, wanted)
	return true, equal, err
}
