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
func (r *NativeJSONRead) reconstructPendingTyped(row *columnDeclaredRow, residual []byte) (result []byte, resultErr error) {
	charge, err := nativeTypedJSONReadCharge(uint64(len(residual)) + 20 + uint64(len(r.meta.Options.ColumnStore.Columns[0].Path)))
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
	// Four root entries use the maintained per-entry quota. Catalog wrapper,
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
	jsonCharge, err := nativeTypedJSONReadCharge(uint64(len(value)) + uint64(len(r.meta.Options.ColumnStore.Columns[0].Path)) + 20)
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
	blockKey, row, locator, err = parseColumnRetainedSemanticStreamV1Locator(value)
	if err != nil {
		return nil, err
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
