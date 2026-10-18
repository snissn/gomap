package collections

import (
	"bytes"
	"errors"
	"sync"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// NativeJSONRead owns one exact complete-cut capture. It exposes no mutable
// legacy handle; returned values are copied before the captured scratch is reused.
type NativeJSONRead struct {
	meta          CollectionMeta
	metadataLease *memtable.COWExternalLease
	mu            sync.Mutex
	capture       backenddb.NativeSemanticCapture
	domain        nativeDomainRead
	privateDomain *nativeDomainState
	scratch       *nativeDomainReadScratch
	scratchLease  *memtable.COWExternalLease
	lease         *memtable.COWExternalLease
	failure       error
}

func (m *NativeJSONManager) Capture(name string) (result *NativeJSONRead, resultErr error) {
	if err := ValidateCollectionName(name); err != nil {
		return nil, err
	}
	if m == nil || m.db == nil {
		return nil, backenddb.ErrClosed
	}
	capture, err := m.db.CaptureNativeSemanticState()
	if err != nil {
		return nil, err
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(NativeJSONRead{})))
	for _, bytes := range valuelog.COWInspectionMetadataAllocationSizes() {
		charge += memtable.COWAllocationCharge(bytes)
	}
	lease, err := capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, errors.Join(err, capture.Close())
	}
	r := &NativeJSONRead{capture: capture, lease: lease}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, r.Close())
		}
	}()
	base, ok := capture.State().(*nativeEngineVersion)
	if capture.State() != nil && !ok {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if base == nil {
		token, ok := capture.Snapshot().StateToken()
		if !ok {
			return nil, backenddb.ErrClosed
		}
		if token.SystemRootPageID == 0 {
			return nil, tree.ErrKeyNotFound
		}
		if _, err := r.physicalScratch(); err != nil {
			return nil, err
		}
		reader, err := capture.Snapshot().NativeReaderAtRoot(token.SystemRootPageID)
		if err != nil {
			return nil, err
		}
		entry, err := reader.GetEntryExactWithFixedScratch([]byte(systemCollectionMetaKey(name)), r.scratch.key[:], r.scratch.leaf[:], r.readLeaf)
		if err != nil {
			return nil, err
		}
		if entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 || bytes.ContainsAny(entry.Value, "[]") || bytes.Contains(entry.Value, []byte("column_store")) {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		if uint64(len(entry.Value)) > (^uint64(0)-uint64(unsafe.Sizeof(CollectionMeta{})))/16 {
			return nil, memtable.ErrCOWCapacity
		}
		transient, err := capture.NativeAllocator().AdmitNativeBytes(memtable.COWAllocationCharge(uint64(len(entry.Value))*16 + uint64(unsafe.Sizeof(CollectionMeta{}))))
		if err != nil {
			return nil, err
		}
		meta, err := decodeCollectionMeta(entry.Value)
		if err == nil {
			meta, err = nativeJSONMeta(meta)
		}
		entry = node.LeafEntry{}
		if err != nil {
			meta = CollectionMeta{}
			transient.Close()
			return nil, err
		}
		if meta.Name != name {
			meta = CollectionMeta{}
			transient.Close()
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		policy, err := collectionRootStoragePolicy(meta, collectionPrimaryRootName(name))
		r.meta, r.metadataLease = meta, transient
		meta = CollectionMeta{}
		if err != nil {
			return nil, err
		}
		overlay, overlayErr := reader.GetEntryExactWithFixedScratch([]byte(systemCollectionRootOverlayKey(collectionPrimaryRootName(name))), r.scratch.key[:], r.scratch.leaf[:], r.readLeaf)
		if overlayErr == nil && overlay.Flags&node.FlagTombstone == 0 {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		if overlayErr != nil && !errors.Is(overlayErr, tree.ErrKeyNotFound) {
			return nil, overlayErr
		}
		overlay = node.LeafEntry{}
		entry, err = reader.GetEntryExactWithFixedScratch([]byte(systemCollectionRootKey(collectionPrimaryRootName(name))), r.scratch.key[:], r.scratch.leaf[:], r.readLeaf)
		root := uint64(0)
		if err == nil {
			if entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
			root, err = decodeRootID(entry.Value)
		}
		if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
			return nil, err
		}
		entry = node.LeafEntry{}
		admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return capture.NativeAllocator().AdmitNativeBytes(n) }
		r.privateDomain, err = newNativeDomainState("primary", root, 1, policy, nil, admit)
		if err != nil {
			return nil, err
		}
		r.domain, err = newNativeDomainRead(r.privateDomain, capture.Snapshot())
		if err != nil {
			return nil, err
		}
		return r, nil
	}

	c, ok := base.names.Get(name)
	if !ok {
		return nil, tree.ErrKeyNotFound
	}
	r.meta = *c.schema.meta
	d, ok := c.domains.Get("primary")
	if !ok {
		return r, nil
	} // newly created collection has no rows or physical root
	r.domain, err = newNativeDomainRead(d, capture.Snapshot())
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (m *NativeJSONManager) Get(collection string, id []byte) (value []byte, resultErr error) {
	r, err := m.Capture(collection)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, r.Close()) }()
	return r.Get(id)
}

// physicalScratch is private to this read scope and serialized by its owner.
// Borrowed txn/import probes retain their original capture; only scratch owns
// this independent receipt. No page storage is needed for a logical-source hit.
func (r *NativeJSONRead) physicalScratch() (*nativeDomainReadScratch, error) {
	if r.scratch == nil {
		lease, err := r.capture.NativeAllocator().AdmitNativeBytes(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDomainReadScratch{}))))
		if err != nil {
			return nil, err
		}
		r.scratch = &nativeDomainReadScratch{}
		r.scratchLease = lease
	}
	return r.scratch, nil
}

func (r *NativeJSONRead) getEntry(id []byte) (node.LeafEntry, error) {
	entry, resolved, err := r.domain.memoryEntry(id)
	if resolved {
		return entry, err
	}
	scratch, err := r.physicalScratch()
	if err != nil {
		return node.LeafEntry{}, err
	}
	return r.domain.baseEntry(id, scratch, r.readLeaf)
}

// detachScratch ends every scope alias before the independent receipt refunds.
// It never closes a borrowed capture, including the original P1 txn core.
func (r *NativeJSONRead) detachScratch() *memtable.COWExternalLease {
	r.scratch = nil
	lease := r.scratchLease
	r.scratchLease = nil
	return lease
}

func (r *NativeJSONRead) Get(id []byte) ([]byte, error) {
	if r == nil {
		return nil, backenddb.ErrClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.capture == nil || r.failure != nil {
		return nil, backenddb.ErrClosed
	}
	if r.domain.domain == nil {
		return nil, tree.ErrKeyNotFound
	}
	entry, err := r.getEntry(id)
	if err != nil {
		return nil, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, tree.ErrKeyNotFound
	}
	if entry.Flags&node.FlagPointer != 0 {
		return r.readValue(entry.ValuePtr)
	}
	// The result belongs to the caller, not to retained database accounting.
	value := make([]byte, len(entry.Value))
	copy(value, entry.Value)
	return value, nil
}

// readValue borrows only this read scope's exact Snapshot file Set. Get holds
// r.mu, so Close cannot detach that Snapshot until decode and scratch cleanup
// finish. Returned bytes are an independent caller-owned copy.
func (r *NativeJSONRead) readValue(ptr page.ValuePtr) ([]byte, error) {
	f, _, err := r.capture.Snapshot().PinnedValueLogFile(ptr.FileID)
	if err != nil {
		return nil, err
	}
	limit := r.capture.NativeAllocator().Limits().MaxInFlightBytes
	shape, err := valuelog.InspectCOWRecord(f, ptr, valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
	if err != nil {
		return nil, err
	}
	var sizes valuelog.COWDecoderAllocationSizes
	if shape.Compressed {
		if shape.DictID != 0 || (shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4) {
			return nil, valuelog.ErrCOWReadUnsupported
		}
		sizes, err = valuelog.COWBlockDecoderRetentionSizes(shape.Codec, shape.RawBytes, shape.PayloadBytes())
		if err != nil {
			return nil, err
		}
	}
	charge := uint64(0)
	for _, bytes := range [4]uint64{shape.PayloadBytes(), shape.RawBytes, shape.ValueBytes, sizes.OwnerWrapper} {
		part := memtable.COWAllocationCharge(bytes)
		if part > ^uint64(0)-charge {
			return nil, memtable.ErrCOWCapacity
		}
		charge += part
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, err
	}
	input, raw, output := make([]byte, shape.PayloadBytes()), make([]byte, shape.RawBytes), make([]byte, shape.ValueBytes)
	var decoder *valuelog.COWDecoder
	var decode valuelog.COWDecodeFunc
	var value []byte
	defer func() {
		if decoder != nil {
			decoder.Close()
			decoder = nil
		}
		decode = nil
		input, raw, output, value = nil, nil, nil, nil
		lease.Close()
	}()
	if shape.Compressed {
		decoder, err = valuelog.NewCOWBlockDecoder(shape.Codec, shape.RawBytes, shape.PayloadBytes(), func(actual valuelog.COWDecoderAllocationSizes) error {
			if actual != sizes {
				return memtable.ErrCOWCapacity
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		decode = decoder.Decode
	}
	value, err = valuelog.ReadCOWRecord(f, ptr, shape, true, input, raw, output, decode)
	if err != nil {
		return nil, err
	}
	owned := make([]byte, len(value))
	copy(owned, value)
	return owned, nil
}

func (r *NativeJSONRead) readLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	f, _, err := r.capture.Snapshot().PinnedValueLogFile(ptr.ValuePtr().FileID)
	if err != nil {
		return nil, err
	}
	limit := r.capture.NativeAllocator().Limits().MaxInFlightBytes
	shape, err := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
	if err != nil {
		return nil, err
	}
	var sizes valuelog.COWDecoderAllocationSizes
	if shape.Compressed {
		if shape.DictID != 0 {
			return nil, valuelog.ErrCOWReadUnsupported
		}
		sizes, err = valuelog.COWBlockDecoderRetentionSizes(shape.Codec, shape.RawBytes, shape.PayloadBytes())
		if err != nil {
			return nil, err
		}
	}
	charge := memtable.COWAllocationCharge(shape.PayloadBytes())
	for _, bytes := range []uint64{shape.RawBytes, sizes.OwnerWrapper} {
		part := memtable.COWAllocationCharge(bytes)
		if part > ^uint64(0)-charge {
			return nil, memtable.ErrCOWCapacity
		}
		charge += part
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, err
	}
	input, raw := make([]byte, shape.PayloadBytes()), make([]byte, shape.RawBytes)
	var decoder *valuelog.COWDecoder
	var decode valuelog.COWDecodeFunc
	defer func() {
		if decoder != nil {
			decoder.Close()
			decoder = nil
		}
		decode = nil
		input, raw = nil, nil
		lease.Close()
	}()
	if shape.Compressed {
		decoder, err = valuelog.NewCOWBlockDecoder(shape.Codec, shape.RawBytes, shape.PayloadBytes(), func(actual valuelog.COWDecoderAllocationSizes) error {
			if actual != sizes {
				return memtable.ErrCOWCapacity
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		decode = decoder.Decode
	}
	return valuelog.ReadCOWLeafPage(f, ptr.ValuePtr(), shape, true, input, raw, dst, decode)
}

func (r *NativeJSONRead) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.failure != nil {
		err := r.failure
		r.mu.Unlock()
		return err
	}
	if r.capture == nil {
		r.mu.Unlock()
		return nil
	}
	r.domain = nativeDomainRead{}
	scratchLease := r.detachScratch()
	r.meta = CollectionMeta{}
	metadataLease := r.metadataLease
	r.metadataLease = nil
	var err error
	if r.privateDomain != nil {
		done, releaseErr := (nativeDomainLifetime{}).Release(r.privateDomain)
		r.privateDomain = nil
		done.Complete()
		err = releaseErr
	}
	err = errors.Join(err, r.capture.Close())
	r.failure = err
	r.capture = nil
	lease := r.lease
	r.lease = nil
	r.mu.Unlock()
	r = nil
	if scratchLease != nil {
		scratchLease.Close()
	}
	if metadataLease != nil {
		metadataLease.Close()
	}
	lease.Close()
	return err
}
