package collections

import (
	"errors"
	"sync"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
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
	database      *backenddb.DB
	metadataLease *memtable.COWExternalLease
	mu            sync.Mutex
	visits        uint32
	closing       bool
	capture       backenddb.NativeSemanticCapture
	domain        nativeDomainRead
	collection    *nativeCollectionState
	privateEngine *nativeEngineVersion
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
	r := &NativeJSONRead{capture: capture, lease: lease, database: m.db}
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
		r.privateEngine, err = importNativeJSONCatalog(capture)
		if err != nil {
			return nil, err
		}
		base = r.privateEngine
	}

	c, ok := base.names.Get(name)
	if !ok {
		return nil, tree.ErrKeyNotFound
	}
	r.meta = *c.schema.meta
	r.collection = c
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
	entry, _, err := r.lookupEntry(id, false)
	return entry, err
}

func (r *NativeJSONRead) lookupEntry(id []byte, copyValue bool) (node.LeafEntry, bool, error) {
	entry, resolved, err := r.domain.memoryEntry(id)
	if resolved {
		return entry, false, err
	}
	if physical, ok := r.capture.(backenddb.NativePhysicalReader); ok {
		entry, err = physical.NativeEntryAtRoot(r.domain.domain.baseRoot, id, copyValue)
		return entry, copyValue, err
	}
	scratch, err := r.physicalScratch()
	if err != nil {
		return node.LeafEntry{}, false, err
	}
	entry, err = r.domain.baseEntry(id, scratch, r.readLeaf)
	return entry, false, err
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
	return r.getAdmitted(id, nil)
}

// Internal ordinary readers retain their independent output on the enclosing
// operation's original receipt. Public Get keeps its caller-owned output contract.
func (r *NativeJSONRead) getAdmitted(id []byte, admitOutput func(uint64) error) ([]byte, error) {
	if r == nil {
		return nil, backenddb.ErrClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.capture == nil || r.failure != nil || r.closing {
		return nil, backenddb.ErrClosed
	}
	typed := nativeTypedMeta(r.meta)
	if value, handled, err := r.pendingTypedValueLocked(id, typed, admitOutput); handled {
		return value, err
	}
	if r.domain.domain == nil {
		return nil, tree.ErrKeyNotFound
	}
	var entry node.LeafEntry
	var owned bool
	var err error
	if admitOutput == nil {
		entry, owned, err = r.lookupEntry(id, true)
	} else {
		// The shared physical point reader deliberately returns no inline
		// bytes without copy-out. Borrow the existing domain/page scratch
		// instead, so original admission precedes the independent output.
		var resolved bool
		entry, resolved, err = r.domain.memoryEntry(id)
		if !resolved {
			var scratch *nativeDomainReadScratch
			scratch, err = r.physicalScratch()
			if err == nil {
				entry, err = r.domain.baseEntry(id, scratch, r.readLeaf)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	return r.materializeEntryLocked(id, entry, owned, admitOutput, typed && r.meta.Options.ColumnStore.RetainedPayload != ColumnRetainedPayloadFull)
}

// pendingTypedValueLocked preserves the point-read fast path before any
// physical lookup, and is also used for a selected scan entry. A handled result
// includes an eligibility refusal or pending tombstone; neither may fall through.
func (r *NativeJSONRead) pendingTypedValueLocked(id []byte, typed bool, admitOutput func(uint64) error) ([]byte, bool, error) {
	if typed && admitOutput != nil && r.meta.Options.ColumnStore.RetainedPayload != ColumnRetainedPayloadFull {
		return nil, true, backenddb.ErrNativeSemanticUnsupported
	}
	if typed && r.domain.domain != nil {
		if row, residual, ok := r.domain.domain.typed.find(id); ok {
			if admitOutput != nil && row != nil {
				if err := admitOutput(memtable.COWAllocationCharge(uint64(len(residual)))); err != nil {
					return nil, true, err
				}
			}
			value, err := r.reconstructPendingTyped(row, residual)
			return value, true, err
		}
	}
	return nil, false, nil
}

// materializeEntryLocked resolves an already selected entry under this read's
// original mutex/capture. Inline bytes and pointer output become independently
// owned before the caller can move its cursor or unlock for a callback.
func (r *NativeJSONRead) materializeEntryLocked(id []byte, entry node.LeafEntry, owned bool, admitOutput func(uint64) error, reconstruct bool) ([]byte, error) {
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, tree.ErrKeyNotFound
	}
	// Full retains the original whole JSON in primary storage. Preserve the
	// ordinary owned pointer/inline output path without touching typed assets.
	if entry.Flags&node.FlagPointer != 0 {
		value, err := r.readValue(entry.ValuePtr, admitOutput)
		if err != nil {
			return nil, err
		}
		if reconstruct {
			return r.reconstructTyped(id, value)
		}
		return value, nil
	}
	if reconstruct {
		return r.reconstructTyped(id, entry.Value)
	}
	if owned {
		return entry.Value, nil
	}
	if admitOutput != nil {
		if err := admitOutput(memtable.COWAllocationCharge(uint64(len(entry.Value)))); err != nil {
			return nil, err
		}
	}
	// Public nil-admission output belongs to the caller. Internal copies remain
	// admitted until their enclosing consumer releases the original receipt.
	value := make([]byte, len(entry.Value))
	copy(value, entry.Value)
	return value, nil
}

// readValue borrows only this read scope's exact Snapshot file Set. Get holds
// r.mu, so Close cannot detach that Snapshot until decode and scratch cleanup
// finish. Returned bytes are an independent caller-owned copy.
func (r *NativeJSONRead) readValue(ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error) {
	if physical, ok := r.capture.(backenddb.NativePhysicalValueReader); ok {
		return physical.NativeValueAtPointer(ptr, admitOutput)
	}
	// Capture implementations without shared physical decoding retain their
	// original scoped buffers and Snappy/LZ4-only, dictionary-free eligibility.
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
	// Public Get returns caller-owned memory. An internal retained consumer must
	// instead admit this independent output before allocation, while decoder
	// temporaries remain owned by the original scoped read receipt.
	if admitOutput != nil {
		if err = admitOutput(memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
			return nil, err
		}
	}
	owned := make([]byte, len(value))
	copy(owned, value)
	return owned, nil
}

func (r *NativeJSONRead) readLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	if r.capture == nil || r.closing || r.failure != nil {
		return nil, backenddb.ErrClosed
	}
	// This scope already retains its exact immutable capture. Reuse only the
	// decoded page still resident in its exclusively admitted point scratch.
	// Importers and iterators also call this reader with OTHER buffers: every
	// such call invalidates the one-page identity before it can write anything.
	// Tree/PRIMARY checksum, type and operand validation still run on each use.
	scratch := r.scratch
	canonical := scratch != nil && cap(dst) == page.PageSize && &dst[:page.PageSize][0] == &scratch.leaf[0]
	if canonical && scratch.leafValid && scratch.leafRef == ptr {
		return scratch.leaf[:], nil
	}
	if scratch != nil {
		scratch.leafValid = false
	}
	data, err := r.loadLeaf(ptr, dst)
	if err == nil && canonical && len(data) == page.PageSize && cap(data) == page.PageSize && &data[0] == &scratch.leaf[0] {
		scratch.leafRef, scratch.leafValid = ptr, true
	}
	return data, err
}

func (r *NativeJSONRead) loadLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	if physical, ok := r.capture.(backenddb.NativePhysicalLeafReader); ok {
		return physical.NativeLeafPage(ptr, dst)
	}
	return readNativeSnapshotLeaf(r.capture.Snapshot(), r.capture.NativeAllocator(), ptr, dst)
}

// The exact enclosing Snapshot and original allocator are borrowed only through
// this synchronous read. Maintenance descriptor reads reuse the same decoder,
// admission and integrity policy without constructing another capture authority.
func readNativeSnapshotLeaf(snapshot *backenddb.Snapshot, allocator backenddb.NativeSemanticAllocator, ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	return readNativeSnapshotLeafWithAdmission(snapshot, allocator.Limits().MaxInFlightBytes, func(charge uint64) (func(), error) {
		lease, err := allocator.AdmitNativeBytes(charge)
		if err != nil {
			return nil, err
		}
		return lease.Close, nil
	}, ptr, dst)
}

// Internal synchronous admission may spend a containing preadmitted build
// workspace. It cannot reenter application code or outlive this read.
func readNativeSnapshotLeafWithAdmission(snapshot *backenddb.Snapshot, limit uint64, admit func(uint64) (func(), error), ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	f, _, err := snapshot.PinnedValueLogFile(ptr.ValuePtr().FileID)
	if err != nil {
		return nil, err
	}
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
	finish, err := admit(charge)
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
		finish()
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
	// A visitor releases the read mutex while invoking user code. Its original
	// scope keeps the capture and containing receipt until that operation ends;
	// Close invalidates new reads now and the last visitor performs disposal.
	if r.visits != 0 {
		r.closing = true
		r.mu.Unlock()
		return nil
	}
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
	r.database = nil
	metadataLease := r.metadataLease
	r.metadataLease = nil
	var err error
	r.collection = nil
	if r.privateEngine != nil {
		record := r.privateEngine.Release()
		r.privateEngine = nil
		err = finishPrivateNative(record)
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
