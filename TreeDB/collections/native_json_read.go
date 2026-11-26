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
	// engine borrows the resolved immutable catalog from capture or privateEngine.
	// Only private successor preparation may produce mutation/publication state.
	engine        *nativeEngineVersion
	privateEngine *nativeEngineVersion
	scratch       *nativeDomainReadScratch
	scratchLease  *memtable.COWExternalLease
	lease         *memtable.COWExternalLease
	failure       error
	oneShot       bool
}

type nativePhysicalEntryIntoReader interface {
	NativeEntryAtRootInto(root uint64, key, dst []byte) (node.LeafEntry, error)
}

type nativePhysicalValueIntoReader interface {
	NativeValueAtPointerInto(ptr page.ValuePtr, dst []byte, admitOutput func(uint64) error) ([]byte, error)
}

func (m *NativeJSONManager) Capture(name string) (result *NativeJSONRead, resultErr error) {
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(NativeJSONRead{})))
	for _, bytes := range valuelog.COWInspectionMetadataAllocationSizes() {
		charge += memtable.COWAllocationCharge(bytes)
	}
	capture, lease, err := m.captureNativeCatalog(name, charge)
	if err != nil {
		return nil, err
	}
	r := &NativeJSONRead{capture: capture, lease: lease, database: m.db}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, r.Close())
		}
	}()
	r.engine, r.collection, r.privateEngine, err = resolveNativeCatalog(capture, name)
	if err != nil {
		return nil, err
	}
	c := r.collection
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

// Static adapter avoids a newly allocated callback closure on every capture.
// Successful import transfers its original reference to the retained cut. On
// error its partial engine returns to the caller's original private cleanup.
func nativeJSONCatalogProjection(capture backenddb.NativeSemanticCapture) (backenddb.NativeSemanticState, error) {
	engine, err := importNativeJSONCatalog(capture)
	if engine == nil {
		return nil, err
	}
	return engine, err
}

func (m *NativeJSONManager) Get(collection string, id []byte) ([]byte, error) {
	return m.getInto(collection, id, nil)
}

// getInto performs a streamlined one-shot point lookup against one pinned
// immutable cut without heap-allocating a NativeJSONRead handle or copying
// CollectionMeta when no reentrant read scope escapes to the caller.
func (m *NativeJSONManager) getInto(collection string, id, dst []byte) (value []byte, resultErr error) {
	value, _, resultErr = m.getIntoWithFormat(collection, id, dst)
	return value, resultErr
}

// getIntoWithFormat performs a one-shot point lookup and also returns the
// collection's DocumentFormat from the exact same pinned cut.
func (m *NativeJSONManager) getIntoWithFormat(collection string, id, dst []byte) (value []byte, format DocumentFormat, resultErr error) {
	if err := ValidateCollectionName(collection); err != nil {
		return nil, "", err
	}
	if m == nil || m.db == nil {
		return nil, "", backenddb.ErrClosed
	}
	if cut, err := m.db.BorrowNativeSemanticCut(); err == nil {
		val, fmtOut, handled, readErr := m.getIntoBorrowedCut(cut, collection, id, dst)
		if handled {
			return val, fmtOut, readErr
		}
	} else if errors.Is(err, backenddb.ErrClosed) {
		return nil, "", err
	}
	return m.getIntoCapture(collection, id, dst)
}

func (m *NativeJSONManager) getIntoBorrowedCut(cut backenddb.NativeSemanticBorrowedCut, collection string, id, dst []byte) (value []byte, format DocumentFormat, handled bool, resultErr error) {
	engine, ok := cut.State().(*nativeEngineVersion)
	if !ok || engine == nil {
		_ = cut.Release()
		return nil, "", false, nil
	}
	col, ok := engine.names.Get(collection)
	if !ok {
		return nil, "", true, errors.Join(tree.ErrKeyNotFound, cut.Release())
	}
	meta := col.schema.meta
	format = meta.Options.DocumentFormat
	typed := nativeTypedMeta(*meta)
	if typed && meta.Options.ColumnStore.RetainedPayload != ColumnRetainedPayloadFull {
		_ = cut.Release()
		return nil, "", false, nil
	}
	d, ok := col.domains.Get("primary")
	if !ok {
		return nil, format, true, errors.Join(tree.ErrKeyNotFound, cut.Release())
	}
	if d == nil || d.refs.Load() <= 0 {
		return nil, format, true, errors.Join(backenddb.ErrNativeSemanticUnsupported, cut.Release())
	}
	if typed {
		if row, residual, found := d.typed.find(id); found {
			if row == nil {
				return nil, format, true, errors.Join(tree.ErrKeyNotFound, cut.Release())
			}
			var out []byte
			if dst != nil {
				out = append(dst[:0], residual...)
			} else {
				out = make([]byte, len(residual))
				copy(out, residual)
			}
			return out, format, true, cut.Release()
		}
	}
	var entry node.LeafEntry
	var resolved bool
	if d.pending != nil {
		if row, found := d.pending.Get(id); found {
			entry, resolved = nativeDomainRecordEntry(row), true
		}
	}
	if !resolved {
		if row, found := d.captured.get(id); found {
			entry, resolved = nativeDomainRecordEntry(row), true
		}
	}
	owned := false
	copiedToDst := false
	if !resolved {
		if d.baseRoot == 0 {
			return nil, format, true, errors.Join(tree.ErrKeyNotFound, cut.Release())
		}
		var err error
		if dst != nil {
			entry, err = cut.NativeEntryAtRootInto(d.baseRoot, id, dst)
			copiedToDst = true
		} else {
			entry, err = cut.NativeEntryAtRoot(d.baseRoot, id, true)
		}
		if err != nil {
			return nil, format, true, errors.Join(err, cut.Release())
		}
		owned = true
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, format, true, errors.Join(tree.ErrKeyNotFound, cut.Release())
	}
	if entry.Flags&node.FlagPointer != 0 {
		var val []byte
		var err error
		if dst != nil {
			val, err = cut.NativeValueAtPointerInto(entry.ValuePtr, dst, nil)
		} else {
			val, err = cut.NativeValueAtPointer(entry.ValuePtr, nil)
		}
		if err != nil {
			return nil, format, true, errors.Join(err, cut.Release())
		}
		return val, format, true, cut.Release()
	}
	if copiedToDst {
		return entry.Value, format, true, cut.Release()
	}
	if dst != nil {
		out := append(dst[:0], entry.Value...)
		return out, format, true, cut.Release()
	}
	if owned {
		return entry.Value, format, true, cut.Release()
	}
	out := make([]byte, len(entry.Value))
	copy(out, entry.Value)
	return out, format, true, cut.Release()
}

func (m *NativeJSONManager) getIntoCapture(collection string, id, dst []byte) (value []byte, format DocumentFormat, resultErr error) {
	capture, err := m.db.CaptureNativeSemanticState()
	if err != nil {
		return nil, "", err
	}
	var col *nativeCollectionState
	var privateEngine *nativeEngineVersion
	var scratchLease *memtable.COWExternalLease
	defer func() {
		col = nil
		resultErr = errors.Join(resultErr, closeNativeCatalog(capture, privateEngine))
		if scratchLease != nil {
			scratchLease.Close()
		}
	}()
	_, col, privateEngine, err = resolveNativeCatalog(capture, collection)
	if err != nil {
		return nil, "", err
	}
	meta := col.schema.meta
	format = meta.Options.DocumentFormat
	typed := nativeTypedMeta(*meta)
	reconstruct := typed && meta.Options.ColumnStore.RetainedPayload != ColumnRetainedPayloadFull
	d, ok := col.domains.Get("primary")
	if !ok {
		return nil, format, tree.ErrKeyNotFound
	}
	if d == nil || d.refs.Load() <= 0 {
		return nil, format, backenddb.ErrNativeSemanticUnsupported
	}
	if typed {
		if row, residual, found := d.typed.find(id); found {
			if row == nil {
				return nil, format, tree.ErrKeyNotFound
			}
			if !reconstruct {
				if dst != nil {
					return append(dst[:0], residual...), format, nil
				}
				out := make([]byte, len(residual))
				copy(out, residual)
				return out, format, nil
			}
			r := NativeJSONRead{meta: *meta, database: m.db, capture: capture, collection: col, oneShot: true}
			out, err := r.reconstructPendingTyped(row, residual)
			scratchLease = r.detachScratch()
			r = NativeJSONRead{}
			if err != nil {
				return nil, format, err
			}
			if dst != nil {
				return append(dst[:0], out...), format, nil
			}
			return out, format, nil
		}
	}
	var entry node.LeafEntry
	var resolved bool
	if d.pending != nil {
		if row, found := d.pending.Get(id); found {
			entry, resolved = nativeDomainRecordEntry(row), true
		}
	}
	if !resolved {
		if row, found := d.captured.get(id); found {
			entry, resolved = nativeDomainRecordEntry(row), true
		}
	}
	owned := false
	copiedToDst := false
	if !resolved {
		if d.baseRoot == 0 {
			return nil, format, tree.ErrKeyNotFound
		}
		if physicalInto, ok := capture.(nativePhysicalEntryIntoReader); ok && dst != nil && !reconstruct {
			entry, err = physicalInto.NativeEntryAtRootInto(d.baseRoot, id, dst)
			if err != nil {
				return nil, format, err
			}
			owned = true
			copiedToDst = true
		} else if physical, ok := capture.(backenddb.NativePhysicalReader); ok {
			entry, err = physical.NativeEntryAtRoot(d.baseRoot, id, true)
			if err != nil {
				return nil, format, err
			}
			owned = true
		} else {
			domain, domainErr := newNativeDomainRead(d, capture.Snapshot())
			if domainErr != nil {
				return nil, format, domainErr
			}
			r := NativeJSONRead{meta: *meta, database: m.db, capture: capture, collection: col, domain: domain, oneShot: true}
			scratch, scratchErr := r.physicalScratch()
			if scratchErr != nil {
				scratchLease = r.detachScratch()
				r = NativeJSONRead{}
				return nil, format, scratchErr
			}
			entry, err = domain.baseEntry(id, scratch, r.readLeaf)
			if err != nil {
				scratchLease = r.detachScratch()
				r = NativeJSONRead{}
				return nil, format, err
			}
			out, matErr := r.materializeEntryLocked(id, entry, false, nil, reconstruct)
			entry = node.LeafEntry{}
			scratchLease = r.detachScratch()
			r = NativeJSONRead{}
			if matErr != nil {
				return nil, format, matErr
			}
			if dst != nil {
				return append(dst[:0], out...), format, nil
			}
			return out, format, nil
		}
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, format, tree.ErrKeyNotFound
	}
	if entry.Flags&node.FlagPointer != 0 {
		var val []byte
		if physicalValInto, ok := capture.(nativePhysicalValueIntoReader); ok && dst != nil && !reconstruct {
			val, err = physicalValInto.NativeValueAtPointerInto(entry.ValuePtr, dst, nil)
			if err != nil {
				return nil, format, err
			}
			return val, format, nil
		} else if physicalVal, ok := capture.(backenddb.NativePhysicalValueReader); ok {
			val, err = physicalVal.NativeValueAtPointer(entry.ValuePtr, nil)
		} else {
			r := NativeJSONRead{meta: *meta, database: m.db, capture: capture, collection: col, oneShot: true}
			val, err = r.readValue(entry.ValuePtr, nil)
			scratchLease = r.detachScratch()
			r = NativeJSONRead{}
		}
		if err != nil {
			return nil, format, err
		}
		if reconstruct {
			r := NativeJSONRead{meta: *meta, database: m.db, capture: capture, collection: col, oneShot: true}
			val, err = r.reconstructTyped(id, val)
			scratchLease = r.detachScratch()
			r = NativeJSONRead{}
			if err != nil {
				return nil, format, err
			}
		}
		if dst != nil {
			return append(dst[:0], val...), format, nil
		}
		return val, format, nil
	}
	if reconstruct {
		r := NativeJSONRead{meta: *meta, database: m.db, capture: capture, collection: col, oneShot: true}
		val, err := r.reconstructTyped(id, entry.Value)
		entry = node.LeafEntry{}
		scratchLease = r.detachScratch()
		r = NativeJSONRead{}
		if err != nil {
			return nil, format, err
		}
		if dst != nil {
			return append(dst[:0], val...), format, nil
		}
		return val, format, nil
	}
	if copiedToDst {
		return entry.Value, format, nil
	}
	if dst != nil {
		return append(dst[:0], entry.Value...), format, nil
	}
	if owned {
		return entry.Value, format, nil
	}
	out := make([]byte, len(entry.Value))
	copy(out, entry.Value)
	return out, format, nil
}

// withOneShotRead pins one immutable cut and executes fn against a private
// stack-local NativeJSONRead without acquiring a redundant handle lease.
// Callers MUST NOT allow the *NativeJSONRead pointer to escape fn.
func (m *NativeJSONManager) withOneShotRead(name string, fn func(*NativeJSONRead) error) (resultErr error) {
	if err := ValidateCollectionName(name); err != nil {
		return err
	}
	if m == nil || m.db == nil {
		return backenddb.ErrClosed
	}
	capture, err := m.db.CaptureNativeSemanticState()
	if err != nil {
		return err
	}
	var r NativeJSONRead
	r.capture = capture
	r.database = m.db
	r.oneShot = true
	defer func() {
		r.domain = nativeDomainRead{}
		scratchLease := r.detachScratch()
		r.meta = CollectionMeta{}
		r.database = nil
		metadataLease := r.metadataLease
		r.metadataLease = nil
		r.collection = nil
		r.engine = nil
		privateEngine := r.privateEngine
		r.privateEngine = nil
		capToClose := r.capture
		r.capture = nil
		resultErr = errors.Join(resultErr, closeNativeCatalog(capToClose, privateEngine))
		if scratchLease != nil {
			scratchLease.Close()
		}
		if metadataLease != nil {
			metadataLease.Close()
		}
	}()
	r.engine, r.collection, r.privateEngine, err = resolveNativeCatalog(capture, name)
	if err != nil {
		return err
	}
	c := r.collection
	r.meta = *c.schema.meta
	if d, ok := c.domains.Get("primary"); ok {
		r.domain, err = newNativeDomainRead(d, capture.Snapshot())
		if err != nil {
			return err
		}
	}
	return fn(&r)
}

func (r *NativeJSONRead) cursorReadLeaf() func(page.LeafLogPtr, []byte) ([]byte, error) {
	if r.oneShot {
		if physical, ok := r.capture.(backenddb.NativePhysicalLeafReader); ok {
			return physical.NativeLeafPage
		}
	}
	return r.readLeaf
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
	return r.getAdmittedLocked(id, admitOutput)
}

func (r *NativeJSONRead) getAdmittedLocked(id []byte, admitOutput func(uint64) error) ([]byte, error) {
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
	f, _, err := r.capture.Snapshot().PinnedValueLogSegment(ptr.FileID)
	if err != nil {
		return nil, err
	}
	limit := r.capture.NativeAllocator().Limits().MaxInFlightBytes
	shape, err := f.InspectCOWRecord(ptr, valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
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
	value, err = f.ReadCOWRecord(ptr, shape, input, raw, output, decode)
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
	return readNativeMaintenanceSnapshotLeaf(snapshot, allocator, ptr, dst, nil)
}

func readNativeMaintenanceSnapshotLeaf(snapshot *backenddb.Snapshot, allocator backenddb.NativeSemanticAllocator, ptr page.LeafLogPtr, dst []byte, leaves backenddb.NativeMaintenanceLeafReader) ([]byte, error) {
	return readNativeSnapshotLeafWithMaintenanceAdmission(snapshot, allocator.Limits().MaxInFlightBytes, func(charge uint64) (func(), error) {
		lease, err := allocator.AdmitNativeBytes(charge)
		if err != nil {
			return nil, err
		}
		return lease.Close, nil
	}, ptr, dst, leaves)
}

// Internal synchronous admission may spend a containing preadmitted build
// workspace. It cannot reenter application code or outlive this read.
func readNativeSnapshotLeafWithAdmission(snapshot *backenddb.Snapshot, limit uint64, admit func(uint64) (func(), error), ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	return readNativeSnapshotLeafWithMaintenanceAdmission(snapshot, limit, admit, ptr, dst, nil)
}

func readNativeSnapshotLeafWithMaintenanceAdmission(snapshot *backenddb.Snapshot, limit uint64, admit func(uint64) (func(), error), ptr page.LeafLogPtr, dst []byte, leaves backenddb.NativeMaintenanceLeafReader) ([]byte, error) {
	f, _, err := snapshot.PinnedValueLogSegment(ptr.ValuePtr().FileID)
	if err != nil {
		return nil, err
	}
	shape, err := f.InspectCOWLeafRecord(ptr.ValuePtr(), valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
	if err != nil {
		return nil, err
	}
	var sizes valuelog.COWDecoderAllocationSizes
	if shape.Compressed {
		if shape.DictID != 0 {
			if leaves != nil {
				return leaves.ReadNativeMaintenanceDictionaryLeaf(snapshot, ptr, shape, dst)
			}
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
	return f.ReadCOWLeafPage(ptr.ValuePtr(), shape, input, raw, dst, decode)
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
	r.engine = nil
	privateEngine := r.privateEngine
	r.privateEngine = nil
	err = closeNativeCatalog(r.capture, privateEngine)
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
