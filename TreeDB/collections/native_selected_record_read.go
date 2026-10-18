package collections

import (
	"os"
	"reflect"
	"sync/atomic"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// nativeSelectedRecordRead is one private, serialized record read scope. It
// retains the exact original inventory, never a live registry or an engine
// ancestor. The complete view must establish selector/pointer lineage before
// construction. This does not supply pending pointer or dictionary closure.
// Reads and result use must finish before release; returned bytes borrow output.
type nativeSelectedRecordRead struct {
	refs      atomic.Int64
	inventory *nativeResourceInventory
	selector  rootpublication.StableResourceSelector
	ptr       page.ValuePtr
	shape     valuelog.COWRecordShape
	input     []byte
	raw       []byte
	output    []byte
	decoder   *valuelog.COWDecoder
	buffers   *memtable.COWExternalLease
	ready     bool
	failure   error
	cleanup   rootpublication.SemanticCleanup
	refund    *nativeEngineRefund
}

// A nonnil partial owns one release edge even when construction fails. All
// partials are adopted into this Work before the next fallible operation.
// allocator is the original complete cut's budget, already enrolled by inventory.
func prepareNativeSelectedRecordRead(inventory *nativeResourceInventory, selector rootpublication.StableResourceSelector, ptr page.ValuePtr, limits valuelog.COWReadLimits, allocator backenddb.NativeSemanticAllocator) (*nativeSelectedRecordRead, error) {
	if inventory == nil || inventory.refs.Load() <= 0 || inventory.resources == nil || inventory.failure != nil || allocator == nil ||
		selector.Kind != rootpublication.ResourceValueLog || selector.Obligation.Reachability != rootpublication.ReachabilityValueLogPointer ||
		selector.Obligation.FileID != uint64(ptr.FileID) || selector.Obligation.Offset < 0 || selector.Obligation.Length <= 0 || ptr.Offset < 4 {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	// Constrain inspection itself to the selected obligation, before even a
	// record header is read. The header length check then bounds every grouped
	// prefix read as well; later full-record validation keeps the same proof.
	start := ptr.Offset - 4
	obligation := selector.Obligation
	if start < uint64(obligation.Offset) || start-uint64(obligation.Offset) > uint64(obligation.Length) {
		return nil, rootpublication.ErrUnresolvedResource
	}
	remaining := uint64(obligation.Length) - (start - uint64(obligation.Offset))
	if remaining < valuelog.HeaderSize {
		return nil, rootpublication.ErrUnresolvedResource
	}
	limits.MaxRecordBytes = min(limits.MaxRecordBytes, remaining)
	charge, err := nativeSchemaBackingCharge(reflect.ValueOf(selector), 0)
	if err != nil {
		return nil, err
	}
	add := func(bytes uint64) bool {
		if bytes > ^uint64(0)-charge {
			return false
		}
		charge += bytes
		return true
	}
	if !add(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeSelectedRecordRead{})))) ||
		!add(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineRefund{})))) {
		return nil, memtable.ErrCOWCapacity
	}
	for _, bytes := range valuelog.COWInspectionMetadataAllocationSizes() {
		if !add(memtable.COWAllocationCharge(bytes)) {
			return nil, memtable.ErrCOWCapacity
		}
	}
	lease, err := allocator.AdmitNativeBytes(charge)
	if err != nil {
		return nil, err
	}
	r := &nativeSelectedRecordRead{selector: selector, ptr: ptr, refund: &nativeEngineRefund{lease: lease}}
	r.refs.Store(1)
	r.cleanup.Work = r
	nativeSchemaOwnStrings(reflect.ValueOf(&r.selector))
	inventory.retain()
	r.inventory = inventory
	err = inventory.resources.WithSelectedResourceFile(r.selector, func(file *os.File) error {
		var err error
		r.shape, err = valuelog.InspectCOWRecord(file, ptr, limits)
		return err
	})
	if err != nil {
		return r, err
	}
	// Exact obligation membership must cover the entire original record, not
	// merely another pointer in the same file. Subindices remain shape-bound.
	obligation = r.selector.Obligation
	if start < uint64(obligation.Offset) || start-uint64(obligation.Offset) > uint64(obligation.Length) ||
		r.shape.RecordBytes > uint64(obligation.Length)-(start-uint64(obligation.Offset)) {
		return r, rootpublication.ErrUnresolvedResource
	}
	if r.shape.Compressed && (r.shape.DictID != 0 || (r.shape.Codec != valuelog.BlockCodecSnappy && r.shape.Codec != valuelog.BlockCodecLZ4)) {
		return r, valuelog.ErrCOWReadUnsupported
	}
	charge = 0
	for _, bytes := range [3]uint64{r.shape.PayloadBytes(), r.shape.RawBytes, r.shape.ValueBytes} {
		if !add(memtable.COWAllocationCharge(bytes)) {
			return r, memtable.ErrCOWCapacity
		}
	}
	var sizes valuelog.COWDecoderAllocationSizes
	if r.shape.Compressed {
		sizes, err = valuelog.COWBlockDecoderRetentionSizes(r.shape.Codec, r.shape.RawBytes, r.shape.PayloadBytes())
		if err != nil {
			return r, err
		}
		// Input/raw are the SAME buffers already charged above. Block decoders
		// retain only their original wrapper; they allocate no codec workspace.
		if !add(memtable.COWAllocationCharge(sizes.OwnerWrapper)) {
			return r, memtable.ErrCOWCapacity
		}
	}
	r.buffers, err = allocator.AdmitNativeBytes(charge)
	if err != nil {
		return r, err
	}
	r.input = make([]byte, r.shape.PayloadBytes())
	r.raw = make([]byte, r.shape.RawBytes)
	r.output = make([]byte, r.shape.ValueBytes)
	if r.shape.Compressed {
		r.decoder, err = valuelog.NewCOWBlockDecoder(r.shape.Codec, r.shape.RawBytes, r.shape.PayloadBytes(), func(actual valuelog.COWDecoderAllocationSizes) error {
			if actual != sizes {
				return memtable.ErrCOWCapacity
			}
			return nil // Every reported constituent is held by r.buffers.
		})
		if err != nil {
			return r, err
		}
	}
	r.ready = true
	return r, nil
}

func (r *nativeSelectedRecordRead) read() ([]byte, error) {
	if r == nil || r.refs.Load() <= 0 || !r.ready || r.inventory == nil || r.failure != nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	var output []byte
	err := r.inventory.resources.WithSelectedResourceFile(r.selector, func(file *os.File) error {
		var decode valuelog.COWDecodeFunc
		if r.decoder != nil {
			decode = r.decoder.Decode
		}
		var err error
		output, err = valuelog.ReadCOWRecord(file, r.ptr, r.shape, true, r.input, r.raw, r.output, decode)
		return err
	})
	return output, err
}

func (r *nativeSelectedRecordRead) release() *rootpublication.SemanticCleanup {
	if r == nil {
		return nil
	}
	refs := r.refs.Add(-1)
	if refs < 0 {
		panic("collections: selected record read released twice")
	}
	if refs == 0 {
		return &r.cleanup
	}
	return nil
}

func (r *nativeSelectedRecordRead) FinishSemanticCleanup() (rootpublication.SemanticCleanupCompletion, error) {
	if r.failure != nil {
		return rootpublication.SemanticCleanupCompletion{}, r.failure
	}
	r.ready = false
	if r.decoder != nil {
		r.decoder.Close()
		r.decoder = nil
	}
	r.input, r.raw, r.output = nil, nil, nil
	r.selector, r.ptr, r.shape = rootpublication.StableResourceSelector{}, page.ValuePtr{}, valuelog.COWRecordShape{}
	if r.buffers != nil {
		buffers := r.buffers
		r.buffers = nil
		buffers.Close() // The independent buffer/decoder aliases have ended.
	}
	if r.inventory != nil {
		work := r.inventory.release()
		if work != nil {
			done, err := work.Finish()
			if err != nil {
				r.failure = err
				return rootpublication.SemanticCleanupCompletion{}, err
			}
			r.refund.inventory = done
		}
		r.inventory = nil
	}
	refund := r.refund
	r.refund = nil
	return rootpublication.SemanticCleanupCompletion{Refund: refund}, nil
}
