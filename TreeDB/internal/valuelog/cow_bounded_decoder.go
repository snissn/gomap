package valuelog

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"unsafe"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// COWBoundedDecoder uses the maintained synchronous continuation, with concrete
// retention charged to the caller's SAME receipt. The caller owns base (wrapper,
// input and raw buffers) and the immutable dictionary until after Close returns.
// Neither this adapter nor the library closes that receipt or dictionary owner.
// Generic COWDecoder and its conservative generic retention envelope are unchanged.
type COWBoundedDecoder struct {
	mu                           sync.Mutex
	decoder                      *zstd.Decoder
	work                         cowDecoderWork
	dictionary                   zstd.BorrowedDictionary
	producerID, rawCap, inputCap uint64
	closed                       bool
}

type cowDecoderReceipt interface {
	Resize(uint64) error
	ResizeWork(uint64, *iterator.OrdinalScanWork) (bool, error)
}

// Finite scheduling quanta, not a new total-read work cap. Existing synchronous
// COW reads have no total-work operand. Each continuation pause consumes the SAME
// pair before a caller replenishes it; capacity errors are never retried as pauses.
const cowDecoderQuantumRecords = 256
const cowDecoderQuantumBytes = 4 << 20

type cowDecoderWork struct {
	quantum                                  iterator.OrdinalScanWork
	receipt                                  cowDecoderReceipt
	base, retained, totalRecords, totalBytes uint64
	started, blocked                         bool
	err                                      error
}

func (w *cowDecoderWork) Reserve(records, bytes uint64) bool {
	if records > math.MaxUint64-w.totalRecords || bytes > math.MaxUint64-w.totalBytes {
		w.err = ErrCOWReadCapacity
		return false
	}
	if !w.quantum.Reserve(records, bytes) {
		w.blocked = true
		return false
	}
	w.totalRecords += records
	w.totalBytes += bytes
	return true
}
func (w *cowDecoderWork) Available() (uint64, uint64) {
	return w.quantum.RecordLimit - w.quantum.Records, w.quantum.ByteLimit - w.quantum.Bytes
}
func (w *cowDecoderWork) Retain(bytes uint64) bool {
	if bytes > math.MaxUint64-w.base || w.retained > math.MaxUint64-w.base-bytes {
		w.err = ErrCOWReadCapacity
		return false
	}
	if err := w.resize(w.base + w.retained + bytes); err != nil {
		w.err = err
		return false
	}
	w.retained += bytes
	return true
}
func (w *cowDecoderWork) Release(bytes uint64) {
	if bytes > w.retained {
		panic("valuelog: bounded decoder refunded unowned capacity")
	}
	w.retained -= bytes // upstream has already detached the final alias
	if err := w.resize(w.base + w.retained); err != nil && w.err == nil {
		w.err = err
	}
}

// A receipt callback is one synchronous indivisible SAME-ledger operation,
// admitted by its maintained ResizeWork primitive. This separate finite local
// operand cannot pause or retry admission; its consumed work joins the totals.
func (w *cowDecoderWork) resize(target uint64) error {
	ledger := iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 2 * memtable.COWRetentionAcquireWorkBytes()}
	if ledger.RecordLimit > math.MaxUint64-w.totalRecords || ledger.ByteLimit > math.MaxUint64-w.totalBytes {
		return ErrCOWReadCapacity
	}
	ready, err := w.receipt.ResizeWork(target, &ledger)
	w.totalRecords += ledger.Records
	w.totalBytes += ledger.Bytes
	if err != nil {
		return err
	}
	if !ready {
		return ErrCOWReadCapacity
	}
	return nil
}

// A false continuation result may represent a completed private phase rather
// than a denied work unit: maintained Huffman cleanup advances such phases
// without Reserve when the optional FSE backing does not exist. Only a REAL
// Reserve refusal on a wholly unused quantum proves an indivisible oversized
// unit. These phase-only yields follow the bounded engine's finite state graph;
// they must not be confused with capacity admission or retried as such.
func (w *cowDecoderWork) nextQuantum() error {
	if w.err != nil {
		return w.err
	}
	if w.started && w.blocked && w.quantum.Records == 0 && w.quantum.Bytes == 0 {
		return ErrCOWReadCapacity // an indivisible unit cannot fit a whole quantum
	}
	w.quantum = iterator.OrdinalScanWork{RecordLimit: cowDecoderQuantumRecords, ByteLimit: cowDecoderQuantumBytes}
	w.started = true
	w.blocked = false
	return nil
}
func (w *cowDecoderWork) beginOperation() {
	w.started, w.blocked = false, false
	w.totalRecords, w.totalBytes = 0, 0
	w.err = nil
}

// COWBoundedDecoderRetentionSizes covers the adapter and caller input/raw
// containing allocations. Library control/tables/backings are admitted exactly
// by Retain before allocation; no generic DecodeAll promise is substituted.
func COWBoundedDecoderRetentionSizes(rawCap, inputCap, windowCap uint64) (COWDecoderAllocationSizes, error) {
	if unsafe.Sizeof(uintptr(0)) != 8 || rawCap == 0 || rawCap > math.MaxUint32 || inputCap == 0 || inputCap > math.MaxUint32 || windowCap < 1024 || windowCap > math.MaxUint32 {
		return COWDecoderAllocationSizes{}, ErrCOWReadCapacity
	}
	return COWDecoderAllocationSizes{OwnerWrapper: uint64(unsafe.Sizeof(COWBoundedDecoder{})), RawBacking: rawCap, InputBacking: inputCap}, nil
}

// NewCOWBoundedDecoder validates exact definition provenance before admission.
// base is the complete caller ledger, including this wrapper and full input/raw
// capacities; Resize(base) is performed before this wrapper is allocated. Each
// subsequently retained library allocation grows this same original receipt.
func NewCOWBoundedDecoder(producerID uint64, definition []byte, dictionaryOwner any, rawCap, inputCap, windowCap uint64, receipt cowDecoderReceipt, base uint64) (*COWBoundedDecoder, error) {
	if producerID != 0 {
		if len(definition) == 0 {
			return nil, ErrMissingDict
		}
		if len(definition) < 8 {
			return nil, ErrCorrupt
		}
		digest := sha256.Sum256(definition)
		if binary.BigEndian.Uint64(digest[:8]) != producerID {
			return nil, ErrCorrupt
		}
		if dictionaryOwner == nil {
			return nil, ErrCOWReadCapacity
		}
	} else if len(definition) != 0 {
		return nil, ErrCorrupt
	}
	if uint64(len(definition)) > math.MaxInt32 {
		return nil, ErrCOWReadCapacity
	}
	sizes, err := COWBoundedDecoderRetentionSizes(rawCap, inputCap, windowCap)
	if err != nil {
		return nil, err
	}
	floor := retainedalloc.AllocationCharge(sizes.OwnerWrapper) + retainedalloc.AllocationCharge(rawCap) + retainedalloc.AllocationCharge(inputCap)
	if receipt == nil || base < floor {
		return nil, ErrCOWReadCapacity
	}
	initial := iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 2 * memtable.COWRetentionAcquireWorkBytes()}
	if ready, err := receipt.ResizeWork(base, &initial); err != nil {
		return nil, err
	} else if !ready {
		return nil, ErrCOWReadCapacity
	}
	d := &COWBoundedDecoder{producerID: producerID, rawCap: rawCap, inputCap: inputCap, work: cowDecoderWork{receipt: receipt, base: base}}
	if producerID != 0 {
		d.dictionary = zstd.BorrowedDictionary{ID: binary.LittleEndian.Uint32(definition[4:8]), Data: definition, Owner: dictionaryOwner}
	}
	d.work.beginOperation()
	for {
		if err = d.work.nextQuantum(); err != nil {
			break
		}
		var ready bool
		d.decoder, ready, err = zstd.NewBoundedReaderWithLimits(&d.work, max(rawCap, windowCap, 1024), windowCap)
		if err != nil || d.work.err != nil {
			err = errors.Join(d.work.err, err)
			break
		}
		if ready {
			break
		}
	}
	if err == nil {
		// Preserve constructor-time dictionary grammar validation using the same
		// engine, not another parser. Empty input completes after cold preparation.
		err = d.reset(nil, nil, &d.dictionary)
		if err == nil {
			err = d.run()
		}
		if err == nil {
			err = d.reset(nil, nil, nil)
		}
	}
	if err != nil {
		d.Close()
		return nil, err
	}
	return d, nil
}

// A nonnil fullDictionary is used only for initial grammar validation. Frame
// rebinds keep the already-validated immutable definition and actual admitted
// decoder buffers, while detaching every previous caller input/output alias.
func (d *COWBoundedDecoder) reset(input, raw []byte, fullDictionary *zstd.BorrowedDictionary) error {
	for {
		var ready bool
		var err error
		// Both maintained operations may pause on finite work. Capacity refusal
		// is propagated once; only a no-error work pause gets another quantum.
		if !d.work.started {
			if err = d.work.nextQuantum(); err != nil {
				return err
			}
		}
		if fullDictionary != nil {
			ready, err = d.decoder.ResetStep(input, d, zstd.BoundedRaw, raw, *fullDictionary)
		} else {
			ready, err = d.decoder.ResetFrameStep(input, d, zstd.BoundedRaw, raw)
		}
		if err != nil || d.work.err != nil {
			return errors.Join(d.work.err, err)
		}
		if ready {
			return nil
		}
		if err = d.work.nextQuantum(); err != nil {
			return err
		}
	}
}
func (d *COWBoundedDecoder) run() error {
	for {
		_, done, err := d.decoder.Step(nil)
		if err != nil || d.work.err != nil {
			return errors.Join(d.work.err, err)
		}
		if done {
			return nil
		}
		if err = d.work.nextQuantum(); err != nil {
			return err
		}
	}
}

// Decode preserves the original exact producer/shape/input/output contracts.
// On every return all engine aliases of caller input/raw have been reset away;
// the immutable definition and actual reusable decoder storage remain charged.
// A failed operation closes this decoder before returning its original error;
// callers reconstruct
// through normal admission on a later read, never retry the failed operation.
func (d *COWBoundedDecoder) Decode(frame FrameHeader, payload, dst []byte) ([]byte, error) {
	if d == nil {
		return nil, ErrCOWDecoderClosed
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.decoder == nil {
		return nil, ErrCOWDecoderClosed
	}
	if frame.DictID != d.producerID {
		return nil, ErrMissingDict
	}
	if uint64(len(payload)) > d.inputCap {
		return nil, ErrCOWReadCapacity
	}
	if frame.Version != FrameVersion || frame.Flags != FrameFlagCompressed || frame.K == 0 || uint64(len(dst)) > d.rawCap || frame.DictID != 0 && frame.Reserved != 0 {
		return nil, ErrCorrupt
	}
	if BlockCodec(frame.Reserved) != BlockCodecNone && BlockCodec(frame.Reserved) != BlockCodecZSTD {
		return nil, ErrCOWReadUnsupported
	}
	need := len(dst)
	dst = dst[:need:need]
	d.work.beginOperation()
	err := d.reset(payload, dst, nil)
	if err == nil {
		err = d.run()
	}
	var out []byte
	if err == nil {
		out, err = d.decoder.RawResult()
		if err == nil && (len(out) != need || need > 0 && unsafe.SliceData(out) != unsafe.SliceData(dst)) {
			err = ErrCorrupt
		}
	}
	if err == nil {
		err = d.reset(nil, nil, nil)
	}
	if err != nil {
		d.closeLocked()
		return nil, err
	}
	return out, nil
}
func (d *COWBoundedDecoder) closeLocked() {
	if d.decoder != nil {
		// Error/refusal never prevents finite, charged cleanup. No Retain occurs
		// in CloseStep; releases follow engine alias detachment phase by phase.
		d.work.beginOperation()
		for {
			if err := d.work.nextQuantum(); err != nil {
				panic("valuelog: bounded decoder cleanup cannot progress")
			}
			done, err := d.decoder.CloseStep()
			if err != nil {
				panic("valuelog: bounded decoder cleanup failed")
			}
			if done {
				break
			}
		}
		d.decoder = nil // drop final exported control pointer BEFORE its refund
		d.work.Release(zstd.BoundedControlSize())
	}
	d.dictionary = zstd.BorrowedDictionary{}
	d.closed = true
}
func (d *COWBoundedDecoder) Close() {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closeLocked()
}

// Closed lets the original serialized workspace retire a failed codec before
// normal admission on a later read. It never retries the failed read.
func (d *COWBoundedDecoder) Closed() bool {
	if d == nil {
		return true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closed
}
