package valuelog

import (
	"math"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Stamps describe actual installed allocation instances under the existing
// writer/lane serializer. A serial never derives from an address, len, load
// factor or request account. The owning Writer keeps current backing live.
type writerBackingAllocation struct {
	serial, class uint64
	base          unsafe.Pointer
}

func (w *Writer) recordWriterBackingBirth(index int, raw uint64, scan bool) {
	class, err := rootpublication.StableBackingClassBytes(raw, scan)
	if err != nil || class == 0 {
		w.backingAllocations[index] = writerBackingAllocation{}
		return
	}
	if w.backingSerial == math.MaxUint64 {
		panic("valuelog: writer allocation identity exhausted")
	}
	w.backingSerial++
	w.backingAllocations[index] = writerBackingAllocation{serial: w.backingSerial, class: class}
}

func (w *Writer) setWriterByteBacking(index int, dst *[]byte, next []byte) {
	old := *dst
	// Both actual arrays are still live during this comparison. A new ownership
	// episode (including a pool acquisition) receives a fresh serial. Reslicing
	// the same base never shrinks its actual constructor-stamped capacity.
	if next == nil {
		w.backingAllocations[index] = writerBackingAllocation{}
	} else if unsafe.SliceData(old) != unsafe.SliceData(next) || cap(next) > cap(old) {
		w.recordWriterBackingBirth(index, uint64(cap(next)), false)
		w.backingAllocations[index].base = unsafe.Pointer(unsafe.SliceData(next))
	}
	*dst = next
}
func (w *Writer) setWriterIovBacking(next [][]byte) {
	old := w.rawWritevIovs
	if next == nil {
		w.backingAllocations[8] = writerBackingAllocation{}
	} else if unsafe.SliceData(old) != unsafe.SliceData(next) || cap(next) > cap(old) {
		w.recordWriterBackingBirth(8, uint64(cap(next))*uint64(unsafe.Sizeof([]byte{})), true)
		w.backingAllocations[8].base = unsafe.Pointer(unsafe.SliceData(next))
	}
	w.rawWritevIovs = next
}
func (w *Writer) setWriterVecBacking(next []writevIovec) {
	old := w.rawWritevVecs
	if next == nil {
		w.backingAllocations[9] = writerBackingAllocation{}
	} else if unsafe.SliceData(old) != unsafe.SliceData(next) || cap(next) > cap(old) {
		w.recordWriterBackingBirth(9, uint64(cap(next))*uint64(unsafe.Sizeof(writevIovec{})), false)
		w.backingAllocations[9].base = unsafe.Pointer(unsafe.SliceData(next))
	}
	w.rawWritevVecs = next
}

func (w *Writer) finiteWriterAllocationCensus(strict bool) ([11]writerBackingAllocation, error) {
	census := w.backingAllocations
	raw := [11]uint64{uint64(cap(w.appendBuf)), uint64(cap(w.scratch)), uint64(cap(w.rawScratch)), uint64(cap(w.encScratch)), uint64(cap(w.blockScratch)), uint64(cap(w.prefixBuf)), 0, uint64(unsafe.Sizeof(Writer{})), uint64(cap(w.rawWritevIovs)) * uint64(unsafe.Sizeof([]byte{})), uint64(cap(w.rawWritevVecs)) * uint64(unsafe.Sizeof(writevIovec{})), uint64(cap(w.rawWritevMeta))}
	bases := [11]unsafe.Pointer{unsafe.Pointer(unsafe.SliceData(w.appendBuf)), unsafe.Pointer(unsafe.SliceData(w.scratch)), unsafe.Pointer(unsafe.SliceData(w.rawScratch)), unsafe.Pointer(unsafe.SliceData(w.encScratch)), unsafe.Pointer(unsafe.SliceData(w.blockScratch)), unsafe.Pointer(unsafe.SliceData(w.prefixBuf)), nil, unsafe.Pointer(w), unsafe.Pointer(unsafe.SliceData(w.rawWritevIovs)), unsafe.Pointer(unsafe.SliceData(w.rawWritevVecs)), unsafe.Pointer(unsafe.SliceData(w.rawWritevMeta))}
	if w.blockCodecScratch.lz4Compressor != nil {
		raw[6] = uint64(unsafe.Sizeof(*w.blockCodecScratch.lz4Compressor))
		bases[6] = unsafe.Pointer(w.blockCodecScratch.lz4Compressor)
	}
	for i, n := range raw {
		class, err := rootpublication.StableBackingClassBytes(n, i == 7 || i == 8)
		if err != nil {
			return census, err
		}
		if n == 0 && bases[i] == nil {
			census[i] = writerBackingAllocation{}
			continue
		}
		if census[i].serial == 0 || census[i].class < class || census[i].base != bases[i] {
			if strict {
				return census, ErrFiniteWriterLoan
			}
			// Legacy fabricated ordinary test/sink writers have no constructor proof;
			// each loan conservatively debits their full current class.
			census[i] = writerBackingAllocation{class: class}
		}
	}
	return census, nil
}
