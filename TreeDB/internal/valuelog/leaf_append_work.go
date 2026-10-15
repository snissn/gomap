package valuelog

import (
	"errors"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// ErrLeafAppendWorkUnsupported is a before-effects refusal for a physical
// producer transition whose retained work stages are not installed yet.
var ErrLeafAppendWorkUnsupported = errors.New("valuelog: outer-leaf append needs retained writer growth, flush or rotation")

func leafAppendReserve(w *iterator.OrdinalScanWork, r, b uint64) (bool, error) {
	if w == nil {
		return true, nil
	}
	if r > w.RecordLimit || b > w.ByteLimit {
		return false, &iterator.OrdinalUnitTooLarge{Records: r, Bytes: b}
	}
	return w.Reserve(r, b), nil
}

// LeafAppendWork lives in the original admitted zipper materialization owner. All page,
// body, codec table and preparer backing belongs to that containing receipt;
// no pool alias or cache owner is added. The input image stays immutable.
type LeafAppendWork struct {
	Payload       [page.PageSize]byte
	Body          [page.PageSize + 256]byte
	Encoded       [page.PageSize + 256]byte
	Compressor    lz4.Compressor
	Preparer      FramePreparer
	Value, Frame  []byte
	Stats         FrameStats
	RID           uint64
	Phase         uint8
	Ptr           page.LeafLogPtr
	Codec         BlockCodec
	Before, After bool
	preparerReady bool
	Err           error
}

func (s *LeafAppendWork) Compact(leaf []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if s.Phase != 0 {
		return true, nil
	}
	if len(leaf) != page.PageSize {
		return false, ErrCorrupt
	}
	ok, e := leafAppendReserve(w, 6, uint64(4*page.PageSize+256))
	if !ok || e != nil {
		return false, e
	}
	value, _, e := MaybeCompactLeafLogPayloadTo(s.Payload[:0], leaf)
	if e != nil {
		return false, e
	}
	// An uncompacted page remains the immutable original frame input.
	s.Value = value
	s.Phase = 1
	return true, nil
}
func (s *LeafAppendWork) Prepare(rid uint64, codec BlockCodec, ioCost, encodeCost, safety float64, w *iterator.OrdinalScanWork) (bool, error) {
	if s.Phase >= 2 {
		return true, nil
	}
	if s.Phase != 1 {
		return false, ErrCorrupt
	}
	if !s.preparerReady {
		s.preparerReady = true
		s.RID = rid
		s.Codec = codec
		s.Preparer.ResetForReuse()
		s.Preparer.SetBlockCompression(codec, true)
		s.Preparer.SetKeepPolicy(ioCost, encodeCost, safety)
		s.Preparer.SetEncodeSampleStride(0)
		s.Preparer.blockScratch = s.Encoded[:0]
		s.Preparer.blockCodecScratch.lz4Compressor = &s.Compressor
	}
	if s.Codec != codec {
		return false, ErrLeafAppendWorkUnsupported
	}
	body, stats, ready, e := s.Preparer.PrepareLeafFrameIntoWithWork(s.Body[:0], s.RID, s.Value, w)
	if !ready || e != nil {
		return false, e
	}
	s.Frame = body
	s.Stats = stats
	s.Phase = 2
	return true, nil
}

// ResetCompletedWithWork releases only completed per-leaf metadata aliases.
// The caller has saved the ChildRef and completed every wrapper/cache hook.
// Fixed byte arrays and the original LZ4 compressor remain owned by this same
// containing receipt; CompressBlock resets its own lookup bitmap for each block.
// No live workspace is copied and no large compressor table is zeroed here.
func (s *LeafAppendWork) ResetCompletedWithWork(w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || s.Phase != 3 || !s.After || s.Err != nil {
		return false, ErrLeafAppendWorkUnsupported
	}
	// This leaf-only producer never acquires a dictionary encoder; do not erase
	// an opaque physical owner if an unsupported producer were bound here.
	if s.Preparer.dictEncoder != nil {
		return false, ErrLeafAppendWorkUnsupported
	}
	n := uint64(unsafe.Sizeof(FramePreparer{}) + unsafe.Sizeof(s.Stats) + unsafe.Sizeof(s.Value) + unsafe.Sizeof(s.Frame) + unsafe.Sizeof(s.Ptr) + 64)
	if ok, e := leafAppendReserve(w, 1, 2*n); !ok || e != nil {
		return false, e
	}
	s.Preparer = FramePreparer{}
	s.Value = nil
	s.Frame = nil
	s.Stats = FrameStats{}
	s.RID = 0
	s.Phase = 0
	s.Ptr = page.LeafLogPtr{}
	s.Codec = 0
	s.Before = false
	s.After = false
	s.preparerReady = false
	s.Err = nil
	return true, nil
}
