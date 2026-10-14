package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"unsafe"
)

// NativeMVCCFrameReaderV6 is a consumer of the SAME retained Snapshot. The
// enclosing native cursor admits this containing wrapper and all Leaf backings
// before construction and retains the Snapshot through phased consumer close.
// It creates no file, registry, cache, publication or retention authority.
type NativeMVCCFrameReaderV6 struct {
	Snapshot         *Snapshot
	Leaf             *valuelog.COWLeafProjectionReader
	ref              page.ChildRef
	dst, image       []byte
	copied           int
	phase            uint8
	active, external bool
}

func (r *NativeMVCCFrameReaderV6) BeginProjectionFrame(ref page.ChildRef, dst []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*r))+uint64(unsafe.Sizeof(ref))) {
		return false, nil
	}
	if r.active || r.Snapshot == nil || len(dst) != page.PageSize {
		return false, tree.ErrProjectionReader
	}
	external := false
	if ref.IsLeafLog() {
		// Admit the actual existing Set lookup and registered File observation.
		// The passed codec borrow must use this exact producer-owned file.
		if !w.Reserve(2, 256) {
			return false, nil
		}
		file, _, err := r.Snapshot.PinnedValueLogFile(ref.Log.FileID)
		if err != nil {
			return false, err
		}
		if r.Leaf == nil || r.Leaf.File != file || r.Leaf.FileID != ref.Log.FileID {
			return false, tree.ErrProjectionReader
		}
		ready, err := r.Leaf.BeginProjectionFrame(ref, dst, w)
		if !ready || err != nil {
			return ready, err
		}
		external = true
	} else if !ref.IsPage() || ref.Page < 2 || ref.Log != (page.LogRecordRef{}) {
		return false, tree.ErrProjectionReader
	}
	r.ref, r.dst, r.copied, r.phase, r.active, r.external = ref, dst, 0, 1, true, external
	return true, nil
}

func (r *NativeMVCCFrameReaderV6) StepProjectionFrame(w *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if !r.active || r.Snapshot == nil {
		return nil, false, tree.ErrProjectionReader
	}
	if r.external {
		image, ready, err := r.Leaf.StepProjectionFrame(w)
		if ready && err == nil {
			r.active = false
		}
		return image, ready, err
	}
	if r.phase == 1 {
		if w == nil || !w.Reserve(1, 128) {
			return nil, false, nil
		}
		if err := r.Snapshot.beginRead(); err != nil {
			return nil, false, err
		}
		if r.Snapshot.idx == nil || r.Snapshot.idx.pager == nil {
			r.Snapshot.endRead()
			return nil, false, ErrClosed
		}
		image, ready, err := r.Snapshot.idx.pager.GetWithWork(r.ref.Page, w)
		r.Snapshot.endRead()
		if !ready || err != nil {
			return nil, ready, err
		}
		if len(image) != page.PageSize {
			return nil, false, tree.ErrProjectionReader
		}
		r.image, r.phase = image, 2
	}
	if r.phase == 2 {
		if w == nil || w.Bytes > w.ByteLimit {
			return nil, false, nil
		}
		amount := int(min(uint64(page.PageSize-r.copied), (w.ByteLimit-w.Bytes)/2))
		if amount == 0 || !w.Reserve(1, 2*uint64(amount)) {
			return nil, false, nil
		}
		copy(r.dst[r.copied:r.copied+amount], r.image[r.copied:r.copied+amount])
		r.copied += amount
		if r.copied != page.PageSize {
			return nil, false, nil
		}
		r.phase = 3
	}
	if !w.Reserve(1, 64) {
		return nil, false, nil
	}
	image := r.dst
	r.image, r.active, r.phase = nil, false, 0
	return image, true, nil
}

func (r *NativeMVCCFrameReaderV6) CancelProjectionFrame(w *iterator.OrdinalScanWork) (bool, error) {
	if r.external && r.Leaf != nil {
		ready, err := r.Leaf.CancelProjectionFrame(w)
		if !ready || err != nil {
			return ready, err
		}
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*r))) {
		return false, nil
	}
	r.ref, r.dst, r.image, r.copied, r.phase, r.active, r.external = page.ChildRef{}, nil, nil, 0, 0, false, false
	return true, nil
}

// NewNativeMVCCProjectionV6 borrows this original Snapshot's exact Tree. The
// original cursor's retained allocation admission precedes this constructor;
// ChargedProjection then prices its actual constructor before allocation.
// cert must be the retained immutable producer certificate supplied by capture.
func (s *Snapshot) NewNativeMVCCProjectionV6(cert *primaryarena.ReadRootCertificateV6, start, end []byte, reader *NativeMVCCFrameReaderV6, w *iterator.OrdinalScanWork) (*tree.ChargedProjection, error) {
	if s == nil || reader == nil || reader.Snapshot != s {
		return nil, tree.ErrProjectionOwner
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*s))) {
		return nil, tree.ErrProjectionPending
	}
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	if s.primaryRoot != nil && (cert == nil || cert.Arena() != s.primaryRoot.arena || cert.Reference() != s.primaryRoot.ref) {
		return nil, tree.ErrProjectionOwner
	}
	return s.tree.NewChargedProjection(cert, start, end, reader, w)
}

// ValidateNativeMVCCSelectionV6 binds actual selected physical bytes to this
// SAME retained Snapshot. It confers no discard-floor or publication authority.
func (s *Snapshot) ValidateNativeMVCCSelectionV6(selection tree.NativeMVCCSelectionV6, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || w == nil || !w.Reserve(1, 128) {
		return false, nil
	}
	if err := s.beginRead(); err != nil {
		return false, err
	}
	defer s.endRead()
	return s.tree.MatchesNativeMVCCSelectionV6(selection, w)
}
