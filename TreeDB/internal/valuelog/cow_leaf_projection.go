package valuelog

import (
	"os"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// COWLeafProjectionReader structurally implements tree.ProjectionFrameReader
// without introducing the existing valuelog->rootpublication->tree import cycle.
// File is the caller's ALREADY resolved/retained immutable file for FileID.
// Input/Raw are exclusive, admitted containing backings. No lookup, manager,
// registry, acquisition, refresh or file close occurs. The enclosing original
// owner establishes exact File/FileID/root binding and whole-cap admission.
// Parent frames require their own genuinely phased reader. Caller must retain
// binding fields immutable until Cancel; repeated Begin resumes SAME ref/dst.
type COWLeafProjectionReader struct {
	TemplateScratch []byte
	File            *os.File
	FileID          uint32
	Limits          COWReadLimits
	Input, Raw      []byte
	VerifyCRC       bool
	ZSTD            *COWZSTDContinuation
	Template        *COWTemplateContinuation
	leaf            COWLeafContinuation

	clearing bool
}

func (r *COWLeafProjectionReader) BeginProjectionFrame(ref page.ChildRef, dst []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if r.clearing {
		return false, ErrCOWReadPending
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(ref))+uint64(unsafe.Sizeof(r.leaf))+128) {
		return false, nil
	}
	if !ref.IsLeafLog() || ref.Page != 0 || ref.Log.FileID != r.FileID || ref.Log.SubIndex > 255 || (!ref.Log.IsGrouped() && ref.Log.SubIndex != 0) {
		return false, ErrCOWReadUnsupported
	}
	r.leaf.ZSTD = r.ZSTD
	r.leaf.Template = r.Template
	r.leaf.TemplateScratch = r.TemplateScratch
	return r.leaf.beginAdmitted(r.File, ref.Log.ValuePtr(), r.Limits, r.Input, r.Raw, dst, r.VerifyCRC)
}
func (r *COWLeafProjectionReader) StepProjectionFrame(w *iterator.OrdinalScanWork) ([]byte, bool, error) {
	return r.leaf.StepWithCodec(w)
}
func (r *COWLeafProjectionReader) CancelProjectionFrame(w *iterator.OrdinalScanWork) (bool, error) {
	if !r.clearing {
		done, err := r.leaf.CancelWithCodec(w)
		if err != nil || !done {
			return false, err
		}
		r.clearing = true
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*r))) {
		return false, nil
	}
	*r = COWLeafProjectionReader{}
	return true, nil
}
