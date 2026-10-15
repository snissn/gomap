package node

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// AddInternalChildRefWithWork admits the ordinary fixed-layout encoder. The
// base-delta encoder needs its own retained finish scratch; it cannot fall back
// to an opaque synchronous finish on a bounded call.
func (b *Builder) AddInternalChildRefWithWork(key []byte, ref page.ChildRef, w *iterator.OrdinalScanWork) (bool, error) {
	if w != nil && b.internalBaseDelta {
		return false, ErrBuilderWorkFormat
	}
	if ok, e := builderReserve(w, 6, uint64(len(key))+64); !ok || e != nil {
		return false, e
	}
	if e := b.AddInternalChildRef(key, ref); e != nil {
		return false, e
	}
	return true, nil
}
func (b *Builder) FinishInternalWithWork(w *iterator.OrdinalScanWork) (bool, error) {
	if b.pType != page.PageTypeInternal || (w != nil && b.internalBaseDelta) {
		return false, ErrBuilderWorkFormat
	}
	if ok, e := builderReserve(w, 2, uint64(len(b.data)+NodeHeaderSize)); !ok || e != nil {
		return false, e
	}
	b.finalize()
	return true, nil
}
