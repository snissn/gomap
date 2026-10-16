package lifecycle

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// EmptyWithWorkV6 admits the original empty Extract branch under the same
// graveyard mutex. No sequence limit, batch or allocator effect is needed when
// there are no batches. Nonempty history is an unsupported native capability.
func (g *Graveyard) EmptyWithWorkV6(w *iterator.OrdinalScanWork) (empty, ready bool) {
	if g == nil || w == nil {
		return false, false
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(*g))) {
		return false, false
	}
	g.mu.Lock()
	empty = len(g.retiredPages) == 0
	g.mu.Unlock()
	return empty, true
}
