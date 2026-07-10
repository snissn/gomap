//go:build !treedb_freelist_instrument

package freelist

import (
	"sync"

	"github.com/snissn/gomap/TreeDB/pager"
)

const allocatorInstrumentationEnabled = false

type Allocator struct {
	pager *pager.Pager
	head  uint64
	mu    sync.Mutex

	lastAlloc    uint64
	regionPages  uint64
	regionRadius int

	stats Stats

	// preferAppend makes Alloc ignore the freelist and allocate new pages by
	// extending the file. This improves locality at the cost of reclaiming space
	// later via vacuum.
	preferAppend bool
}

func (*Allocator) observePageOperation(allocatorPageOperation, uint64) {}

func (*Allocator) injectedGetForWriteError(uint64) error { return nil }
