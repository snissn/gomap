//go:build !treedb_freelist_instrument

package freelist

import (
	"sync"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/pager"
)

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
	cow          *allocatorCOWStateV1
}

func (a *Allocator) batchGetForWrite(pageID uint64) ([]byte, error) {
	return a.pager.GetForWrite(pageID)
}

func (*Allocator) batchVerifyChecksum(_ uint64, n *node.Node) bool {
	return n.VerifyChecksum()
}

func (*Allocator) batchUpdateChecksum(_ uint64, n *node.Node) {
	n.UpdateChecksum()
}
