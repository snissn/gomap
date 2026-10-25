package tree

import (
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// LeafLogPointViewReader returns either a leased immutable page with validation
// state, or an unleased page aliasing dst exactly. The caller owns the lease on
// every return, including error. It must never decode/store/evict while retaining
// an earlier lease on the same cache. This callback adds no page/record policy.
type LeafLogPointViewReader func(page.LeafLogPtr, []byte) ([]byte, LeafLogPageViewLease, LeafLogPageReadState, error)

type pointLeafView struct {
	read  LeafLogPointViewReader
	lease LeafLogPageViewLease
}

func (v *pointLeafView) release() {
	if v.lease != nil {
		v.lease.ReleaseLeafLogPageView()
		v.lease = nil
	}
}

// GetEntryWithFixedScratchView shares GetEntry's complete descent, including
// negatives, fences and PRIMARY operand validation. A successful returned lease
// owns all borrowed page bytes until ReleaseLeafLogPageView; the caller must
// consume/copy the entry before releasing it. A nil lease retains the original
// fixed-scratch/snapshot lifetime. Errors release every acquired lease. No lease
// or bytes are cached locally, and original fixed-scratch methods are unchanged.
func (t *Tree) GetEntryWithFixedScratchView(key, keyScratch, leafScratch []byte, readLeaf LeafLogPointViewReader) (node.LeafEntry, LeafLogPageViewLease, error) {
	if cap(keyScratch) < page.PageSize || cap(leafScratch) < page.PageSize {
		return node.LeafEntry{}, nil, node.ErrCorruptedNode
	}
	view := pointLeafView{read: readLeaf}
	entry, err := t.getEntry(key, keyScratch, leafScratch, nil, true, &view)
	if err != nil {
		view.release()
		return node.LeafEntry{}, nil, err
	}
	return entry, view.lease, nil
}

func (t *Tree) loadPointReadRefInto(n *node.Node, ref page.ChildRef, verify bool, keyScratch, leafScratch []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error), bounded bool, view *pointLeafView) error {
	if view == nil {
		return t.loadReadRefInto(n, ref, verify, keyScratch, leafScratch, readLeaf, bounded)
	}
	// End the previous node's aliases before any callback, fallback or new read.
	view.release()
	if !bounded || ref.Kind != page.ChildRefLeafLog {
		return t.loadReadRefInto(n, ref, verify, keyScratch, leafScratch, readLeaf, bounded)
	}
	if view.read == nil {
		return ErrOwnedIteratorLeafReader
	}
	data, lease, state, err := view.read(ref.Log, leafScratch[:0])
	view.lease = lease
	if err != nil {
		view.release()
		return err
	}
	if lease == nil && (len(data) != page.PageSize || cap(data) != page.PageSize || &data[0] != &leafScratch[:page.PageSize][0]) {
		return ErrOwnedIteratorLeafReader
	}
	// VerifyOnRead requires the actual page check even when an immutable cache
	// carries earlier proof. Record verification alone never substitutes for it.
	if lease == nil || verify || (t.pager != nil && t.pager.VerifyOnRead()) {
		state.PageChecksumVerified = false
	}
	verifiedNow, err := validateLeafLogNodeIntoWithState(n, data, ref.Log, t.shouldVerifyLeafRefChecksum(), false, state)
	if err != nil {
		return err
	}
	if verifiedNow && state.RecordChecksumVerified && state.CacheEntryPresent && lease != nil {
		if marker, ok := lease.(LeafLogPageViewChecksumMarker); ok {
			marker.MarkLeafLogPageViewChecksumVerified()
		}
	}
	if keyScratch != nil {
		n.SetFixedKeyScratch(keyScratch)
	}
	return nil
}
