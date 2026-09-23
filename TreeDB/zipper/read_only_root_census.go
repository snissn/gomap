package zipper

import (
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/page"
)

// ReadOnlyRootCensus counts the entire captured root, including empty leaves.
// It is useful when a pure-point delta has a bounded number of puts but some
// of their keys are assigned only after the command WAL append. The whole-root
// counts conservatively cover every leaf those puts could touch.
type ReadOnlyRootCensus struct {
	RootID           uint64
	Pages            uint64
	LeafPages        uint64
	LeafEntries      uint64
	InternalChildren uint64
	MaxDepth         uint32
	MaxKeyBytes      uint32
}

// ReadOnlyRootCensusLimits bound the walk before descending into another node.
// Every limit must be positive. The caller must separately admit the peak of
// one node read and the zipper's traversal scratch before calling this method.
type ReadOnlyRootCensusLimits struct {
	MaxPages   uint64
	MaxEntries uint64
	MaxDepth   uint32
}

func (z *Zipper) CensusRootReadOnly(rootID uint64, limits ReadOnlyRootCensusLimits) (ReadOnlyRootCensus, error) {
	result := ReadOnlyRootCensus{RootID: rootID}
	if limits.MaxPages == 0 || limits.MaxEntries == 0 || limits.MaxDepth == 0 {
		return result, ErrReadOnlyPrepareProfileLimit
	}
	if rootID == 0 {
		result.MaxDepth = 1
		return result, nil
	}
	scratch := z.acquireApplyScratch()
	defer z.releaseApplyScratch(scratch)
	var visit func(page.ChildRef, uint32) error
	visit = func(ref page.ChildRef, depth uint32) error {
		if depth > limits.MaxDepth || result.Pages >= limits.MaxPages {
			return ErrReadOnlyPrepareProfileLimit
		}
		result.Pages++
		if depth > result.MaxDepth {
			result.MaxDepth = depth
		}
		n, _, leafScratch, leafScratchRef, _, err := z.loadNodeRef(ref, scratch)
		if err != nil {
			return err
		}
		if leafScratchRef {
			defer releaseLeafPageScratch(scratch, leafScratch)
		}
		count := uint64(n.Count())
		if count > limits.MaxEntries-result.LeafEntries-result.InternalChildren {
			return ErrReadOnlyPrepareProfileLimit
		}
		switch n.Type() {
		case page.PageTypeLeaf, 0:
			result.LeafPages++
			result.LeafEntries += count
			for i := uint16(0); i < n.Count(); i++ {
				key, _, err := n.GetLeafKeyFlagsView(i)
				if err != nil {
					return err
				}
				if len(key) > math.MaxUint32 {
					return ErrReadOnlyPrepareProfileLimit
				}
				if uint32(len(key)) > result.MaxKeyBytes {
					result.MaxKeyBytes = uint32(len(key))
				}
			}
		case page.PageTypeInternal:
			result.InternalChildren += count
			for i := uint16(0); i < n.Count(); i++ {
				key, child, err := n.GetInternalEntryRefView(i)
				if err != nil {
					return err
				}
				if len(key) > math.MaxUint32 {
					return ErrReadOnlyPrepareProfileLimit
				}
				if uint32(len(key)) > result.MaxKeyBytes {
					result.MaxKeyBytes = uint32(len(key))
				}
				if err := visit(child, depth+1); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("zipper: invalid root census page type %d", n.Type())
		}
		return nil
	}
	return result, visit(page.PageChildRef(rootID), 1)
}

// PurePointOutputPageUpperBound applies the serial pure-point page bound to
// the whole captured tree. The caller must still prove pure puts, same root,
// two-child fit at the largest old/new key width, and serial apply.
func (c ReadOnlyRootCensus) PurePointOutputPageUpperBound(pointOps int) (uint64, error) {
	if pointOps < 0 || c.MaxDepth == 0 {
		return 0, ErrReadOnlyPrepareProfileLimit
	}
	return (ReadOnlyPrepareResult{
		RootID:                     c.RootID,
		ColdBuild:                  c.RootID == 0,
		PointOps:                   pointOps,
		TouchedOldLeafPages:        c.LeafPages,
		TouchedOldLeafEntries:      c.LeafEntries,
		TouchedOldInternalChildren: c.InternalChildren,
		MaxTouchedDepth:            c.MaxDepth,
		countTouchedOldEntries:     true,
	}).PurePointOutputPageUpperBound()
}
