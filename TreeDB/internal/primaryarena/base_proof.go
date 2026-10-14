package primaryarena

import (
	"bytes"
	"crypto/sha256"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// PinnedBaseSourceV5 supplies actual immutable DATA bytes. Its independent
// registry/generation lease must remain alive through preparation, fresh consume
// and charged Finish. A proof cannot acquire or extend this lease implicitly.
type PinnedBaseSourceV5 interface {
	BaseReadHeaderSizeV5() uint64
	BaseGenerationV5() *freelist.FreelistGenerationV1
	BaseReadAliveV5() bool
	BaseReadPageV5(uint64, []byte, *iterator.OrdinalScanWork) (bool, error)
}

// BaseKeyProofV5 is prepared immutable DATA authority, never current directory
// authority. All fields are private and every selected operand is classified,
// checksummed and searched from the actual pinned generation before ready.
type BaseKeyProofV5 struct {
	source       PinnedBaseSourceV5
	generation   *freelist.FreelistGenerationV1
	base         node.PrimaryOperand
	sequence     uint64
	key          []byte
	revision     page.EntryRevision
	classifier   freelist.SnapshotPageClassifierV1
	pageID       uint64
	image        [page.PageSize]byte
	phase, depth uint8
	lo, hi       int
	ready        bool
}

func PrepareBaseKeyProofV5(source PinnedBaseSourceV5, base node.PrimaryOperand, sequence uint64, key []byte, expected page.EntryRevision, w *iterator.OrdinalScanWork) (*BaseKeyProofV5, bool, error) {
	cost := 2*uint64(unsafe.Sizeof(BaseKeyProofV5{})) + 2*uint64(len(key)) + 2*uint64(unsafe.Sizeof(freelist.FreelistGenerationV1{}))
	if source != nil {
		cost += source.BaseReadHeaderSizeV5()
	}
	if !reserve(w, 1, cost) {
		return nil, false, nil
	}
	if source == nil || !source.BaseReadAliveV5() || base.Ref.Kind != page.ChildRefPage || IsPage(base.Ref.Page) || base.Ref.Page < 2 || base.Digest == ([32]byte{}) || sequence == 0 || expected == 0 {
		return nil, false, ErrStale
	}
	generation := source.BaseGenerationV5()
	if generation == nil || generation.CommitSeq() == 0 || generation.CommitSeq() == ^uint64(0) {
		return nil, false, ErrStale
	}
	c := &BaseKeyProofV5{source: source, generation: generation, base: base, sequence: sequence, key: bytes.Clone(key), revision: expected, pageID: base.Ref.Page}
	return c, true, nil
}

// Step makes finite progress on one immutable base page/probe at a time. An
// exhausted return retains only private copied page/key storage and the genuine
// source lease. No mutable head or old neighboring directory is authority.
func (c *BaseKeyProofV5) Step(w *iterator.OrdinalScanWork) (ready, progressed bool, err error) {
	if c == nil || c.source == nil {
		return false, false, ErrStale
	}
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(freelist.FreelistGenerationV1{}))+c.source.BaseReadHeaderSizeV5()) {
		return false, false, nil
	}
	if !c.source.BaseReadAliveV5() || c.source.BaseGenerationV5() != c.generation {
		return false, false, ErrStale
	}
	if c.ready {
		return true, false, nil
	}
	switch c.phase {
	case 0:
		ok, e := c.classifier.Reset(c.generation, c.pageID, c.generation.CommitSeq()+1, w)
		if !ok || e != nil {
			return false, false, e
		}
		c.phase = 1
	case 1:
		ready, progress, unused, e := c.classifier.Step(w)
		if e != nil || unused {
			if e == nil {
				e = ErrStale
			}
			return false, progress, e
		}
		if ready {
			c.phase = 2
		}
		return false, progress, nil
	case 2:
		// Admit the actual page copy before acquiring its borrowed mapping view.
		if !reserve(w, 1, 2*page.PageSize) {
			return false, false, nil
		}
		ok, e := c.source.BaseReadPageV5(c.pageID, c.image[:], w)
		if !ok || e != nil {
			return false, false, e
		}
		c.phase = 3
	case 3:
		if !reserve(w, 1, 4*page.PageSize+2*uint64(unsafe.Sizeof(node.Node{}))) {
			return false, false, nil
		}
		if !page.VerifyChecksumNonMutating(c.image[:]) || page.DecodeHeader(c.image[:]).PageID != c.pageID {
			return false, false, ErrFormat
		}
		if c.depth == 0 && sha256.Sum256(c.image[:]) != c.base.Digest {
			return false, false, ErrFormat
		}
		n := node.NewNode(c.image[:])
		if n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal {
			return false, false, ErrFormat
		}
		c.lo = 0
		c.hi = int(n.Count())
		c.phase = 4
	case 4:
		if !reserve(w, 1, 2*page.PageSize+2*uint64(len(c.key))+2*uint64(unsafe.Sizeof(node.Node{}))) {
			return false, false, nil
		}
		n := node.NewNode(c.image[:])
		if c.lo < c.hi {
			mid := c.lo + (c.hi-c.lo)/2
			var key []byte
			var e error
			if n.Type() == page.PageTypeInternal {
				key, _, e = n.GetInternalEntryRefView(uint16(mid))
			} else {
				key, _, _, _, _, e = n.GetLeafEntryViewWithRevision(uint16(mid))
			}
			if e != nil {
				return false, false, e
			}
			cmp := bytes.Compare(key, c.key)
			if cmp < 0 || (n.Type() == page.PageTypeInternal && cmp == 0) {
				c.lo = mid + 1
			} else {
				c.hi = mid
			}
		} else {
			c.phase = 5
		}
	case 5:
		if !reserve(w, 1, 2*page.PageSize+2*uint64(len(c.key))+2*uint64(unsafe.Sizeof(node.Node{}))) {
			return false, false, nil
		}
		n := node.NewNode(c.image[:])
		if n.Type() == page.PageTypeInternal {
			if n.Count() == 0 || c.depth >= 49 {
				return false, false, ErrFormat
			}
			slot := max(c.lo-1, 0)
			_, child, e := n.GetInternalEntryRefView(uint16(slot))
			// External leaves need their genuine bounded frame reader; never invoke an
			// allocating ordinary reader under a native scalar reservation.
			if e != nil {
				return false, false, e
			}
			if child.Kind != page.ChildRefPage || IsPage(child.Page) || child.Page < 2 {
				return false, false, ErrFormat
			}
			c.pageID = child.Page
			c.depth++
			c.phase = 0
		} else {
			if c.lo >= int(n.Count()) {
				return false, false, ErrStale
			}
			key, _, _, flags, revision, e := n.GetLeafEntryViewWithRevision(uint16(c.lo))
			if e != nil {
				return false, false, e
			}
			if !bytes.Equal(key, c.key) || flags&node.FlagTombstone != 0 || revision != c.revision {
				return false, false, ErrStale
			}
			c.ready = true
			return true, true, nil
		}
	default:
		return false, false, ErrFormat
	}
	return false, true, nil
}

func (c *BaseKeyProofV5) matches(base node.PrimaryOperand, sequence uint64, key []byte, revision page.EntryRevision, w *iterator.OrdinalScanWork) (bool, bool) {
	if c == nil || c.source == nil {
		return false, true
	}
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(freelist.FreelistGenerationV1{}))+c.source.BaseReadHeaderSizeV5()+2*uint64(len(c.key))+2*uint64(len(key))) {
		return false, false
	}
	return c.ready && c.source.BaseReadAliveV5() && c.source.BaseGenerationV5() == c.generation && c.base == base && c.sequence == sequence && c.revision == revision && bytes.Equal(c.key, key), true
}
