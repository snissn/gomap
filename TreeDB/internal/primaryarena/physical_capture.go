package primaryarena

import (
	"crypto/sha256"
	"fmt"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"unsafe"
)

// CapturePhysicalPagesOrdinary walks the exact immutable record closures. The
// caller owns independent incoming handles throughout export. No allocated but
// unowned or private destination becomes part of the captured projection.
// This is an ordinary snapshot operation, never a bounded native consume helper.
func (a *Arena) CapturePhysicalPagesOrdinary(extent uint64, roots []Ref) ([]bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if extent < a.firstBank || extent > a.next {
		return nil, ErrFormat
	}
	pages := make([]bool, int(extent))
	pages[0] = true
	seenReadRoots := make(map[uint64]bool)
	var visit func(Ref) error
	visit = func(ref Ref) error {
		local := Local(ref.PageID)
		readRoot := a.capsuleFormat && local >= pager.PrimaryReadRootLocalBaseV6
		if !IsPage(ref.PageID) || (!readRoot && local >= extent) {
			return ErrFormat
		}
		if (readRoot && seenReadRoots[local]) || (!readRoot && pages[local]) {
			return nil
		}
		b := a.valid(ref)
		if b == nil || !b.sealed || b.privatePublication || sha256.Sum256(b.image) != b.digest || !page.VerifyChecksumNonMutating(b.image) {
			return fmt.Errorf("primary capture bank %d: sealed=%t private=%t: %w", ref.PageID, b != nil && b.sealed, b != nil && b.privatePublication, ErrFormat)
		}
		if readRoot {
			seenReadRoots[local] = true
		} else {
			pages[local] = true
		}
		a.counters.EdgeReads++
		a.counters.Bytes += 4*page.PageSize + 2*uint64(unsafe.Sizeof(bank{}))
		if b.class == Directory {
			for slot := 0; slot < groupSize; slot++ {
				g := b.groups[0]
				entry, e := b.directory.ClassEntry(uint16(slot))
				if g == nil {
					// An independently complete empty directory has no component
					// custody group. Its physical class table must also be empty.
					if e == nil && !entry.InlineAbsence() {
						return ErrFormat
					}
					continue
				}
				if g.owner != a || g.refs == 0 || (g.base != nil && (g.base.owner != a || g.base.refs == 0 || g.base.base != nil)) {
					return ErrFormat
				}
				child := g.componentAt(slot)
				a.counters.EdgeReads++
				a.counters.Bytes += 2 * uint64(unsafe.Sizeof(Ref{}))
				if child == (Ref{}) {
					if e == nil && !entry.InlineAbsence() {
						return ErrFormat
					}
					continue
				}
				if e != nil || entry.Operand.Ref.Page != child.PageID {
					return ErrFormat
				}
				if e = visit(child); e != nil {
					return e
				}
			}
		}
		{
			for _, child := range b.edges {
				a.counters.EdgeReads++
				a.counters.Bytes += 2 * uint64(unsafe.Sizeof(Ref{}))
				if e := visit(child); e != nil {
					return e
				}
			}
		}
		return nil
	}
	for _, root := range roots {
		if root.PageID != 0 {
			if e := visit(root); e != nil {
				return nil, e
			}
		}
	}
	a.counters.Bytes += uint64(len(pages)) * 2
	return pages, nil
}
