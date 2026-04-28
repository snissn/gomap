package freelist

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
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
}

func freelistNextPageID(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data[page.PageHeaderSize : page.PageHeaderSize+8])
}

func freelistIDAt(data []byte, idx int) uint64 {
	off := page.PageHeaderSize + 8 + idx*8
	return binary.LittleEndian.Uint64(data[off : off+8])
}

func setFreelistIDAt(data []byte, idx int, id uint64) {
	off := page.PageHeaderSize + 8 + idx*8
	binary.LittleEndian.PutUint64(data[off:off+8], id)
}

func clearFreelistIDAt(data []byte, idx int) {
	off := page.PageHeaderSize + 8 + idx*8
	clear(data[off : off+8])
}

// TestHookFreeBeforeChecksum is a test-only hook that fires after a freelist
// entry is written but before the page checksum is updated. It should remain
// nil in production.
var TestHookFreeBeforeChecksum func()

// Stats reports freelist metadata under the allocator lock.
type Stats struct {
	Head             uint64
	Pages            uint64
	FreeIDs          uint64
	AllocPages       uint64
	AppendAllocPages uint64
	ReuseAllocPages  uint64
	FreePages        uint64
}

// ReclaimablePages returns the total number of reclaimable pages tracked in the freelist.
func (s Stats) ReclaimablePages() uint64 {
	return s.Pages + s.FreeIDs
}

func New(p *pager.Pager, head uint64) *Allocator {
	return &Allocator{
		pager: p,
		head:  head,
	}
}

func (a *Allocator) Head() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.head
}

func (a *Allocator) SetHead(h uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.head = h
}

func (a *Allocator) SetPreferAppend(prefer bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.preferAppend = prefer
}

func (a *Allocator) SetFreelistRegion(pages uint64, radius int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if pages == 0 || radius <= 0 {
		a.regionPages = 0
		a.regionRadius = 0
		return
	}
	a.regionPages = pages
	a.regionRadius = radius
}

// AllocMany allocates up to count pages in one pass. It returns any allocated
// IDs even if an error occurs so callers can retire them.
func (a *Allocator) AllocMany(count int, hint uint64) ([]uint64, error) {
	if count <= 0 {
		return nil, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.regionPages > 0 && a.regionRadius > 0 {
		ids := make([]uint64, 0, count)
		var err error
		for len(ids) < count {
			id, allocErr := a.allocLocked(hint)
			if allocErr != nil {
				err = allocErr
				break
			}
			ids = append(ids, id)
			hint = id
		}
		return ids, err
	}

	ids := make([]uint64, 0, count)
	for len(ids) < count {
		if a.preferAppend || a.head == 0 {
			allocCount := count - len(ids)
			id, err := a.pager.Alloc(allocCount)
			if err != nil {
				return ids, err
			}
			a.stats.AllocPages += uint64(allocCount)
			a.stats.AppendAllocPages += uint64(allocCount)
			for i := 0; i < allocCount; i++ {
				ids = append(ids, id+uint64(i))
			}
			a.lastAlloc = ids[len(ids)-1]
			return ids, nil
		}

		data, err := a.pager.GetForWrite(a.head)
		if err != nil {
			return ids, err
		}
		n := node.NewNode(data)
		if !n.VerifyChecksum() {
			return ids, errors.New("freelist head corrupted (AllocMany)")
		}
		if n.Type() != page.PageTypeFreelist {
			return ids, errors.New("invalid freelist page type")
		}

		countFree := int(n.Count())
		if countFree == 0 {
			next := freelistNextPageID(data)

			recycled := a.head
			a.head = next
			a.pager.MarkUnverified(recycled)
			a.lastAlloc = recycled
			a.stats.AllocPages++
			a.stats.ReuseAllocPages++
			ids = append(ids, recycled)
			continue
		}

		for countFree > 0 && len(ids) < count {
			idx := countFree - 1
			id := freelistIDAt(data, idx)
			clearFreelistIDAt(data, idx)
			a.pager.MarkUnverified(id)
			a.lastAlloc = id
			a.stats.AllocPages++
			a.stats.ReuseAllocPages++
			ids = append(ids, id)
			countFree--
		}

		n.SetCount(uint16(countFree))
		n.UpdateChecksum()
	}
	return ids, nil
}

func (a *Allocator) allocLocked(hint uint64) (uint64, error) {
	if a.preferAppend {
		id, err := a.pager.Alloc(1)
		if err == nil {
			a.lastAlloc = id
			a.stats.AllocPages++
			a.stats.AppendAllocPages++
		}
		return id, err
	}

	if a.head == 0 {
		id, err := a.pager.Alloc(1)
		if err == nil {
			a.lastAlloc = id
			a.stats.AllocPages++
			a.stats.AppendAllocPages++
		}
		return id, err
	}

	data, err := a.pager.GetForWrite(a.head)
	if err != nil {
		return 0, err
	}
	n := node.NewNode(data)
	if !n.VerifyChecksum() {
		return 0, errors.New("freelist head corrupted (Alloc)")
	}
	if n.Type() != page.PageTypeFreelist {
		return 0, errors.New("invalid freelist page type")
	}

	count := int(n.Count())
	if count > 0 {
		id := freelistIDAt(data, count-1)

		target := hint
		if target == 0 {
			target = a.lastAlloc
		}

		if a.regionPages > 0 && a.regionRadius > 0 && target != 0 {
			targetRegion := target / a.regionPages
			idx := -1
			for i := count - 1; i >= 0; i-- {
				candidate := freelistIDAt(data, i)
				candidateRegion := candidate / a.regionPages
				var diff uint64
				if candidateRegion >= targetRegion {
					diff = candidateRegion - targetRegion
				} else {
					diff = targetRegion - candidateRegion
				}
				if diff <= uint64(a.regionRadius) {
					idx = i
					id = candidate
					break
				}
			}
			if idx >= 0 {
				lastIdx := count - 1
				if idx != lastIdx {
					setFreelistIDAt(data, idx, freelistIDAt(data, lastIdx))
				}
				clearFreelistIDAt(data, lastIdx)
				n.SetCount(uint16(lastIdx))
				n.UpdateChecksum()

				a.pager.MarkUnverified(id)
				a.lastAlloc = id
				a.stats.AllocPages++
				a.stats.ReuseAllocPages++
				return id, nil
			}
		}

		lastIdx := count - 1
		clearFreelistIDAt(data, lastIdx)
		n.SetCount(uint16(lastIdx))
		n.UpdateChecksum()

		a.pager.MarkUnverified(id)
		a.lastAlloc = id
		a.stats.AllocPages++
		a.stats.ReuseAllocPages++
		return id, nil
	}

	next := freelistNextPageID(data)

	recycled := a.head
	a.head = next

	a.pager.MarkUnverified(recycled)
	a.lastAlloc = recycled
	a.stats.AllocPages++
	a.stats.ReuseAllocPages++
	return recycled, nil
}

// Alloc allocates a single page.
// hint is a page ID that the caller would like the new page to be close to.
// If hint is 0, the allocator uses its own heuristics (e.g. lastAlloc).
func (a *Allocator) Alloc(hint uint64) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.allocLocked(hint)
}

// Free adds a page to the freelist.
func (a *Allocator) Free(id uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if id == 0 {
		return errors.New("cannot free page 0")
	}

	if a.head == 0 {
		// Start new list with this page
		if err := a.initHead(id, 0); err != nil {
			return err
		}
		a.stats.FreePages++
		return nil
	}

	// Load Head
	data, err := a.pager.GetForWrite(a.head)
	if err != nil {
		return err
	}
	n := node.NewNode(data)
	if !n.VerifyChecksum() {
		return errors.New("freelist head corrupted (Free)")
	}
	if n.Type() != page.PageTypeFreelist {
		return errors.New("invalid freelist page type")
	}

	count := n.Count()
	if count < page.MaxFreeIDs {
		// Append without reallocating: offset = header + next(8) + count*8.
		slotOff := page.PageHeaderSize + 8 + int(count)*8
		binary.LittleEndian.PutUint64(data[slotOff:slotOff+8], id)
		n.SetCount(count + 1)
		if TestHookFreeBeforeChecksum != nil {
			TestHookFreeBeforeChecksum()
		}
		n.UpdateChecksum()
		a.stats.FreePages++
		return nil
	}

	// Head is full.
	// Use `id` as NEW Head.
	if err := a.initHead(id, a.head); err != nil {
		return err
	}
	a.stats.FreePages++
	return nil
}

func (a *Allocator) initHead(id, next uint64) error {
	data, err := a.pager.GetForWrite(id)
	if err != nil {
		return err
	}

	n := node.NewNode(data)
	n.SetPageID(id)
	n.SetType(page.PageTypeFreelist)
	n.SetCount(0)

	body := page.FreelistPageBody{
		NextPageID: next,
		FreeIDs:    nil,
	}
	body.Encode(data[page.PageHeaderSize:])

	n.UpdateChecksum()

	a.head = id
	return nil
}

// Stats returns freelist page counts while holding the allocator lock to avoid
// concurrent freelist mutations.
func (a *Allocator) Stats(pageLimit uint64) (Stats, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out, err := readStatsLocked(a.pager, a.head, pageLimit)
	out.AllocPages = a.stats.AllocPages
	out.AppendAllocPages = a.stats.AppendAllocPages
	out.ReuseAllocPages = a.stats.ReuseAllocPages
	out.FreePages = a.stats.FreePages
	return out, err
}

func readStatsLocked(p *pager.Pager, head uint64, pageLimit uint64) (Stats, error) {
	out := Stats{Head: head}
	if head == 0 || pageLimit == 0 {
		return out, nil
	}

	remaining := pageLimit
	cur := head
	for cur != 0 && remaining > 0 {
		remaining--

		data, err := p.ReadPage(cur)
		if err != nil {
			return out, err
		}

		n := node.NewNode(data)
		if !n.VerifyChecksum() {
			return out, fmt.Errorf("freelist checksum mismatch on page %d", cur)
		}
		if n.Type() != page.PageTypeFreelist {
			return out, fmt.Errorf("invalid freelist page type %d on page %d", n.Type(), cur)
		}

		out.Pages++
		out.FreeIDs += uint64(n.Count())

		body := page.DecodeFreelistBody(data[page.PageHeaderSize:], n.Count())
		cur = body.NextPageID
	}

	if remaining == 0 && cur != 0 {
		return out, fmt.Errorf("freelist walk exceeded page limit (%d)", pageLimit)
	}

	return out, nil
}
