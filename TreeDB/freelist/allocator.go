package freelist

import (
	"encoding/binary"
	"errors"
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

	// preferAppend makes Alloc ignore the freelist and allocate new pages by
	// extending the file. This improves locality at the cost of reclaiming space
	// later via vacuum.
	preferAppend bool
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
			id, err := a.pager.Alloc(count - len(ids))
			if err != nil {
				return ids, err
			}
			for i := 0; i < count-len(ids); i++ {
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
			body := page.DecodeFreelistBody(data[page.PageHeaderSize:], 0)
			next := body.NextPageID

			recycled := a.head
			a.head = next
			a.pager.MarkUnverified(recycled)
			a.lastAlloc = recycled
			ids = append(ids, recycled)
			continue
		}

		body := page.DecodeFreelistBody(data[page.PageHeaderSize:], uint16(countFree))
		for countFree > 0 && len(ids) < count {
			countFree--
			id := body.FreeIDs[countFree]
			slotOff := page.PageHeaderSize + 8 + countFree*8
			clear(data[slotOff : slotOff+8])
			a.pager.MarkUnverified(id)
			a.lastAlloc = id
			ids = append(ids, id)
		}

		body.FreeIDs = body.FreeIDs[:countFree]
		body.Encode(data[page.PageHeaderSize:])
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
		}
		return id, err
	}

	if a.head == 0 {
		id, err := a.pager.Alloc(1)
		if err == nil {
			a.lastAlloc = id
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

	count := n.Count()
	if count > 0 {
		body := page.DecodeFreelistBody(data[page.PageHeaderSize:], count)
		id := body.FreeIDs[count-1]

		target := hint
		if target == 0 {
			target = a.lastAlloc
		}

		if a.regionPages > 0 && a.regionRadius > 0 && target != 0 {
			targetRegion := target / a.regionPages
			idx := -1
			for i := int(count) - 1; i >= 0; i-- {
				candidate := body.FreeIDs[i]
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
				lastIdx := int(count) - 1
				if idx != lastIdx {
					body.FreeIDs[idx] = body.FreeIDs[lastIdx]
				}
				body.FreeIDs = body.FreeIDs[:lastIdx]
				body.Encode(data[page.PageHeaderSize:])
				slotOff := page.PageHeaderSize + 8 + lastIdx*8
				clear(data[slotOff : slotOff+8])
				n.SetCount(count - 1)
				n.UpdateChecksum()

				a.pager.MarkUnverified(id)
				a.lastAlloc = id
				return id, nil
			}
		}

		lastIdx := int(count) - 1
		body.FreeIDs = body.FreeIDs[:lastIdx]
		body.Encode(data[page.PageHeaderSize:])
		slotOff := page.PageHeaderSize + 8 + lastIdx*8
		clear(data[slotOff : slotOff+8])
		n.SetCount(count - 1)
		n.UpdateChecksum()

		a.pager.MarkUnverified(id)
		a.lastAlloc = id
		return id, nil
	}

	body := page.DecodeFreelistBody(data[page.PageHeaderSize:], 0)
	next := body.NextPageID

	recycled := a.head
	a.head = next

	a.pager.MarkUnverified(recycled)
	a.lastAlloc = recycled
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
		return a.initHead(id, 0)
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
		n.UpdateChecksum()
		return nil
	}

	// Head is full.
	// Use `id` as NEW Head.
	return a.initHead(id, a.head)
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
