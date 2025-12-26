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

// Alloc allocates a single page.
// hint is a page ID that the caller would like the new page to be close to.
// If hint is 0, the allocator uses its own heuristics (e.g. lastAlloc).
func (a *Allocator) Alloc(hint uint64) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

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

	// Read Head
	data, err := a.pager.GetForWrite(a.head)
	if err != nil {
		return 0, err
	}

	// Decode
	n := node.NewNode(data) // Helper for header
	if !n.VerifyChecksum() {
		return 0, errors.New("freelist head corrupted (Alloc)")
	}
	if n.Type() != page.PageTypeFreelist {
		return 0, errors.New("invalid freelist page type")
	}

	count := n.Count()
	if count > 0 {
		// Pop from body
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

				// This page may have been verified under a previous incarnation. Ensure
				// it is treated as unverified when reused.
				a.pager.MarkUnverified(id)
				a.lastAlloc = id
				return id, nil
			}
		}

		// Update page body and header.
		lastIdx := int(count) - 1
		body.FreeIDs = body.FreeIDs[:lastIdx]
		body.Encode(data[page.PageHeaderSize:])
		slotOff := page.PageHeaderSize + 8 + lastIdx*8
		clear(data[slotOff : slotOff+8])
		n.SetCount(count - 1)
		n.UpdateChecksum()

		// This page may have been verified under a previous incarnation. Ensure
		// it is treated as unverified when reused.
		a.pager.MarkUnverified(id)
		a.lastAlloc = id
		return id, nil
	}

	// Count == 0. This page is empty.
	// We consume this page itself.
	// Next head = Body.NextPageID.
	body := page.DecodeFreelistBody(data[page.PageHeaderSize:], 0) // Count 0
	next := body.NextPageID

	recycled := a.head
	a.head = next

	// This page is being repurposed; ensure it is treated as unverified.
	a.pager.MarkUnverified(recycled)
	a.lastAlloc = recycled
	return recycled, nil
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
