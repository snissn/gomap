package freelist

import (
	"errors"
	"sync"

	"github.com/snissn/gomap/GeminiTreeDB/node"
	"github.com/snissn/gomap/GeminiTreeDB/page"
	"github.com/snissn/gomap/GeminiTreeDB/pager"
)

type Allocator struct {
	pager *pager.Pager
	head  uint64
	mu    sync.Mutex
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

// Alloc allocates a single page.
func (a *Allocator) Alloc() (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.head == 0 {
		return a.pager.Alloc(1)
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
		
		// Update page
		n.SetCount(count - 1)
		// No need to resize slice, just update count.
		// Encode back?
		// We only need to update header checksum if we rely on it.
		// And Body isn't changed structurally, just logical count.
		// But checksum covers body. Body bytes for popped item are stale.
		// We should zero them? Not strictly needed if Count is authority.
		// But checksum calculation covers them.
		// So we must Encode back or Zero.
		// Simpler: Just update Count and Checksum.
		// The garbage bytes at end are part of checksum.
		n.UpdateChecksum()
		return id, nil
	}
	
	// Count == 0. This page is empty.
	// We consume this page itself.
	// Next head = Body.NextPageID.
	body := page.DecodeFreelistBody(data[page.PageHeaderSize:], 0) // Count 0
	next := body.NextPageID
	
	recycled := a.head
	a.head = next
	
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
		// Append
		// We need to decode, append, encode.
		body := page.DecodeFreelistBody(data[page.PageHeaderSize:], count)
		body.FreeIDs = append(body.FreeIDs, id) // This might alloc new slice if capacity low?
		// Note: Decode creates slice of size `count`. Append will alloc.
		// We need to write it back to `data`.
		
		// Optimization: Decode just reads. We can write directly at offset?
		// Offset = Header + Next(8) + Count*8.
		// Write ID
		// Use simple binary put?
		// Need binary import?
		// Or use body.Encode logic.
		// Let's use body logic to be safe.
		body.Encode(data[page.PageHeaderSize:]) // Writes Next + Array
		// But Wait, `body.FreeIDs` has new item. `Encode` writes all.
		// Correct.
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
