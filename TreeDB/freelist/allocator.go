package freelist

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
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

var errCannotFreePageZero = errors.New("cannot free page 0")

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

type regionSlotHeap []int

func (h *regionSlotHeap) init() {
	for parent := len(*h)/2 - 1; parent >= 0; parent-- {
		h.down(parent)
	}
}

func (h *regionSlotHeap) push(slot int) {
	*h = append(*h, slot)
	child := len(*h) - 1
	for child > 0 {
		parent := (child - 1) / 2
		if (*h)[parent] >= (*h)[child] {
			break
		}
		(*h)[parent], (*h)[child] = (*h)[child], (*h)[parent]
		child = parent
	}
}

func (h *regionSlotHeap) pop() int {
	last := len(*h) - 1
	maximum := (*h)[0]
	(*h)[0] = (*h)[last]
	*h = (*h)[:last]
	if len(*h) > 0 {
		h.down(0)
	}
	return maximum
}

func (h *regionSlotHeap) down(parent int) {
	for {
		left := parent*2 + 1
		if left >= len(*h) {
			return
		}
		child := left
		right := left + 1
		if right < len(*h) && (*h)[right] > (*h)[left] {
			child = right
		}
		if (*h)[parent] >= (*h)[child] {
			return
		}
		(*h)[parent], (*h)[child] = (*h)[child], (*h)[parent]
		parent = child
	}
}

// regionSlotIndex finds the highest current freelist slot in a region range.
// Per-region heaps use lazy deletion while the segment tree indexes their
// current maxima.
type regionSlotIndex struct {
	regions     []uint64
	buckets     []regionSlotHeap
	slotBuckets []int
	tree        []int
	treeBase    int
}

func newRegionSlotIndex(data []byte, count int, regionPages uint64) *regionSlotIndex {
	slotRegions := make([]uint64, count)
	regions := make([]uint64, count)
	for slot := 0; slot < count; slot++ {
		region := freelistIDAt(data, slot) / regionPages
		slotRegions[slot] = region
		regions[slot] = region
	}
	sort.Slice(regions, func(i, j int) bool { return regions[i] < regions[j] })
	unique := regions[:0]
	for _, region := range regions {
		if len(unique) == 0 || unique[len(unique)-1] != region {
			unique = append(unique, region)
		}
	}
	regions = unique

	bucketByRegion := make(map[uint64]int, len(regions))
	for bucket, region := range regions {
		bucketByRegion[region] = bucket
	}
	idx := &regionSlotIndex{
		regions:     regions,
		buckets:     make([]regionSlotHeap, len(regions)),
		slotBuckets: make([]int, count),
		treeBase:    1,
	}
	for idx.treeBase < len(regions) {
		idx.treeBase *= 2
	}
	idx.tree = make([]int, idx.treeBase*2)
	for i := range idx.tree {
		idx.tree[i] = -1
	}
	for slot, region := range slotRegions {
		bucket := bucketByRegion[region]
		idx.slotBuckets[slot] = bucket
		idx.buckets[bucket] = append(idx.buckets[bucket], slot)
	}
	for bucket := range idx.buckets {
		idx.buckets[bucket].init()
		idx.tree[idx.treeBase+bucket] = idx.buckets[bucket][0]
	}
	for treeIdx := idx.treeBase - 1; treeIdx > 0; treeIdx-- {
		idx.tree[treeIdx] = max(idx.tree[treeIdx*2], idx.tree[treeIdx*2+1])
	}
	return idx
}

func (idx *regionSlotIndex) highestInRange(targetRegion uint64, radius int) int {
	distance := uint64(radius)
	low := uint64(0)
	if targetRegion > distance {
		low = targetRegion - distance
	}
	high := targetRegion + distance
	if high < targetRegion {
		high = ^uint64(0)
	}
	left := sort.Search(len(idx.regions), func(i int) bool { return idx.regions[i] >= low })
	right := sort.Search(len(idx.regions), func(i int) bool { return idx.regions[i] > high })
	if left == right {
		return -1
	}
	return idx.rangeMax(left, right)
}

func (idx *regionSlotIndex) rangeMax(left, right int) int {
	best := -1
	for left, right = left+idx.treeBase, right+idx.treeBase; left < right; left, right = left/2, right/2 {
		if left%2 == 1 {
			best = max(best, idx.tree[left])
			left++
		}
		if right%2 == 1 {
			right--
			best = max(best, idx.tree[right])
		}
	}
	return best
}

func (idx *regionSlotIndex) remove(slot, lastSlot int) {
	removedBucket := idx.slotBuckets[slot]
	lastBucket := idx.slotBuckets[lastSlot]
	idx.slotBuckets[lastSlot] = -1
	if slot != lastSlot {
		idx.slotBuckets[slot] = lastBucket
		if lastBucket != removedBucket {
			idx.buckets[lastBucket].push(slot)
		}
	}
	idx.refreshBucket(removedBucket)
	if lastBucket != removedBucket {
		idx.refreshBucket(lastBucket)
	}
}

func (idx *regionSlotIndex) refreshBucket(bucket int) {
	h := &idx.buckets[bucket]
	for len(*h) > 0 && idx.slotBuckets[(*h)[0]] != bucket {
		h.pop()
	}
	maximum := -1
	if len(*h) > 0 {
		maximum = (*h)[0]
	}
	treeIdx := idx.treeBase + bucket
	idx.tree[treeIdx] = maximum
	for treeIdx /= 2; treeIdx > 0; treeIdx /= 2 {
		idx.tree[treeIdx] = max(idx.tree[treeIdx*2], idx.tree[treeIdx*2+1])
	}
}

// TestHookFreeBeforeChecksum is a test-only hook that fires after a freelist
// entry is written but before the page checksum is updated. It should remain
// nil in production.
var TestHookFreeBeforeChecksum func()

// TestHookFreeManyBeforeChecksum is a test-only hook that fires once for each
// freelist page FreeMany updates, immediately before its checksum is written.
// It should remain nil in production.
var TestHookFreeManyBeforeChecksum func()

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
	a.stats.Pages = 0
	a.stats.FreeIDs = 0
}

// RefreshStats seeds the cheap freelist counters from the on-disk freelist
// chain. Callers use this after opening an existing DB so Counters can report
// current reclaimable debt without walking the chain on every maintenance tick.
func (a *Allocator) RefreshStats(pageLimit uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	out, err := readStatsLocked(a.pager, a.head, pageLimit)
	if err != nil {
		return err
	}
	a.stats.Pages = out.Pages
	a.stats.FreeIDs = out.FreeIDs
	return nil
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
		return a.allocManyRegionLocked(count, hint)
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
			if a.stats.Pages > 0 {
				a.stats.Pages--
			}
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
			if a.stats.FreeIDs > 0 {
				a.stats.FreeIDs--
			}
			ids = append(ids, id)
			countFree--
		}

		n.SetCount(uint16(countFree))
		n.UpdateChecksum()
	}
	return ids, nil
}

// allocManyRegionLocked consumes all region-preferred entries from a verified
// head page before falling back to LIFO entries from that same page. Region
// preference is evaluated from the incoming hint (or last allocation); each
// returned ID still advances lastAlloc for the next head-page selection.
func (a *Allocator) allocManyRegionLocked(count int, hint uint64) ([]uint64, error) {
	ids := make([]uint64, 0, count)
	target := hint
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
			if a.stats.Pages > 0 {
				a.stats.Pages--
			}
			ids = append(ids, recycled)
			target = recycled
			continue
		}

		start := len(ids)
		if target == 0 {
			target = a.lastAlloc
		}
		regionSlots := newRegionSlotIndex(data, countFree, a.regionPages)
		for countFree > 0 && len(ids) < count {
			slot := -1
			if target != 0 {
				slot = regionSlots.highestInRange(target/a.regionPages, a.regionRadius)
			}
			lastSlot := countFree - 1
			if slot < 0 {
				slot = lastSlot
			}
			id := freelistIDAt(data, slot)
			if slot != lastSlot {
				setFreelistIDAt(data, slot, freelistIDAt(data, lastSlot))
			}
			clearFreelistIDAt(data, lastSlot)
			regionSlots.remove(slot, lastSlot)
			ids = append(ids, id)
			countFree--
			target = id
		}

		n.SetCount(uint16(countFree))
		n.UpdateChecksum()
		for _, id := range ids[start:] {
			a.recordReuseAllocation(id)
		}
	}
	return ids, nil
}

func (a *Allocator) recordReuseAllocation(id uint64) {
	a.pager.MarkUnverified(id)
	a.lastAlloc = id
	a.stats.AllocPages++
	a.stats.ReuseAllocPages++
	if a.stats.FreeIDs > 0 {
		a.stats.FreeIDs--
	}
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
				if a.stats.FreeIDs > 0 {
					a.stats.FreeIDs--
				}
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
		if a.stats.FreeIDs > 0 {
			a.stats.FreeIDs--
		}
		return id, nil
	}

	next := freelistNextPageID(data)

	recycled := a.head
	a.head = next

	a.pager.MarkUnverified(recycled)
	a.lastAlloc = recycled
	a.stats.AllocPages++
	a.stats.ReuseAllocPages++
	if a.stats.Pages > 0 {
		a.stats.Pages--
	}
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
		return errCannotFreePageZero
	}

	if a.head == 0 {
		// Start new list with this page
		if err := a.initHead(id, 0); err != nil {
			return err
		}
		a.stats.FreePages++
		a.stats.Pages++
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
		a.stats.FreeIDs++
		return nil
	}

	// Head is full.
	// Use `id` as NEW Head.
	if err := a.initHead(id, a.head); err != nil {
		return err
	}
	a.stats.FreePages++
	a.stats.Pages++
	return nil
}

// FreeMany adds multiple pages to the freelist while holding the allocator
// lock once. It validates page zero before mutating state. Duplicate IDs are
// intentionally accepted and recorded exactly as repeated calls to Free would
// record them; production callers remain responsible for avoiding double free.
//
// An I/O error after a valid prefix may leave that prefix freed, matching the
// per-ID Free loop semantics. Callers that need prefix accounting continue to
// use Free one ID at a time.
func (a *Allocator) FreeMany(ids []uint64) error {
	for _, id := range ids {
		if id == 0 {
			return errCannotFreePageZero
		}
	}
	if len(ids) == 0 {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	for len(ids) > 0 {
		if a.head == 0 {
			consumed, err := a.initHeadWithFreeIDs(ids[0], 0, ids[1:], true)
			if err != nil {
				return err
			}
			a.recordFreedPages(consumed, true)
			ids = ids[consumed:]
			continue
		}

		data, err := a.pager.GetForWrite(a.head)
		if err != nil {
			return err
		}
		n := node.NewNode(data)
		if !n.VerifyChecksum() {
			return errors.New("freelist head corrupted (FreeMany)")
		}
		if n.Type() != page.PageTypeFreelist {
			return errors.New("invalid freelist page type")
		}

		count := int(n.Count())
		if count >= page.MaxFreeIDs {
			consumed, err := a.initHeadWithFreeIDs(ids[0], a.head, ids[1:], true)
			if err != nil {
				return err
			}
			a.recordFreedPages(consumed, true)
			ids = ids[consumed:]
			continue
		}

		take := page.MaxFreeIDs - count
		if take > len(ids) {
			take = len(ids)
		}
		for i := 0; i < take; i++ {
			setFreelistIDAt(data, count+i, ids[i])
			n.SetCount(uint16(count + i + 1))
			if TestHookFreeBeforeChecksum != nil {
				TestHookFreeBeforeChecksum()
			}
		}
		if TestHookFreeManyBeforeChecksum != nil {
			TestHookFreeManyBeforeChecksum()
		}
		n.UpdateChecksum()
		a.recordFreedPages(take, false)
		ids = ids[take:]
	}
	return nil
}

func (a *Allocator) initHeadWithFreeIDs(id, next uint64, ids []uint64, batchHook bool) (int, error) {
	take := len(ids)
	if take > page.MaxFreeIDs {
		take = page.MaxFreeIDs
	}
	data, err := a.pager.GetForWrite(id)
	if err != nil {
		return 0, err
	}
	n := node.NewNode(data)
	n.SetPageID(id)
	n.SetType(page.PageTypeFreelist)
	n.SetCount(0)
	body := page.FreelistPageBody{NextPageID: next}
	body.Encode(data[page.PageHeaderSize:])
	for i := 0; i < take; i++ {
		setFreelistIDAt(data, i, ids[i])
		n.SetCount(uint16(i + 1))
		if TestHookFreeBeforeChecksum != nil {
			TestHookFreeBeforeChecksum()
		}
	}
	if batchHook && TestHookFreeManyBeforeChecksum != nil {
		TestHookFreeManyBeforeChecksum()
	}
	n.UpdateChecksum()
	a.head = id
	return take + 1, nil
}

func (a *Allocator) recordFreedPages(count int, newHead bool) {
	a.stats.FreePages += uint64(count)
	if newHead {
		a.stats.Pages++
		count--
	}
	a.stats.FreeIDs += uint64(count)
}

func (a *Allocator) initHead(id, next uint64) error {
	_, err := a.initHeadWithFreeIDs(id, next, nil, false)
	return err
}

// Stats returns freelist page counts while holding the allocator lock to avoid
// concurrent freelist mutations.
func (a *Allocator) Stats(pageLimit uint64) (Stats, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out, err := readStatsLocked(a.pager, a.head, pageLimit)
	if err == nil {
		a.stats.Pages = out.Pages
		a.stats.FreeIDs = out.FreeIDs
	}
	out.AllocPages = a.stats.AllocPages
	out.AppendAllocPages = a.stats.AppendAllocPages
	out.ReuseAllocPages = a.stats.ReuseAllocPages
	out.FreePages = a.stats.FreePages
	return out, err
}

// Counters reports cheap in-memory allocator counters without walking freelist
// pages. Use Stats when callers explicitly need on-disk reclaimable page counts.
func (a *Allocator) Counters() Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Stats{
		Head:             a.head,
		Pages:            a.stats.Pages,
		FreeIDs:          a.stats.FreeIDs,
		AllocPages:       a.stats.AllocPages,
		AppendAllocPages: a.stats.AppendAllocPages,
		ReuseAllocPages:  a.stats.ReuseAllocPages,
		FreePages:        a.stats.FreePages,
	}
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
