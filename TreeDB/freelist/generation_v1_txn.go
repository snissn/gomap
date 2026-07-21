package freelist

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/snissn/gomap/TreeDB/page"
)

type FreelistGenerationV1 struct {
	generationID, commitSeq             uint64
	parentGenerationID, parentCommitSeq uint64
	highWater                           uint64
	root                                *stateNode
	ref                                 GenerationRefV1
	record                              ReservationRecordV1
	metadataPages                       []uint64
}

func NewFreelistGenerationV1(generationID, highWater uint64, free []uint64, retired map[uint64]uint64) (*FreelistGenerationV1, error) {
	if generationID == 0 || highWater < 2 {
		return nil, ErrGenerationFormat
	}
	g := &FreelistGenerationV1{generationID: generationID, commitSeq: generationID, highWater: highWater, root: &stateNode{}}
	seen := make(map[uint64]struct{}, len(free)+len(retired))
	for _, id := range free {
		if err := validateManagedPageID(id, highWater); err != nil {
			return nil, err
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, ErrGenerationFormat
		}
		seen[id] = struct{}{}
		g.root = mutateChunk(g.root, id>>freelistChunkShift, 0, func(c *stateChunk) { c.setFree(id&(freelistChunkSize-1), true) })
	}
	for id, seq := range retired {
		if err := validateManagedPageID(id, highWater); err != nil || seq == 0 {
			return nil, ErrGenerationFormat
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, ErrGenerationFormat
		}
		seen[id] = struct{}{}
		g.root = mutateChunk(g.root, id>>freelistChunkShift, 0, func(c *stateChunk) { c.retired[id&(freelistChunkSize-1)] = seq })
	}
	return g, g.Validate()
}

func MustNewFreelistGenerationV1(generationID, highWater uint64, free []uint64, retired map[uint64]uint64) *FreelistGenerationV1 {
	g, err := NewFreelistGenerationV1(generationID, highWater, free, retired)
	if err != nil {
		panic(err)
	}
	return g
}

func (g *FreelistGenerationV1) GenerationID() uint64 {
	if g == nil {
		return 0
	}
	return g.generationID
}
func (g *FreelistGenerationV1) CommitSeq() uint64 {
	if g == nil {
		return 0
	}
	return g.commitSeq
}
func (g *FreelistGenerationV1) HighWater() uint64 {
	if g == nil {
		return 0
	}
	return g.highWater
}
func (g *FreelistGenerationV1) FreeCount() uint64 {
	if g == nil || g.root == nil {
		return 0
	}
	return g.root.freeCount
}
func (g *FreelistGenerationV1) RetiredCount() uint64 {
	if g == nil || g.root == nil {
		return 0
	}
	return g.root.retiredCount
}
func (g *FreelistGenerationV1) GenerationRef() GenerationRefV1 {
	if g == nil {
		return GenerationRefV1{}
	}
	return g.ref
}
func (g *FreelistGenerationV1) ReservationRecord() ReservationRecordV1 {
	if g == nil {
		return ReservationRecordV1{}
	}
	return g.record
}
func (g *FreelistGenerationV1) Allocatable(id uint64) bool {
	if g == nil || id >= g.highWater {
		return false
	}
	c := lookupChunk(g.root, id>>freelistChunkShift)
	return c != nil && c.isFree(id&(freelistChunkSize-1))
}

func (g *FreelistGenerationV1) Validate() error {
	if g == nil || g.generationID == 0 || g.highWater < 2 || g.root == nil {
		return ErrGenerationFormat
	}
	var freeCount, retiredCount uint64
	err := walkState(g.root, 0, func(c *stateChunk) error {
		for offset := uint64(0); offset < freelistChunkSize; offset++ {
			id := c.chunkNo<<freelistChunkShift | offset
			free, seq := c.isFree(offset), c.retired[offset]
			if (free || seq != 0) && (id < 2 || id >= g.highWater) {
				return fmt.Errorf("%w: state page %d outside high-water", ErrGenerationFormat, id)
			}
			if free && seq != 0 {
				return fmt.Errorf("%w: page %d is free and retired", ErrGenerationFormat, id)
			}
			if free {
				freeCount++
			}
			if seq != 0 {
				retiredCount++
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if freeCount != g.root.freeCount || retiredCount != g.root.retiredCount {
		return ErrGenerationFormat
	}
	return nil
}

type ReuseCapability struct {
	oldestRecoverableCommitSeq uint64
	minPinnedSnapshotCommitSeq uint64
	historyFloorCommitSeq      uint64
}

func NewReuseCapability(oldestRecoverable, minPinned, historyFloor uint64) (ReuseCapability, error) {
	if oldestRecoverable == 0 {
		return ReuseCapability{}, fmt.Errorf("%w: missing recoverable root horizon", ErrGenerationFormat)
	}
	return ReuseCapability{oldestRecoverableCommitSeq: oldestRecoverable, minPinnedSnapshotCommitSeq: minPinned, historyFloorCommitSeq: historyFloor}, nil
}

func (h ReuseCapability) permits(last uint64) bool {
	return h.oldestRecoverableCommitSeq != 0 && last < h.oldestRecoverableCommitSeq && (h.minPinnedSnapshotCommitSeq == 0 || last < h.minPinnedSnapshotCommitSeq) && (h.historyFloorCommitSeq == 0 || last < h.historyFloorCommitSeq)
}

// RecoveryHorizon is retained for standalone model callers. Production code
// must construct and pass the opaque ReuseCapability returned by recovery.
type RecoveryHorizon struct{ OldestRecoverableCommitSeq, MinPinnedSnapshotCommitSeq, HistoryFloorCommitSeq uint64 }

func (h RecoveryHorizon) capability() ReuseCapability {
	return ReuseCapability{h.OldestRecoverableCommitSeq, h.MinPinnedSnapshotCommitSeq, h.HistoryFloorCommitSeq}
}

type FreelistTxnStats struct {
	COWChunks, COWPages, COWBytes, LogicalDelta               uint64
	StateMutationPaths, StateMutationItems                    uint64
	AppendAllocations, ReuseAllocations, Reservations         uint64
	FreeIDs, RetiredIDs, ReuseLag, PageVisits                 uint64
	GenerationID, ReservationRecords                          uint64
	PendingMetadataRetirements                                uint64
	CandidatePageIsolationCopies, CandidatePageIsolationBytes uint64
	OldestRecoverableCommitSeq, MinPinnedSnapshotCommitSeq    uint64
	HistoryFloorCommitSeq                                     uint64
}

type allocatedPage struct {
	id   uint64
	kind ReservationKindV1
}

type FreelistTxn struct {
	base             *FreelistGenerationV1
	ledger           *ReservationLedger
	root             *stateNode
	highWater        uint64
	allocated        []allocatedPage
	abandonedAppends []ReservationExtentV1
	consumed         bool
	changedChunks    map[uint64]struct{}
	replacedMetadata map[uint64]struct{}
	stats            FreelistTxnStats
}

// cloneForAllocatorPrepare creates an isolated staging transaction. Tree nodes
// are persistent copy-on-write values, so sharing the root is safe; every
// mutable slice and map must be copied because candidate materialization
// consumes and annotates the staged transaction.
func (t *FreelistTxn) cloneForAllocatorPrepare() (*FreelistTxn, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	clone := *t
	clone.allocated = append([]allocatedPage(nil), t.allocated...)
	clone.abandonedAppends = append([]ReservationExtentV1(nil), t.abandonedAppends...)
	clone.changedChunks = make(map[uint64]struct{}, len(t.changedChunks))
	for chunk := range t.changedChunks {
		clone.changedChunks[chunk] = struct{}{}
	}
	clone.replacedMetadata = make(map[uint64]struct{}, len(t.replacedMetadata))
	for id := range t.replacedMetadata {
		clone.replacedMetadata[id] = struct{}{}
	}
	return &clone, nil
}

func BeginCandidateV1(base *FreelistGenerationV1, expectedParent GenerationRefV1, ledger *ReservationLedger) (*FreelistTxn, error) {
	if base == nil || base.root == nil {
		return nil, ErrGenerationFormat
	}
	if base.ref.HeaderPageID != 0 && (expectedParent.GenerationID != base.ref.GenerationID || expectedParent.Digest != base.ref.Digest || expectedParent.HeaderPageID != base.ref.HeaderPageID) {
		return nil, ErrGenerationParent
	}
	if ledger == nil {
		ledger = NewReservationLedger()
	}
	t := &FreelistTxn{base: base, ledger: ledger, root: base.root, highWater: base.highWater, changedChunks: map[uint64]struct{}{}, replacedMetadata: map[uint64]struct{}{}}
	if base.ref.HeaderPageID != 0 {
		t.replacedMetadata[base.ref.HeaderPageID] = struct{}{}
	}
	for _, id := range base.record.pageIDs {
		t.replacedMetadata[id] = struct{}{}
	}
	t.retireMany(base.record.pendingMetadata())
	return t, nil
}

func NewFreelistTxn(base *FreelistGenerationV1, ledger *ReservationLedger) *FreelistTxn {
	t, _ := BeginCandidateV1(base, func() GenerationRefV1 {
		if base == nil {
			return GenerationRefV1{}
		}
		return base.ref
	}(), ledger)
	return t
}

func (t *FreelistTxn) valid() error {
	if t == nil || t.base == nil || t.root == nil || t.changedChunks == nil {
		return ErrGenerationFormat
	}
	if t.consumed {
		return ErrCandidateConsumed
	}
	return nil
}

func (t *FreelistTxn) markReplacedPath(chunkNo uint64) {
	n := t.root
	for depth := 0; n != nil && depth <= chunkTrieDepth; depth++ {
		if n.pageID != 0 {
			t.replacedMetadata[n.pageID] = struct{}{}
		}
		if depth == chunkTrieDepth {
			if n.chunk != nil && n.chunk.pageID != 0 {
				t.replacedMetadata[n.chunk.pageID] = struct{}{}
			}
			break
		}
		n = n.child[chunkNibble(chunkNo, depth)]
	}
}

func (t *FreelistTxn) mutateMany(chunkNo, items uint64, f func(*stateChunk)) {
	t.markReplacedPath(chunkNo)
	t.root = mutateChunk(t.root, chunkNo, 0, f)
	t.changedChunks[chunkNo] = struct{}{}
	t.stats.StateMutationPaths++
	t.stats.StateMutationItems += items
}

func (t *FreelistTxn) mutate(chunkNo uint64, f func(*stateChunk)) {
	t.mutateMany(chunkNo, 1, f)
}

func (t *FreelistTxn) Allocate(regionHint uint64) (uint64, error) {
	if err := t.valid(); err != nil {
		return 0, err
	}
	if id, ok := chooseUnreservedFreePage(t.root, regionHint, t.ledger, &t.stats.PageVisits); ok {
		offset := id & (freelistChunkSize - 1)
		t.mutate(id>>freelistChunkShift, func(c *stateChunk) { c.setFree(offset, false) })
		t.allocated = append(t.allocated, allocatedPage{id, ReservationReusedData})
		t.stats.ReuseAllocations++
		return id, nil
	}
	return t.allocateAppend()
}

// AllocateAppend reserves a new page at the logical high-water mark without
// consulting the reusable set. It preserves append-only maintenance and the
// public PreferAppendAlloc contract while still recording exact candidate
// ownership in the COW transaction.
func (t *FreelistTxn) AllocateAppend() (uint64, error) {
	if err := t.valid(); err != nil {
		return 0, err
	}
	return t.allocateAppend()
}

func (t *FreelistTxn) allocateAppend() (uint64, error) {
	if t.highWater == math.MaxUint64 {
		return 0, ErrNoAllocatablePage
	}
	start := t.highWater
	id, ok := t.ledger.firstUnreservedAtOrAfter(start)
	if !ok || id == math.MaxUint64 {
		return 0, ErrNoAllocatablePage
	}
	if id > start {
		t.abandonedAppends = appendReservationRange(t.abandonedAppends, start, id-start, ReservationAbandonedAppend, 0)
	}
	t.highWater = id + 1
	t.allocated = append(t.allocated, allocatedPage{id, ReservationAppendedData})
	t.stats.AppendAllocations++
	return id, nil
}

// allocateAppendedRange reserves one contiguous data-page extent above the
// transaction high-water. Durable dependency manifests use this for their
// positional page chain; ordinary allocations should continue to use
// Allocate so reusable pages remain available to index writers.
func (t *FreelistTxn) allocateAppendedRange(count int) ([]uint64, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, ErrGenerationFormat
	}
	if count == 0 {
		return nil, nil
	}
	width := uint64(count)
	start := t.highWater
	for {
		candidate, ok := t.ledger.firstUnreservedAtOrAfter(start)
		if !ok || candidate > math.MaxUint64-width {
			return nil, ErrNoAllocatablePage
		}
		conflict := uint64(0)
		for id := candidate; id < candidate+width; id++ {
			if t.ledger.Reserved(id) {
				conflict = id
				break
			}
		}
		if conflict != 0 {
			if conflict == math.MaxUint64 {
				return nil, ErrNoAllocatablePage
			}
			start = conflict + 1
			continue
		}
		if candidate > t.highWater {
			t.abandonedAppends = appendReservationRange(t.abandonedAppends, t.highWater, candidate-t.highWater, ReservationAbandonedAppend, 0)
		}
		ids := make([]uint64, count)
		for i := range ids {
			ids[i] = candidate + uint64(i)
			t.allocated = append(t.allocated, allocatedPage{ids[i], ReservationAppendedData})
		}
		t.highWater = candidate + width
		t.stats.AppendAllocations += width
		return ids, nil
	}
}

func (t *FreelistTxn) ReservePage(id uint64) error {
	if err := t.valid(); err != nil {
		return err
	}
	if !t.base.Allocatable(id) || !t.rootAllocatable(id) || t.ledger.Reserved(id) {
		return ErrPageReserved
	}
	for _, allocation := range t.allocated {
		if allocation.id == id {
			return ErrPageReserved
		}
	}
	offset := id & (freelistChunkSize - 1)
	t.mutate(id>>freelistChunkShift, func(c *stateChunk) { c.setFree(offset, false) })
	t.allocated = append(t.allocated, allocatedPage{id, ReservationReusedData})
	return nil
}

func (t *FreelistTxn) rootAllocatable(id uint64) bool {
	c := lookupChunk(t.root, id>>freelistChunkShift)
	return c != nil && c.isFree(id&(freelistChunkSize-1))
}

func (t *FreelistTxn) retire(id, seq uint64) {
	if t == nil || t.consumed || id < 2 || id >= t.highWater || seq == 0 {
		return
	}
	offset := id & (freelistChunkSize - 1)
	t.mutate(id>>freelistChunkShift, func(c *stateChunk) { c.setFree(offset, false); c.retired[offset] = seq })
}

// retireMany applies all retirements for one state chunk through a single
// persistent-tree mutation. The previous one-ID-at-a-time path cloned the
// same chunk and its full trie path repeatedly when a commit retired adjacent
// index or allocator-metadata pages.
func (t *FreelistTxn) retireMany(retired []retiredPage) {
	if t == nil || t.consumed || len(retired) == 0 {
		return
	}
	valid := retired[:0]
	for _, item := range retired {
		if item.id < 2 || item.id >= t.highWater || item.lastReachableCommitSeq == 0 {
			continue
		}
		valid = append(valid, item)
	}
	if len(valid) == 0 {
		return
	}
	if len(valid) == 1 {
		t.retire(valid[0].id, valid[0].lastReachableCommitSeq)
		return
	}
	// Stable ordering preserves last-writer behavior for duplicate IDs with
	// different retirement sequences while making every chunk one run.
	sort.SliceStable(valid, func(i, j int) bool {
		return valid[i].id>>freelistChunkShift < valid[j].id>>freelistChunkShift
	})
	for start := 0; start < len(valid); {
		chunkNo := valid[start].id >> freelistChunkShift
		end := start + 1
		for end < len(valid) && valid[end].id>>freelistChunkShift == chunkNo {
			end++
		}
		items := valid[start:end]
		t.mutateMany(chunkNo, uint64(len(items)), func(c *stateChunk) {
			for _, item := range items {
				offset := item.id & (freelistChunkSize - 1)
				c.setFree(offset, false)
				c.retired[offset] = item.lastReachableCommitSeq
			}
		})
		start = end
	}
}

func (t *FreelistTxn) Retire(id, seq uint64) {
	if t != nil {
		t.retire(id, seq)
	}
}

func capabilityThreshold(c ReuseCapability) uint64 {
	threshold := c.oldestRecoverableCommitSeq
	for _, value := range []uint64{c.minPinnedSnapshotCommitSeq, c.historyFloorCommitSeq} {
		if value != 0 && (threshold == 0 || value < threshold) {
			threshold = value
		}
	}
	return threshold
}

func collectPrunable(n *stateNode, depth int, cap ReuseCapability, out *[]retiredPage, visits *uint64) {
	if n == nil || n.retiredCount == 0 || n.minRetiredSeq == 0 || n.minRetiredSeq >= capabilityThreshold(cap) {
		return
	}
	*visits++
	if depth == chunkTrieDepth {
		for offset, seq := range n.chunk.retired {
			if seq != 0 && cap.permits(seq) {
				*out = append(*out, retiredPage{n.chunk.chunkNo<<freelistChunkShift | uint64(offset), seq})
			}
		}
		return
	}
	for _, child := range n.child {
		collectPrunable(child, depth+1, cap, out, visits)
	}
}

func (t *FreelistTxn) PruneWithCapability(cap ReuseCapability) {
	if t == nil || t.consumed || cap.oldestRecoverableCommitSeq == 0 {
		return
	}
	t.stats.OldestRecoverableCommitSeq = cap.oldestRecoverableCommitSeq
	t.stats.MinPinnedSnapshotCommitSeq = cap.minPinnedSnapshotCommitSeq
	t.stats.HistoryFloorCommitSeq = cap.historyFloorCommitSeq
	var promote []retiredPage
	collectPrunable(t.root, 0, cap, &promote, &t.stats.PageVisits)
	for _, retired := range promote {
		if lag := cap.oldestRecoverableCommitSeq - retired.lastReachableCommitSeq; lag > t.stats.ReuseLag {
			t.stats.ReuseLag = lag
		}
	}
	if len(promote) == 1 {
		item := promote[0]
		offset := item.id & (freelistChunkSize - 1)
		t.mutate(item.id>>freelistChunkShift, func(c *stateChunk) {
			c.retired[offset] = 0
			c.setFree(offset, true)
		})
		return
	}
	sort.SliceStable(promote, func(i, j int) bool {
		return promote[i].id>>freelistChunkShift < promote[j].id>>freelistChunkShift
	})
	for start := 0; start < len(promote); {
		chunkNo := promote[start].id >> freelistChunkShift
		end := start + 1
		for end < len(promote) && promote[end].id>>freelistChunkShift == chunkNo {
			end++
		}
		items := promote[start:end]
		t.mutateMany(chunkNo, uint64(len(items)), func(c *stateChunk) {
			for _, item := range items {
				offset := item.id & (freelistChunkSize - 1)
				c.retired[offset] = 0
				c.setFree(offset, true)
			}
		})
		start = end
	}
}

func (t *FreelistTxn) Prune(h RecoveryHorizon) { t.PruneWithCapability(h.capability()) }

func (t *FreelistTxn) Reserve(candidate CandidateIDV1) error {
	if err := t.valid(); err != nil {
		return err
	}
	ids := make([]uint64, len(t.allocated))
	for i, allocation := range t.allocated {
		ids[i] = allocation.id
	}
	if err := t.ledger.reserve(candidate, ids); err != nil {
		return err
	}
	t.stats.Reservations = uint64(len(ids))
	return nil
}

type FreelistCandidateV1 struct {
	generation *FreelistGenerationV1
	pages      []PageImageV1
	dirtyIDs   []uint64
}

func (c *FreelistCandidateV1) Generation() *FreelistGenerationV1      { return c.generation }
func (c *FreelistCandidateV1) GenerationRef() GenerationRefV1         { return c.generation.GenerationRef() }
func (c *FreelistCandidateV1) DirtyPageIDs() []uint64                 { return append([]uint64(nil), c.dirtyIDs...) }
func (c *FreelistCandidateV1) ReservationRecord() ReservationRecordV1 { return c.generation.record }
func (c *FreelistCandidateV1) Pages() []PageImageV1 {
	out := make([]PageImageV1, len(c.pages))
	for i := range c.pages {
		out[i] = PageImageV1{c.pages[i].PageID, append([]byte(nil), c.pages[i].Data...)}
	}
	return out
}

func (c *FreelistCandidateV1) PageCount() int {
	if c == nil {
		return 0
	}
	return len(c.pages)
}

// WritePagesToV1 visits candidate-owned pages through opaque read-only views,
// allowing a writer to copy directly into its final destination.
func (c *FreelistCandidateV1) WritePagesToV1(writer CandidatePageWriterV1) error {
	if c == nil || writer == nil {
		return ErrGenerationFormat
	}
	for i := range c.pages {
		view := CandidatePageViewV1{data: c.pages[i].Data}
		if err := writer.WriteCandidatePageV1(c.pages[i].PageID, view); err != nil {
			return fmt.Errorf("write candidate page %d: %w", c.pages[i].PageID, err)
		}
	}
	return nil
}

type recordingSink struct {
	sink             AppendPageSink
	pages            []PageImageV1
	beforeFirstWrite func() error
	writeStarted     bool
}

func (s *recordingSink) write(id uint64, data []byte) error {
	if !s.writeStarted {
		if s.beforeFirstWrite != nil {
			if err := s.beforeFirstWrite(); err != nil {
				return err
			}
		}
		s.writeStarted = true
	}
	sinkData := data
	if _, ownsCandidateBytes := s.sink.(*CandidatePageSinkV1); !ownsCandidateBytes {
		// Arbitrary sinks may retain or mutate their input. Give them an
		// isolated copy while the candidate keeps the fresh encoded buffer.
		sinkData = append([]byte(nil), data...)
	}
	if err := s.sink.WritePage(id, sinkData); err != nil {
		return err
	}
	s.pages = append(s.pages, PageImageV1{id, data})
	return nil
}

func emitStatePages(n *stateNode, depth int, generationID uint64, next *uint64, sink *recordingSink) error {
	if n == nil || n.freeCount+n.retiredCount == 0 || n.pageID != 0 {
		return nil
	}
	if depth == chunkTrieDepth {
		if n.chunk == nil {
			return ErrGenerationFormat
		}
		if n.chunk.pageID == 0 {
			n.chunk.pageID = *next
			*next++
			b := encodeChunkPage(n.chunk.pageID, generationID, n.chunk)
			n.chunk.checksum = binary.LittleEndian.Uint32(b[8:12])
			if err := sink.write(n.chunk.pageID, b); err != nil {
				return err
			}
		}
	} else {
		for _, child := range n.child {
			if err := emitStatePages(child, depth+1, generationID, next, sink); err != nil {
				return err
			}
		}
	}
	n.pageID = *next
	*next++
	b, err := encodeIndexPage(n.pageID, generationID, n, depth)
	if err != nil {
		return err
	}
	n.checksum = binary.LittleEndian.Uint32(b[8:12])
	return sink.write(n.pageID, b)
}

func appendIDExtents(extents []ReservationExtentV1, ids []uint64, kind ReservationKindV1, seq uint64) []ReservationExtentV1 {
	for _, id := range sortedUnique(ids) {
		extents = append(extents, ReservationExtentV1{StartPageID: id, Count: 1, Kind: kind, LastReachableCommitSeq: seq})
	}
	return extents
}

func appendReservationRange(extents []ReservationExtentV1, start, count uint64, kind ReservationKindV1, seq uint64) []ReservationExtentV1 {
	for count > 0 {
		chunk := min(count, uint64(^uint32(0)))
		extents = append(extents, ReservationExtentV1{StartPageID: start, Count: uint32(chunk), Kind: kind, LastReachableCommitSeq: seq})
		start += chunk
		count -= chunk
	}
	return extents
}

func countUnmaterializedStatePages(n *stateNode, depth int) uint64 {
	if n == nil || n.freeCount+n.retiredCount == 0 || n.pageID != 0 {
		return 0
	}
	count := uint64(1) // This index page.
	if depth == chunkTrieDepth {
		if n.chunk != nil && n.chunk.pageID == 0 {
			count++
		}
		return count
	}
	for _, child := range n.child {
		count += countUnmaterializedStatePages(child, depth+1)
	}
	return count
}

func (t *FreelistTxn) MaterializeCandidate(generationID, commitSeq uint64, candidateID CandidateIDV1, sink AppendPageSink) (*FreelistCandidateV1, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	if generationID == 0 || commitSeq == 0 || sink == nil {
		return nil, ErrGenerationFormat
	}
	if candidateID == (CandidateIDV1{}) {
		return nil, fmt.Errorf("%w: zero candidate identity", ErrGenerationFormat)
	}
	if t.base.ref.HeaderPageID != 0 && generationID <= t.base.generationID {
		return nil, ErrGenerationParent
	}
	// Page IDs are assigned while writing. Once materialization starts, success
	// or failure consumes this transaction; retry must begin from the immutable
	// base so a partial sink failure cannot retain unwritten page identities.
	t.consumed = true
	t.root = detachUnmaterialized(t.root, 0)
	if t.root.pageID != 0 {
		t.replacedMetadata[t.root.pageID] = struct{}{}
		// The selected generation's root is exact-generation authority. A
		// no-COW transaction may share descendants, but must publish a new root.
		t.root = cloneStateNode(t.root)
	}
	var reused, appended []uint64
	dataIDs := make([]uint64, 0, len(t.allocated))
	for _, allocation := range t.allocated {
		dataIDs = append(dataIDs, allocation.id)
		if allocation.kind == ReservationReusedData {
			reused = append(reused, allocation.id)
		} else {
			appended = append(appended, allocation.id)
		}
	}
	var extents []ReservationExtentV1
	extents = appendIDExtents(extents, reused, ReservationReusedData, 0)
	extents = appendIDExtents(extents, appended, ReservationAppendedData, 0)
	extents = append(extents, t.abandonedAppends...)
	// The target metadata extent includes the COW pages, the reservation chain,
	// and the generation header. Its count does not change the number of
	// normalized reservation extents, so compute the chain length first.
	replaced := make([]uint64, 0, len(t.replacedMetadata))
	for id := range t.replacedMetadata {
		replaced = append(replaced, id)
	}
	extents = appendIDExtents(extents, replaced, ReservationPendingMetadataRetirement, t.base.commitSeq)
	minimumMetadataStart := t.highWater
	if minimumMetadataStart == math.MaxUint64 {
		return nil, ErrNoAllocatablePage
	}
	extents, err := normalizeExtents(extents)
	if err != nil {
		return nil, err
	}
	statePageCount := countUnmaterializedStatePages(t.root, 0)
	if t.root.freeCount+t.root.retiredCount == 0 {
		statePageCount = 1
	}
	metadataStart, reservedMetadataCount, err := t.ledger.reserveTail(candidateID, minimumMetadataStart, statePageCount, dataIDs, extents)
	if err != nil {
		return nil, err
	}
	if metadataStart > minimumMetadataStart {
		extents = appendReservationRange(extents, minimumMetadataStart, metadataStart-minimumMetadataStart, ReservationAbandonedAppend, 0)
	}
	extents = append(extents, ReservationExtentV1{StartPageID: metadataStart, Count: 1, Kind: ReservationTargetMetadata})
	pageCount := reservationPagesForEntries(uint64(len(extents)))
	if pageCount > uint64(^uint16(0)) || statePageCount+pageCount+1 != reservedMetadataCount {
		return nil, ErrGenerationFormat
	}
	next := metadataStart
	recorded := &recordingSink{
		sink: sink,
		beforeFirstWrite: func() error {
			return t.ledger.markTailWriteAttempted(candidateID)
		},
	}
	if t.root.freeCount+t.root.retiredCount == 0 {
		// An empty tree still has one immutable exact-generation root page.
		t.root.pageID = next
		next++
		b, err := encodeIndexPage(t.root.pageID, generationID, t.root, 0)
		if err != nil {
			return nil, err
		}
		t.root.checksum = binary.LittleEndian.Uint32(b[8:12])
		if err := recorded.write(t.root.pageID, b); err != nil {
			return nil, err
		}
	} else if err := emitStatePages(t.root, 0, generationID, &next, recorded); err != nil {
		return nil, err
	}
	reservationID := next
	headerID := reservationID + pageCount
	next = headerID + 1
	for i := range extents {
		if extents[i].Kind == ReservationTargetMetadata {
			metadataCount := next - metadataStart
			if metadataCount > uint64(^uint32(0)) {
				return nil, ErrGenerationFormat
			}
			extents[i].Count = uint32(metadataCount)
			break
		}
	}
	record := ReservationRecordV1{CandidateID: candidateID, GenerationID: generationID, BaseID: t.base.generationID, BaseDigest: t.base.ref.Digest, Extents: extents}
	recordPages, record, err := encodeNormalizedReservationPages(reservationID, record)
	if err != nil {
		return nil, err
	}
	for i, recordPage := range recordPages {
		if err := recorded.write(reservationID+uint64(i), recordPage); err != nil {
			return nil, err
		}
	}
	g := &FreelistGenerationV1{generationID: generationID, commitSeq: commitSeq, parentGenerationID: t.base.generationID, parentCommitSeq: t.base.commitSeq, highWater: next, root: t.root, record: record}
	g.metadataPages = make([]uint64, 0, next-metadataStart)
	for id := metadataStart; id < next; id++ {
		g.metadataPages = append(g.metadataPages, id)
	}
	header := encodeGenerationPage(headerID, g, g.root.checksum)
	if err := recorded.write(headerID, header); err != nil {
		return nil, err
	}
	copy(g.ref.Digest[:], header[152:184])
	g.ref = GenerationRefV1{HeaderPageID: headerID, GenerationID: generationID, CommitSeq: commitSeq, HighWater: next, Digest: g.ref.Digest}
	t.stats.LogicalDelta = uint64(len(t.changedChunks))
	t.stats.COWChunks = uint64(len(t.changedChunks))
	t.stats.COWPages = uint64(len(recorded.pages))
	t.stats.COWBytes = t.stats.COWPages * page.PageSize
	if _, ownsCandidateBytes := sink.(*CandidatePageSinkV1); !ownsCandidateBytes {
		t.stats.CandidatePageIsolationCopies = t.stats.COWPages
		t.stats.CandidatePageIsolationBytes = t.stats.COWBytes
	}
	t.stats.FreeIDs, t.stats.RetiredIDs = g.root.freeCount, g.root.retiredCount
	t.stats.GenerationID = generationID
	t.stats.ReservationRecords = uint64(len(record.pageIDs))
	t.stats.Reservations = uint64(len(dataIDs)) + (next - metadataStart)
	t.stats.PendingMetadataRetirements = uint64(len(record.pendingMetadata()))
	dirty := make([]uint64, len(recorded.pages))
	for i := range recorded.pages {
		dirty[i] = recorded.pages[i].PageID
	}
	return &FreelistCandidateV1{generation: g, pages: recorded.pages, dirtyIDs: dirty}, nil
}

func candidateIDFromString(value string) CandidateIDV1 {
	sum := sha256.Sum256([]byte(value))
	var id CandidateIDV1
	copy(id[:], sum[:16])
	return id
}

func (t *FreelistTxn) Materialize(id uint64) (*FreelistGenerationV1, error) {
	store := NewMemoryPageStoreV1()
	candidate, err := t.MaterializeCandidate(id, id, candidateIDFromString(fmt.Sprintf("generation-%d", id)), store)
	if err != nil {
		return nil, err
	}
	return candidate.generation, nil
}

func (t *FreelistTxn) Stats() FreelistTxnStats {
	if t == nil {
		return FreelistTxnStats{}
	}
	return t.stats
}
