package db

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// FragmentationProbeEvent identifies structural maintenance probe phases for
// deterministic tests. These events are intentionally coarse: the index vacuum
// trigger walks only the structures included in IndexVacuumTriggerReport, not
// the fill-percentile/full-report path used by FragmentationReport.
type FragmentationProbeEvent string

const (
	FragmentationProbeEventFullReport                FragmentationProbeEvent = "full_report"
	FragmentationProbeEventFullUserTreeWalk          FragmentationProbeEvent = "full_user_tree_walk"
	FragmentationProbeEventFullFreelistChainWalk     FragmentationProbeEvent = "full_freelist_chain_walk"
	FragmentationProbeEventFullCollectionRootWalk    FragmentationProbeEvent = "full_collection_root_walk"
	FragmentationProbeEventTriggerReport             FragmentationProbeEvent = "index_vacuum_trigger_report"
	FragmentationProbeEventTriggerUserTreeWalk       FragmentationProbeEvent = "index_vacuum_trigger_user_tree_walk"
	FragmentationProbeEventTriggerFreelistCounters   FragmentationProbeEvent = "index_vacuum_trigger_freelist_counters"
	FragmentationProbeEventTriggerCollectionRootWalk FragmentationProbeEvent = "index_vacuum_trigger_collection_root_walk"
)

var fragmentationProbeHook struct {
	mu sync.RWMutex
	fn func(FragmentationProbeEvent)
}

// SetFragmentationProbeHookForTest installs a process-wide fragmentation probe
// hook and returns a restore function. It is intended for deterministic tests;
// callers must serialize installation and should not use it in production code.
func SetFragmentationProbeHookForTest(fn func(FragmentationProbeEvent)) func() {
	fragmentationProbeHook.mu.Lock()
	prev := fragmentationProbeHook.fn
	fragmentationProbeHook.fn = fn
	fragmentationProbeHook.mu.Unlock()
	return func() {
		fragmentationProbeHook.mu.Lock()
		fragmentationProbeHook.fn = prev
		fragmentationProbeHook.mu.Unlock()
	}
}

func emitFragmentationProbeEvent(event FragmentationProbeEvent) {
	fragmentationProbeHook.mu.RLock()
	fn := fragmentationProbeHook.fn
	fragmentationProbeHook.mu.RUnlock()
	if fn != nil {
		fn(event)
	}
}

// IndexVacuumTriggerReport is the cheap subset of FragmentationReport used by
// automatic index-vacuum trigger decisions.
//
// Contract for background maintenance callers:
//   - CommitSeq is the snapshot state used for this report.
//   - User* fields are computed by one user-index page walk and preserve the
//     treedb.user.pages.span_ratio_ppm semantics from FragmentationReport.
//   - Freelist* fields come from allocator.Counters(); Pages/FreeIDs are seeded
//     at open and maintained incrementally so the trigger avoids per-probe
//     freelist-chain walks. Freelist reclaimable fields are valid only when the
//     allocator reports a non-empty freelist and total pages are known.
//   - CollectionRoot* fields are computed by walking collection root descriptors
//     and their pager-backed roots. Span-ratio fields are valid only when a
//     pager-backed collection root exists.
type IndexVacuumFreelistDebtSnapshot struct {
	TotalPages               uint64
	FreelistHead             uint64
	FreelistPages            uint64
	FreelistFreeIDs          uint64
	FreelistReclaimable      uint64
	FreelistReclaimablePPM   uint64
	FreelistReclaimableValid bool
}

type IndexVacuumTriggerReport struct {
	CommitSeq uint64

	UserPages        uint64
	UserMinPageID    uint64
	UserMaxPageID    uint64
	UserSpan         uint64
	UserSpanRatioPPM uint64

	TotalPages uint64

	FreelistHead                  uint64
	FreelistPages                 uint64
	FreelistFreeIDs               uint64
	FreelistReclaimablePages      uint64
	FreelistReclaimableRatioPPM   uint64
	FreelistReclaimableValid      bool
	FreelistAllocPagesTotal       uint64
	FreelistAppendAllocPagesTotal uint64
	FreelistReuseAllocPagesTotal  uint64
	FreelistFreePagesTotal        uint64

	CollectionRootCount             uint64
	CollectionRootPagerRoots        uint64
	CollectionRootPages             uint64
	CollectionRootMinPageID         uint64
	CollectionRootMaxPageID         uint64
	CollectionRootSpan              uint64
	CollectionRootSpanRatioPPM      uint64
	CollectionRootSpanRatioValid    bool
	CollectionRootDuplicatePageRefs uint64
	CollectionRootLeafPages         uint64
	CollectionRootInternalPages     uint64
}

// IndexVacuumFreelistDebtSnapshot returns the cheap freelist-debt fields used
// to invalidate unchanged-CommitSeq background-vacuum trigger caches. It does
// not walk the user tree, collection roots, or freelist chain.
func (db *DB) IndexVacuumFreelistDebtSnapshot() (IndexVacuumFreelistDebtSnapshot, bool) {
	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.idx.allocator == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return IndexVacuumFreelistDebtSnapshot{}, false
	}
	defer func() { _ = snap.Close() }()

	out := IndexVacuumFreelistDebtSnapshot{}
	if snap.idx.pager != nil {
		out.TotalPages = snap.idx.pager.PageCount()
	}
	emitFragmentationProbeEvent(FragmentationProbeEventTriggerFreelistCounters)
	fs := snap.idx.allocator.Counters()
	out.FreelistHead = fs.Head
	out.FreelistPages = fs.Pages
	out.FreelistFreeIDs = fs.FreeIDs
	out.FreelistReclaimable = fs.ReclaimablePages()
	if fs.Head != 0 && out.TotalPages > 0 {
		out.FreelistReclaimablePPM = (out.FreelistReclaimable * 1_000_000) / out.TotalPages
		out.FreelistReclaimableValid = true
	}
	return out, true
}

// IndexVacuumTriggerReport returns the trigger report for automatic index
// vacuum. Unlike FragmentationReport, it avoids fill-percentile allocation and
// full-report map parsing; freelist reclaimable debt uses seeded incremental
// counters rather than walking the on-disk freelist chain.
func (db *DB) IndexVacuumTriggerReport() (IndexVacuumTriggerReport, error) {
	return db.IndexVacuumTriggerReportContext(context.Background())
}

// IndexVacuumTriggerReportContext returns the automatic-vacuum trigger report
// and stops its user and collection tree walks when ctx is canceled.
func (db *DB) IndexVacuumTriggerReportContext(ctx context.Context) (IndexVacuumTriggerReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return IndexVacuumTriggerReport{}, err
	}
	emitFragmentationProbeEvent(FragmentationProbeEventTriggerReport)

	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return IndexVacuumTriggerReport{}, fmt.Errorf("missing index")
	}
	defer func() { _ = snap.Close() }()

	out := IndexVacuumTriggerReport{CommitSeq: snap.state.CommitSeq}
	if snap.idx.pager != nil {
		out.TotalPages = snap.idx.pager.PageCount()
	}

	emitFragmentationProbeEvent(FragmentationProbeEventTriggerUserTreeWalk)
	err := snap.tree.WalkPages(func(pageID uint64, _ node.Node) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if out.UserPages == 0 {
			out.UserMinPageID = pageID
			out.UserMaxPageID = pageID
		} else {
			if pageID < out.UserMinPageID {
				out.UserMinPageID = pageID
			}
			if pageID > out.UserMaxPageID {
				out.UserMaxPageID = pageID
			}
		}
		out.UserPages++
		return nil
	})
	if err != nil {
		return out, err
	}
	if out.UserPages > 0 {
		out.UserSpan = (out.UserMaxPageID - out.UserMinPageID) + 1
		out.UserSpanRatioPPM = (out.UserSpan * 1_000_000) / out.UserPages
	}

	if snap.idx.allocator != nil {
		emitFragmentationProbeEvent(FragmentationProbeEventTriggerFreelistCounters)
		fs := snap.idx.allocator.Counters()
		out.FreelistHead = fs.Head
		out.FreelistPages = fs.Pages
		out.FreelistFreeIDs = fs.FreeIDs
		out.FreelistReclaimablePages = fs.ReclaimablePages()
		if fs.Head != 0 && out.TotalPages > 0 {
			out.FreelistReclaimableRatioPPM = (out.FreelistReclaimablePages * 1_000_000) / out.TotalPages
			out.FreelistReclaimableValid = true
		}
		out.FreelistAllocPagesTotal = fs.AllocPages
		out.FreelistAppendAllocPagesTotal = fs.AppendAllocPages
		out.FreelistReuseAllocPagesTotal = fs.ReuseAllocPages
		out.FreelistFreePagesTotal = fs.FreePages
	}

	emitFragmentationProbeEvent(FragmentationProbeEventTriggerCollectionRootWalk)
	if collection, err := collectionRootFragmentationStatsWithContext(ctx, snap.idx, snap.state); err == nil {
		out.CollectionRootCount = collection.roots
		out.CollectionRootPagerRoots = collection.pagerRoots
		out.CollectionRootPages = collection.pages
		out.CollectionRootDuplicatePageRefs = collection.duplicatePages
		out.CollectionRootLeafPages = collection.leafPages
		out.CollectionRootInternalPages = collection.internalPages
		if collection.hasPagerBackedID {
			out.CollectionRootMinPageID = collection.minPageID
			out.CollectionRootMaxPageID = collection.maxPageID
			out.CollectionRootSpan = (collection.maxPageID - collection.minPageID) + 1
			if collection.pages > 0 {
				out.CollectionRootSpanRatioPPM = (out.CollectionRootSpan * 1_000_000) / collection.pages
				out.CollectionRootSpanRatioValid = true
			}
		}
	} else if ctx.Err() != nil {
		return out, ctx.Err()
	}
	return out, nil
}

// FragmentationReport returns best-effort structural stats about the user index
// that help diagnose scan regressions after churn.
func (db *DB) FragmentationReport() (map[string]string, error) {
	emitFragmentationProbeEvent(FragmentationProbeEventFullReport)
	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return nil, fmt.Errorf("missing index")
	}
	defer func() { _ = snap.Close() }()

	idx := snap.idx
	tr := snap.tree

	totalPages := idx.pager.PageCount()

	var pages uint64
	var leafPages uint64
	var internalPages uint64
	var minID uint64
	var maxID uint64

	var leafFillSum float64
	var internalFillSum float64
	var leafFillPPM []uint32
	var internalFillPPM []uint32

	emitFragmentationProbeEvent(FragmentationProbeEventFullUserTreeWalk)
	err := tr.WalkPages(func(pageID uint64, n node.Node) error {
		if pages == 0 {
			minID = pageID
			maxID = pageID
		} else {
			if pageID < minID {
				minID = pageID
			}
			if pageID > maxID {
				maxID = pageID
			}
		}
		pages++

		fill := float64(page.PageSize-n.FreeSpace()) / float64(page.PageSize)
		fillPPM := uint32(fill * 1_000_000)
		switch n.Type() {
		case page.PageTypeLeaf:
			leafPages++
			leafFillSum += fill
			leafFillPPM = append(leafFillPPM, fillPPM)
		case page.PageTypeInternal:
			internalPages++
			internalFillSum += fill
			internalFillPPM = append(internalFillPPM, fillPPM)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	out := make(map[string]string, 16)
	out["treedb.pages.total"] = fmt.Sprintf("%d", totalPages)
	out["treedb.user.pages"] = fmt.Sprintf("%d", pages)
	out["treedb.user.pages.leaf"] = fmt.Sprintf("%d", leafPages)
	out["treedb.user.pages.internal"] = fmt.Sprintf("%d", internalPages)
	if pages > 0 {
		out["treedb.user.pages.min"] = fmt.Sprintf("%d", minID)
		out["treedb.user.pages.max"] = fmt.Sprintf("%d", maxID)
		span := (maxID - minID) + 1
		out["treedb.user.pages.span"] = fmt.Sprintf("%d", span)
		out["treedb.user.pages.span_ratio_ppm"] = fmt.Sprintf("%d", (span*1_000_000)/pages)
	}
	if leafPages > 0 {
		out["treedb.user.leaf_fill_ppm_avg"] = fmt.Sprintf("%d", uint64((leafFillSum/float64(leafPages))*1_000_000))
	}
	if internalPages > 0 {
		out["treedb.user.internal_fill_ppm_avg"] = fmt.Sprintf("%d", uint64((internalFillSum/float64(internalPages))*1_000_000))
	}

	if p := fillPercentiles(leafFillPPM); p.valid {
		out["treedb.user.leaf_fill_ppm_min"] = fmt.Sprintf("%d", p.min)
		out["treedb.user.leaf_fill_ppm_p10"] = fmt.Sprintf("%d", p.p10)
		out["treedb.user.leaf_fill_ppm_p50"] = fmt.Sprintf("%d", p.p50)
		out["treedb.user.leaf_fill_ppm_p90"] = fmt.Sprintf("%d", p.p90)
		out["treedb.user.leaf_fill_ppm_p99"] = fmt.Sprintf("%d", p.p99)
		out["treedb.user.leaf_fill_ppm_max"] = fmt.Sprintf("%d", p.max)
	}
	if p := fillPercentiles(internalFillPPM); p.valid {
		out["treedb.user.internal_fill_ppm_min"] = fmt.Sprintf("%d", p.min)
		out["treedb.user.internal_fill_ppm_p10"] = fmt.Sprintf("%d", p.p10)
		out["treedb.user.internal_fill_ppm_p50"] = fmt.Sprintf("%d", p.p50)
		out["treedb.user.internal_fill_ppm_p90"] = fmt.Sprintf("%d", p.p90)
		out["treedb.user.internal_fill_ppm_p99"] = fmt.Sprintf("%d", p.p99)
		out["treedb.user.internal_fill_ppm_max"] = fmt.Sprintf("%d", p.max)
	}

	emitFragmentationProbeEvent(FragmentationProbeEventFullFreelistChainWalk)
	fs, err := idx.allocator.Stats(totalPages)
	out["treedb.freelist.head"] = fmt.Sprintf("%d", fs.Head)
	out["treedb.freelist.alloc_pages_total"] = fmt.Sprintf("%d", fs.AllocPages)
	out["treedb.freelist.append_alloc_pages_total"] = fmt.Sprintf("%d", fs.AppendAllocPages)
	out["treedb.freelist.reuse_alloc_pages_total"] = fmt.Sprintf("%d", fs.ReuseAllocPages)
	out["treedb.freelist.free_pages_total"] = fmt.Sprintf("%d", fs.FreePages)
	if fs.Head != 0 && totalPages > 0 {
		if err != nil {
			out["treedb.freelist.error"] = err.Error()
		} else {
			out["treedb.freelist.pages"] = fmt.Sprintf("%d", fs.Pages)
			out["treedb.freelist.free_ids"] = fmt.Sprintf("%d", fs.FreeIDs)
			out["treedb.freelist.reclaimable_pages"] = fmt.Sprintf("%d", fs.ReclaimablePages())
			out["treedb.freelist.reclaimable_ratio_ppm"] = fmt.Sprintf("%d", (fs.ReclaimablePages()*1_000_000)/totalPages)
		}
	}

	graveyard := idx.graveyard.Stats()
	out["treedb.graveyard.batches"] = fmt.Sprintf("%d", graveyard.Batches)
	out["treedb.graveyard.pages"] = fmt.Sprintf("%d", graveyard.Pages)
	out["treedb.graveyard.min_seq"] = fmt.Sprintf("%d", graveyard.MinSeq)
	out["treedb.graveyard.max_seq"] = fmt.Sprintf("%d", graveyard.MaxSeq)

	emitFragmentationProbeEvent(FragmentationProbeEventFullCollectionRootWalk)
	if collection, err := collectionRootFragmentationStats(idx, snap.state); err != nil {
		out["treedb.collection_roots.error"] = err.Error()
	} else {
		collection.into(out)
	}

	return out, nil
}

type collectionRootFragmentation struct {
	roots            uint64
	leafRefRoots     uint64
	pagerRoots       uint64
	pages            uint64
	leafPages        uint64
	internalPages    uint64
	uniquePageIDs    map[uint64]struct{}
	duplicatePages   uint64
	minPageID        uint64
	maxPageID        uint64
	hasPagerBackedID bool
}

func collectionRootFragmentationStats(idx *indexGen, state *DBState) (collectionRootFragmentation, error) {
	return collectionRootFragmentationStatsWithContext(context.Background(), idx, state)
}

func collectionRootFragmentationStatsWithContext(ctx context.Context, idx *indexGen, state *DBState) (collectionRootFragmentation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return collectionRootFragmentation{}, err
	}
	out := collectionRootFragmentation{uniquePageIDs: make(map[uint64]struct{})}
	if idx == nil || idx.pager == nil || state == nil || state.SystemRootPageID == 0 {
		return out, nil
	}
	reader := newValueReader(state.ValueLogSet)
	descriptors, err := vacuumCollectCollectionRootDescriptorsWithContext(ctx, idx.pager, reader, state.SystemRootPageID)
	if err != nil {
		return out, err
	}
	out.roots = uint64(len(descriptors))
	for _, descriptor := range descriptors {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		rootID := descriptor.rootID
		if rootID == 0 {
			continue
		}
		out.pagerRoots++
		tr := tree.New(idx.pager, reader, rootID)
		if err := tr.WalkPages(func(pageID uint64, n node.Node) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, seen := out.uniquePageIDs[pageID]; seen {
				out.duplicatePages++
				return nil
			}
			out.uniquePageIDs[pageID] = struct{}{}
			out.pages++
			if !out.hasPagerBackedID {
				out.minPageID = pageID
				out.maxPageID = pageID
				out.hasPagerBackedID = true
			} else {
				if pageID < out.minPageID {
					out.minPageID = pageID
				}
				if pageID > out.maxPageID {
					out.maxPageID = pageID
				}
			}
			switch n.Type() {
			case page.PageTypeLeaf:
				out.leafPages++
			case page.PageTypeInternal:
				out.internalPages++
			}
			return nil
		}); err != nil {
			return out, err
		}
	}
	return out, nil
}

func (c collectionRootFragmentation) into(out map[string]string) {
	out["treedb.collection_roots.count"] = fmt.Sprintf("%d", c.roots)
	out["treedb.collection_roots.leafref_roots"] = fmt.Sprintf("%d", c.leafRefRoots)
	out["treedb.collection_roots.pager_roots"] = fmt.Sprintf("%d", c.pagerRoots)
	out["treedb.collection_roots.pages"] = fmt.Sprintf("%d", c.pages)
	out["treedb.collection_roots.pages.leaf"] = fmt.Sprintf("%d", c.leafPages)
	out["treedb.collection_roots.pages.internal"] = fmt.Sprintf("%d", c.internalPages)
	out["treedb.collection_roots.pages.duplicate_refs"] = fmt.Sprintf("%d", c.duplicatePages)
	if c.hasPagerBackedID {
		out["treedb.collection_roots.pages.min"] = fmt.Sprintf("%d", c.minPageID)
		out["treedb.collection_roots.pages.max"] = fmt.Sprintf("%d", c.maxPageID)
		span := (c.maxPageID - c.minPageID) + 1
		out["treedb.collection_roots.pages.span"] = fmt.Sprintf("%d", span)
		if c.pages > 0 {
			out["treedb.collection_roots.pages.span_ratio_ppm"] = fmt.Sprintf("%d", (span*1_000_000)/c.pages)
		}
	}
}

type fillStats struct {
	valid bool
	min   uint32
	p10   uint32
	p50   uint32
	p90   uint32
	p99   uint32
	max   uint32
}

func fillPercentiles(ppm []uint32) fillStats {
	if len(ppm) == 0 {
		return fillStats{}
	}

	sort.Slice(ppm, func(i, j int) bool { return ppm[i] < ppm[j] })
	n := len(ppm)

	at := func(p int) uint32 {
		if n == 1 {
			return ppm[0]
		}
		idx := (p * (n - 1)) / 100
		return ppm[idx]
	}

	return fillStats{
		valid: true,
		min:   ppm[0],
		p10:   at(10),
		p50:   at(50),
		p90:   at(90),
		p99:   at(99),
		max:   ppm[n-1],
	}
}
