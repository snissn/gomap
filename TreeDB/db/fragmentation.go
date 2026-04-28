package db

import (
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// FragmentationReport returns best-effort structural stats about the user index
// that help diagnose scan regressions after churn.
func (db *DB) FragmentationReport() (map[string]string, error) {
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
	out := collectionRootFragmentation{uniquePageIDs: make(map[uint64]struct{})}
	if idx == nil || idx.pager == nil || state == nil || state.SystemRootPageID == 0 {
		return out, nil
	}
	reader := newValueReader(state.ValueLogSet)
	descriptors, err := vacuumCollectCollectionRootDescriptors(idx.pager, reader, state.SystemRootPageID)
	if err != nil {
		return out, err
	}
	out.roots = uint64(len(descriptors))
	for _, descriptor := range descriptors {
		rootID := descriptor.rootID
		if rootID == 0 {
			continue
		}
		if _, ok := page.DecodeLeafRef(rootID); ok {
			out.leafRefRoots++
			continue
		}
		out.pagerRoots++
		tr := tree.New(idx.pager, reader, rootID)
		if err := tr.WalkPages(func(pageID uint64, n node.Node) error {
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
