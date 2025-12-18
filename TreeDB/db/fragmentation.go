package db

import (
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

// FragmentationReport returns best-effort structural stats about the user index
// that help diagnose scan regressions after churn.
func (db *DB) FragmentationReport() (map[string]string, error) {
	db.mu.RLock()
	state := db.state.Load()
	tr := tree.New(db.pager, state.SlabSet, state.RootPageID)
	db.mu.RUnlock()

	freelistHead := db.allocator.Head()
	totalPages := db.pager.PageCount()

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

	out["treedb.freelist.head"] = fmt.Sprintf("%d", freelistHead)
	if freelistHead != 0 && totalPages > 0 {
		fs, err := readFreelistStats(db.pager, freelistHead, totalPages)
		if err != nil {
			out["treedb.freelist.error"] = err.Error()
		} else {
			out["treedb.freelist.pages"] = fmt.Sprintf("%d", fs.pages)
			out["treedb.freelist.free_ids"] = fmt.Sprintf("%d", fs.freeIDs)
			out["treedb.freelist.reclaimable_pages"] = fmt.Sprintf("%d", fs.reclaimablePages())
			out["treedb.freelist.reclaimable_ratio_ppm"] = fmt.Sprintf("%d", (fs.reclaimablePages()*1_000_000)/totalPages)
		}
	}

	return out, nil
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

type freelistStats struct {
	pages   uint64
	freeIDs uint64
}

func (s freelistStats) reclaimablePages() uint64 {
	return s.pages + s.freeIDs
}

func readFreelistStats(p *pager.Pager, head uint64, pageLimit uint64) (freelistStats, error) {
	var out freelistStats

	// Guard against cycles/corruption: never walk more pages than exist.
	remaining := pageLimit
	cur := head

	for cur != 0 && remaining > 0 {
		remaining--

		data, err := p.ReadPage(cur)
		if err != nil {
			return freelistStats{}, err
		}

		n := node.NewNode(data)
		if !n.VerifyChecksum() {
			return freelistStats{}, fmt.Errorf("freelist checksum mismatch on page %d", cur)
		}
		if n.Type() != page.PageTypeFreelist {
			return freelistStats{}, fmt.Errorf("invalid freelist page type %d on page %d", n.Type(), cur)
		}

		out.pages++
		out.freeIDs += uint64(n.Count())

		body := page.DecodeFreelistBody(data[page.PageHeaderSize:], n.Count())
		cur = body.NextPageID
	}

	if remaining == 0 && cur != 0 {
		return freelistStats{}, fmt.Errorf("freelist walk exceeded page limit (%d)", pageLimit)
	}

	return out, nil
}
