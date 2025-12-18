package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// FragmentationReport returns best-effort structural stats about the user index
// that help diagnose scan regressions after churn.
func (db *DB) FragmentationReport() (map[string]string, error) {
	db.mu.RLock()
	state := db.state.Load()
	tr := tree.New(db.pager, state.SlabSet, state.RootPageID)
	db.mu.RUnlock()

	var pages uint64
	var leafPages uint64
	var internalPages uint64
	var minID uint64
	var maxID uint64

	var leafFillSum float64
	var internalFillSum float64

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
		switch n.Type() {
		case page.PageTypeLeaf:
			leafPages++
			leafFillSum += fill
		case page.PageTypeInternal:
			internalPages++
			internalFillSum += fill
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	out := make(map[string]string, 16)
	out["treedb.user.pages"] = fmt.Sprintf("%d", pages)
	out["treedb.user.pages.leaf"] = fmt.Sprintf("%d", leafPages)
	out["treedb.user.pages.internal"] = fmt.Sprintf("%d", internalPages)
	if pages > 0 {
		out["treedb.user.pages.min"] = fmt.Sprintf("%d", minID)
		out["treedb.user.pages.max"] = fmt.Sprintf("%d", maxID)
		out["treedb.user.pages.span"] = fmt.Sprintf("%d", (maxID-minID)+1)
	}
	if leafPages > 0 {
		out["treedb.user.leaf_fill_ppm_avg"] = fmt.Sprintf("%d", uint64((leafFillSum/float64(leafPages))*1_000_000))
	}
	if internalPages > 0 {
		out["treedb.user.internal_fill_ppm_avg"] = fmt.Sprintf("%d", uint64((internalFillSum/float64(internalPages))*1_000_000))
	}
	return out, nil
}
