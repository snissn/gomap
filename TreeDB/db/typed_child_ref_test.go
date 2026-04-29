package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func requireLeafLogRootChildren(t *testing.T, db *DB, root uint64) []page.LeafLogPtr {
	t.Helper()
	children, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(db.Pager(), root)
	if err != nil {
		t.Fatalf("collect leaf-log children for root %d: %v", root, err)
	}
	if !allLeafRefs || len(children) == 0 {
		t.Fatalf("root=%d want leaf-log children", root)
	}
	ptrs := make([]page.LeafLogPtr, 0, len(children))
	for _, child := range children {
		if child.childRef.Kind != page.ChildRefLeafLog {
			t.Fatalf("root=%d child kind=%d want leaf-log", root, child.childRef.Kind)
		}
		ptrs = append(ptrs, child.childRef.Log)
	}
	return ptrs
}
