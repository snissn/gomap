package db

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// newSnapshotFixtureIndexes supplies real constructor lifetimes to snapshot
// metadata tests. Every generation in one fixture borrows the same original
// Owner, created before any Pager, index, registry or Snapshot holder.
func newSnapshotFixtureIndexes(t *testing.T, ids ...uint64) []*indexGen {
	t.Helper()
	owner, err := residentcredit.NewOrdinary(128 << 20)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	var indexes []*indexGen
	var pendingPager *pager.Pager
	t.Cleanup(func() {
		for _, idx := range indexes {
			if err := idx.close(); err != nil {
				t.Errorf("fixture index %d Close: %v", idx.id, err)
			}
		}
		if pendingPager != nil {
			if err := pendingPager.Close(); err != nil {
				t.Errorf("partial fixture Pager Close: %v", err)
			}
		}
		owner.Close()
		if stats := owner.Stats(); stats.Live != 0 || stats.Refs != 0 {
			t.Errorf("fixture retained constructor backing: %+v", stats)
		}
	})
	for _, id := range ids {
		p, err := pager.OpenWithOptions(filepath.Join(dir, "index-"+strconv.FormatUint(id, 10)+".db"), page.PageSize*64, pager.OpenOptions{ResidentOwner: owner})
		pendingPager = p
		if err != nil {
			t.Fatal(err)
		}
		idx, err := newConstructorIndexGen(id, p, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, idx)
		pendingPager = nil
		if !owner.OwnsScope(idx.creator) || len(indexes) > 1 && !indexes[0].creator.SameOwner(idx.creator) {
			t.Fatal("fixture generation changed original constructor owner")
		}
	}
	return indexes
}
