//go:build getmany_routing_overlay

package tree

import "testing"

// The disposable overlay counts calls to the existing checked node methods.
// Keep this out of timed and normal builds: it checks routing work, not latency.
func TestTreeGetManySharedTraversalRoutingWork(t *testing.T) {
	tr, _, keys, _ := sharedGetManyFixture(t)
	for _, view := range []bool{false, true} {
		algorithmWorkChildSearches, algorithmWorkEntryRefs = 0, 0
		if view {
			if err := tr.GetManyView(keys, func(int, []byte, []byte, bool) error { return nil }); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := tr.GetManyAppend(keys, make([][]byte, len(keys)), nil); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("view=%v child_searches=%d entry_refs=%d", view, algorithmWorkChildSearches, algorithmWorkEntryRefs)
		if algorithmWorkChildSearches >= uint64(len(keys)) || algorithmWorkEntryRefs > 80 {
			t.Fatal("routing still searches internal separators for every probe")
		}
	}
}
