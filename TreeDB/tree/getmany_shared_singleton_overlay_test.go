//go:build algorithm_work_overlay && getmany_singleton_overlay

package tree

import "testing"

// Both public modes must reach the real checked loads; entry counts alone
// would not prove this path is used. Dense union coverage remains separate.
func TestTreeGetManySharedTraversalSingletonWork(t *testing.T) {
	tr, _, _, _ := sharedGetManyFixture(t)
	keys := sharedGetManySparseKeys()
	for _, view := range []bool{false, true} {
		algorithmWorkIntervalEntries = 0
		AlgorithmWorkBegin()
		var err error
		if view {
			err = tr.GetManyView(keys, func(int, []byte, []byte, bool) error { return nil })
		} else {
			_, err = tr.GetManyAppend(keys, make([][]byte, len(keys)), nil)
		}
		actual, unique := AlgorithmWorkEnd()
		t.Logf("view=%v interval_entries=%d actual_internal_loads=%d unique=%d", view, algorithmWorkIntervalEntries, actual, unique)
		if err != nil {
			t.Fatal(err)
		}
		if algorithmWorkIntervalEntries != 3 || actual != 5 || unique != 5 {
			t.Errorf("want entries3 loads5/5")
		}
	}
}
