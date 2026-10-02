//go:build algorithm_work_overlay

package tree

import "testing"

// The disposable checked-loader overlay supplies AlgorithmWorkBegin/End.
// This asserts real production loads, independent of planner/helper counters.
func TestTreeGetManySharedTraversalInternalLoads(t *testing.T) {
	tr, _, keys, _ := sharedGetManyFixture(t)
	out := make([][]byte, len(keys))
	for _, view := range []bool{false, true} {
		name := "owned"
		if view {
			name = "view"
		}
		t.Run(name, func(t *testing.T) {
			AlgorithmWorkBegin()
			var err error
			if view {
				err = tr.GetManyView(keys, func(int, []byte, []byte, bool) error { return nil })
			} else {
				_, err = tr.GetManyAppend(keys, out, make([]byte, 0, 2048))
			}
			actual, unique := AlgorithmWorkEnd()
			if err != nil {
				t.Fatal(err)
			}
			if actual != 7 || unique != 7 {
				t.Fatalf("view=%v actual internal loads=%d unique=%d want 7/7", view, actual, unique)
			}
		})
	}
}
