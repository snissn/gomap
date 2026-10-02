package tree

import (
	"bytes"
	"testing"
)

// Keep the public batch grouped, then let root fences leave one probe in each
// half. Both checked mixed-encoding tails become singleton intervals.
func sharedGetManySparseKeys() [][]byte {
	keys := make([][]byte, 64)
	for i := range keys {
		keys[i] = []byte("k128")
	}
	keys[3], keys[47] = []byte("k000"), []byte("k096")
	return keys
}

func TestTreeGetManySharedTraversalSparse(t *testing.T) {
	tr, _, _, want := sharedGetManyFixture(t)
	keys := sharedGetManySparseKeys()
	for _, view := range []bool{false, true} {
		values := make([][]byte, len(keys))
		if view {
			calls := make([]int, len(keys))
			err := tr.GetManyView(keys, func(i int, key, value []byte, found bool) error {
				calls[i]++
				expected, exists := want[string(keys[i])]
				if !bytes.Equal(key, keys[i]) || found != exists || !bytes.Equal(value, expected) {
					t.Fatalf("index=%d found=%v value=%q", i, found, value)
				}
				values[i] = bytes.Clone(value)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			for i, count := range calls {
				if count != 1 {
					t.Fatalf("index=%d calls=%d", i, count)
				}
			}
		} else if _, err := tr.GetManyAppend(keys, values, nil); err != nil {
			t.Fatal(err)
		}
		for i, value := range values {
			if !bytes.Equal(value, want[string(keys[i])]) {
				t.Fatalf("view=%v index=%d value=%q", view, i, value)
			}
			if i != 3 && i != 47 && value != nil {
				t.Fatalf("present root-fence miss at %d", i)
			}
		}
	}
}
