package memtable

import (
	"sort"
	"testing"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWOrderedTraversalUnpositionedAndEndDirections(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		name := "ordered"
		if mixed {
			name = "mixed_raw_above"
		}
		t.Run(name, func(t *testing.T) {
			budget, writer := cowOrderedLifecycleWriter(t)
			entries := []COWMutation{
				cowOrderedStorageMutation(t, "a", 1),
				cowOrderedStorageMutation(t, "a", 2),
				cowOrderedStorageMutation(t, "b", 1),
			}
			if mixed {
				// Keep the raw maximum above every canonical ordered key, so
				// a spurious fresh ordered Prev cannot hide behind raw output.
				entries = append(entries, COWMutation{Key: []byte("\xffraw"), Value: []byte("raw"), Flags: node.FlagInline})
			}
			root := cowTestPublish(t, writer, entries)
			keys := make([]string, len(entries))
			for i := range entries {
				keys[i] = string(entries[i].Key)
			}
			sort.Strings(keys)
			work := COWTraversalWork{RecordLimit: 100, ByteLimit: 1 << 20}
			cursor, err := root.NewTraversal(&work, 0)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				retirement := traversalClose(t, cursor)
				retirement.Drain()
				cowTestRelease(root)
				cowTestClose(writer)
				budget.Close()
			})
			request := func(op COWTraversalOperation, key string, expected int) {
				t.Helper()
				record, valid := cowOrderedMergedRequest(t, cursor, op, key)
				if expected < 0 {
					if valid {
						t.Fatalf("operation %d unexpectedly positioned at %x", op, record.Key)
					}
					return
				}
				if !valid || record.Key != keys[expected] {
					t.Fatalf("operation %d = %x,%v; want %x", op, record.Key, valid, keys[expected])
				}
			}
			// Prev is a no-op before any positioning, while Next begins at
			// First. Each source must obey this contract independently.
			request(btree.Prev, "", -1)
			request(btree.Prev, "", -1)
			request(btree.Next, "", 0)
			request(btree.Prev, "", -1)
			request(btree.Prev, "", -1)
			request(btree.Next, "", 0)
			request(btree.Next, "", 1)
			// Crossing an end invalidates the record. Reversing direction
			// then returns that end item, including the raw maximum.
			request(btree.Last, "", len(keys)-1)
			request(btree.Next, "", -1)
			request(btree.Next, "", -1)
			request(btree.Prev, "", len(keys)-1)
			request(btree.Prev, "", len(keys)-2)
			request(btree.SeekGE, "\xff\xff", -1)
			request(btree.Prev, "", len(keys)-1)
			request(btree.SeekLE, "", -1)
			request(btree.Next, "", 0)
		})
	}
}
