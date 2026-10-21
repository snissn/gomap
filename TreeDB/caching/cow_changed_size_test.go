package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWChangedSizeCanonicalFinalMutations(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		name := "raw"
		if ordered {
			name = "ordered"
		}
		t.Run(name, func(t *testing.T) {
			mutation := func(key, value string, flags byte, remove bool) memtable.COWMutation {
				m := memtable.COWMutation{Key: []byte(key), Value: []byte(value), Flags: flags, Remove: remove,
					Ptr: page.ValuePtr{FileID: 3, Offset: 17, Length: 43}, Revision: 29}
				if ordered {
					var err error
					m.Key, err = mvcckey.Encode(m.Key, 7)
					if err != nil {
						t.Fatal(err)
					}
					m.MVCC, m.Timestamp = true, 7
				}
				return m
			}
			cases := []struct {
				name  string
				group []memtable.COWMutation
			}{
				{"unchanged", nil},
				{"pointer_to_inline", []memtable.COWMutation{mutation("a", "tiny", node.FlagInline, false)}},
				{"inline_to_pointer_with_payload", []memtable.COWMutation{mutation("b", "pointer metadata", node.FlagPointer, false)}},
				{"inline_to_tombstone", []memtable.COWMutation{mutation("b", "ignored tombstone payload", node.FlagTombstone, false)}},
				{"remove_existing", []memtable.COWMutation{mutation("a", "ignored removal payload", node.FlagPointer, true)}},
				{"remove_missing", []memtable.COWMutation{mutation("missing", "ignored", node.FlagInline, true)}},
				{"remove_then_set", []memtable.COWMutation{
					mutation("a", "", node.FlagInline, true), mutation("a", "final", node.FlagInline, false),
				}},
				{"set_then_remove", []memtable.COWMutation{
					mutation("a", "long replacement", node.FlagInline, false), mutation("a", "ignored", node.FlagPointer, true),
				}},
				{"duplicate_and_distinct_final_records", []memtable.COWMutation{
					mutation("a", "first", node.FlagInline, false), mutation("b", "ignored", node.FlagTombstone, false),
					mutation("a", "pointer payload", node.FlagPointer, false), mutation("new", "longer", node.FlagInline, false),
					mutation("a", "ignored", node.FlagTombstone, false), mutation("new", "", node.FlagInline, true),
					mutation("b", "final b", node.FlagInline, false), mutation("new", "final new", node.FlagInline, false),
				}},
			}
			for _, test := range cases {
				t.Run(test.name, func(t *testing.T) {
					budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
					if err != nil {
						t.Fatal(err)
					}
					var writer *memtable.COWWriter
					if ordered {
						writer, err = memtable.NewCOWOrderedWriter(budget)
					} else {
						writer, err = memtable.NewCOWWriter(budget)
					}
					if err != nil {
						t.Fatal(err)
					}
					var roots []*memtable.COWRoot
					t.Cleanup(func() {
						for _, root := range roots {
							retired := root.Release()
							retired.Drain()
						}
						retired := writer.Close()
						retired.Drain()
						budget.Close()
						if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 {
							t.Fatalf("root/writer cleanup retained admission: %+v", stats)
						}
					})
					publish := func(group []memtable.COWMutation) *memtable.COWRoot {
						prepared, err := writer.Prepare(group, memtable.COWPrepareOptions{})
						if err != nil {
							t.Fatal(err)
						}
						root := prepared.Publish()
						roots = append(roots, root)
						return root
					}
					census := func(root *memtable.COWRoot) (size int64) {
						root.ScanOwned(func(record memtable.COWRecord) bool {
							size += cowRecordSize(record)
							return true
						})
						return size
					}
					oldRoot := publish([]memtable.COWMutation{
						mutation("a", "carried pointer bytes", node.FlagPointer, false),
						mutation("b", "old inline", node.FlagInline, false),
						mutation("untouched", "retained", node.FlagInline, false),
					})
					old := cowTable{root: oldRoot, size: census(oldRoot)}
					got := cowChangedSize(old, test.group)
					postimage := publish(test.group)
					if want := census(postimage); got != want {
						t.Fatalf("changed size=%d, full postimage census=%d", got, want)
					}
					if got := census(oldRoot); got != old.size {
						t.Fatalf("private preparation changed opening root: %d -> %d", old.size, got)
					}
				})
			}
		})
	}
}
