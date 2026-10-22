package caching

import (
	"math"
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
			unqualified := func(m memtable.COWMutation) memtable.COWMutation {
				m.MVCC, m.Timestamp = false, 0
				return m
			}
			cases := []struct {
				name  string
				group []memtable.COWMutation
				seed  []memtable.COWMutation
			}{
				{name: "unchanged"},
				{name: "pointer_to_inline", group: []memtable.COWMutation{mutation("a", "tiny", node.FlagInline, false)}},
				{name: "inline_to_pointer_with_payload", group: []memtable.COWMutation{mutation("b", "pointer metadata", node.FlagPointer, false)}},
				{name: "inline_to_tombstone", group: []memtable.COWMutation{mutation("b", "ignored tombstone payload", node.FlagTombstone, false)}},
				{name: "remove_existing", group: []memtable.COWMutation{mutation("a", "ignored removal payload", node.FlagPointer, true)}},
				{name: "remove_missing", group: []memtable.COWMutation{mutation("missing", "ignored", node.FlagInline, true)}},
				{"remove_then_set", []memtable.COWMutation{
					mutation("a", "", node.FlagInline, true), mutation("a", "final", node.FlagInline, false),
				}, nil},
				{"set_then_remove", []memtable.COWMutation{
					mutation("a", "long replacement", node.FlagInline, false), mutation("a", "ignored", node.FlagPointer, true),
				}, nil},
				{"duplicate_and_distinct_final_records", []memtable.COWMutation{
					mutation("a", "first", node.FlagInline, false), mutation("b", "ignored", node.FlagTombstone, false),
					mutation("a", "pointer payload", node.FlagPointer, false), mutation("new", "longer", node.FlagInline, false),
					mutation("a", "ignored", node.FlagTombstone, false), mutation("new", "", node.FlagInline, true),
					mutation("b", "final b", node.FlagInline, false), mutation("new", "final new", node.FlagInline, false),
				}, nil},
				{"raw_migration_and_sequential_overrides", []memtable.COWMutation{
					unqualified(mutation("a", "intermediate raw", node.FlagInline, false)),
					mutation("a", "qualified pointer prefix", node.FlagPointer, false),
					unqualified(mutation("a", "ignored", node.FlagInline, true)),
					unqualified(mutation("b", "unrelated", node.FlagInline, false)),
					mutation("a", "promoted again", node.FlagInline, false),
					unqualified(mutation("a", "final tombstone payload", node.FlagTombstone, false)),
				}, []memtable.COWMutation{unqualified(mutation("a", "old raw pointer bytes", node.FlagPointer, false))}},
				{"raw_migration_removed_finally", []memtable.COWMutation{
					mutation("a", "first qualified", node.FlagInline, false),
					mutation("b", "unrelated", node.FlagInline, false),
					unqualified(mutation("a", "ignored", node.FlagInline, true)),
				}, []memtable.COWMutation{unqualified(mutation("a", "old raw payload", node.FlagInline, false))}},
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
					census := func(root *memtable.COWRoot) (size int64) {
						root.ScanOwned(func(record memtable.COWRecord) bool {
							size += cowRecordSize(record)
							return true
						})
						return size
					}
					var previousSize int64
					publish := func(group []memtable.COWMutation) *memtable.COWRoot {
						prepared, err := writer.Prepare(group, memtable.COWPrepareOptions{})
						if err != nil {
							t.Fatal(err)
						}
						delta, valid := prepared.LogicalSizeDelta()
						root := prepared.Publish()
						roots = append(roots, root)
						want := census(root)
						if !valid || delta != want-previousSize {
							t.Fatalf("prepared delta=%d valid=%t, census change=%d", delta, valid, want-previousSize)
						}
						previousSize = want
						return root
					}
					seed := test.seed
					if seed == nil {
						seed = []memtable.COWMutation{
							mutation("a", "carried pointer bytes", node.FlagPointer, false),
							mutation("b", "old inline", node.FlagInline, false),
							mutation("untouched", "retained", node.FlagInline, false),
						}
					}
					oldRoot := publish(seed)
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

func TestCOWApplySizeDeltaBounds(t *testing.T) {
	for _, test := range []struct {
		size, delta, want int64
		valid             bool
	}{
		{0, 0, 0, true}, {1, -1, 0, true}, {math.MaxInt64 - 1, 1, math.MaxInt64, true},
		{0, -1, 0, false}, {math.MaxInt64, 1, 0, false}, {math.MaxInt64, math.MinInt64, 0, false},
		{-1, 1, 0, false},
	} {
		got, valid := cowApplySizeDelta(test.size, test.delta)
		if got != test.want || valid != test.valid {
			t.Fatalf("size=%d delta=%d: got=(%d,%t), want=(%d,%t)", test.size, test.delta, got, valid, test.want, test.valid)
		}
	}
}
