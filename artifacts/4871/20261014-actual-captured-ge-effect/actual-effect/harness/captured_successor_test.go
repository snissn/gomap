package sprintperf

import (
	"bytes"
	"fmt"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	mvcc "github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

// Inspect only the pager of this already captured snapshot. External leaf refs
// are leaves; no mutable/current backend lookup participates in this proof.
func populatedTreeProof(s treedb.Snapshot) (depth, leaves int, err error) {
	var walk func(page.ChildRef, int) error
	walk = func(ref page.ChildRef, d int) error {
		if d > 50 {
			return fmt.Errorf("cyclic/deep proof")
		}
		if d > depth {
			depth = d
		}
		if ref.Kind == page.ChildRefLeafLog {
			leaves++
			return nil
		}
		data, e := s.Pager().Get(ref.Page)
		if e != nil {
			return e
		}
		n := node.NewNodeView(data)
		switch n.Type() {
		case page.PageTypePrimaryDirectory:
			dir, e := node.DecodePrimaryDirectory(data)
			if e != nil {
				return e
			}
			base, _ := dir.Base()
			return walk(base.Ref, d)
		case page.PageTypeLeaf:
			leaves++
			return nil
		case page.PageTypeInternal:
			for i := 0; i < int(n.Count()); i++ {
				child, e := n.GetInternalChildRef(uint16(i))
				if e != nil {
					return e
				}
				if e = walk(child, d+1); e != nil {
					return e
				}
			}
			return nil
		default:
			return fmt.Errorf("unexpected proof page %d", n.Type())
		}
	}
	err = walk(page.PageChildRef(s.State().RootPageID), 1)
	return
}

func seedPopulated(db *treedb.DB, st *mvcc.Store, groups []mvcc.CommitGroup, mode mvcc.CommitMode) error {
	for lo := 0; lo < len(groups[0].Mutations); lo += 128 {
		hi := min(lo+128, len(groups[0].Mutations))
		chunk := make([]mvcc.CommitGroup, len(groups))
		for i, g := range groups {
			chunk[i] = mvcc.CommitGroup{Timestamp: g.Timestamp, Mutations: g.Mutations[lo:hi]}
		}
		if err := st.CommitGroupAt(chunk, mode); err != nil {
			return err
		}
		if len(groups[0].Mutations) > 128 {
			if err := db.Checkpoint(); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestPopulatedCapturedSuccessorAllocation(t *testing.T) {
	for _, size := range []int{0, 8192} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			o := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir())
			o.DisableSideStores = true
			o.BackgroundCheckpointInterval = -1
			o.MemtableMode = "cow_btree"
			o.MemtableShards = 4
			o.FlushThreshold = 16 << 20
			o.ValueLog.PointerThreshold = 1 << 30
			db, e := treedb.Open(o)
			if e != nil {
				t.Fatal(e)
			}
			defer db.Close()
			st := mvcc.New(db)
			v := bytes.Repeat([]byte{0x63}, 256)
			key := []byte("c3-a")
			groups := []mvcc.CommitGroup{{Timestamp: 10, Mutations: []mvcc.Mutation{{Key: key, Value: v}, {Key: []byte("c3-ab"), Value: v}}}, {Timestamp: 20, Mutations: []mvcc.Mutation{{Key: key, Value: v}, {Key: []byte("c3-ab"), Value: v}}}}
			for i := 0; i < size; i++ {
				k := []byte(fmt.Sprintf("bulk-%08d", i))
				for j := range groups {
					groups[j].Mutations = append(groups[j].Mutations, mvcc.Mutation{Key: k, Value: v})
				}
			}
			if size > 0 {
				groups = append(groups, mvcc.CommitGroup{Timestamp: 30, Mutations: groups[0].Mutations})
			}
			if e = seedPopulated(db, st, groups, mvcc.CommitRelaxed); e != nil {
				t.Fatal(e)
			}
			if e = db.Checkpoint(); e != nil {
				t.Fatal(e)
			}
			stats := db.Stats()
			if stats["treedb.cache.mutable_bytes"] != "0" || stats["treedb.cache.queue_len"] != "0" {
				t.Fatal("not drained")
			}
			snap := db.AcquireSnapshot()
			depth, leaves, e := populatedTreeProof(snap)
			if e != nil {
				t.Fatal(e)
			}
			if size > 0 {
				boundary, err := populatedBoundaryKey(snap)
				if err != nil {
					t.Fatal(err)
				}
				key = boundary
				t.Logf("proven cross-leaf historical key=%q", key)
			}
			if e = snap.Close(); e != nil {
				t.Fatal(e)
			}
			if size > 0 && (depth < 3 || leaves < 3) {
				t.Fatalf("depth=%d leaves=%d", depth, leaves)
			}
			t.Logf("depth=%d leaves=%d", depth, leaves)
			result := testing.Benchmark(func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					r, e := st.GetAt(key, 15)
					if e != nil || r.Timestamp != 10 || r.State != mvcc.Present || !bytes.Equal(r.Value, v) {
						b.Fatalf("wrong historical winner: %+v %v", r, e)
					}
				}
			})
			// A generously bounded public guard rejects constructing the 57KB complete
			// iterator for a single historical winner; output ownership remains required.
			t.Logf("historical GetAt allocation: %d B/op, %d allocs/op", result.AllocedBytesPerOp(), result.AllocsPerOp())
			if got := result.AllocedBytesPerOp(); got >= 48<<10 {
				t.Fatalf("public historical GetAt allocates %d B/op; want <48KiB", got)
			}
		})
	}
}

// A separator whose first physical row is version10 proves GetAt15 must leave
// the preceding leaf (versions20/30 precede it in physical order).
func populatedBoundaryKey(s treedb.Snapshot) ([]byte, error) {
	var visit func(page.ChildRef) ([]byte, error)
	visit = func(ref page.ChildRef) ([]byte, error) {
		if ref.Kind == page.ChildRefLeafLog {
			return nil, nil
		}
		data, e := s.Pager().Get(ref.Page)
		if e != nil {
			return nil, e
		}
		n := node.NewNodeView(data)
		if n.Type() == page.PageTypePrimaryDirectory {
			d, e := node.DecodePrimaryDirectory(data)
			if e != nil {
				return nil, e
			}
			base, _ := d.Base()
			return visit(base.Ref)
		}
		if n.Type() != page.PageTypeInternal {
			return nil, nil
		}
		for i := 0; i < int(n.Count()); i++ {
			physical, child, e := n.GetInternalEntryRefView(uint16(i))
			if e != nil {
				return nil, e
			}
			if i > 0 {
				key, ts, e := mvcckey.Decode(physical)
				if e == nil && ts == 10 {
					return append([]byte(nil), key...), nil
				}
			}
			key, e := visit(child)
			if e != nil || key != nil {
				return key, e
			}
		}
		return nil, nil
	}
	key, e := visit(page.PageChildRef(s.State().RootPageID))
	if e == nil && key == nil {
		e = fmt.Errorf("no proven between-version leaf separator")
	}
	return key, e
}
