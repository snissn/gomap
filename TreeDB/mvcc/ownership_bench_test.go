package mvcc

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

// This fixture compiles unchanged on the pre-EntryView baseline. Dispatch once
// per opened iterator; both paths inspect identical bytes while valid.
func ownershipInspector(it *VersionIterator) func() Version {
	if view, ok := any(it).(interface{ EntryView() Version }); ok {
		return view.EntryView
	}
	return it.Entry
}

func prepareOwnershipFixture(t testing.TB, width, depth int, pointers bool) (*treedb.DB, *Store) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.ForcePointers = pointers
	if pointers {
		opts.ValueLog.PointerThreshold = 1
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	store := New(db)
	for ts := 1; ts <= depth; ts++ {
		if err := store.CommitAt(uint64(ts), []Mutation{{Key: []byte("key"), Value: bytes.Repeat([]byte{byte(ts)}, width)}}, CommitRelaxed); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db, store
}

var ownershipChecksum uint64
var ownedResultSink Result
var ownedVersionsSink []Version

func BenchmarkMVCCOwnership(b *testing.B) {
	for _, width := range []int{8, 16, 4096, 32768} {
		for _, depth := range []int{1, 8, 64} {
			for _, pointers := range []bool{false, true} {
				b.Run(fmt.Sprintf("width=%d/depth=%d/pointers=%t", width, depth, pointers), func(b *testing.B) {
					db, store := prepareOwnershipFixture(b, width, depth, pointers)
					defer db.Close()
					b.Run("GetAt", func(b *testing.B) {
						b.ReportAllocs()
						b.SetBytes(int64(width))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							result, err := store.GetAt([]byte("key"), uint64(depth))
							if err != nil || result.State != Present || result.Timestamp != uint64(depth) || len(result.Value) != width || result.Value[0] != byte(depth) || result.Value[width-1] != byte(depth) {
								b.Fatalf("result=%+v err=%v", result, err)
							}
							ownedResultSink = result
						}
					})
					for _, owned := range []bool{false, true} {
						name := "Inspect"
						if owned {
							name = "Owned"
						}
						b.Run(name, func(b *testing.B) {
							var firstEntryNs int64
							b.ReportAllocs()
							b.SetBytes(int64(width * depth))
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								started := time.Now()
								it, err := store.IterateVersions(VersionIteratorOptions{ExactKey: []byte("key")})
								if err != nil {
									b.Fatal(err)
								}
								entry := it.Entry
								if !owned {
									entry = ownershipInspector(it)
								}
								seen := 0
								var sum uint64
								var outputs []Version
								if owned {
									outputs = make([]Version, 0, depth)
								}
								for it.Valid() {
									v := entry()
									if seen == 0 {
										firstEntryNs += time.Since(started).Nanoseconds()
									}
									if string(v.Key) != "key" || v.State != Present || v.Timestamp != uint64(depth-seen) || len(v.Value) != width || v.Value[0] != byte(v.Timestamp) || v.Value[width-1] != byte(v.Timestamp) {
										b.Fatalf("version=%+v", v)
									}
									sum += v.Timestamp + uint64(v.Value[0]) + uint64(len(v.Key))
									seen++
									if owned {
										outputs = append(outputs, v)
									}
									it.Next()
								}
								iterErr := it.Error()
								closeErr := it.Close()
								if iterErr != nil || closeErr != nil || seen != depth {
									b.Fatalf("seen=%d err=%v close=%v", seen, iterErr, closeErr)
								}
								if owned {
									if len(outputs) != depth {
										b.Fatalf("owned outputs=%d, want %d", len(outputs), depth)
									}
									for index, v := range outputs {
										if string(v.Key) != "key" || v.State != Present || v.Timestamp != uint64(depth-index) || len(v.Value) != width || v.Value[0] != byte(v.Timestamp) || v.Value[width-1] != byte(v.Timestamp) {
											b.Fatalf("owned output after Close=%+v", v)
										}
									}
								}
								ownershipChecksum = sum
								if owned {
									ownedVersionsSink = outputs
								}
							}
							b.ReportMetric(float64(depth), "versions/op")
							b.ReportMetric(float64(firstEntryNs)/float64(b.N), "first_entry_ns/op")
						})
					}
				})
			}
		}
	}
}

// Explicit opt-in diagnostic, not a noisy heap threshold in ordinary CI. Run
// one selected subtest per fresh process on each binary with identical source.
func TestMVCCRetainedOwnedOutputs(t *testing.T) {
	if os.Getenv("GOMAP_MVCC_RETAINED_OUTPUTS") != "1" {
		t.Skip("retained-heap diagnostic requires explicit opt-in")
	}
	const count = 4096
	for _, width := range []int{8, 16, 4096, 32768} {
		for _, pointers := range []bool{false, true} {
			t.Run(fmt.Sprintf("width=%d/pointers=%t", width, pointers), func(t *testing.T) {
				runtime.GC()
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				outputs := retainOwnershipResults(t, width, count, pointers)
				runtime.GC()
				runtime.GC()
				runtime.ReadMemStats(&after)
				var bytesRetained int
				for _, result := range outputs {
					if result.State != Present || result.Timestamp != 1 || len(result.Value) != width || result.Value[0] != 1 || result.Value[width-1] != 1 {
						t.Fatalf("invalid retained result=%+v", result)
					}
					bytesRetained += len(result.Value)
				}
				t.Logf("count=%d width=%d pointers=%t payload_bytes=%d heap_before=%d heap_after=%d heap_delta=%d objects_before=%d objects_after=%d", count, width, pointers, bytesRetained, before.HeapAlloc, after.HeapAlloc, int64(after.HeapAlloc)-int64(before.HeapAlloc), before.HeapObjects, after.HeapObjects)
				runtime.KeepAlive(outputs)
			})
		}
	}
}

func retainOwnershipResults(t testing.TB, width, count int, pointers bool) []Result {
	db, store := prepareOwnershipFixture(t, width, 1, pointers)
	outputs := make([]Result, count)
	for i := range outputs {
		result, err := store.GetAt([]byte("key"), 1)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		outputs[i] = result
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Neither DB nor Store escapes: the post-return GCs retain only the outputs,
	// not a still-live snapshot or value-log/cache owner.
	return outputs
}
