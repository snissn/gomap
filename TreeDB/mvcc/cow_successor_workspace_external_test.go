package mvcc_test

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	mvcc "github.com/snissn/gomap/TreeDB/mvcc"
	"github.com/snissn/gomap/TreeDB/node"
)

type successorWorkspaceFixture struct {
	db    *treedb.DB
	store *mvcc.Store
	keys  [][]byte
	depth int
}

func successorWorkspaceValue(key, timestamp int) []byte {
	return bytes.Repeat([]byte{byte(key + 1), byte(timestamp)}, 64)
}

// This fixture deliberately checkpoints a physical history before warmup.
// Growing-history benchmarks with an empty disk basis do not reach the scratch
// allocation selected by #5214. Setup, activation, warmup and Close are outside
// benchmark timing; only public Store read/iterator lifetimes are timed.
func prepareSuccessorWorkspace(t testing.TB, depth int, pointers bool) *successorWorkspaceFixture {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.MemtableMode = "cow_ordered"
	opts.MemtableShards = 4
	opts.FlushThreshold = 64 << 20
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.Compression = treedb.ValueLogCompressionOff
	opts.ValueLog.PointerThreshold = 1 << 30
	opts.ValueLog.ForcePointers = pointers
	if pointers {
		opts.ValueLog.PointerThreshold = 1
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f := &successorWorkspaceFixture{db: db, store: mvcc.New(db), depth: depth, keys: make([][]byte, 32)}
	for i := range f.keys {
		f.keys[i] = []byte(fmt.Sprintf("scratch/%02d", i))
	}
	for ts := 1; ts <= depth; ts++ {
		mutations := make([]mvcc.Mutation, len(f.keys))
		for i, key := range f.keys {
			mutations[i] = mvcc.Mutation{Key: key, Value: successorWorkspaceValue(i, ts)}
		}
		if err := f.store.CommitAt(uint64(ts), mutations, mvcc.CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	stats := db.Stats()
	handoffs, err := strconv.ParseUint(stats["treedb.cache.cow.handoffs_total"], 10, 64)
	if err != nil || handoffs == 0 || db.ResolvedProfile() != treedb.ProfileNoWALFast || stats["treedb.cache.memtable_mode"] != "cow_ordered" || stats["treedb.cache.queue_len"] != "0" || stats["treedb.cache.mutable_bytes"] != "0" || !db.SupportsMVCCReadCut() {
		t.Fatalf("physical fixture activation: profile=%s mode=%s handoffs=%d queue=%s mutable=%s err=%v", db.ResolvedProfile(), stats["treedb.cache.memtable_mode"], handoffs, stats["treedb.cache.queue_len"], stats["treedb.cache.mutable_bytes"], err)
	}
	// Inspect a real selected record after all queued/mutable entries drained.
	// This is setup-only activation, never an alternate timed read API.
	cut := db.AcquireSnapshot()
	physical, err := mvcckey.Encode(f.keys[0], uint64(depth))
	if err != nil {
		t.Fatal(err)
	}
	entry, readErr := cut.GetEntryExact(physical)
	selected := bytes.Equal(entry.Key, physical) && (entry.Flags&node.FlagPointer != 0) == pointers
	closeErr := cut.Close()
	if readErr != nil || closeErr != nil || !selected {
		t.Fatalf("physical value-layout activation: flags=%d read=%v close=%v", entry.Flags, readErr, closeErr)
	}
	// Warm every measured point and both complete iterator lifetimes equally.
	for i := range f.keys {
		if _, err := f.point(i, uint64(depth)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.history(false); err != nil {
		t.Fatal(err)
	}
	if err := f.history(true); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *successorWorkspaceFixture) point(key int, timestamp uint64) (mvcc.Result, error) {
	result, err := f.store.GetAt(f.keys[key], timestamp)
	if err != nil {
		return result, err
	}
	if result.State != mvcc.Present || result.Timestamp != timestamp || len(result.Value) != 128 {
		return result, fmt.Errorf("point key=%d ts=%d: %+v", key, timestamp, result)
	}
	for i, value := range result.Value {
		want := byte(key + 1)
		if i&1 != 0 {
			want = byte(timestamp)
		}
		if value != want {
			return result, fmt.Errorf("point key=%d ts=%d byte=%d got=%d want=%d", key, timestamp, i, value, want)
		}
	}
	return result, nil
}

func (f *successorWorkspaceFixture) history(prefix bool) (err error) {
	opts := mvcc.VersionIteratorOptions{ExactKey: f.keys[0], ReadTimestamp: uint64(f.depth)}
	wantKeys := 1
	if prefix {
		opts.ExactKey, opts.Prefix = nil, []byte("scratch/")
		wantKeys = len(f.keys)
	}
	it, err := f.store.IterateVersions(opts)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	count := 0
	for it.Valid() {
		entry := it.EntryView()
		key, ts := count/f.depth, uint64(f.depth-count%f.depth)
		if key >= wantKeys || !bytes.Equal(entry.Key, f.keys[key]) || entry.Timestamp != ts || entry.State != mvcc.Present || len(entry.Value) != 128 || entry.Value[0] != byte(key+1) || entry.Value[127] != byte(ts) {
			return fmt.Errorf("history prefix=%t position=%d: %+v", prefix, count, entry)
		}
		count++
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	if count != wantKeys*f.depth || it.Stats().Visited != uint64(count) {
		return fmt.Errorf("history prefix=%t count=%d want=%d stats=%+v", prefix, count, wantKeys*f.depth, it.Stats())
	}
	return nil
}

func TestCOWSuccessorWorkspacePublicGetAtAndHistory(t *testing.T) {
	for _, pointers := range []bool{false, true} {
		for _, depth := range []int{1, 8} {
			t.Run(fmt.Sprintf("pointer=%t/depth=%d", pointers, depth), func(t *testing.T) {
				f := prepareSuccessorWorkspace(t, depth, pointers)
				owned, err := f.point(0, 1)
				if err != nil {
					t.Fatal(err)
				}
				var wg sync.WaitGroup
				errorsSeen := make(chan error, 9)
				for reader := 0; reader < 8; reader++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for n := 0; n < 32; n++ {
							if _, err := f.point((reader+n)%len(f.keys), uint64(n%depth+1)); err != nil {
								errorsSeen <- err
								return
							}
						}
					}()
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					for n := 1; n <= 3; n++ {
						if err := f.store.CommitAt(uint64(depth+n), []mvcc.Mutation{{Key: []byte("other"), Value: []byte{byte(n)}}}, mvcc.CommitRelaxed); err != nil {
							errorsSeen <- err
							return
						}
						if err := f.db.Checkpoint(); err != nil {
							errorsSeen <- err
							return
						}
					}
				}()
				wg.Wait()
				close(errorsSeen)
				for err := range errorsSeen {
					t.Error(err)
				}
				if !bytes.Equal(owned.Value, successorWorkspaceValue(0, 1)) {
					t.Fatal("concurrent successor reads changed previously owned output")
				}
				if err := f.history(false); err != nil {
					t.Fatal(err)
				}
				if err := f.history(true); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// Diagnostic only. The candidate and baseline compile this exact public
// fixture. Fixed N is TOTAL completed calls, not N per concurrent worker.
func BenchmarkCOWSuccessorWorkspace(b *testing.B) {
	for _, pointers := range []bool{false, true} {
		for _, depth := range []int{1, 8} {
			for _, concurrency := range []int{1, 8} {
				b.Run(fmt.Sprintf("GetAt/pointer=%t/depth=%d/concurrency=%d", pointers, depth, concurrency), func(b *testing.B) {
					f := prepareSuccessorWorkspace(b, depth, pointers)
					start := make(chan struct{})
					failures := make(chan error, concurrency)
					var wg, ready sync.WaitGroup
					ready.Add(concurrency)
					for reader := 0; reader < concurrency; reader++ {
						wg.Add(1)
						go func() {
							defer wg.Done()
							ready.Done()
							<-start
							for n := reader; n < b.N; n += concurrency {
								if _, err := f.point(n%len(f.keys), uint64(n%depth+1)); err != nil {
									failures <- err
									return
								}
							}
						}()
					}
					ready.Wait()
					b.ReportAllocs()
					b.ResetTimer()
					close(start)
					wg.Wait()
					b.StopTimer()
					close(failures)
					for err := range failures {
						b.Fatal(err)
					}
				})
			}
		}
		// The unchanged iterator path is an allocation/time guard, not part of
		// the claimed point improvement. One call consumes all 256 versions.
		b.Run(fmt.Sprintf("Prefix/pointer=%t/depth=8/concurrency=1", pointers), func(b *testing.B) {
			f := prepareSuccessorWorkspace(b, 8, pointers)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if err := f.history(true); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
