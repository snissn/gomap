package cowbench

import (
	"bytes"
	"errors"
	"fmt"
	cowhelpers "github.com/snissn/gomap/TreeDB/internal/cowbench"
	mvcc "github.com/snissn/gomap/TreeDB/mvcc"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
)

// cowC3ValueLayout owns one public snapshot and probes every seeded physical
// record. Its callers put setup before the counter baseline and teardown after
// the endpoint. Non-COW snapshots can rotate pending memtables; COW captures
// affect lifetime residency high-water marks, but Close releases their leases.
func cowC3ValueLayout(db *treedb.DB, groups []mvcc.CommitGroup, pointers, cow bool) (inline, pointer uint64, err error) {
	before := db.Stats()
	snapshot := db.AcquireSnapshot()
	if snapshot == nil {
		return 0, 0, fmt.Errorf("layout snapshot missing")
	}
	defer func() {
		err = errors.Join(err, snapshot.Close())
		if cow {
			after := db.Stats()
			for _, key := range []string{"external_leases", "views", "active_cuts"} {
				name := "treedb.cache.cow." + key
				if before[name] == "" || before[name] != after[name] {
					err = errors.Join(err, fmt.Errorf("layout probe did not restore %s", name))
				}
			}
		}
	}()
	for _, group := range groups {
		for _, mutation := range group.Mutations {
			physical, e := mvcckey.Encode(mutation.Key, group.Timestamp)
			if e != nil {
				return inline, pointer, e
			}
			entry, e := snapshot.GetEntryExact(physical)
			if e != nil {
				return inline, pointer, e
			}
			if !bytes.Equal(entry.Key, physical) {
				return inline, pointer, fmt.Errorf("missing/wrong physical layout entry")
			}
			if e := cowhelpers.ValueLayout(entry, pointers); e != nil {
				return inline, pointer, e
			}
			if entry.Flags&node.FlagPointer != 0 {
				pointer++
			} else {
				inline++
			}
		}
	}
	return inline, pointer, nil
}

// BenchmarkC3PublicReadAdmission is a bounded, fixed-history actual Store
// fixture. Use fixed -benchtime=128x (smoke) or 1024x (collection), fresh process
// per leaf. Seed/setup/Close and stats inspection are excluded from timing.
// Sequential operations overwrite the same physical versions, keeping output
// and history identical on baseline/candidate. Concurrent timing covers one
// group writer, one point reader and one all-version reader, each N calls.
// No observer, artificial paused writer, instrumentation flag or bypass API is
// used. Calls completed with writerActive are ordinary overlapping calls; this
// alone does not identify their internal publication phase or prove liveness.
func BenchmarkC3PublicReadAdmission(b *testing.B) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		for _, mode := range []string{"cow_btree", "append_only", "btree"} {
			for _, pointers := range []bool{false, true} {
				shape := "inline"
				if pointers {
					shape = "forced_pointer"
				}
				for _, workload := range []string{"point", "group_versions", "concurrent"} {
					b.Run(fmt.Sprintf("%s/%s/%s/%s", profile, mode, shape, workload), func(b *testing.B) {
						if b.N > 4096 {
							b.Fatal("bounded fixture requires fixed benchtime <=4096x")
						}
						opts := treedb.OptionsFor(profile, b.TempDir())
						opts.DisableSideStores = true
						opts.BackgroundCheckpointInterval = -1
						opts.MemtableMode = mode
						opts.MemtableShards = 4
						opts.FlushThreshold = 16 << 20
						opts.ValueLog.ForcePointers = pointers
						opts.ValueLog.PointerThreshold = 1 << 30
						if pointers {
							opts.ValueLog.PointerThreshold = 1
						}
						db, err := treedb.Open(opts)
						if err != nil {
							b.Fatal(err)
						}
						defer func() {
							if err := db.Close(); err != nil {
								b.Error(err)
							}
						}()
						receipt := db.Stats()
						if receipt["treedb.profile.resolved"] != string(profile) || receipt["treedb.cache.memtable_mode"] != mode || receipt["treedb.profile.ordinary_ack_class"] != profile.OrdinaryAckClass() {
							b.Fatal("resolved fixture mismatch")
						}
						if err := cowhelpers.ACKRouting(profile, receipt); err != nil {
							b.Fatal(err)
						}
						store := mvcc.New(db)
						key := []byte("c3-a")
						other := []byte("c3-ab")
						value := bytes.Repeat([]byte{0x63}, 256)
						// Both histories always contain exactly two retained records. Replacements
						// are equal, so every concurrent cut has the same logical output.
						groups := []mvcc.CommitGroup{{Timestamp: 10, Mutations: []mvcc.Mutation{{Key: key, Value: value}, {Key: other, Value: value}}}, {Timestamp: 20, Mutations: []mvcc.Mutation{{Key: key, Value: value}, {Key: other, Value: value}}}}
						if err := store.CommitGroupAt(groups, mvcc.CommitRelaxed); err != nil {
							b.Fatal(err)
						}
						point := func() error {
							r, e := store.GetAt(key, 100)
							if e == nil && (r.State != Present || r.Timestamp != 20 || !bytes.Equal(r.Value, value)) {
								e = fmt.Errorf("wrong point: %+v", r)
							}
							return e
						}
						history := func() (mvcc.VersionIteratorStats, error) {
							it, e := store.IterateVersions(mvcc.VersionIteratorOptions{ExactKey: key, ReadTimestamp: 100})
							if e != nil {
								return mvcc.VersionIteratorStats{}, e
							}
							var count uint64
							for it.Valid() {
								v := it.EntryView()
								if count >= 2 || v.State != Present || v.Timestamp != 20-10*count || !bytes.Equal(v.Key, key) || !bytes.Equal(v.Value, value) {
									e = fmt.Errorf("wrong retained history at %d: %+v", count, v)
									break
								}
								count++
								it.Next()
							}
							stats := it.Stats()
							e = errors.Join(e, it.Error(), it.Close())
							if e == nil && (count != 2 || stats.Visited != 2 || stats.Retained != 2 || stats.Skipped != 0) {
								e = fmt.Errorf("output=%d stats=%+v", count, stats)
							}
							return stats, e
						}
						if err := point(); err != nil {
							b.Fatal(err)
						}
						if _, err := history(); err != nil {
							b.Fatal(err)
						}
						expectedLayout := uint64(0)
						for _, group := range groups {
							expectedLayout += uint64(len(group.Mutations))
						}
						preInline, prePointer, e := cowC3ValueLayout(db, groups, pointers, mode == "cow_btree")
						if e != nil || preInline+prePointer != expectedLayout {
							b.Fatalf("seed physical layout: %d/%d: %v", preInline, prePointer, e)
						}
						before := db.Stats()
						if err := cowhelpers.ACKRouting(profile, before); err != nil {
							b.Fatal(err)
						}

						writerTimes := make([]int64, b.N)
						pointTimes := make([]int64, b.N)
						scanTimes := make([]int64, b.N)
						var pointCount, scanCount, visited uint64
						var pointPhase, scanPhase time.Duration
						var overlapping atomic.Uint64
						var writerActive atomic.Bool
						b.ReportAllocs()
						b.StopTimer()
						var started time.Time
						if workload == "concurrent" {
							start := make(chan struct{})
							ready := make(chan struct{}, 3)
							results := make(chan error, 3)
							var phaseStarted time.Time
							go func() {
								ready <- struct{}{}
								<-start
								for n := 0; n < b.N; n++ {
									writerActive.Store(true)
									begin := time.Now()
									e := store.CommitGroupAt(groups, mvcc.CommitRelaxed)
									writerTimes[n] = time.Since(begin).Nanoseconds()
									writerActive.Store(false)
									if e != nil {
										results <- e
										return
									}
								}
								results <- nil
							}()
							go func() {
								ready <- struct{}{}
								<-start
								for n := 0; n < b.N; n++ {
									begin := time.Now()
									e := point()
									pointTimes[n] = time.Since(begin).Nanoseconds()
									if e != nil {
										results <- e
										return
									}
									pointCount++
									if writerActive.Load() {
										overlapping.Add(1)
									}
								}
								pointPhase = time.Since(phaseStarted)
								results <- nil
							}()
							go func() {
								ready <- struct{}{}
								<-start
								for n := 0; n < b.N; n++ {
									begin := time.Now()
									stats, e := history()
									scanTimes[n] = time.Since(begin).Nanoseconds()
									if e != nil {
										results <- e
										return
									}
									scanCount++
									visited += stats.Visited
									if writerActive.Load() {
										overlapping.Add(1)
									}
								}
								scanPhase = time.Since(phaseStarted)
								results <- nil
							}()
							for n := 0; n < 3; n++ {
								<-ready
							}
							b.ResetTimer()
							b.StartTimer()
							started = time.Now()
							phaseStarted = started
							close(start)
							for n := 0; n < 3; n++ {
								if e := <-results; e != nil {
									err = errors.Join(err, e)
								}
							}
						} else {
							b.ResetTimer()
							b.StartTimer()
							started = time.Now()
							for n := 0; n < b.N; n++ {
								begin := time.Now()
								if workload == "point" {
									err = store.CommitAt(20, []mvcc.Mutation{{Key: key, Value: value}}, mvcc.CommitRelaxed)
								} else {
									err = store.CommitGroupAt(groups, mvcc.CommitRelaxed)
								}
								writerTimes[n] = time.Since(begin).Nanoseconds()
								if err != nil {
									break
								}
								begin = time.Now()
								if workload == "point" {
									err = point()
									pointTimes[n] = time.Since(begin).Nanoseconds()
									pointCount++
								} else {
									var stats mvcc.VersionIteratorStats
									stats, err = history()
									scanTimes[n] = time.Since(begin).Nanoseconds()
									scanCount++
									visited += stats.Visited
								}
								if err != nil {
									break
								}
							}
						}
						elapsed := time.Since(started)
						b.StopTimer()
						if err != nil {
							b.Fatal(err)
						}
						after := db.Stats()
						if err := cowhelpers.ACKRouting(profile, after); err != nil {
							b.Fatal(err)
						}
						for _, stats := range []map[string]string{before, after} {
							if stats["treedb.profile.resolved"] != string(profile) || stats["treedb.cache.memtable_mode"] != mode || stats["treedb.profile.ordinary_ack_class"] != profile.OrdinaryAckClass() {
								b.Fatal("actual boundary fixture mismatch")
							}
						}
						postInline, postPointer, e := cowC3ValueLayout(db, groups, pointers, mode == "cow_btree")
						if e != nil || postInline+postPointer != expectedLayout {
							b.Fatalf("final physical layout: %d/%d: %v", postInline, postPointer, e)
						}
						b.ReportMetric(float64(expectedLayout), "layout_expected_records")
						b.ReportMetric(float64(preInline), "layout_before_inline")
						b.ReportMetric(float64(prePointer), "layout_before_pointer")
						b.ReportMetric(float64(postInline), "layout_after_inline")
						b.ReportMetric(float64(postPointer), "layout_after_pointer")
						b.ReportMetric(1, "ack_routing_before_ok")
						b.ReportMetric(1, "ack_routing_after_ok")

						if workload == "concurrent" {
							if pointPhase <= 0 || scanPhase <= 0 {
								b.Fatal("missing reader-phase elapsed")
							}
							b.ReportMetric(float64(pointPhase.Nanoseconds()), "point_phase_elapsed_ns")
							b.ReportMetric(float64(scanPhase.Nanoseconds()), "scan_phase_elapsed_ns")
							b.ReportMetric(float64(b.N)/pointPhase.Seconds(), "point_phase_ops/s")
							b.ReportMetric(float64(b.N)/scanPhase.Seconds(), "scan_phase_ops/s")
						}
						b.ReportMetric(float64(b.N)/elapsed.Seconds(), "writer_ops/s")
						b.ReportMetric(float64(pointCount+scanCount)/elapsed.Seconds(), "reader_ops/s")
						b.ReportMetric(float64(pointCount)/float64(b.N), "point_calls/op")
						b.ReportMetric(float64(scanCount)/float64(b.N), "scan_calls/op")
						b.ReportMetric(float64(visited)/float64(b.N), "visited/op")
						b.ReportMetric(float64(2*scanCount+pointCount)/float64(b.N), "output/op")
						b.ReportMetric(float64(overlapping.Load())/float64(b.N), "readers_while_writer_active/op")
						for _, latency := range []struct {
							name   string
							values []int64
							active bool
						}{{"writer", writerTimes, true}, {"point", pointTimes, pointCount > 0}, {"scan", scanTimes, scanCount > 0}} {
							if latency.active {
								sort.Slice(latency.values, func(i, j int) bool { return latency.values[i] < latency.values[j] })
								for _, p := range []int{50, 95, 99} {
									b.ReportMetric(float64(latency.values[(b.N-1)*p/100]), fmt.Sprintf("%s_p%d_ns", latency.name, p))
								}
							}
						}
						for _, counter := range []struct{ metric, key, absolute string }{{"wal_appends/op", "treedb.command_wal.append.count_total", "wal_appends"}, {"wal_syncs/op", "treedb.command_wal.file_sync.calls_total", "wal_syncs"}} {
							x, y, e := cowhelpers.WALCounters(profile, before, after, counter.key)
							if e != nil {
								b.Fatal(e)
							}
							b.ReportMetric(float64(y-x)/float64(b.N), counter.metric)
							b.ReportMetric(float64(x), counter.absolute+"_before")
							b.ReportMetric(float64(y), counter.absolute+"_after")
						}
						for _, counter := range []struct{ metric, key string }{{"snapshot_rotations/op", "treedb.cache.snapshot.rotations_total"}, {"rotated_shards/op", "treedb.cache.snapshot.rotated_shards_total"}, {"enqueued_records/op", "treedb.cache.snapshot.enqueued_records_total"}} {
							x := cowIntegrationCounter(b, before, counter.key)
							y := cowIntegrationCounter(b, after, counter.key)
							if y < x {
								b.Fatal("snapshot counter regression")
							}
							b.ReportMetric(float64(y-x)/float64(b.N), counter.metric)
						}
						if mode == "cow_btree" {
							for _, name := range []string{"capture_calls_total", "prepare_calls_total", "publications_total"} {
								key := "treedb.cache.cow." + name
								x := cowIntegrationCounter(b, before, key)
								y := cowIntegrationCounter(b, after, key)
								if y < x {
									b.Fatal("COW counter regression")
								}
								b.ReportMetric(float64(y-x)/float64(b.N), "cow_"+name+"/op")
							}
							for _, name := range []string{"total_bytes", "peak_bytes", "views", "external_leases", "active_cuts"} {
								b.ReportMetric(float64(cowIntegrationCounter(b, after, "treedb.cache.cow."+name)), "cow_end_"+name)
							}
						}
						if err := db.Close(); err != nil {
							b.Fatal(err)
						}
						b.ReportMetric(1, "close_ok")
					})
				}
			}
		}
	}
}

func cowIntegrationCounter(b *testing.B, stats map[string]string, key string) uint64 {
	b.Helper()
	value, ok := stats[key]
	if !ok {
		b.Fatalf("missing actual counter %s", key)
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		b.Fatalf("counter %s: %v", key, err)
	}
	return n
}
