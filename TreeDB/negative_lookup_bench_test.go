package treedb

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/tree"
)

var negativeLookupBenchSink []byte

// BenchmarkNegativeLookup qualifies ordinary public selection after checkpoint,
// with misses interleaved inside the persisted key range (not outside fences).
// TREEDB_HOT_PATH_STATS=1 adds exact-descent/rejection counters; keep it off for
// throughput qualification and run a separate counter proof.
func BenchmarkNegativeLookup(b *testing.B) {
	for _, payload := range []string{"compressible256", "random4096"} {
		b.Run(payload, func(b *testing.B) {
			for _, enabled := range []bool{false, true} {
				b.Run(fmt.Sprintf("enabled=%v", enabled), func(b *testing.B) {
					d, keys, misses := openNegativeLookupBench(b, payload, enabled)
					defer func() {
						if err := d.Close(); err != nil {
							b.Error(err)
						}
					}()
					old := d.AcquireSnapshot()
					defer func() {
						if err := old.Close(); err != nil {
							b.Error(err)
						}
					}()
					oldFilter := negativeLookupBenchSnapshotFilter(b, old)
					// Keep a genuinely older physical root while all public routes use the updated
					// view. Updating existing keys leaves membership density unchanged.
					updateNegativeLookupBenchFixture(b, d, keys[:64], enabled)
					if enabled && oldFilter == 0 {
						b.Fatal("old snapshot lacks active coverage")
					}
					for _, distribution := range []string{"uniform", "zipf"} {
						b.Run(distribution, func(b *testing.B) {
							for _, missPercent := range []int{0, 50, 90, 99} {
								b.Run(fmt.Sprintf("miss=%d", missPercent), func(b *testing.B) {
									rng := rand.New(rand.NewSource(342))
									zipf := rand.NewZipf(rng, 1.1, 1, uint64(len(keys)-1))
									const scheduleSize = 10000
									schedule := make([][]byte, scheduleSize)
									for i := range schedule {
										n := rng.Intn(len(keys))
										if distribution == "zipf" {
											n = int(zipf.Uint64())
										}
										schedule[i] = keys[n]
										if i%100 < missPercent {
											schedule[i] = misses[n]
										}
									}
									for _, route := range []string{"Get", "GetAppend", "GetMany64", "GetManyView64", "OldSnapshot"} {
										b.Run(route, func(b *testing.B) {
											dst := make([]byte, 0, 4096)
											batchKeys := make([][]byte, 64)
											consume := func(_ int, _, v []byte, _ bool) error { negativeLookupBenchSink = v; return nil }
											activeBytes := negativeLookupBenchActiveBytes(b, d, enabled)
											before := tree.ReadPathStatsSnapshot()
											b.ReportAllocs()
											b.ResetTimer()
											for i := 0; i < b.N; i++ {
												k := schedule[i%len(schedule)]
												var err error
												switch route {
												case "Get":
													negativeLookupBenchSink, err = d.Get(k)
												case "GetAppend":
													negativeLookupBenchSink, err = d.GetAppend(k, dst[:0])
													if errors.Is(err, tree.ErrKeyNotFound) {
														err = nil
													}
												case "OldSnapshot":
													negativeLookupBenchSink, err = old.Get(k)
													if errors.Is(err, tree.ErrKeyNotFound) {
														err = nil
													}
												default:
													for j := range batchKeys {
														batchKeys[j] = schedule[(i*len(batchKeys)+j)%len(schedule)]
													}
													if route == "GetMany64" {
														var out [][]byte
														out, err = d.GetMany(batchKeys)
														if len(out) > 0 {
															negativeLookupBenchSink = out[0]
														}
													} else {
														err = d.GetManyView(batchKeys, consume)
													}
												}
												if err != nil {
													b.Fatal(err)
												}
											}
											b.StopTimer()
											after := tree.ReadPathStatsSnapshot()
											reads := uint64(b.N)
											if route == "GetMany64" || route == "GetManyView64" {
												reads *= 64
											}
											if after.PointDescentsTotal+after.NegativeRejectsTotal > before.PointDescentsTotal+before.NegativeRejectsTotal {
												b.ReportMetric(float64(after.PointDescentsTotal-before.PointDescentsTotal)/float64(reads), "descents/key")
												b.ReportMetric(float64(after.NegativeRejectsTotal-before.NegativeRejectsTotal)/float64(reads), "rejects/key")
											}
											b.ReportMetric(float64(reads)/float64(b.N), "keys/op")
											if enabled {
												b.ReportMetric(float64(activeBytes), "filter-bytes")
											}
										})
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

func openNegativeLookupBench(b testing.TB, payload string, enabled bool) (*DB, [][]byte, [][]byte) {
	b.Helper()
	const count = 8192
	opts := Options{Dir: b.TempDir(), KeepRecent: 10000, IndexOuterLeavesInValueLog: true, LeafPrefixCompression: true, IndexColumnarLeaves: true, IndexPackedValuePtr: true, BackgroundCheckpointInterval: -1, BackgroundCheckpointIdleDuration: -1, MaxWALBytes: -1, BackgroundIndexVacuumInterval: -1, DisableBackgroundPrune: true}
	opts.ValueLog.PointerThreshold = 1
	if enabled {
		opts.NegativeLookupFilterBytes = count * 10 / 8
	}
	d, e := Open(opts)
	if e != nil {
		b.Fatal(e)
	}
	keys := make([][]byte, count)
	misses := make([][]byte, count)
	rng := rand.New(rand.NewSource(342))
	for base := 0; base < count; base += 256 {
		batch := d.NewBatch()
		for i := base; i < base+256; i++ {
			keys[i] = []byte(fmt.Sprintf("record/%08d/0", i))
			misses[i] = []byte(fmt.Sprintf("record/%08d/1", i))
			value := make([]byte, 256)
			for j := range value {
				value[j] = 'v'
			}
			if payload == "random4096" {
				value = make([]byte, 4096)
				rng.Read(value)
			}
			if e := batch.Set(keys[i], value); e != nil {
				b.Fatal(e)
			}
		}
		if e := batch.WriteSync(); e != nil {
			b.Fatal(e)
		}
		if e := batch.Close(); e != nil {
			b.Fatal(e)
		}
	}
	if e := d.Checkpoint(); e != nil {
		b.Fatal(e)
	}
	negativeLookupBenchActiveBytes(b, d, enabled)
	for _, key := range keys {
		if v, e := d.Get(key); e != nil || len(v) == 0 {
			b.Fatal(e)
		}
	}
	return d, keys, misses
}

func negativeLookupBenchSnapshotFilter(tb testing.TB, snapshot Snapshot) uintptr {
	tb.Helper()
	snap, ok := snapshot.(*backenddb.Snapshot)
	if !ok || snap == nil {
		tb.Fatal("checkpointed fixture did not select captured backend snapshot")
	}
	return reflect.ValueOf(snap).Elem().FieldByName("tree").FieldByName("negativeFilter").Pointer()
}

func negativeLookupBenchActiveBytes(tb testing.TB, d *DB, enabled bool) int {
	tb.Helper()
	active, err := strconv.Atoi(d.backend.Stats()["treedb.negative_lookup_filter.active_bytes"])
	expected := 0
	if enabled {
		expected = 10240
	}
	snap := d.backend.AcquireSnapshot()
	filter := negativeLookupBenchSnapshotFilter(tb, snap)
	if err := snap.Close(); err != nil {
		tb.Fatal(err)
	}
	if err != nil || active != expected || (filter != 0) != enabled {
		tb.Fatal("fixture active coverage mismatch", active, expected, filter, err)
	}
	return active
}

// Preserve payload size and entropy when producing the newer physical root.
func updateNegativeLookupBenchFixture(tb testing.TB, d *DB, keys [][]byte, enabled bool) []byte {
	tb.Helper()
	first, err := d.Get(keys[0])
	if err != nil || len(first) == 0 {
		tb.Fatal("missing update fixture", err)
	}
	update := d.NewBatch()
	for _, key := range keys {
		value, err := d.Get(key)
		if err != nil || len(value) == 0 {
			tb.Fatal("missing update fixture", err)
		}
		value[0] ^= 1 // Get owns its output; the old captured root stays untouched.
		if err := update.Set(key, value); err != nil {
			tb.Fatal(err)
		}
	}
	if err := update.WriteSync(); err != nil {
		tb.Fatal(err)
	}
	if err := update.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		tb.Fatal(err)
	}
	negativeLookupBenchActiveBytes(tb, d, enabled)
	return first
}

// The two public update boundaries separate cached durable acknowledgement from
// checkpointed backend publication. Neither is an isolated hash microbenchmark.
func BenchmarkNegativeLookupUpdate(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("enabled=%v", enabled), func(b *testing.B) {
			for _, checkpoint := range []bool{false, true} {
				name := "WriteSync"
				if checkpoint {
					name = "WriteSyncCheckpoint"
				}
				b.Run(name, func(b *testing.B) {
					d, keys, _ := openNegativeLookupBench(b, "compressible256", enabled)
					defer func() {
						if err := d.Close(); err != nil {
							b.Error(err)
						}
					}()
					value := make([]byte, 256)
					negativeLookupBenchActiveBytes(b, d, enabled)
					before := d.backend.State().CommitSeq
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						batch := d.NewBatch()
						for j := 0; j < 64; j++ {
							if err := batch.Set(keys[(i*64+j)%len(keys)], value); err != nil {
								b.Fatal(err)
							}
						}
						if err := batch.WriteSync(); err != nil {
							b.Fatal(err)
						}
						if err := batch.Close(); err != nil {
							b.Fatal(err)
						}
						if checkpoint {
							if err := d.Checkpoint(); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.StopTimer()
					negativeLookupBenchActiveBytes(b, d, enabled)
					publications := d.backend.State().CommitSeq - before
					if checkpoint && publications < uint64(b.N) {
						b.Fatal("checkpoint update did not publish every batch", publications, b.N)
					}
					b.ReportMetric(float64(publications)/float64(b.N), "publications/op")
					b.ReportMetric(64, "keys/op")
				})
			}
		})
	}
}

// Open includes bounded keys-only bootstrap plus ordinary backend open costs.
func BenchmarkNegativeLookupBootstrap(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("enabled=%v", enabled), func(b *testing.B) {
			d, _, _ := openNegativeLookupBench(b, "compressible256", false)
			dir := d.dir
			if err := d.Close(); err != nil {
				b.Fatal(err)
			}
			opts := Options{Dir: dir, DisableBackgroundPrune: true}
			if enabled {
				opts.NegativeLookupFilterBytes = 8192 * 10 / 8
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				backend, cleanup, err := OpenBackend(opts)
				if err != nil {
					b.Fatal(err)
				}
				active := backend.Stats()["treedb.negative_lookup_filter.active_bytes"]
				expected := "0"
				if enabled {
					expected = "10240"
				}
				if active != expected {
					b.Fatal("bootstrap active coverage mismatch", active, expected)
				}
				if err := cleanup(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
