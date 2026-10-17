package treedb

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

// BenchmarkPublicCOWTxn measures enabled transaction cost, not a speedup against
// the unsupported pre-P1 entrypoint. A deliberate unrelated write occurs between
// capture and commit. Scan/ReverseScan early-close after inspecting its first record.
// Wait/hold metrics cover both publications per iteration; validation is the
// writing transaction validation. Setup, final drain and reporting are untimed.
func BenchmarkPublicCOWTxn(b *testing.B) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		for _, shape := range []string{"Point1", "Point10", "Scan", "ReverseScan", "ScanNewer2048", "ReverseScanNewer2048"} {
			for _, history := range []int{0, 2048} {
				if history != 0 && strings.HasSuffix(shape, "Newer2048") {
					continue
				}
				b.Run(fmt.Sprintf("%s/%s/History%d", profile, shape, history), func(b *testing.B) {
					opts := OptionsFor(profile, b.TempDir())
					opts.MemtableMode = "cow_btree"
					opts.DisableSideStores = true
					opts.BackgroundCheckpointInterval = -1
					opts.BackgroundCheckpointIdleDuration = -1
					opts.BackgroundIndexVacuumInterval = -1
					opts.ValueLog.ForcePointers = false
					opts.ValueLog.PointerThreshold = 1 << 20
					db, err := Open(opts)
					if err != nil {
						b.Fatal(err)
					}
					defer func() {
						if err := db.Close(); err != nil {
							b.Fatal(err)
						}
					}()
					depKeys := make([][]byte, 10)
					for n := range depKeys {
						depKeys[n] = []byte(fmt.Sprintf("dep/%02d", n))
					}
					seed := db.NewBatchWithSize(history + 10)
					for n := 0; n < history; n++ {
						if err := seed.Set([]byte(fmt.Sprintf("history/%04d", n)), []byte("v")); err != nil {
							b.Fatal(err)
						}
					}
					for n := 0; n < 10; n++ {
						if err := seed.Set(depKeys[n], []byte("v")); err != nil {
							b.Fatal(err)
						}
					}
					if err := seed.Write(); err != nil {
						b.Fatal(err)
					}
					if err := seed.Close(); err != nil {
						b.Fatal(err)
					}
					newerKeys := make([][]byte, 2048)
					for n := range newerKeys {
						newerKeys[n] = []byte(fmt.Sprintf("unrelated/%04d", n))
					}
					before := db.Stats()
					b.ReportAllocs()
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						tx, err := db.NewConditionalTxn()
						if err != nil {
							b.Fatal(err)
						}
						if strings.Contains(shape, "Scan") {
							open := tx.Iterator
							if strings.HasPrefix(shape, "Reverse") {
								open = tx.ReverseIterator
							}
							it, err := open([]byte("dep/"), []byte("dep0"))
							if err != nil {
								b.Fatal(err)
							}
							if !it.Valid() {
								b.Fatal("missing seeded range")
							}
							if err := it.Error(); err != nil {
								b.Fatal(err)
							}
							if err := it.Close(); err != nil {
								b.Fatal(err)
							}
						} else {
							count := 1
							if shape == "Point10" {
								count = 10
							}
							for k := 0; k < count; k++ {
								if _, err := tx.Get(depKeys[k]); err != nil {
									b.Fatal(err)
								}
							}
						}
						if strings.HasSuffix(shape, "Newer2048") {
							newer := db.NewBatchWithSize(len(newerKeys))
							for _, key := range newerKeys {
								if err := newer.Set(key, []byte("v")); err != nil {
									b.Fatal(err)
								}
							}
							if err := newer.Write(); err != nil {
								b.Fatal(err)
							}
							if err := newer.Close(); err != nil {
								b.Fatal(err)
							}
						} else if err := db.Set([]byte("unrelated"), []byte("v")); err != nil {
							b.Fatal(err)
						}
						if err := tx.Set([]byte("result"), []byte("v")); err != nil {
							b.Fatal(err)
						}
						if err := tx.Commit(); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					after := db.Stats()
					for _, metric := range []string{"journal_effects", "journal_bytes", "index_points", "too_old_total", "barrier_effect_capacity_total", "barrier_byte_capacity_total", "barrier_conflicts_total"} {
						value := cowTxnRequiredStat(b, after, "treedb.cache.cow.txn."+metric)
						b.ReportMetric(value, metric)
					}
					for _, metric := range []string{"validation_ns", "commit_wait_ns", "commit_hold_ns"} {
						a := cowTxnRequiredStat(b, after, "treedb.cache.cow.txn."+metric)
						z := cowTxnRequiredStat(b, before, "treedb.cache.cow.txn."+metric)
						b.ReportMetric((a-z)/float64(b.N), metric+"/iteration")
					}
					memory := db.cached.COWMemoryStats()
					b.ReportMetric(float64(memory.TotalBytes), "live-engine-B")
					b.ReportMetric(float64(memory.PeakBytes), "peak-engine-B")
					drainStarted := time.Now()
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(float64(time.Since(drainStarted).Nanoseconds()), "final-drain-ns")
				})
			}
		}
	}
}

func cowTxnRequiredStat(b *testing.B, stats map[string]string, key string) float64 {
	b.Helper()
	text, found := stats[key]
	value, err := strconv.ParseFloat(text, 64)
	if !found || err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		b.Fatalf("required statistic %s=%q: present=%t parse=%v", key, text, found, err)
	}
	return value
}
