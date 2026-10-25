package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// This separate opt-in diagnostic deliberately charges timestamps and periodic
// observation. It must not replace the uninstrumented throughput/allocation
// benchmark. All latency slots are allocated before requests begin; each writer
// owns its slots. No sample is discarded or retried after a failed request.
func TestStandalonePublicResourceDiagnostic(t *testing.T) {
	if os.Getenv("GOMAP_MONGO_PUBLIC_DIAGNOSTIC") != "1" {
		t.Skip("opt-in public latency/resource diagnostic")
	}
	operations := publicDiagnosticInt(t, "GOMAP_MONGO_DIAGNOSTIC_OPERATIONS", 10000)
	payload := publicDiagnosticInt(t, "GOMAP_MONGO_DIAGNOSTIC_PAYLOAD", 4096)
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed} {
		for _, writers := range []int{1, 8} {
			t.Run(fmt.Sprintf("%s/writers=%d", profile, writers), func(t *testing.T) {
				server, col, ctx, dir, finish := openStandalonePublicFixture(t, profile, writers, payload)
				ordinary, err := server.Collections.OpenCollection("publicbench.rows")
				if err != nil {
					t.Fatal(err)
				}
				held, err := ordinary.OpenCollectionReadView()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = held.Close() })
				var seedIDs [][]byte
				if err := held.VisitIndexValueIDs("n_1", bson.RawValue{Type: bson.TypeInt64, Value: make([]byte, 8)}, func(id []byte) error {
					seedIDs = append(seedIDs, bytes.Clone(id))
					return nil
				}); err != nil || len(seedIDs) != writers*64 {
					t.Fatalf("held seed membership=%d: %v", len(seedIDs), err)
				}
				latencies := make([]time.Duration, operations)
				succeeded := make([]bool, operations)
				failures := make(chan error, writers)
				start := time.Now()
				var samples []publicResourceSample
				samples = append(samples, samplePublicResources(t, server, dir, "before", start))
				stop, sampled := make(chan struct{}), make(chan struct{})
				go func() {
					defer close(sampled)
					ticker := time.NewTicker(time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-stop:
							return
						case <-ticker.C:
							samples = append(samples, samplePublicResources(t, server, dir, "held-churn", start))
						}
					}
				}()
				var wg sync.WaitGroup
				ackStart := time.Now()
				for writer := 0; writer < writers; writer++ {
					wg.Add(1)
					go func(writer int) {
						defer wg.Done()
						for operation := writer; operation < operations; operation += writers {
							local := operation / writers
							id := int64(writer*64 + local%64)
							requestStart := time.Now()
							result, err := col.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(local/64 + 1)}}}})
							latencies[operation] = time.Since(requestStart)
							if err != nil {
								failures <- fmt.Errorf("operation %d: %w", operation, err)
								return
							}
							if result.MatchedCount != 1 || result.ModifiedCount != 1 {
								failures <- fmt.Errorf("operation %d result=%+v", operation, result)
								return
							}
							succeeded[operation] = true
						}
					}(writer)
				}
				wg.Wait()
				ackElapsed := time.Since(ackStart)
				close(stop)
				<-sampled
				close(failures)
				for err := range failures {
					t.Error(err)
				}
				samples = append(samples, samplePublicResources(t, server, dir, "after-ACK-held", start))
				var heldIDs [][]byte
				if err := held.VisitIndexValueIDs("n_1", bson.RawValue{Type: bson.TypeInt64, Value: make([]byte, 8)}, func(id []byte) error { heldIDs = append(heldIDs, bytes.Clone(id)); return nil }); err != nil || !equalPublicIDs(seedIDs, heldIDs) {
					t.Errorf("held membership changed: count=%d error=%v", len(heldIDs), err)
				}
				got, err := held.FetchDocumentsByID(seedIDs[:1], collections.DocumentFetchOptions{})
				heldN, valid := int64(0), false
				if err == nil && len(got.Results) == 1 && got.Results[0].Found {
					heldN, valid = bson.Raw(got.Results[0].Document).Lookup("n").Int64OK()
				}
				if err != nil || !valid || heldN != 0 {
					t.Errorf("held document changed: %+v/%v", got, err)
				}
				if err := held.Close(); err != nil {
					t.Error(err)
				}
				samples = append(samples, samplePublicResources(t, server, dir, "reader-released", start))
				// Observe natural idle maintenance; do not checkpoint between phases.
				time.Sleep(2 * time.Second)
				samples = append(samples, samplePublicResources(t, server, dir, "idle-2s", start))
				finish()
				runtime.GC()
				samples = append(samples, samplePublicResources(t, server, dir, "closed-GC", start))
				attempted, completed := 0, 0
				for i, duration := range latencies {
					if duration > 0 {
						attempted++
					}
					if succeeded[i] {
						completed++
					}
				}
				var p50, p95, p99, maximum *int64
				if !t.Failed() && completed == operations {
					sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
					a, b, c, d := int64(latencies[(operations-1)*50/100]), int64(latencies[(operations-1)*95/100]), int64(latencies[(operations-1)*99/100]), int64(latencies[operations-1])
					p50, p95, p99, maximum = &a, &b, &c, &d
				}
				report := struct {
					Operations                 int     `json:"operations"`
					Attempted                  int     `json:"attempted"`
					Completed                  int     `json:"completed"`
					PayloadBytes               int     `json:"payload_bytes"`
					Failed                     bool    `json:"failed"`
					ACKSeconds                 float64 `json:"ack_seconds"`
					P50NS, P95NS, P99NS, MaxNS *int64
					Samples                    []publicResourceSample `json:"samples"`
				}{operations, attempted, completed, payload, t.Failed(), ackElapsed.Seconds(), p50, p95, p99, maximum, samples}
				encoded, err := json.Marshal(report)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("PUBLIC_RESOURCE_JSON %s", encoded)
			})
		}
	}
}

func publicDiagnosticInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	if value := os.Getenv(name); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 8 || n > 1000000 {
			t.Fatalf("%s must be in [8,1000000]", name)
		}
		return n
	}
	return fallback
}

func equalPublicIDs(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

type publicResourceSample struct {
	Phase                                     string  `json:"phase"`
	Seconds                                   float64 `json:"seconds"`
	HeapAlloc, HeapInuse, HeapSys, TotalAlloc uint64
	RSSBytes                                  *uint64           `json:"rss_bytes"`
	FileBytes                                 int64             `json:"file_bytes"`
	Stats                                     map[string]string `json:"stats"`
}

func samplePublicResources(t *testing.T, server *mongogateway.StandaloneServer, dir, phase string, start time.Time) publicResourceSample {
	t.Helper()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	out := publicResourceSample{Phase: phase, Seconds: time.Since(start).Seconds(), HeapAlloc: memory.HeapAlloc, HeapInuse: memory.HeapInuse, HeapSys: memory.HeapSys, TotalAlloc: memory.TotalAlloc, Stats: make(map[string]string)}
	// Portable harness overlay: the historical standalone only exposes backend
	// diagnostics. Missing owning-COW counters remain absent, never invented zero.
	var stats map[string]string
	if owner, ok := any(server).(interface{ Stats() map[string]string }); ok {
		stats = owner.Stats()
	} else {
		stats = server.Backend.Stats()
	}
	for key, value := range stats {
		if strings.HasPrefix(key, "treedb.cache.cow.") || strings.HasPrefix(key, "treedb.command_wal.") || strings.HasPrefix(key, "treedb.cache.checkpoint.") || key == "treedb.applied_command_lsn" {
			out.Stats[key] = value
		}
	}
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				fields := strings.Fields(line)
				if len(fields) == 3 {
					if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
						rss := kb * 1024
						out.RSSBytes = &rss
					}
				}
			}
		}
	}
	if err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if os.IsNotExist(err) {
				return nil
			} // a concurrent maintenance unlink
			if err != nil {
				return err
			}
			out.FileBytes += info.Size()
		}
		return nil
	}); err != nil {
		t.Errorf("resource directory sample: %v", err)
	}
	return out
}
