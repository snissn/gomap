package db_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	dbpkg "github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkPL06ExternalVacuumCollectionForegroundChurn(b *testing.B) {
	for _, tc := range []struct {
		name      string
		valueSize int
	}{
		{name: "bytes_1x", valueSize: 16},
		{name: "bytes_64x", valueSize: 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			database := openPL06PublicChurnBenchmarkDB(b, tc.valueSize)
			defer func() { _ = database.Close() }()

			var recording atomic.Bool
			stop := make(chan struct{})
			warmed := make(chan struct{})
			writerDone := make(chan pl06PublicChurnResult, 1)
			go runPL06PublicChurnWriter(database, &recording, stop, warmed, writerDone)
			<-warmed

			b.ReportAllocs()
			b.ResetTimer()
			recording.Store(true)
			var vacuumErrors uint64
			for i := 0; i < b.N; i++ {
				if err := database.VacuumIndexOnline(context.Background()); err != nil {
					vacuumErrors++
				}
			}
			recording.Store(false)
			close(stop)
			foreground := <-writerDone
			b.StopTimer()
			if foreground.err != nil {
				b.Fatalf("foreground churn: %v", foreground.err)
			}

			b.ReportMetric(float64(pl06PublicChurnPercentile(foreground.latencies, 95).Nanoseconds()), "foreground-p95-ns")
			b.ReportMetric(float64(pl06PublicChurnPercentile(foreground.latencies, 99).Nanoseconds()), "foreground-p99-ns")
			b.ReportMetric(float64(len(foreground.latencies))/float64(b.N), "foreground-samples/op")
			b.ReportMetric(float64(foreground.points)/float64(b.N), "foreground-points/op")
			b.ReportMetric(float64(foreground.ranges)/float64(b.N), "foreground-ranges/op")
			b.ReportMetric(float64(vacuumErrors)/float64(b.N), "vacuum-errors/op")
		})
	}
}

func openPL06PublicChurnBenchmarkDB(b *testing.B, valueSize int) *dbpkg.DB {
	b.Helper()
	database, err := dbpkg.Open(dbpkg.Options{
		Dir:       b.TempDir(),
		ChunkSize: 1 << 20,
		ValueLog: dbpkg.ValueLogOptions{
			PointerThreshold: 4096,
		},
	})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	manager := collections.NewCollectionManager(database)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "pl06_public_churn",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
		},
	}); err != nil {
		_ = database.Close()
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("pl06_public_churn")
	if err != nil {
		_ = database.Close()
		b.Fatalf("open collection: %v", err)
	}
	ids := make([][]byte, 1024)
	documents := make([][]byte, len(ids))
	document := []byte(`{"v":"` + strings.Repeat("x", valueSize-8) + `"}`)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		documents[i] = document
	}
	if _, err := collection.InsertBatch(ids, documents); err != nil {
		_ = database.Close()
		b.Fatalf("seed collection: %v", err)
	}
	return database
}

type pl06PublicChurnResult struct {
	latencies []time.Duration
	points    uint64
	ranges    uint64
	err       error
}

func runPL06PublicChurnWriter(database *dbpkg.DB, recording *atomic.Bool, stop <-chan struct{}, warmed chan<- struct{}, done chan<- pl06PublicChurnResult) {
	const warmOperations = 256
	result := pl06PublicChurnResult{latencies: make([]time.Duration, 0, warmOperations*4)}
	var warmOnce sync.Once
	defer func() {
		warmOnce.Do(func() { close(warmed) })
		done <- result
	}()
	for operation := 0; ; operation++ {
		select {
		case <-stop:
			return
		default:
		}
		recordBefore := recording.Load()
		started := time.Now()
		if operation%2 == 0 {
			key := []byte(fmt.Sprintf("foreground/point/%06d", operation%256))
			value := []byte(fmt.Sprintf("value/%06d", operation))
			result.err = database.Set(key, value)
			if recordBefore || recording.Load() {
				result.points++
			}
		} else {
			batch := database.NewBatch()
			rangeID := operation % 64
			startKey := []byte(fmt.Sprintf("foreground/range/%03d/a", rangeID))
			endKey := []byte(fmt.Sprintf("foreground/range/%03d/z", rangeID))
			result.err = batch.DeleteRange(startKey, endKey)
			if result.err == nil {
				result.err = batch.Write()
			}
			if closeErr := batch.Close(); result.err == nil {
				result.err = closeErr
			}
			if recordBefore || recording.Load() {
				result.ranges++
			}
		}
		if recordBefore || recording.Load() {
			result.latencies = append(result.latencies, time.Since(started))
		}
		if operation+1 >= warmOperations {
			warmOnce.Do(func() { close(warmed) })
		}
		if result.err != nil {
			return
		}
	}
}

func pl06PublicChurnPercentile(latencies []time.Duration, percentile int) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := (len(sorted)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	if index > len(sorted) {
		index = len(sorted)
	}
	return sorted[index-1]
}
