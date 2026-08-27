package db

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

var benchmarkVacuumOnlineStatsSink VacuumOnlineStats
var benchmarkVacuumDurableResourceSummarySink uint64

func BenchmarkVacuumOnlineStatsSnapshot(b *testing.B) {
	database := &DB{}
	database.vacuumOnlineLast.Store(&VacuumOnlineStats{})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkVacuumOnlineStatsSink = database.vacuumOnlineStatsSnapshot()
	}
}

func BenchmarkVacuumDurableResourceSummary(b *testing.B) {
	for _, benchmark := range []struct {
		resources int
		rids      int
		mixed     bool
	}{
		{resources: 1}, {resources: 64}, {resources: 1024}, {resources: 1024, mixed: true},
		{resources: 1, rids: 1024}, {resources: 64, rids: 16},
	} {
		kinds := 1
		if benchmark.mixed {
			kinds = 2
		}
		b.Run(fmt.Sprintf("resources=%d/rids=%d/kinds=%d", benchmark.resources, benchmark.rids, kinds), func(b *testing.B) {
			set := benchmarkVacuumDurableResourceSet(b, benchmark.resources, benchmark.rids, benchmark.mixed)
			b.Cleanup(set.Release)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				descriptors, bytes := vacuumDurableResourceSummary(set)
				benchmarkVacuumDurableResourceSummarySink = descriptors + bytes
			}
		})
	}
}

func benchmarkVacuumDurableResourceSet(b *testing.B, resources, rids int, mixed bool) *rootpublication.StableResourceSet {
	b.Helper()
	file, err := os.CreateTemp(b.TempDir(), "vacuum-resource")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := file.WriteString("x"); err != nil {
		_ = file.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = file.Close() })
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityValueLogPointer)
	var ridValues []uint64
	if rids > 0 {
		ridValues = make([]uint64, rids)
		for i := range ridValues {
			ridValues[i] = uint64(i + 1)
		}
	}
	for i := range resources {
		var objectID [16]byte
		binary.LittleEndian.PutUint64(objectID[:], uint64(i+1))
		frontier := rootpublication.DurableFrontier{Bytes: 1}
		if rids > 0 {
			frontier = rootpublication.NewRIDFrontier(ridValues)
			frontier.Bytes = 1
		}
		kind, reachability, lane := rootpublication.ResourceValueLog, rootpublication.ReachabilityValueLogPointer, "benchmark"
		if mixed && i%2 != 0 {
			kind, reachability, lane = rootpublication.ResourceColumnAsset, rootpublication.ReachabilityColumnManifest, "columns"
		}
		token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
			Kind: kind, LogicalLane: lane, ResourceID: fmt.Sprint(i + 1),
			Generation: uint64(i + 1), DiagnosticPath: "benchmark/vacuum-resource", File: file,
			Frontier: frontier, Reachability: reachability,
			ContentSynced: true, StableIdentityOverride: rootpublication.StableIdentity{Platform: "benchmark", ObjectID: objectID},
		})
		if err != nil {
			builder.Abandon()
			b.Fatal(err)
		}
		if err := builder.Add(token); err != nil {
			builder.Abandon()
			b.Fatal(err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		b.Fatal(err)
	}
	return set
}
