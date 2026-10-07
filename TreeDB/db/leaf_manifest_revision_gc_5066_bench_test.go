//go:build darwin || linux || freebsd || netbsd || openbsd

package db

import (
	"context"
	"testing"
)

// Setup and Close are excluded. One operation reclaims 32 released immutable
// revisions through the public GC, including recovery capture and namespace sync.
func BenchmarkLeafManifestRevisionGC5066(b *testing.B) {
	b.ReportAllocs()
	var deleted int
	for range b.N {
		b.StopTimer()
		database, err := Open(Options{Dir: b.TempDir(), IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true})
		if err != nil {
			b.Fatal(err)
		}
		for range 32 {
			closure, err := database.PrepareLeafGenerationManifestStableClosure()
			if err != nil {
				b.Fatal(err)
			}
			closure.Release()
		}
		b.StartTimer()
		stats, err := database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if stats.ManifestRevisionsDeleted != 32 {
			b.Fatalf("deleted=%d want=32", stats.ManifestRevisionsDeleted)
		}
		deleted += stats.ManifestRevisionsDeleted
		if err := database.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(deleted)/float64(b.N), "revisions/op")
}
