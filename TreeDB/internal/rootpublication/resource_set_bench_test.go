package rootpublication

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

func BenchmarkStableResourceBuilderDistinctPhysicalAdd(b *testing.B) {
	for _, entries := range []int{8, 4096} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			file := writeStableResourceFixture(b, b.TempDir(), "scale.bin", "x")
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tokens := make([]*StableResourceToken, entries)
				for index := range tokens {
					tokens[index] = distinctPhysicalTokenFixture(b, file, uint64(index+1))
				}
				b.StartTimer()
				builder := NewStableResourceSetBuilder()
				for _, token := range tokens {
					if err := builder.Add(token); err != nil {
						b.Fatal(err)
					}
				}
				work := builder.ClosureWorkSnapshot()
				b.StopTimer()
				if entries > stableResourceEntryLinearLookupLimit && (work.PhysicalEntryLookupProbes != uint64(entries-stableResourceEntryLinearLookupLimit) || work.PhysicalEntryLookupComparisons != 0) {
					builder.Abandon()
					b.Fatalf("work=%+v", work)
				}
				b.ReportMetric(float64(work.PhysicalEntryLookupProbes)/float64(entries), "physical-lookup-probes/entry")
				b.ReportMetric(float64(work.PhysicalEntryLookupComparisons)/float64(entries), "physical-lookup-comparisons/entry")
				b.ReportMetric(float64(work.PhysicalEntryLookupAdmissions)/float64(entries), "physical-lookup-admissions/entry")
				builder.Abandon()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkStableResourceBuilderImmutableGenerations(b *testing.B) {
	for _, entries := range []int{8, 4096} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			file := writeStableResourceFixture(b, b.TempDir(), "generation.pack", "immutable-pack")
			digest := sha256.Sum256([]byte("immutable-pack"))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tokens := make([]*StableResourceToken, entries)
				for index := range tokens {
					tokens[index] = immutableGenerationTokenFixture(b, file, uint64(index+1), digest)
				}
				builder := NewStableResourceSetBuilder(ReachabilityOuterLeafPackedPointer)
				b.StartTimer()
				for _, token := range tokens {
					if err := builder.Add(token); err != nil {
						b.Fatal(err)
					}
				}
				work := builder.ClosureWorkSnapshot()
				b.StopTimer()
				var indexedEntries uint64
				if entries > stableResourceEntryLinearLookupLimit {
					indexedEntries = uint64(entries - stableResourceEntryLinearLookupLimit)
				}
				if work.PhysicalEntryLookupProbes != indexedEntries || work.PhysicalEntryLookupComparisons != 0 || work.PhysicalEntryLookupAdmissions != indexedEntries {
					b.Fatalf("work=%+v", work)
				}
				builder.Abandon()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkStableResourceBuilderDistinctPhysicalMerge(b *testing.B) {
	for _, entries := range []int{8, 4096} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			file := writeStableResourceFixture(b, b.TempDir(), "scale.bin", "x")
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				parent, childBuilder := NewStableResourceSetBuilder(), NewStableResourceSetBuilder()
				for index := 0; index < entries; index++ {
					if err := parent.Add(distinctPhysicalTokenFixture(b, file, uint64(index+1))); err != nil {
						b.Fatal(err)
					}
					if err := childBuilder.Add(distinctPhysicalTokenFixture(b, file, uint64(entries+index+1))); err != nil {
						b.Fatal(err)
					}
				}
				child, err := childBuilder.Freeze()
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if err := parent.Merge(child); err != nil {
					b.Fatal(err)
				}
				work := parent.ClosureWorkSnapshot()
				b.StopTimer()
				if entries > stableResourceEntryLinearLookupLimit && work.PhysicalEntryLookupComparisons != 0 {
					b.Fatalf("work=%+v", work)
				}
				b.ReportMetric(float64(work.PhysicalEntryLookupComparisons)/float64(entries), "physical-lookup-comparisons/child-entry")
				parent.Abandon()
				child.Release()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkStableResourceSetCloneDistinctPhysical(b *testing.B) {
	for _, entries := range []int{8, 4096} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			file := writeStableResourceFixture(b, b.TempDir(), "scale.bin", "x")
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				sourceBuilder := NewStableResourceSetBuilder()
				for index := 0; index < entries; index++ {
					if err := sourceBuilder.Add(distinctPhysicalTokenFixture(b, file, uint64(index+1))); err != nil {
						b.Fatal(err)
					}
				}
				source, err := sourceBuilder.Freeze()
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				cloned, work, err := CloneStableResourceSetForLogicalObligationsWithWork(source, StableLogicalObligationRequirements{})
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if work.SourceEntriesInspected != uint64(entries) || work.PhysicalHandleShares != uint64(entries) || work.PhysicalHandleCopies != 0 {
					b.Fatalf("clone work=%+v", work)
				}
				b.ReportMetric(float64(work.SourceEntriesInspected)/float64(entries), "source-entry-visits/entry")
				b.ReportMetric(float64(work.PhysicalHandleShares)/float64(entries), "physical-handle-shares/entry")
				cloned.Release()
				source.Release()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkStableResourceSetDependencyManifestCached(b *testing.B) {
	for _, entries := range []int{8, 4096} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			file := writeStableResourceFixture(b, b.TempDir(), "manifest-cache.bin", "x")
			builder := NewStableResourceSetBuilder()
			for id := uint64(1); id <= uint64(entries); id++ {
				if err := builder.Add(distinctPhysicalTokenFixture(b, file, id)); err != nil {
					builder.Abandon()
					b.Fatal(err)
				}
			}
			resources, err := builder.Freeze()
			if err != nil {
				b.Fatal(err)
			}
			defer resources.Release()
			if _, _, err := resources.DependencyManifestV1(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, work, err := resources.DependencyManifestV1()
				if err != nil {
					b.Fatal(err)
				}
				if work.EntriesEncoded != 0 || work.BytesEncoded != 0 {
					b.Fatalf("cached manifest work=%+v", work)
				}
			}
		})
	}
}
