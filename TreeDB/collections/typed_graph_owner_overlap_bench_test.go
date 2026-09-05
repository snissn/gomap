package collections

import (
	"fmt"
	"testing"
)

// 1024x8D/M16, K10/ef128, same durability and output ownership as the frozen
// read-owner benchmark. Fixture/bootstrap/writes/Flush and keeper creation/
// release are outside timers. OpenClose measures a fresh current owner; warm
// queries reuse its pin/scratch. Keeper is a real old coherent owner here, NOT
// a proposed service lifetime: it retains an old snapshot and must be accounted.
func BenchmarkTypedGraphOwnerOverlap(b *testing.B) {
	for _, d := range []int{0, 32, 256} {
		b.Run(fmt.Sprintf("suffix%d", d), func(b *testing.B) {
			col, fixtureBase, ids, retained, columns, _ := openTypedGraphQualityFixture(b, 1024)
			if err := fixtureBase.Close(); err != nil {
				b.Fatal(err)
			}
			limits := typedGraphOverlapLimits()
			if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 2048, OwnedBytes: 8 << 20}, limits.Cold); err != nil {
				b.Fatal(err)
			}
			keeper, err := col.openTypedGraphReadOwner(limits)
			if err != nil {
				b.Fatal(err)
			}
			defer keeper.Close()
			if d > 0 {
				changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:d]}, {Name: "content", Strings: make([]string, d)}, {Name: "user", Strings: columns[2].Strings[:d]}, {Name: "path", Strings: columns[3].Strings[:d]}}
				for i := range changed[1].Strings {
					changed[1].Strings[i] = "changed current content"
				}
				if _, err := col.ReplaceTypedBatch(ids[:d], retained[:d], changed); err != nil {
					b.Fatal(err)
				}
				if err := col.Flush(); err != nil {
					b.Fatal(err)
				}
			}
			b.Run("overlap/OpenClose", func(b *testing.B) {
				before := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					owner, err := col.openTypedGraphReadOwner(limits)
					if err != nil {
						b.Fatal(err)
					}
					if err := owner.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				after := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
				if after.CacheBuilds != before.CacheBuilds || after.CacheHits-before.CacheHits != uint64(b.N) {
					b.Fatalf("unexpected build/hit counts: before=%+v after=%+v", before, after)
				}
				b.ReportMetric(float64(after.CacheBuilds-before.CacheBuilds)/float64(b.N), "prepared-builds/op")
				b.ReportMetric(float64(after.CacheHits-before.CacheHits)/float64(b.N), "prepared-hits/op")
			})
			for _, docs := range []bool{false, true} {
				b.Run(fmt.Sprintf("overlap/documents%t", docs), func(b *testing.B) {
					owner, err := col.openTypedGraphReadOwner(limits)
					if err != nil {
						b.Fatal(err)
					}
					defer owner.Close()
					var buffer VectorIndexSearchBuffer
					run := func() {
						results, _, err := owner.overlay.search(columns[0].Float32Vectors[17], 10, 128, 4096, &buffer)
						if err != nil || len(results) != 10 {
							b.Fatalf("results=%d err=%v", len(results), err)
						}
						if docs {
							fetched, err := owner.overlay.current.FetchDocumentsForVectorIndexSearchResults(results, DocumentFetchOptions{})
							if err != nil || len(fetched.Results) != 10 {
								b.Fatalf("documents=%d err=%v", len(fetched.Results), err)
							}
						}
					}
					run()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						run()
					}
					b.StopTimer()
				})
			}
			if err := keeper.Close(); err != nil {
				b.Fatal(err)
			}
			b.Run("noKeeper/OpenClose", func(b *testing.B) {
				before := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					owner, err := col.openTypedGraphReadOwner(limits)
					if err != nil {
						b.Fatal(err)
					}
					if err := owner.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				after := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
				if after.CacheBuilds-before.CacheBuilds != uint64(b.N) {
					b.Fatalf("no-keeper build count: before=%+v after=%+v", before, after)
				}
				b.ReportMetric(float64(after.CacheBuilds-before.CacheBuilds)/float64(b.N), "prepared-builds/op")
			})
		})
	}
}
