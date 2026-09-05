package collections

import (
	"fmt"
	"testing"
)

// Same 1024x8D base, M16, K10/ef128. Fixture, typed bootstrap and mutation/Flush
// are outside timers. OpenClose includes cross-domain drain, pins and release;
// reused query and query+full TopK documents retain one immutable current pin.
// These are internal owner boundaries, not public Minima service qualification.
func BenchmarkTypedGraphReadOwner(b *testing.B) {
	for _, d := range []int{0, 32, 256} {
		b.Run(fmt.Sprintf("suffix%d", d), func(b *testing.B) {
			col, fixtureBase, ids, retained, columns, _ := openTypedGraphQualityFixture(b, 1024)
			if err := fixtureBase.Close(); err != nil {
				b.Fatal(err)
			}
			limits := typedGraphReadOwnerLimits{Owners: 4, States: 4, StateBytes: 16 << 20, AssetBytes: 64 << 20, Cold: typedGraphColdLimits{ManifestRecords: 4096, ManifestBytes: 4 << 20, AssetBytes: 32 << 20, DecodedTermBytes: 32 << 20}}
			if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 2048, OwnedBytes: 8 << 20}, limits.Cold); err != nil {
				b.Fatal(err)
			}
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
			query := columns[0].Float32Vectors[17]
			if d == 0 {
				b.Run("ordinary/OpenClose", func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						s, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding_graph"})
						if err != nil {
							b.Fatal(err)
						}
						if err := s.Close(); err != nil {
							b.Fatal(err)
						}
					}
				})
				for _, docs := range []bool{false, true} {
					b.Run(fmt.Sprintf("ordinary/documents%t", docs), func(b *testing.B) {
						s, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding_graph"})
						if err != nil {
							b.Fatal(err)
						}
						defer s.Close()
						var buffer VectorIndexSearchBuffer
						opts := VectorIndexSearcherSearchOptions{Query: query, TopK: 10, EfSearch: 128, IncludeDocuments: docs, StatsMode: VectorIndexSearchStatsModeMinimal}
						if _, err := s.SearchWithBuffer(opts, &buffer); err != nil {
							b.Fatal(err)
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							response, err := s.SearchWithBuffer(opts, &buffer)
							if err != nil || len(response.Results) != 10 {
								b.Fatalf("ordinary results=%d err=%v", len(response.Results), err)
							}
						}
						b.StopTimer()
					})
				}
			}
			b.Run("coherent/OpenClose", func(b *testing.B) {
				warm, err := col.openTypedGraphReadOwner(limits)
				if err != nil {
					b.Fatal(err)
				}
				_ = warm.Close()
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
			})
			for _, docs := range []bool{false, true} {
				b.Run(fmt.Sprintf("coherent/documents%t", docs), func(b *testing.B) {
					owner, err := col.openTypedGraphReadOwner(limits)
					if err != nil {
						b.Fatal(err)
					}
					defer owner.Close()
					var buffer VectorIndexSearchBuffer
					run := func() {
						results, _, err := owner.overlay.search(query, 10, 128, 4096, &buffer)
						if err != nil || len(results) != 10 {
							b.Fatalf("coherent results=%d err=%v", len(results), err)
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
					b.ReportMetric(float64(d), "shared-suffix-rows")
				})
			}
		})
	}
}
