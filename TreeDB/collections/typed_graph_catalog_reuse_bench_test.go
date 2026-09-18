package collections

import "testing"

// Owner open/close on the public normalized fixture. Cold deliberately makes
// the existing local entry stale without removing its immutable metadata.
func BenchmarkTypedGraphReadOwnerCatalogCache(b *testing.B) {
	requireTypedGraphPublicServingTest(b)
	fixture := openTypedGraphVectorReadViewBenchFixture(b, 8, 1024, VectorIndexRepresentationCosineNormalizedF32V1)
	defer fixture.close()
	limits := fixture.col.typedGraphServingPolicy().options.Owners
	for _, cold := range []bool{false, true} {
		name := "warm"
		if cold {
			name = "cold"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if cold {
					fixture.col.catalogMu.Lock()
					fixture.col.catalogCommitSeq++
					fixture.col.catalogMu.Unlock()
				}
				owner, err := fixture.col.openTypedGraphReadOwner(limits)
				if err != nil {
					b.Fatal(err)
				}
				if err := owner.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
