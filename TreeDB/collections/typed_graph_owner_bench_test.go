package collections

import (
	"fmt"
	"testing"
)

// OpenClose includes canonical storage-barrier acquisition, complete manifest
// lease construction and release. WarmQuery excludes owner setup and uses the
// same immutable pin. Fixture write/rebuild and warmup are outside both timers.
func BenchmarkTypedGraphOwner(b *testing.B) {
	const rows = 1024
	_, db, col := openTypedMinimaCollection(b)
	defer db.Close()
	ids, retained := make([][]byte, rows), make([][]byte, rows)
	vectors := make([][]float32, rows)
	content, users, paths := make([]string, rows), make([]string, rows), make([]string, rows)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("row-%04d", i))
		retained[i] = []byte(fmt.Sprintf(`{"id":"row-%04d"}`, i))
		vectors[i] = []float32{1, float32(i%97) / 97, float32(i%89) / 89, float32(i%83) / 83, float32(i%79) / 79, float32(i%73) / 73, float32(i%71) / 71, float32(i%67) / 67}
		content[i], users[i], paths[i] = "owned content", "tenant", "source"
	}
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: vectors}, {Name: "content", Strings: content}, {Name: "user", Strings: users}, {Name: "path", Strings: paths}}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		b.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		b.Fatal(err)
	}
	open := func() *VectorIndexSearcher {
		s, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding_graph"})
		if err != nil {
			b.Fatal(err)
		}
		return s
	}
	warm := open()
	if err := warm.Close(); err != nil {
		b.Fatal(err)
	}
	b.Run("OpenClose", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s := open()
			if err := s.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("WarmQuery", func(b *testing.B) {
		s := open()
		defer s.Close()
		var buffer VectorIndexSearchBuffer
		opts := VectorIndexSearcherSearchOptions{Query: vectors[17], TopK: 10, EfSearch: 128, StatsMode: VectorIndexSearchStatsModeMinimal}
		if _, err := s.SearchWithBuffer(opts, &buffer); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			response, err := s.SearchWithBuffer(opts, &buffer)
			if err != nil || len(response.Results) != 10 {
				b.Fatalf("query results=%d err=%v", len(response.Results), err)
			}
		}
		b.StopTimer()
	})
}
