package collections

import (
	"fmt"
	"testing"
)

// Same logical eight-row source and durable atomic publisher. Legacy accepts
// full JSON; typed accepts explicit carriers plus residual JSON. Input creation,
// initial graph capture/bootstrap and Close are excluded. Every timed call
// deletes/reinserts the same IDs, producing sixteen physical versions.
func BenchmarkTypedSourceReplacement(b *testing.B) {
	for _, typed := range []bool{false, true} {
		b.Run(fmt.Sprintf("typed%t", typed), func(b *testing.B) {
			if b.N > 64 {
				b.Skip("bounded source diagnostic: use -benchtime=10x")
			}
			b.StopTimer()
			_, db, c := openTypedMinimaCollection(b)
			defer db.Close()
			var ids, retained, legacy [][]byte
			columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
			for i := 0; i < 8; i++ {
				id := fmt.Sprintf("source%02d", i)
				ids = append(ids, []byte(id))
				retained = append(retained, []byte(fmt.Sprintf(`{"id":%q,"extra":"kept"}`, id)))
				legacy = append(legacy, []byte(fmt.Sprintf(`{"id":%q,"extra":"kept","embedding":[1,0,0,0,0,0,0,0],"content":"alpha beta","meta":{"user_id":"u","fpath":"p"}}`, id)))
				columns[0].Float32Vectors = append(columns[0].Float32Vectors, []float32{1, 0, 0, 0, 0, 0, 0, 0})
				columns[1].Strings = append(columns[1].Strings, "alpha beta")
				columns[2].Strings = append(columns[2].Strings, "u")
				columns[3].Strings = append(columns[3].Strings, "p")
			}
			if _, err := c.ReplaceTypedSourceByID(nil, ids, retained, columns); err != nil {
				b.Fatal(err)
			}
			if _, err := c.RebuildVectorIndex("embedding_graph"); err != nil {
				b.Fatal(err)
			}
			if err := c.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: b.N * 16, Tombstones: b.N * 8, ValueSlots: b.N * 32, OwnedBytes: 4 << 20, EncodedOutputBytes: 32 << 20}, typedGraphColdLimits{ManifestRecords: 256, ManifestBytes: 1 << 20, AssetBytes: 8 << 20, DecodedTermBytes: 8 << 20}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				var deleted int
				var err error
				if typed {
					deleted, err = c.ReplaceTypedSourceByID(ids, ids, retained, columns)
				} else {
					deleted, err = c.replaceSourceDocumentsWithCommandWALIntent(ids, ids, legacy, nil, nil)
				}
				if err != nil || deleted != 8 {
					b.Fatalf("deleted=%d err=%v", deleted, err)
				}
			}
			b.StopTimer()
			state := c.typedGraphPublicationSnapshot()
			if state.invalid || state.physicalRows != b.N*16 {
				b.Fatal("incomplete source publication")
			}
			b.ReportMetric(float64(c.collectionSchemaCoordinator().typedPublicationEncodedBytes)/float64(b.N), "reserved-encoded-B/batch")
			b.ReportMetric(8, "rows/batch")
		})
	}
}
