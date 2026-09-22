package collections

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
)

func TestTypedColumnPointFetchDoesNotExpandFP32Part(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	for _, rows := range []int{128, 1024} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			col, base, _, _, columns, _ := openTypedGraphQualityFixture(t, rows)
			defer base.Close()
			opts := typedGraphPublicTestOptions()
			opts.FoldRows = rows
			if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, opts); err != nil {
				t.Fatal(err)
			}
			var buffer VectorIndexSearchBuffer
			res, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{IndexName: base.indexName, Query: columns[0].Float32Vectors[0], TopK: 4, EfSearch: 16, StatsMode: VectorIndexSearchStatsModeMinimal}, &buffer)
			if err != nil {
				t.Fatal(err)
			}
			defer view.Close()
			got, err := view.FetchDocumentsForVectorIndexSearchResults(res.Results, DocumentFetchOptions{})
			if err != nil || len(got.Results) != 4 {
				t.Fatalf("fetch count=%d err=%v", len(got.Results), err)
			}
			for _, result := range got.Results {
				var doc struct {
					Embedding []float32 `json:"embedding"`
				}
				if err := json.Unmarshal(result.Document, &doc); err != nil || len(doc.Embedding) != 8 {
					t.Fatalf("point vector=%v err=%v", doc.Embedding, err)
				}
			}
			decodedElements := 0
			for _, part := range view.typedColumnReconstructionCache.Parts {
				for _, values := range part.Values {
					for _, value := range values {
						decodedElements += len(value.Float32Vector)
					}
				}
			}
			if decodedElements > 4*len(columns[0].Float32Vectors[0]) {
				t.Fatalf("four point fetches retained %d decoded FP32 elements for %d-row part", decodedElements, rows)
			}
		})
	}
}

func TestTypedColumnPointFP32OwnershipAndBounds(t *testing.T) {
	for _, rows := range []int{128, 1024} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			field := typedColumnAdapterField("embedding", ColumnStoreValueFloat32Vector)
			field.VectorDims = 8
			input := make([]typedColumnAdapterRow, rows)
			for i := range input {
				vector := make([]float32, 8)
				for d := range vector {
					vector[d] = float32(i*8 + d)
				}
				input[i] = typedColumnAdapterRow{PrimaryID: int64(i), Values: map[string]columnDeclaredValue{"embedding": {Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: vector}}}
			}
			part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 42, RowsPerGranule: 64, Fields: []TypedStorageField{field}}, input)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := part.scanDecodedValuesSelectedForReconstruction(nil, false)
			if err != nil {
				t.Fatal(err)
			}
			// Simulate the next read-at operation overwriting all source scratch.
			for _, block := range part.Part.Columns["embedding"].Blocks {
				clear(block.Granule.Payload)
			}
			var dst [1]columnDeclaredValue
			for _, row := range []int{0, 63, 64, rows - 1} {
				values, err := decoded.valuesForRowInto(row, dst[:0])
				if err != nil || !slices.Equal(values[0].Float32Vector, input[row].Values["embedding"].Float32Vector) {
					t.Fatalf("row=%d values=%v err=%v", row, values, err)
				}
				values[0].Float32Vector[0] = -1
				again, err := decoded.valuesForRowInto(row, dst[:0])
				if err != nil || again[0].Float32Vector[0] != float32(row*8) {
					t.Fatalf("returned vector aliases raw row=%d err=%v", row, err)
				}
			}
			for _, row := range []int{-1, rows} {
				if _, err := decoded.valuesForRowInto(row, nil); err == nil {
					t.Fatalf("accepted row=%d", row)
				}
			}
			if !collectionsRaceEnabled {
				allocs := testing.AllocsPerRun(16, func() {
					for _, row := range []int{0, 63, 64, rows - 1} {
						if _, err := decoded.valuesForRowInto(row, dst[:0]); err != nil {
							panic(err)
						}
					}
				})
				// Exactly one owned vector per requested row, independent of part rows.
				if allocs > 4 {
					t.Fatalf("four vector point reads allocated %g times at rows=%d", allocs, rows)
				}
			}
			// Identity parts may omit the locator; explicitly supply a corrupt
			// locator to retain the malformed-nonidentity rejection check.
			decoded.RowByPrimaryID = []int{-1}
			if _, err := decoded.valuesForRowInto(0, nil); err == nil {
				t.Fatal("accepted missing primary locator")
			}
			raw := decoded.RawFloat32Columns[0]
			tooMany := raw
			tooMany.Blocks = slices.Clone(raw.Blocks[:1])
			tooMany.Blocks[0].Descriptor.RowCount = 1<<20 + 1
			if err := validateTypedColumnPointFloat32(tooMany, 1<<20+1); err == nil || err.Error() != "typedcolumn: rows=1048577 exceed cap 1048576" {
				t.Fatalf("excessive granule rows err=%v", err)
			}
			for _, corrupt := range []string{"minmax", "payload-offset", "payload-length", "row-gap"} {
				bad := raw
				bad.Blocks = slices.Clone(raw.Blocks)
				switch corrupt {
				case "minmax":
					bad.Blocks[0].Granule.HasMinMax = true
				case "payload-offset":
					bad.Blocks[0].Granule.PayloadRef.Offset = 1
				case "payload-length":
					bad.Blocks[0].Granule.PayloadRef.Length--
				case "row-gap":
					bad.Blocks[0].Descriptor.FirstRow = 1
				}
				if err := validateTypedColumnPointFloat32(bad, rows); err == nil {
					t.Fatalf("accepted %s", corrupt)
				}
			}
			raw.Blocks[0].Granule.Payload = raw.Blocks[0].Granule.Payload[:1]
			if err := validateTypedColumnPointFloat32(raw, rows); err == nil {
				t.Fatal("accepted truncated raw vector")
			}
			if _, err := typedColumnPointFloat32Vector(raw, 0); err == nil {
				t.Fatal("point read accepted truncated raw vector")
			}
		})
	}
}

func TestTypedColumnPointReadAtRetainsVectorsAcrossGenerations(t *testing.T) {
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 128)
	defer base.Close()
	// Read-at scratch ownership is required even on hosts without prepared
	// holders. Also retain selected-serving coverage where it is supported.
	if columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		if err := col.EnsureColumnGraphServing(context.Background(), base.indexName, typedGraphPublicTestOptions()); err != nil {
			t.Fatal(err)
		}
	}
	changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{9, 8, 7, 6, 5, 4, 3, 2}}}, {Name: "content", Strings: []string{"replaced"}}, {Name: "user", Strings: []string{"new"}}, {Name: "path", Strings: []string{"new"}}}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	if _, err := col.DeleteBatch(ids[1:2]); err != nil {
		t.Fatal(err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	view.forceAssetReadAtFallbackForTest = true
	// Load base, then suffix (overwrites source scratch), then base again.
	for _, index := range []int{2, 0, 2, 1} {
		got, err := view.FetchDocumentsByID(ids[index:index+1], DocumentFetchOptions{})
		if err != nil || len(got.Results) != 1 {
			t.Fatalf("fetch index=%d err=%v", index, err)
		}
		if index == 1 {
			if got.Results[0].Found {
				t.Fatal("deleted document returned")
			}
			continue
		}
		var doc struct {
			Embedding []float32 `json:"embedding"`
		}
		if err := json.Unmarshal(got.Results[0].Document, &doc); err != nil {
			t.Fatal(err)
		}
		want := columns[0].Float32Vectors[index]
		if index == 0 {
			want = changed[0].Float32Vectors[0]
		}
		if !slices.Equal(doc.Embedding, want) {
			t.Fatalf("index=%d vector=%v want=%v", index, doc.Embedding, want)
		}
	}
}
