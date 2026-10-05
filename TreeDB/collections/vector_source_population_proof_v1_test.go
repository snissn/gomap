package collections

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"testing"
)

// The test oracle sorts the known input IDs independently of the product's
// current-root iterator, and writes exact FP32 bits (including negative zero).
func vectorPopulationTestExpectationV1(rows []columnGraphRebuildInputRowV2A, dimensions int) VectorSourcePopulationExpectationV1 {
	ordered := append([]columnGraphRebuildInputRowV2A(nil), rows...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].id < ordered[j].id })
	h := sha256.New()
	var word [4]byte
	for _, row := range ordered {
		binary.LittleEndian.PutUint32(word[:], uint32(len(row.id)))
		h.Write(word[:])
		h.Write([]byte(row.id))
		for _, v := range row.vector {
			binary.LittleEndian.PutUint32(word[:], math.Float32bits(v))
			h.Write(word[:])
		}
	}
	return VectorSourcePopulationExpectationV1{Rows: uint64(len(rows)), Dimensions: dimensions, SHA256: hex.EncodeToString(h.Sum(nil)), Limits: VectorSourcePopulationLimitsV1{MaxRows: 65536, MaxIDBytes: 65536, MaxSourceRecordBytes: 1 << 20, MaxTotalBytes: 512 << 20, MaxInspected: 1 << 20}}
}

func vectorPopulationTestProofV1(ctx context.Context, c *Collection, def VectorIndexDefinition, p VectorSourcePopulationExpectationV1) (VectorSourcePopulationProofV1, error) {
	var proof VectorSourcePopulationProofV1
	err := c.WithPreparedCommandWALMutation(func(owner *CommandWALAdmittedCollection) error {
		var err error
		proof, err = owner.ProveVectorSourcePopulationV1(ctx, VectorPartitionManifestV1{Collection: c.name, IndexName: def.Name, IndexDefinitionDigest: VectorIndexDefinitionDigestV1(def)}, p)
		return err
	})
	return proof, err
}

func TestVectorSourcePopulationEmptyV1(t *testing.T) {
	_, db, c, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, nil)
	defer db.Close()
	p := vectorPopulationTestExpectationV1(nil, 2)
	proof, err := vectorPopulationTestProofV1(context.Background(), c, def, p)
	if err != nil || proof.Rows != 0 || proof.SHA256 != p.SHA256 || proof.AssetBytes != 0 || proof.SourceRecordBytes != 0 {
		t.Fatalf("empty proof=%+v err=%v", proof, err)
	}
}

func TestVectorSourcePopulationDirectoryV2(t *testing.T) {
	_, db, c := openSourceImportDirectoryCollectionV2(t)
	defer db.Close()
	ownership, input := sourceImportFixtureV2(t, c, 2)
	input.DocumentRevisions = []uint64{1, 2}
	ids, retained, columns := sourceImportRowsV2("b", "a")
	if _, err := c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	meta := c.Meta()
	if meta.Options.ColumnStore.ActiveManifest.Format != columnSourceDirectoryFormatV2 {
		t.Fatal("fixture did not select the directory point-probe path")
	}
	rows := []columnGraphRebuildInputRowV2A{{id: "b", vector: columns[0].Float32Vectors[0]}, {id: "a", vector: columns[0].Float32Vectors[1]}}
	p := vectorPopulationTestExpectationV1(rows, 8)
	proof, err := vectorPopulationTestProofV1(context.Background(), c, meta.VectorIndexes[0], p)
	if err != nil || proof.Rows != 2 || proof.AssetBytes == 0 {
		t.Fatalf("directory proof=%+v err=%v", proof, err)
	}
	p.Limits.MaxInspected = proof.Inspected
	if _, err := vectorPopulationTestProofV1(context.Background(), c, meta.VectorIndexes[0], p); err != nil {
		t.Fatalf("exact directory budget: %v", err)
	}
	p.Limits.MaxInspected--
	if got, err := vectorPopulationTestProofV1(context.Background(), c, meta.VectorIndexes[0], p); err == nil || got != (VectorSourcePopulationProofV1{}) {
		t.Fatalf("directory inspection refusal %+v %v", got, err)
	}
}

func TestVectorSourcePopulationCurrentProjectionV1(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "z", vector: []float32{1, math.Float32frombits(0x80000000)}}, {id: "a", vector: []float32{0, 1}}, {id: "m", vector: []float32{-1, 0}}}
	_, db, c, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	defer db.Close()
	p := vectorPopulationTestExpectationV1(rows, 2)
	proof, err := vectorPopulationTestProofV1(context.Background(), c, def, p)
	if err != nil || proof.Rows != 3 || proof.SHA256 != p.SHA256 || proof.Encoding != VectorSourcePopulationEncodingV1 || proof.AssetBytes == 0 || proof.SourceRecordBytes < 3*8 {
		t.Fatalf("proof=%+v err=%v", proof, err)
	}
	// Exact exhaustion is valid; reducing the shared physical budget by one is
	// refused even though all visible IDs can have already reached the hash.
	exact := p
	exact.Limits.MaxInspected = proof.Inspected
	if _, err := vectorPopulationTestProofV1(context.Background(), c, def, exact); err != nil {
		t.Fatalf("exact inspection limit: %v", err)
	}
	exact.Limits.MaxInspected--
	if failed, err := vectorPopulationTestProofV1(context.Background(), c, def, exact); err == nil || failed != (VectorSourcePopulationProofV1{}) {
		t.Fatalf("inspection limit produced %+v err=%v", failed, err)
	}
	for name, modify := range map[string]func(*VectorSourcePopulationExpectationV1){
		"wrong bits": func(p *VectorSourcePopulationExpectationV1) {
			r := append([]columnGraphRebuildInputRowV2A(nil), rows...)
			r[0].vector = []float32{1, 0}
			p.SHA256 = vectorPopulationTestExpectationV1(r, 2).SHA256
		},
		"missing":    func(p *VectorSourcePopulationExpectationV1) { p.Rows-- },
		"extra":      func(p *VectorSourcePopulationExpectationV1) { p.Rows++ },
		"dimensions": func(p *VectorSourcePopulationExpectationV1) { p.Dimensions++ },
		"id cap":     func(p *VectorSourcePopulationExpectationV1) { p.Limits.MaxIDBytes = 0 },
		"record cap": func(p *VectorSourcePopulationExpectationV1) { p.Limits.MaxSourceRecordBytes = 8 },
		"total cap":  func(p *VectorSourcePopulationExpectationV1) { p.Limits.MaxTotalBytes = 1 },
		"overflow":   func(p *VectorSourcePopulationExpectationV1) { p.Limits.MaxTotalBytes = math.MaxUint64 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := p
			modify(&bad)
			if got, err := vectorPopulationTestProofV1(context.Background(), c, def, bad); err == nil || got != (VectorSourcePopulationProofV1{}) {
				t.Fatalf("bad proof=%+v err=%v", got, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := vectorPopulationTestProofV1(ctx, c, def, p); !errors.Is(err, context.Canceled) || got != (VectorSourcePopulationProofV1{}) {
		t.Fatalf("canceled=%+v err=%v", got, err)
	}
	if got, err := vectorPopulationTestProofV1(&vectorPopulationCancelContextV1{Context: context.Background(), remaining: 12}, c, def, p); !errors.Is(err, context.Canceled) || got != (VectorSourcePopulationProofV1{}) {
		t.Fatalf("midscan cancel=%+v err=%v", got, err)
	}
	// Mutations of an ID outside any six-write witness set must affect CURRENT
	// population truth. Reusing immutable prepared source rows would pass here.
	insertColumnGraphRebuildRowsV2A(t, c, []columnGraphRebuildInputRowV2A{{id: "later", vector: []float32{0, -1}}})
	if got, err := vectorPopulationTestProofV1(context.Background(), c, def, p); err == nil || got != (VectorSourcePopulationProofV1{}) {
		t.Fatalf("extra current row accepted: %+v %v", got, err)
	}
	rows = append(rows, columnGraphRebuildInputRowV2A{id: "later", vector: []float32{0, -1}})
	if _, err := vectorPopulationTestProofV1(context.Background(), c, def, vectorPopulationTestExpectationV1(rows, 2)); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete([]byte("later")); err != nil {
		t.Fatal(err)
	}
	if _, err := vectorPopulationTestProofV1(context.Background(), c, def, p); err != nil {
		t.Fatalf("tombstone winner: %v", err)
	}
	if _, err := c.Replace([]byte("a"), []byte(`{"embedding":[0,-1],"kind":"changed","time_us":2,"did":"a"}`)); err != nil {
		t.Fatal(err)
	}
	if got, err := vectorPopulationTestProofV1(context.Background(), c, def, p); err == nil || got != (VectorSourcePopulationProofV1{}) {
		t.Fatalf("same-count stale current vector accepted: %+v %v", got, err)
	}
	if err := c.Delete([]byte("m")); err != nil {
		t.Fatal(err)
	}
	if got, err := vectorPopulationTestProofV1(context.Background(), c, def, p); err == nil || got != (VectorSourcePopulationProofV1{}) {
		t.Fatalf("missing current vector accepted: %+v %v", got, err)
	}
}

type vectorPopulationCancelContextV1 struct {
	context.Context
	remaining int
}

func (c *vectorPopulationCancelContextV1) Err() error {
	c.remaining--
	if c.remaining <= 0 {
		return context.Canceled
	}
	return nil
}

func TestVectorSourcePopulationRetainedJSONV1(t *testing.T) {
	_, db, _, _ := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, nil)
	defer db.Close()
	for i, raw := range []string{`{"embedding":[1,-0]}`, `{"embedding":null}`, `{"other":[1,0]}`, `{"embedding":[1]}`, `{"embedding":[1,0,2]}`, `{"embedding":["bad",0]}`, `{"embedding":[1e40,0]}`, `{"embedding":[0,0]}`} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			manager := NewCollectionManager(db)
			name := fmt.Sprintf("retained%d", i)
			if _, err := manager.CreateCollection(&CollectionMeta{Name: name, Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}); err != nil {
				t.Fatal(err)
			}
			c, err := manager.OpenCollection(name)
			if err != nil {
				t.Fatal(err)
			}
			// The definition is declared after storing the fixture: malformed
			// vectors must be refused by the proof, not merely by vector ingestion.
			if _, err := c.Insert([]byte("id"), []byte(raw)); err != nil {
				t.Fatal(err)
			}
			meta, err := c.CreateVectorIndex(VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine})
			if err != nil {
				t.Fatal(err)
			}
			def := meta.VectorIndexes[0]
			p := vectorPopulationTestExpectationV1([]columnGraphRebuildInputRowV2A{{id: "id", vector: []float32{1, math.Float32frombits(0x80000000)}}}, 2)
			proof, err := vectorPopulationTestProofV1(context.Background(), c, def, p)
			if i == 0 {
				if err != nil || proof.SourceRecordBytes != uint64(len(raw)) || proof.AssetBytes != 0 {
					t.Fatalf("retained proof %+v %v", proof, err)
				}
			} else if err == nil || proof != (VectorSourcePopulationProofV1{}) {
				t.Fatalf("invalid source %+v %v", proof, err)
			}
		})
	}
}

func TestVectorPopulationPhysicalIteratorCancellationV1(t *testing.T) {
	table := newCollectionRunTable(65)
	defer resetCollectionRunTable(table)
	for i := 0; i < 65; i++ {
		table.DeleteSteal([]byte(fmt.Sprintf("%03d", i)))
	}
	table.Freeze()
	inspected := 0
	ctx := &vectorPopulationCancelContextV1{Context: context.Background(), remaining: 8}
	it := newBufferedRootRunIteratorSourcesIteratorWithInspectionError([]bufferedRootRunIteratorSource{{iter: table.NewIterator(nil, nil)}}, nil, nil, false, true, false, 64, func(n int) { inspected += n }, ctx.Err)
	defer it.Close()
	// No visible row callback exists. Cancellation must stop the compositor's
	// skipped tombstone work itself, rather than waiting for a visible winner.
	for it.Valid() {
		it.Next()
	}
	if !errors.Is(it.Error(), context.Canceled) || inspected >= 64 {
		t.Fatalf("tombstone inspected=%d err=%v", inspected, it.Error())
	}
}

func BenchmarkVectorSourcePopulationProofV1(b *testing.B) {
	for _, count := range []int{512, 10000} {
		b.Run(fmt.Sprintf("%dx128", count), func(b *testing.B) {
			rows := make([]columnGraphRebuildInputRowV2A, count)
			for i := range rows {
				v := make([]float32, 128)
				v[i%128] = 1
				rows[i] = columnGraphRebuildInputRowV2A{id: fmt.Sprintf("doc-%06d", i), vector: v}
			}
			_, db, c, def := openColumnGraphTypedColumnVectorTestCollection1782(b, 128, 16, rows)
			defer db.Close()
			p := vectorPopulationTestExpectationV1(rows, 128)
			b.ReportAllocs()
			b.SetBytes(int64(count * 128 * 4))
			b.ResetTimer()
			var proof VectorSourcePopulationProofV1
			for i := 0; i < b.N; i++ {
				var err error
				proof, err = vectorPopulationTestProofV1(context.Background(), c, def, p)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(proof.Inspected), "inspected/op")
			b.ReportMetric(float64(proof.AssetBytes), "asset-bytes/op")
			b.ReportMetric(float64(proof.SourceRecordBytes), "source-bytes/op")
		})
	}
}
