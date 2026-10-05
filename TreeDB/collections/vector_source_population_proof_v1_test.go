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

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
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

func TestVectorSourcePopulationPartIdentityMismatchV1(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0}}}
	_, db, c, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	defer db.Close()
	expectation := vectorPopulationTestExpectationV1(rows, 2)
	if _, err := vectorPopulationTestProofV1(context.Background(), c, def, expectation); err != nil {
		t.Fatalf("valid population: %v", err)
	}
	state, err := c.loadColumnAssetRewriteManifestState()
	if err != nil {
		t.Fatal(err)
	}
	var ref ColumnAssetRef
	partRecord := -1
	for i, record := range state.records {
		if _, _, err := decodeColumnManifestPartRecordKey(record.key); err != nil {
			continue
		}
		asset, err := decodeColumnManifestPartRecord(record.value)
		if err != nil {
			t.Fatal(err)
		}
		if asset.AssetRef.Kind == ColumnAssetKindTCS1TypedColumnPart {
			ref = asset.AssetRef
			partRecord = i
			break
		}
	}
	if ref.PartID != typedColumnPartAssetPartID {
		t.Fatalf("missing typed-column reference: %+v", ref)
	}
	raw, err := readColumnPhysicalAssetFromManager(db.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatal(err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		t.Fatal(err)
	}
	part, err := typedcolumn.ColumnPartFromImage(image)
	if err != nil {
		t.Fatal(err)
	}
	// Rebuild a structurally valid image with a different identity, retaining
	// the manifest identity and vector contents. The asset writer computes a
	// valid checksum, so checksum failure cannot satisfy this regression.
	part.Descriptor.PartID++
	for row, locator := range part.Locators {
		locator.PartID = part.Descriptor.PartID
		part.Locators[row] = locator
	}
	dictionaries, err := image.Dictionaries()
	if err != nil {
		t.Fatal(err)
	}
	badImage, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{Dictionaries: dictionaries})
	if err != nil {
		t.Fatal(err)
	}
	badRef, err := writeTypedColumnPartAssetToManager(db.ColumnAssetRootDir(), state.cfg, badImage.Bytes, ref.Generation, ref.PartID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readColumnPhysicalAssetFromManager(db.ColumnAssetRootDir(), badRef); err != nil {
		t.Fatalf("mismatch asset checksum: %v", err)
	}
	records := cloneColumnManifestRecords(state.records)
	asset, err := decodeColumnManifestPartRecord(records[partRecord].value)
	if err != nil {
		t.Fatal(err)
	}
	records[partRecord].value, err = encodeColumnManifestPartRecord(ColumnPreparedAsset{Ref: badRef, Rows: asset.Rows, Bytes: badRef.Length, PublishID: asset.PublishID, GenerationID: asset.GenerationID, Reason: asset.Reason, PartRole: asset.PartRole, SortKey: columnSortKeyMatchString(asset.SortKey)})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := columnAssetRewriteUpdatedIdentity(state, records)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := columnAssetRewriteUpdatedMeta(state.meta, identity)
	if err != nil {
		t.Fatal(err)
	}
	systemRoot, roots, err := c.publishColumnAssetRewriteManifestState(state, meta, identity, records)
	if err != nil {
		t.Fatal(err)
	}
	catalog := cloneCatalogWithRootUpdates(state.catalog, meta, []string{state.rootName}, roots)
	c.meta = meta
	c.rememberCatalogAtSystemRoot(systemRoot, catalog)
	c.noteWriteDomainCatalog(systemRoot, catalog)
	proof, err := vectorPopulationTestProofV1(context.Background(), c, def, expectation)
	if !errors.Is(err, ErrVectorIndexPartitionLiveMismatchV1) || proof != (VectorSourcePopulationProofV1{}) {
		t.Fatalf("mismatched part issued proof=%+v err=%v", proof, err)
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
	for _, id := range ids {
		requireCollectionPrimaryEntryPointer(t, db, c.name, id)
	}
	p := vectorPopulationTestExpectationV1(rows, 8)
	p.Limits.MaxSourceRecordBytes = 16 + 4*8
	proof, err := vectorPopulationTestProofV1(context.Background(), c, meta.VectorIndexes[0], p)
	if err != nil || proof.Rows != 2 || proof.AssetBytes == 0 || proof.SourceRecordBytes != 2*(16+4*8) {
		t.Fatalf("directory proof=%+v err=%v", proof, err)
	}
	tooSmall := p
	tooSmall.Limits.MaxSourceRecordBytes--
	if got, err := vectorPopulationTestProofV1(context.Background(), c, meta.VectorIndexes[0], tooSmall); err == nil || got != (VectorSourcePopulationProofV1{}) {
		t.Fatalf("pointer projection byte refusal %+v %v", got, err)
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
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
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
			rows := []columnGraphRebuildInputRowV2A{{id: "id", vector: []float32{1, math.Float32frombits(0x80000000)}}}
			recordBytes := uint64(len(raw))
			if i == 0 {
				// Unsorted distinct rows exercise scratch reuse in the JSON decoder.
				rows = append(rows, columnGraphRebuildInputRowV2A{id: "a", vector: []float32{0, 1}}, columnGraphRebuildInputRowV2A{id: "z", vector: []float32{-1, 0}})
				for _, row := range rows[1:] {
					document := fmt.Sprintf(`{"embedding":[%g,%g]}`, row.vector[0], row.vector[1])
					if _, err := c.Insert([]byte(row.id), []byte(document)); err != nil {
						t.Fatal(err)
					}
					recordBytes += uint64(len(document))
				}
			}
			meta, err := c.CreateVectorIndex(VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine})
			if err != nil {
				t.Fatal(err)
			}
			def := meta.VectorIndexes[0]
			p := vectorPopulationTestExpectationV1(rows, 2)
			proof, err := vectorPopulationTestProofV1(context.Background(), c, def, p)
			if i == 0 {
				if err != nil || proof.Rows != 3 || proof.SHA256 != p.SHA256 || proof.SourceRecordBytes != recordBytes || proof.AssetBytes != 0 {
					t.Fatalf("retained proof %+v %v", proof, err)
				}
				p.Limits.MaxSourceRecordBytes = uint64(len(raw))
				p.Limits.MaxTotalBytes = proof.TotalBytes
				if _, err := vectorPopulationTestProofV1(context.Background(), c, def, p); err != nil {
					t.Fatalf("exact inline byte bounds: %v", err)
				}
				p.Limits.MaxTotalBytes--
				if got, err := vectorPopulationTestProofV1(context.Background(), c, def, p); err == nil || got != (VectorSourcePopulationProofV1{}) {
					t.Fatalf("inline total byte refusal %+v %v", got, err)
				}
				p.Limits.MaxTotalBytes = proof.TotalBytes
				p.Limits.MaxSourceRecordBytes--
				if got, err := vectorPopulationTestProofV1(context.Background(), c, def, p); err == nil || got != (VectorSourcePopulationProofV1{}) {
					t.Fatalf("inline record byte refusal %+v %v", got, err)
				}
			} else if err == nil || proof != (VectorSourcePopulationProofV1{}) {
				t.Fatalf("invalid source %+v %v", proof, err)
			}
		})
	}
}

// A JSON pointer's frame can include other rows and omitted length hints.
// Refuse it before any value-log decode rather than mislabel a frame bound as
// a source payload bound. Column pointers are covered by the real V2 fixture.
func TestVectorSourcePopulationJSONPointerRefusesV1(t *testing.T) {
	// Collection pointerization requires the cached backend's appender, not
	// only backend value-log options. Flush/checkpoint the real winning entry
	// before asserting representation; an unflushed inline row tests nothing.
	opts := treedb.OptionsFor(treedb.ProfileFast, t.TempDir())
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanup() })
	if !db.HasValueLogAppender() {
		t.Fatal("cached pointer fixture has no value-log appender")
	}
	manager := NewCollectionManager(db)
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "pointers", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("pointers")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Insert([]byte("id"), []byte(`{"embedding":[1,-0]}`)); err != nil {
		t.Fatal(err)
	}
	meta, err := c.CreateVectorIndex(VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	requireCollectionPrimaryEntryPointer(t, db, c.name, []byte("id"))
	p := vectorPopulationTestExpectationV1([]columnGraphRebuildInputRowV2A{{id: "id", vector: []float32{1, math.Float32frombits(0x80000000)}}}, 2)
	got, err := vectorPopulationTestProofV1(context.Background(), c, meta.VectorIndexes[0], p)
	if err == nil || err.Error() != "collections: source population JSON pointer unsupported" || got != (VectorSourcePopulationProofV1{}) {
		t.Fatalf("pointer proof %+v %v", got, err)
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

func TestVectorPopulationQueuedIteratorCancellationV1(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, tc := range []struct {
			name      string
			sameKey   bool
			cancelAt  int
			maxWork   int
			extraRows bool
			nextSteps int
			wantValid bool
			wantErr   error
		}{
			{"initialization", false, 2, 64, false, 0, false, context.Canceled},
			{"next", false, 3, 64, false, 1, false, context.Canceled},
			{"shadowed", true, 3, 64, false, 0, false, context.Canceled},
			{"next-shadowed", false, 5, 64, true, 2, false, context.Canceled},
			{"legacy-shadowed-work-cap", true, 0, 2, false, 0, true, errCollectionIndexScanWorkCap},
			{"callback-shadowed-work-cap", true, -1, 2, false, 0, true, errCollectionIndexScanWorkCap},
		} {
			t.Run(fmt.Sprintf("%s/reverse=%v", tc.name, reverse), func(t *testing.T) {
				first, second := newCollectionRunTable(1), newCollectionRunTable(1)
				defer resetCollectionRunTable(first)
				defer resetCollectionRunTable(second)
				setCollectionRunValue(first, []byte("a"), []byte("new"))
				key := "b"
				if tc.sameKey {
					key = "a"
				}
				setCollectionRunValue(second, []byte(key), []byte("old"))
				if tc.extraRows {
					shared := "c"
					if reverse {
						shared = "0"
					}
					setCollectionRunValue(first, []byte(shared), []byte("new"))
					setCollectionRunValue(second, []byte(shared), []byte("old"))
				}
				first.Freeze()
				second.Freeze()
				sources := []bufferedRootRunIteratorSource{{iter: first.NewIterator(nil, nil)}, {iter: second.NewIterator(nil, nil), priority: 1}}
				if reverse {
					_ = sources[0].iter.Close()
					_ = sources[1].iter.Close()
					sources[0].iter = first.NewReverseIterator(nil, nil)
					sources[1].iter = second.NewReverseIterator(nil, nil)
				}
				var inspectionError func() error
				if tc.cancelAt > 0 {
					ctx := &vectorPopulationCancelContextV1{Context: context.Background(), remaining: tc.cancelAt}
					inspectionError = ctx.Err
				} else if tc.cancelAt < 0 {
					inspectionError = func() error { return nil }
				}
				it := newBufferedRootRunIteratorSourcesIteratorWithInspectionError(sources, nil, nil, false, true, reverse, tc.maxWork, nil, inspectionError)
				defer it.Close()
				for i := 0; i < tc.nextSteps; i++ {
					if !it.Valid() {
						t.Fatal("missing row before cancellation")
					}
					it.Next()
				}
				if it.Valid() != tc.wantValid || !errors.Is(it.Error(), tc.wantErr) {
					t.Fatalf("queued row after failure: valid=%v err=%v want %v", it.Valid(), it.Error(), tc.wantErr)
				}
				it.Seek([]byte("a"))
				if it.Valid() {
					t.Fatal("seek revived a failed iterator")
				}
			})
		}
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
