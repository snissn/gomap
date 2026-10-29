package collections

import (
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func nativeOrdinaryGraphPhaseFixture(t *testing.T, count int) (CollectionMeta, VectorIndexDefinition, nativeColumnGraphConstructionPlan, [][]byte, [][]float32) {
	t.Helper()
	def, ids, vectors := nativeColumnGraphScratchFixture(t, count, false)
	def.EfSearch = 64
	cfg := &ColumnStoreConfig{Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
		Columns: []ColumnStoreColumn{{Name: def.Field, Path: def.Field, Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueFloat32Vector, VectorDims: def.Dimensions}}}

	normalized, err := normalizeColumnStoreConfig("docs", cfg)
	if err != nil {
		t.Fatal(err)
	}
	meta := CollectionMeta{Name: "docs", Options: CollectionOptions{ColumnStore: normalized}, VectorIndexes: []VectorIndexDefinition{def}}
	construction, err := nativeColumnGraphConstructionCapacityForWorkers(def, uint64(count), 4, func(visit func([]byte) error) error {
		for _, id := range ids {
			if err := visit(id); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return meta, def, construction, ids, vectors
}

// Real maintained builder -> all generated raw assets -> fresh state encoder.
// The planner has no placeholder Assets slice and must predict the SAME fresh
// metadata length despite knowing only IDs/levels/source census before work.
func TestNativeOrdinaryColumnGraphPhaseActualAssets(t *testing.T) {
	meta, def, c, ids, vectors := nativeOrdinaryGraphPhaseFixture(t, 32)
	root := t.TempDir()
	before, err := nativeOrdinaryColumnGraphMetadataPreflightCapacity(meta, def, root)
	if err != nil {
		t.Fatal(err)
	}
	p, err := nativeOrdinaryColumnGraphPhaseCapacity(meta, def, root, c, nativeOrdinaryColumnGraphSourceCensus{})
	if err != nil {
		t.Fatal(err)
	}
	if p.NamespaceScanBytes != columnAssetSegmentFileIDScanWorkspaceBytes {
		t.Fatal("namespace scan fixed workspace lost")
	}
	if before != p.MetadataPreflightBytes || before == 0 {
		t.Fatal("bootstrap is not independently available before factory calls")
	}
	rows := make([]columnVectorGraphAssetRow, len(ids))
	for i := range rows {
		rows[i] = columnVectorGraphAssetRow{ID: ids[i], Vector: vectors[i], BaseRowRef: DocumentRowRef{Generation: 1, PartID: 1, RowIndex: i, AppliedCommandLSN: 1}}
	}
	if err = buildColumnVectorGraphAdjacency(rows, def); err != nil {
		t.Fatal(err)
	}
	_, _, state, _, err := prepareColumnVectorGraphRebuildAssets(meta.Name, *meta.Options.ColumnStore, def, 1, 17, 1, nil, rows, root, nil, nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	actual := encodeColumnVectorIndexStateRecordV2(state)
	if p.AssetCount != uint64(len(state.Assets)) || p.FreshStateRawBytes != uint64(len(actual)) {
		t.Fatalf("preaccepted actual metadata shape assets=%d bytes=%d; emitted assets=%d bytes=%d", p.AssetCount, p.FreshStateRawBytes, len(state.Assets), len(actual))
	}
	slot, err := nativeFreshVectorStateCapacityV2(uint64(len(actual)), uint64(len(actual)), uint64(len(state.Assets)))
	if err != nil {
		t.Fatal(err)
	}
	if p.RequirementBytes < 2*slot {
		t.Fatal("independent source requirement slots not reserved")
	}
	suffix := p.AssetsRetainedBytes + max(p.SerialAssetBytes, p.PackBytes, p.ValidationBytes, p.RequirementBytes)
	peak := max(p.SourceReadBytes, p.GraphPrivateBytes, suffix)
	if p.AssetsRetainedBytes == 0 || p.TotalBytes != p.PersistentBytes+peak || p.GraphPrivateBytes != c.PrivateBytes {
		t.Fatalf("phase split or independent HNSW component changed: %+v", p)
	}
}

func TestNativeOrdinaryColumnGraphPhaseSourceCensusAndOverflow(t *testing.T) {
	meta, def, c, _, _ := nativeOrdinaryGraphPhaseFixture(t, 2)
	root := t.TempDir()
	source := nativeOrdinaryColumnGraphSourceCensus{ManifestRecords: 3, ManifestBackingBytes: 1024, TypedSourceParts: 1, TypedSourceStoredBytes: 4096, TypedSourceDecodedBytes: 2048, DictionarySlots: 3, TypedSourceReadPeak: 65536, TypedSourceDecoderBytes: 8192, PendingBuildBytes: 32768}
	base, err := nativeOrdinaryColumnGraphPhaseCapacity(meta, def, root, c, source)
	if err != nil {
		t.Fatal(err)
	}
	for name, modify := range map[string]func(*nativeOrdinaryColumnGraphSourceCensus){
		"pending retained source":   func(s *nativeOrdinaryColumnGraphSourceCensus) { s.PendingBuildBytes += 4096 },
		"metadata":                  func(s *nativeOrdinaryColumnGraphSourceCensus) { s.ManifestBackingBytes += 4096 },
		"typed all-section decoded": func(s *nativeOrdinaryColumnGraphSourceCensus) { s.TypedSourceDecodedBytes += 4096 },
		"dictionary":                func(s *nativeOrdinaryColumnGraphSourceCensus) { s.DictionarySlots += 100 },
		"source controls":           func(s *nativeOrdinaryColumnGraphSourceCensus) { s.TypedSourceParts++ },
		"external schema owner":     func(s *nativeOrdinaryColumnGraphSourceCensus) { s.SchemaBasisBytes = 4096 },
	} {
		t.Run(name, func(t *testing.T) {
			s := source
			modify(&s)
			p, err := nativeOrdinaryColumnGraphPhaseCapacity(meta, def, root, c, s)
			if err != nil {
				t.Fatal(err)
			}
			if p.PersistentBytes <= base.PersistentBytes {
				t.Fatal("actual retained cohort omitted")
			}
		})
	}
	// FullJSON/old graph payload bytes are intentionally not census fields.
	// Only actually opened typed assets may affect consumed-source payload costs.
	if base.SourceReadBytes < source.TypedSourceReadPeak+source.TypedSourceDecoderBytes {
		t.Fatal("fixed decoder lost from source phase")
	}
	for _, s := range []nativeOrdinaryColumnGraphSourceCensus{{ManifestBackingBytes: ^uint64(0)}, {TypedSourceDecodedBytes: ^uint64(0)}, {TypedSourceParts: ^uint64(0)}, {DictionarySlots: ^uint64(0)}, {TypedSourceReadPeak: ^uint64(0), TypedSourceDecoderBytes: 1}} {
		if _, err := nativeOrdinaryColumnGraphPhaseCapacity(meta, def, root, c, s); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("overflow did not refuse: %v", err)
		}
	}
	c.LevelRows[0]++
	if _, err := nativeOrdinaryColumnGraphPhaseCapacity(meta, def, root, c, source); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("inconsistent original census: %v", err)
	}
}

// Exact original borrowed ID stream, captured four-worker component. This logs
// the complete calculated terms for ROOT's actual source census wiring. Zero
// physical source here is only a geometry test, NOT the public fit assertion.
func TestNativeOrdinaryColumnGraphPhaseOriginalIDGeometry(t *testing.T) {
	meta, def, c, _, _ := nativeOrdinaryGraphPhaseFixture(t, 4096)
	p, err := nativeOrdinaryColumnGraphPhaseCapacity(meta, def, t.TempDir(), c, nativeOrdinaryColumnGraphSourceCensus{})
	if err != nil {
		t.Fatal(err)
	}
	if p.AssetCount != 11 {
		t.Fatalf("actual original ID levels produce %d assets", p.AssetCount)
	}
	t.Logf("original 4096 geometry (source census deliberately absent; no fit claim): %+v", p)
	if p.TotalBytes <= c.PrivateBytes {
		t.Fatal("recipe dropped independent persistent owners")
	}
	for _, n := range []int{1, 7, 33} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			m, d, cp, _, _ := nativeOrdinaryGraphPhaseFixture(t, n)
			if _, err := nativeOrdinaryColumnGraphPhaseCapacity(m, d, t.TempDir(), cp, nativeOrdinaryColumnGraphSourceCensus{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
