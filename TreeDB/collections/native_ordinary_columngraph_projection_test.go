package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Same public producer/canonical JSON journal used by documentservice's deferred
// ColumnGraph seed. The original implementation refuses this API before staging.
func TestNativeOrdinaryColumnGraphCanonicalProjection(t *testing.T) {
	dir := t.TempDir()
	opts := nativeTypedGroupOptions(dir)
	var owner *treedb.DB
	var backend *backenddb.DB
	var manager *CollectionManager
	var col *Collection
	closeAll := func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
			manager = nil
		}
		if owner != nil {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
			owner = nil
		}
	}
	t.Cleanup(closeAll)
	open := func(create bool) {
		var err error
		owner, err = treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err = owner.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = NewCollectionManager(backend)
		if create {
			_, err = manager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
				Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
				Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}},
			}}, VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}})
			if err != nil {
				t.Fatal(err)
			}
		}
		col, err = manager.OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
	}
	open(true)
	ids := [][]byte{[]byte("b"), []byte("a")}
	docs := [][]byte{[]byte(`{"embedding":[1,-0],"content":"second"}`), []byte(`{"embedding":[0,1],"content":"first"}`)}
	vectors := [][]float32{{1, math.Float32frombits(1 << 31)}, {0, 1}}
	got, stats, err := col.InsertBatchWithStatsValidatedFloat32Projection(ids, docs, "embedding", VectorMetricCosine, vectors)
	if err != nil || !reflect.DeepEqual(got, ids) || stats.Documents != 2 {
		t.Fatalf("canonical projection: ids=%q stats=%+v err=%v", got, stats, err)
	}
	if col.registeredColumnGraphPartitionLiveCarrier("vec") != nil {
		t.Fatal("ordinary ColumnGraph write installed HNSW")
	}
	if err = col.Flush(); err != nil {
		t.Fatalf("ordinary ColumnGraph Flush invoked HNSW: %v", err)
	}
	if col.registeredColumnGraphPartitionLiveCarrier("vec") != nil {
		t.Fatal("ordinary ColumnGraph Flush installed HNSW")
	}
	before := col.LastInsertStats()
	duplicateLSN := backend.CommandWALNextLSN()
	if _, failed, e := col.InsertBatchWithStatsValidatedFloat32Projection(ids, docs, "embedding", VectorMetricCosine, vectors); !errors.Is(e, ErrDocumentExists) || backenddb.CommitPublicationAccepted(e) || !reflect.DeepEqual(failed, CollectionInsertStats{}) {
		t.Fatalf("duplicate: stats=%+v err=%v", failed, e)
	}
	if backend.CommandWALNextLSN() != duplicateLSN || !reflect.DeepEqual(col.LastInsertStats(), before) {
		t.Fatal("duplicate changed WAL/stats")
	}
	for _, vector := range [][]float32{{0, 1}, {1, 0}} {
		lsn := backend.CommandWALNextLSN()
		got, failed, e := col.InsertBatchWithStatsValidatedFloat32Projection([][]byte{[]byte("bad")}, [][]byte{docs[0]}, "embedding", VectorMetricCosine, [][]float32{vector})
		if e == nil || backenddb.CommitPublicationAccepted(e) || !strings.Contains(e.Error(), "projection differs from JSON") || got != nil || !reflect.DeepEqual(failed, CollectionInsertStats{}) {
			t.Fatalf("mismatch: ids=%q stats=%+v err=%v", got, failed, e)
		}
		if backend.CommandWALNextLSN() != lsn || !reflect.DeepEqual(col.LastInsertStats(), before) {
			t.Fatal("refusal changed WAL/stats")
		}
		if _, e = col.nativeJSON.Get("docs", []byte("bad")); !errors.Is(e, tree.ErrKeyNotFound) {
			t.Fatalf("refused row: %v", e)
		}
	}
	// No checkpoint rescue. Close/reopen must preserve the original accepted
	// canonical member, exact ID binding and independently retained vector bits.
	closeAll()
	if t.Failed() {
		return
	}
	open(false)
	read, err := col.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	for i, id := range ids {
		got, e := read.Get(id)
		if e != nil || !bytes.Equal(got, docs[i]) {
			t.Fatalf("reopen %q: %s %v", id, got, e)
		}
		found, equal, e := read.typedRowEqual(id, docs[i], []columnDeclaredValue{{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: vectors[i]}})
		if e != nil || !found || !equal {
			t.Fatalf("reopen vector %q: found=%v equal=%v err=%v", id, found, equal, e)
		}
		if i == 0 {
			changed := append([]float32(nil), vectors[i]...)
			changed[1] = 0
			found, equal, e = read.typedRowEqual(id, docs[i], []columnDeclaredValue{{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: changed}})
			if e != nil || !found || equal {
				t.Fatalf("reopen signed-zero mismatch %q: found=%v equal=%v err=%v", id, found, equal, e)
			}
		}
	}
}

// The JSON materializer passes the original retained carrier directly; the
// non-column/semantic carrier keeps the old path-map promise. This discriminator
// would fail on candidate2, whose planner charged the absent phase to both.
func TestNativeOrdinaryColumnGraphBuildPromiseCarrier(t *testing.T) {
	meta := CollectionMeta{Name: "docs", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
		Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
		Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 8, Owner: TypedStorageOwnerColumnPart}},
	}}, VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 8, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}}
	if !nativeOrdinaryColumnGraphMeta(meta) {
		t.Fatal("fixture is outside selected carrier")
	}
	const idBytes, retainedBytes = uint64(7), uint64(512)
	full, err := nativeTypedDeltaRowBuildCapacity(meta, idBytes, retainedBytes)
	if err != nil {
		t.Fatal(err)
	}
	unit := uint64(4*preparedSemanticStreamMapEntryReserveBytes + 2*preparedSemanticStreamMaxPathBytes + preparedSemanticStreamMaxCursorDepth*int(unsafe.Sizeof("")) + int(unsafe.Sizeof(columnRetainedSemanticStreamPathNode{})))
	for _, encoding := range []ColumnRetainedPayloadEncoding{ColumnRetainedPayloadEncodingJSON, ColumnRetainedPayloadEncodingSemanticStreamV1} {
		other := meta
		cfg := *meta.Options.ColumnStore
		other.Options.ColumnStore = &cfg
		cfg.RetainedPayload = ColumnRetainedPayloadNonColumn
		cfg.RetainedPayloadEncoding = encoding
		old, err := nativeTypedDeltaRowBuildCapacity(other, idBytes, retainedBytes)
		if err != nil {
			t.Fatal(err)
		}
		if old-full != memtable.COWAllocationCharge(retainedBytes*unit) || full == 0 {
			t.Fatalf("carrier %s full=%d old=%d", encoding, full, old)
		}
		if _, err = nativeTypedDeltaRowBuildCapacity(other, idBytes, ^uint64(0)/unit+1); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("carrier overflow %s: %v", encoding, err)
		}
	}
	// Full is refined ONLY for the selected ordinary strategy. An excluded
	// representation keeps the complete old structural allowance.
	excluded := meta
	excluded.VectorIndexes = append([]VectorIndexDefinition(nil), meta.VectorIndexes...)
	excluded.VectorIndexes[0].Strategy = VectorIndexStrategy("native_runtime")
	old, err := nativeTypedDeltaRowBuildCapacity(excluded, idBytes, retainedBytes)
	if err != nil || old-full != memtable.COWAllocationCharge(retainedBytes*unit) {
		t.Fatalf("excluded carrier full=%d old=%d err=%v", full, old, err)
	}
	if _, err = nativeTypedDeltaRowBuildCapacity(meta, idBytes, ^uint64(0)); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("raw-copy overflow: %v", err)
	}
}

// This uses the same JSON shape and numeric vectors as the locked first128
// documentservice input, without changing that protected public fixture.
func TestNativeOrdinaryColumnGraphPublic128PromiseSource(t *testing.T) {
	meta := CollectionMeta{Name: "docs", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON, Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 8, Owner: TypedStorageOwnerColumnPart}}}}, VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 8, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}}
	fallback := meta
	cfg := *meta.Options.ColumnStore
	fallback.Options.ColumnStore = &cfg
	cfg.RetainedPayload = ColumnRetainedPayloadNonColumn
	var removed uint64
	for i := 0; i < 128; i++ {
		vector := make([]float32, 8)
		vector[0] = 1
		for d := 1; d < 8; d++ {
			vector[d] = float32((i+d)%17) / 17
		}
		doc := struct {
			ID        string         `json:"id"`
			Content   string         `json:"content,omitempty"`
			Embedding []float32      `json:"embedding,omitempty"`
			Meta      map[string]any `json:"meta,omitempty"`
		}{fmt.Sprintf("d%06d", i), "refund policy " + strings.Repeat("payload ", 32), vector, map[string]any{"tenant": fmt.Sprintf("t%02d", i%16), "rank": i}}
		raw, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		selected, err := nativeTypedDeltaRowBuildCapacity(meta, uint64(len(doc.ID)), uint64(len(raw)))
		if err != nil {
			t.Fatal(err)
		}
		original, err := nativeTypedDeltaRowBuildCapacity(fallback, uint64(len(doc.ID)), uint64(len(raw)))
		if err != nil || original < selected {
			t.Fatalf("row%d: original=%d selected=%d err=%v", i, original, selected, err)
		}
		removed += original - selected
	}
	// Original observed typed306722640 and total310822416 include this cohort.
	// Its independent per-row charges are all8192-aligned; removing the cohort
	// does not discount the remaining whole-child rounding or any fixed floor.
	if unsafe.Sizeof(uintptr(0)) == 8 && removed != 289742848 {
		t.Fatalf("source-derived removed cohort=%d want289742848", removed)
	}
	t.Logf("first128 removed=%d predicted typed=%d total=%d child=%d", removed, uint64(306722640)-removed, uint64(310822416)-removed, uint64(33554432))
}
