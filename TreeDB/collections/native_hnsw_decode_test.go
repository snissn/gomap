package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeHNSWDecodeRecordCodec(t *testing.T) {
	for _, tc := range []struct {
		name       string
		key, value []byte
	}{
		{"node", vectorIndexNativeNodeKey(0), []byte(`{"document_id":"row-0","vector":[1,0.25],"level":0}`)},
		{"escaped", vectorIndexNativeNodeKey(0), []byte(`{"document_id":"a\\\"\u1234","vector":[null,1],"level":0,"ignored":{"nested":[1,{}]}}`)},
		{"case", vectorIndexNativeNodeKey(0), []byte(`{"DOCUMENT_ID":"x","VECTOR":[1,2],"LEVEL":0}`)},
		{"reset", vectorIndexNativeNodeKey(0), []byte(`{"document_id":"x","vector":[1,2,3],"vector":null,"vector":[],"vector":[4,5],"level":0}`)},
		{"quantized", vectorIndexNativeNodeKey(0), []byte(`{"document_id":"x","quantized":[-128,127,0],"quant_scale":0.5,"level":0}`)},
		{"edge", vectorIndexNativeEdgeKey(0, 0), []byte(`{"node_id":0,"layer":0,"neighbors":[0,1,2],"distances":[0,1,2]}`)},
		{"edgeReset", vectorIndexNativeEdgeKey(0, 0), []byte(`{"node_id":0,"layer":0,"neighbors":[1,2,3],"neighbors":[],"neighbors":[4,5],"distances":null}`)},
		{"empty", vectorIndexNativeNodeKey(0), []byte(`{"document_id":"x","vector":[],"level":0}`)},
		{"doc", vectorIndexNativeDocKey("a-long-document-id"), []byte(`0`)},
		{"tomb", vectorIndexNativeTombstoneKey(0), []byte(`1`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var admitted uint64
			plan, reason, err := planNativeHNSWDecode(tc.key, tc.value, func(n uint64) error { admitted += n; return nil })
			if err != nil || reason != "" {
				t.Fatalf("plan: %s %v", reason, err)
			}
			node, edge := plan.seeds()
			witness := memtable.COWAllocationCharge(uint64(cap(node.Vector))*4) + memtable.COWAllocationCharge(uint64(cap(node.Quantized))) + memtable.COWAllocationCharge(uint64(cap(edge.Neighbor))*uint64(unsafe.Sizeof(int(0)))) + memtable.COWAllocationCharge(uint64(cap(edge.Distances))*4)
			if admitted < witness {
				t.Fatalf("backing=%d admitted=%d", witness, admitted)
			}
			var old, current vectorIndexPersistSnapshot
			old.DocMap.Current, current.DocMap.Current = make(map[string]int), make(map[string]int)
			oldNodes, nodes := make(map[int]vectorIndexPersistNode), make(map[int]vectorIndexPersistNode)
			beforeMax, afterMax := -1, -1
			before := appendVectorIndexNativeSnapshotRecordV2(&old, oldNodes, &beforeMax, tc.key, tc.value)
			after := appendVectorIndexNativeSnapshotSeededRecordV2(&current, nodes, &afterMax, tc.key, tc.value, node, edge)
			if before != after || beforeMax != afterMax || !reflect.DeepEqual(old, current) || !reflect.DeepEqual(oldNodes, nodes) {
				t.Fatalf("codec changed: reasons=%q/%q snapshots=%+v/%+v nodes=%+v/%+v", before, after, old, current, oldNodes, nodes)
			}
		})
	}
}

func TestNativeHNSWDecodeMalformedAndAdmission(t *testing.T) {
	key := vectorIndexNativeNodeKey(0)
	for _, value := range []string{
		`{"document_id":"x","vector":[{},1],"level":0}`,
		`{"document_id":"x","vector":[1e9999],"level":0}`,
		`{"document_id":"x","quantized":[128],"level":0}`,
		`{"document_id":"x","level":1.25}`,
		`{"document_id":"x","vector":[1,}`,
		`{"document_id":{},"level":0}`,
	} {
		_, reason, err := planNativeHNSWDecode(key, []byte(value), func(uint64) error { return nil })
		if err != nil || reason != vectorIndexFallbackInvalidGraphRootEntry {
			t.Fatalf("%s: reason=%s err=%v", value, reason, err)
		}
	}
	value := []byte(`{"document_id":"x","vector":[1,2,3],"level":0}`)
	for _, stage := range []int{1, 2} {
		calls := 0
		_, reason, err := planNativeHNSWDecode(key, value, func(uint64) error {
			calls++
			if calls == stage {
				return memtable.ErrCOWCapacity
			}
			return nil
		})
		if !errors.Is(err, memtable.ErrCOWCapacity) || reason != "" || calls != stage {
			t.Fatalf("stage=%d calls=%d reason=%s err=%v", stage, calls, reason, err)
		}
	}
	// Ignored fields are still seen by encoding/json's initial scanner. Its
	// allocation envelope is admitted before the borrowed walker/typed decoder.
	deep := []byte(`{"document_id":"x","level":0,"ignored":` + strings.Repeat("[", 5000) + `0` + strings.Repeat("]", 5000) + `}`)
	var first uint64
	_, _, err := planNativeHNSWDecode(key, deep, func(n uint64) error { first = n; return memtable.ErrCOWCapacity })
	if !errors.Is(err, memtable.ErrCOWCapacity) || first < 5000*uint64(unsafe.Sizeof(int(0))) {
		t.Fatalf("scanner: admitted=%d err=%v", first, err)
	}
	if _, err = nativeHNSWDecodeGrowth(^uint64(0), 8); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("overflow=%v", err)
	}
	var p nativeHNSWDecodePlan
	if err = p.allocation(^uint64(0), 8); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("allocation overflow=%v", err)
	}
}

func TestNativeHNSWLoadSearchErrorClassification(t *testing.T) {
	for _, cause := range []error{memtable.ErrCOWCapacity, backenddb.ErrClosed, errors.New("codec failure"), errors.New("read failure")} {
		classified := nativeHNSWLoadSearchError(fmt.Errorf("decode: %w", cause))
		if !errors.Is(classified, cause) || errors.Is(classified, ErrVectorIndexSnapshotMismatch) || !errors.Is(classified, ErrVectorIndexSearchUnavailable) {
			t.Fatalf("cause=%v classified=%v", cause, classified)
		}
	}
	if err := nativeHNSWLoadSearchError(fmt.Errorf("install: %w", backenddb.ErrConcurrentModification)); !errors.Is(err, ErrVectorIndexSnapshotMismatch) || !errors.Is(err, backenddb.ErrConcurrentModification) {
		t.Fatal(err)
	}
}

// Check the preallocated arrays are the only numeric backing needed by normal
// persisted records, while the maintained codec still rejects corrupt arrays.
func TestNativeHNSWDecodeSeedCapacity(t *testing.T) {
	for _, count := range []int{0, 1, 16, 32, 257, 1024} {
		values := bytes.TrimSuffix(bytes.Repeat([]byte("1,"), count), []byte(","))
		raw := []byte(`{"node_id":0,"layer":0,"neighbors":[` + string(values) + `],"distances":[` + string(values) + `]}`)
		plan, reason, err := planNativeHNSWDecode(vectorIndexNativeEdgeKey(0, 0), raw, func(uint64) error { return nil })
		if err != nil || reason != "" {
			t.Fatalf("n=%d %s %v", count, reason, err)
		}
		_, edge := plan.seeds()
		neighbors, distances := cap(edge.Neighbor), cap(edge.Distances)
		if err = json.Unmarshal(raw, &edge); err != nil {
			t.Fatal(err)
		}
		if cap(edge.Neighbor) != neighbors || cap(edge.Distances) != distances || len(edge.Neighbor) != count {
			t.Fatalf("n=%d array backing grew", count)
		}
	}
}

// The public cold loader constructs final rows, not an insertion build. The
// original runtime receipt must nevertheless grow before a later insertion.
func TestNativeHNSWDirectLoadThenMutation(t *testing.T) {
	c, closeAll, checkpoint, def, _ := openHNSWPublicFixture(t, "native", 32, 2)
	closed := false
	defer func() {
		if !closed {
			closeAll()
		}
	}()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err = checkpoint(); err != nil {
		t.Fatal(err)
	}
	c.UnregisterVectorIndex(def.Name)
	idx, status, err := c.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil || !status.Loaded || idx == nil {
		t.Fatalf("load=%+v err=%v", status, err)
	}
	if idx.nativeHNSWConstructionPeak != 0 || !reflect.DeepEqual(idx.insertScratch, vectorIndexSearchScratch{}) {
		t.Fatalf("cold conversion retained insertion scratch: peak=%d scratch=%+v", idx.nativeHNSWConstructionPeak, idx.insertScratch)
	}
	if len(idx.nodes) != 32 || len(idx.currentNode) != 32 {
		t.Fatalf("nodes=%d current=%d", len(idx.nodes), len(idx.currentNode))
	}
	before := idx.nativeHNSWCharge
	doc, err := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{1.0, 0.0}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Insert([]byte("later-row"), doc); err != nil {
		t.Fatal(err)
	}
	if idx.nativeHNSWConstructionPeak == 0 || idx.nativeHNSWCharge <= before {
		t.Fatalf("later insertion not admitted: before=%d after=%d peak=%d", before, idx.nativeHNSWCharge, idx.nativeHNSWConstructionPeak)
	}
	if _, ok := idx.currentNode["later-row"]; !ok {
		t.Fatal("later insertion missing")
	}
	if _, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true}); err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	budget := read.capture.NativeAllocator().(interface{ Stats() memtable.COWStats })
	if err = read.Close(); err != nil {
		t.Fatal(err)
	}
	closeAll()
	closed = true
	if stats := budget.Stats(); stats.ReservedBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("final owner retained %+v", stats)
	}
}

// Both snapshot and direct loading call these exact maintained validators.
// Missing distances retain saved-load's recomputation semantics; malformed
// references never become a current runtime.
func TestNativeHNSWDirectLoadSharedValidators(t *testing.T) {
	meta := vectorIndexPersistMeta{Field: "embedding", Metric: VectorMetricCosine, Encoding: VectorIndexEncodingFloat32, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 16, Entry: 0, RebuildDeletedRatio: 0.3}
	makeNodes := func() []vectorIndexNode {
		t.Helper()
		idx := &VectorIndex{m: 4, nativeHNSWOwned: true}
		var nodes []vectorIndexNode
		for _, row := range []vectorIndexPersistNode{{DocumentID: "a", Vector: []float32{1, 0}, Level: 0}, {DocumentID: "b", Vector: []float32{0, 1}, Level: 0}} {
			node, reason := idx.convertVectorIndexPersistNode(row, meta)
			if reason != "" {
				t.Fatal(reason)
			}
			nodes = append(nodes, node)
		}
		return nodes
	}
	idx := &VectorIndex{m: 4, nativeHNSWOwned: true}
	nodes := makeNodes()
	if reason := idx.applyVectorIndexPersistEdge(nodes, vectorIndexPersistEdges{NodeID: 0, Layer: 0, Neighbor: []int{1}}, meta); reason != "" {
		t.Fatal(reason)
	}
	if got := nodes[0].neighbors[0]; len(got) != 1 || got[0].distance != 1 {
		t.Fatalf("normalized edge=%+v", got)
	}
	for _, edge := range []vectorIndexPersistEdges{{NodeID: 2}, {NodeID: 0, Layer: 1}, {NodeID: 0, Neighbor: []int{2}}, {NodeID: 0, Neighbor: []int{1}, Distances: []float32{1, 2}}, {NodeID: 0, Neighbor: make([]int, 9)}} {
		if reason := idx.applyVectorIndexPersistEdge(makeNodes(), edge, meta); reason == "" {
			t.Fatalf("accepted malformed edge=%+v", edge)
		}
	}
	if reason := validateVectorIndexPersistDocMapNode(nodes, "wrong", 0); reason != "docmap_document_mismatch" {
		t.Fatal(reason)
	}
	nodes[0].deleted = true
	if reason := validateVectorIndexPersistDocMapNode(nodes, "a", 0); reason != "docmap_points_to_deleted_node" {
		t.Fatal(reason)
	}
	if reason := validateVectorIndexPersistShape(meta, 0, 0, 0, 0); reason != vectorIndexFallbackInvalidEntry {
		t.Fatal(reason)
	}
}

// Inventory still rejects unknown families, although the direct loader scans
// only supported family ranges. The malformed image is installed through the
// real no-WAL native publication owner, not a fabricated root or reader.
func TestNativeHNSWDirectLoadUnknownFamily(t *testing.T) {
	c, closeAll, checkpoint, def, _ := openHNSWPublicFixture(t, "native", 4, 2)
	defer closeAll()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err = checkpoint(); err != nil {
		t.Fatal(err)
	}
	c.UnregisterVectorIndex(def.Name)
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	domain, _ := read.collection.domains.Get("vector-index/" + def.Name)
	tx, err := c.nativeJSON.db.BeginNativeConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	base := tx.State().(*nativeEngineVersion)
	next, err := prepareNativeNamedDomains(base, []nativeNamedCollectionChange{{collection: read.collection, sourceGeneration: read.collection.documentGeneration, domains: []nativeNamedDomainChange{{name: "vector-index/" + def.Name, expectedInterpretation: domain.interpretation, entries: []memtable.COWMutation{{Key: []byte("unknown/record"), Value: []byte("{}"), Flags: node.FlagInline}}}}}})
	if err != nil {
		if next != nil {
			err = errors.Join(err, finishPrivateNative(next.Release()))
		}
		t.Fatal(err)
	}
	result, err := tx.CommitNative(&backenddb.NativePreparedPublication{Basis: base, State: next, StartSequence: tx.LogicalSequence()}, nil, false)
	if err != nil || !result.LogicalInstalled {
		t.Fatalf("publish=%+v err=%v", result, err)
	}
	loaded, status, err := c.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil || loaded != nil || status.Loaded || status.ExactFallbackReason != vectorIndexFallbackInvalidGraphRootKey {
		t.Fatalf("load=%v status=%+v err=%v", loaded, status, err)
	}
	if err = read.Close(); err != nil {
		t.Fatal(err)
	}
	// Recovery uses the same maintained canonical builder, restoring a valid
	// graph image before ordinary manager/DB teardown.
	idx, err = c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
}
