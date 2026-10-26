package collections

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeHNSWTemplateScalarDDLBinding(t *testing.T) {
	f := newTemplateHNSWBindingFixture(t, treedb.ProfileNoWALFast, false)
	original, err := f.c.BuildVectorIndex(VectorIndexOptions{Name: "embedding"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = original.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	f.c.UnregisterVectorIndex("embedding")
	idx, status, err := f.c.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding"})
	if err != nil || !status.Loaded {
		t.Fatalf("no-scalar template Load: %+v %v", status, err)
	}
	// Create the immutable search plane, then a real facade mutation activates
	// the maintained delta plane. Both planes must use the same read resolver.
	if _, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = f.c.Update([]byte("c"), func([]byte) ([]byte, bool, error) {
		return templateHNSWBindingDocument(t, "two", "policy updated", "ddl_new_shape", []float32{0.9, 0.1}), true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if idx.liveDelta == nil || len(idx.liveDelta.nodes) == 0 {
		t.Fatal("setup did not exercise live delta")
	}
	before, _ := idx.persistSnapshot()
	beforeDelta, _ := idx.liveDelta.persistSnapshot()
	if _, err = f.c.CreateIndex(IndexDefinition{Name: "tenant", Field: "tenant", ValueType: IndexValueString}); err != nil {
		t.Fatalf("template scalar DDL: %v", err)
	}
	if f.c.registeredVectorIndex("embedding") != idx {
		t.Fatal("scalar DDL replaced registered receiver")
	}
	after, _ := idx.persistSnapshot()
	afterDelta, _ := idx.liveDelta.persistSnapshot()
	if !reflect.DeepEqual(before.Nodes, after.Nodes) || !reflect.DeepEqual(before.DocMap, after.DocMap) || !reflect.DeepEqual(beforeDelta.Nodes, afterDelta.Nodes) || !reflect.DeepEqual(beforeDelta.DocMap, afterDelta.DocMap) {
		t.Fatal("scalar DDL changed base/delta graph payload")
	}
	templateHNSWBindingSearch(t, idx, "tenant", "one", "a")
	if _, err = f.c.DropIndex("tenant"); err != nil {
		t.Fatal(err)
	}
	if f.c.registeredVectorIndex("embedding") != idx {
		t.Fatal("scalar drop replaced receiver")
	}
	if _, err = f.c.CreateIndex(IndexDefinition{Name: "tenant", Field: "tenant", ValueType: IndexValueString}); err != nil {
		t.Fatal(err)
	}
	templateHNSWBindingSearch(t, idx, "tenant", "one", "a")
}

func TestNativeHNSWTemplateHeldScalarCut(t *testing.T) {
	f := newTemplateHNSWBindingFixture(t, treedb.ProfileNoWALFast, true)
	idx, err := f.c.BuildVectorIndex(VectorIndexOptions{Name: "embedding"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = idx.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	old, err := f.c.nativeJSON.Capture(f.c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	stored, err := old.Get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	stored = bytes.Clone(stored)
	oldGeneration := old.collection.documentGeneration
	if _, _, err = f.c.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
		return templateHNSWBindingDocument(t, "two", "current policy", "only_in_new_cut", []float32{0, 1}), true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = f.db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// A public materializer is independently captured at the current cut.
	currentMaterializer, err := f.c.NewStoredDocumentJSONMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	defer currentMaterializer.Close()
	current, err := f.c.Get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	currentJSON, err := currentMaterializer.StoredDocumentJSON(current)
	if err != nil || !bytes.Contains(currentJSON, []byte("only_in_new_cut")) {
		t.Fatalf("current template shape: %s %v", currentJSON, err)
	}
	heldMaterializer := &StoredDocumentJSONMaterializer{documentFormat: DocumentFormatTemplateV1, nativeRead: old}
	oldJSON, err := heldMaterializer.StoredDocumentJSON(stored)
	if err != nil || !bytes.Contains(oldJSON, []byte("shape_a")) || bytes.Contains(oldJSON, []byte("only_in_new_cut")) {
		t.Fatalf("held dictionary cut: %s %v", oldJSON, err)
	}
	// Decode the saved graph on the OLD supplied read; do not install it as
	// current. This isolates the shared helper's original-cut obligation.
	private, scratch, admit := templateHNSWBindingGraphAtRead(t, f.c, old)
	defer scratch.Close()
	defer private.releaseNativeHNSW()
	if err = populateNativeHNSWScalarRows(private, old, admit, private); err != nil {
		t.Fatalf("old-cut scalar reconstruction: %v", err)
	}
	if private.sourceDocumentGeneration != oldGeneration || f.c.registeredVectorIndex("embedding") != idx {
		t.Fatal("private old-cut reconstruction changed current owner")
	}
	foundOldRow := false
	for row, node := range private.nodes {
		if string(node.documentID) != "a" {
			continue
		}
		foundOldRow = true
		scalar := private.nativeScalarRowAtLocked(row)
		expected, err := private.nativeScalarRow(heldMaterializer, stored)
		if err != nil || !reflect.DeepEqual(scalar, expected) {
			t.Fatalf("old scalar=%v expected=%v err=%v", scalar, expected, err)
		}
	}
	if !foundOldRow {
		t.Fatal("saved old graph omitted row a")
	}
	templateHNSWBindingSearch(t, idx, "tenant", "one", "c")
}

func TestNativeHNSWTemplateScalarConversionRefusal(t *testing.T) {
	f := newTemplateHNSWBindingFixture(t, treedb.ProfileNoWALFast, true)
	original, err := f.c.BuildVectorIndex(VectorIndexOptions{Name: "embedding"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = original.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	read, err := f.c.nativeJSON.Capture(f.c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	private, scratch, _ := templateHNSWBindingGraphAtRead(t, f.c, read)
	defer scratch.Close()
	defer private.releaseNativeHNSW()
	// Fully preadmit primary-read scratch on its original operation receipt.
	// This callback cannot promise more than its already owned 1MiB. The real
	// template converter's separate, existing operation receipt must refuse.
	primaryLease, err := read.capture.NativeAllocator().AdmitNativeBytes(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	defer primaryLease.Close()
	var primaryCharge uint64
	primaryAdmit := func(n uint64) error {
		if n > (1<<20)-primaryCharge {
			return memtable.ErrCOWCapacity
		}
		primaryCharge += n
		return nil
	}
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	available := f.budget.Limits().MaxInFlightBytes - f.budget.Stats().ReservedBytes
	if err = blocker.Resize(available); err != nil {
		t.Fatal(err)
	}
	before := f.budget.Stats()
	beforeCharge := private.nativeHNSWCharge
	beforeLSN := f.c.nativeJSON.db.CommandWALNextLSN()
	err = populateNativeHNSWScalarRows(private, read, primaryAdmit, private)
	if !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("template conversion must refuse real capacity, got %v", err)
	}
	if primaryCharge == 0 || primaryCharge >= 1<<20 {
		t.Fatalf("refusal did not follow admitted primary read: %d", primaryCharge)
	}
	after := f.budget.Stats()
	if after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases || private.nativeHNSWCharge != beforeCharge || f.c.registeredVectorIndex("embedding") != original || f.c.nativeJSON.db.CommandWALNextLSN() != beforeLSN {
		t.Fatalf("refused conversion changed custody/WAL: before=%+v after=%+v", before, after)
	}
	for name, column := range private.scalarColumns {
		if column.rows != 0 {
			t.Fatalf("partial scalar output %q rows=%d", name, column.rows)
		}
	}
	blocker.Close()
	if err = populateNativeHNSWScalarRows(private, read, primaryAdmit, private); err != nil {
		t.Fatalf("same owned read after capacity release: %v", err)
	}
	if err = private.validateNativeScalarColumnLengthsLocked(); err != nil {
		t.Fatal(err)
	}
	latest, err := f.c.nativeJSON.Capture(f.c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer latest.Close()
	if latest.capture.LogicalSequence() != read.capture.LogicalSequence() || latest.collection.incarnation != read.collection.incarnation || latest.collection.documentGeneration != read.collection.documentGeneration || !collectionMetaValuesEqual(latest.meta, read.meta) {
		t.Fatal("conversion/recovery published logical state")
	}
}

// Uses only pre-existing4d91 maintained graph/meta decoders, real original
// receipts and a real saved graph; no fake runtime or resolver is installed.
func templateHNSWBindingGraphAtRead(t *testing.T, c *Collection, read *NativeJSONRead) (*VectorIndex, *memtable.COWExternalLease, func(uint64) error) {
	t.Helper()
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(64 << 10)
	if err != nil {
		t.Fatal(err)
	}
	charge := uint64(64 << 10)
	admit := func(n uint64) error {
		if n > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charge + n); err != nil {
			return err
		}
		charge += n
		return nil
	}
	def, ok := findVectorIndex(read.meta.VectorIndexes, "embedding")
	if !ok {
		lease.Close()
		t.Fatal("missing vector declaration")
	}
	def, err = normalizeVectorIndexDefinition(def)
	if err != nil {
		lease.Close()
		t.Fatal(err)
	}
	meta, _, reason, err := readNativeHNSWMeta(read, def, admit)
	if err != nil || reason != "" {
		lease.Close()
		t.Fatalf("saved meta: reason=%q err=%v", reason, err)
	}
	graph, _, reason, err := c.loadNativeHNSWGraphPhase(read, def, meta, &charge, admit)
	if err != nil || reason != "" {
		lease.Close()
		t.Fatalf("saved graph: reason=%q err=%v", reason, err)
	}
	return graph, lease, admit
}
