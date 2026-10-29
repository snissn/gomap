package collections

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// This is the retained-document lane, not the optimized source importer or a
// typed/normalized/quantized capability. Scalar and text indexes consume the
// exact complete JSON document through their existing native planner.
// ValidateOrdinaryColumnGraphCollectionMeta validates a complete proposed
// retained-document ColumnGraph schema before an adapter publishes any effects.
// Generic collection creation also supports other explicitly selected shapes.
func ValidateOrdinaryColumnGraphCollectionMeta(meta CollectionMeta) error {
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		return err
	}
	if !nativeOrdinaryColumnGraphMeta(normalized) {
		return fmt.Errorf("%w: ordinary ColumnGraph requires a fully configured retained JSON float32 vector schema", backenddb.ErrNativeSemanticUnsupported)
	}
	return nil
}

func nativeOrdinaryColumnGraphMeta(meta CollectionMeta) bool {
	cfg := meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.RetainedPayload != ColumnRetainedPayloadFull || columnRetainedPayloadEffectiveEncoding(cfg) != ColumnRetainedPayloadEncodingJSON || normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON || meta.Options.AllowArrayValuesInIndex || len(cfg.Columns) != 1 || len(cfg.AggregateMetadata) != 0 || len(meta.VectorIndexes) != 1 {
		return false
	}
	def, col := meta.VectorIndexes[0], cfg.Columns[0]
	return def.Strategy == VectorIndexStrategyColumnGraph && def.Metric == VectorMetricCosine && def.Representation == "" && len(def.QuantizedIndexes) == 0 && def.Dimensions > 0 && col.Path == def.Field && col.Owner == TypedStorageOwnerColumnPart && col.ValueType == ColumnStoreValueFloat32Vector && !col.Nullable && columnStoreFloat32VectorElementsPerRow(col) == def.Dimensions
}

// The same final rows feed both the ordinary scalar/text delta and the retained
// typed producer. No canonical member or document callback is replayed here.
// The typed delta owns its own original receipt through physical handoff.
func (t *NativeJSONTxn) attachNativeOrdinaryColumnGraph(g *nativeJSONTxnBinding) (resultErr error) {
	if !nativeOrdinaryColumnGraphMeta(g.probe.meta) || g.plan == nil || g.typed != nil || len(t.members) == 0 {
		return backenddb.ErrNativeSemanticUnsupported
	}
	admit := func(n uint64) error { return t.admitStage(g, n) }
	for _, width := range []uint64{uint64(unsafe.Sizeof([]byte{})), uint64(unsafe.Sizeof([]byte{}))} {
		if err := admitNativeInsertArray(len(g.rows.rows), width, admit); err != nil {
			return err
		}
	}
	ids := make([][]byte, 0, len(g.rows.rows))
	documents := make([][]byte, 0, len(g.rows.rows))
	deletedCount := 0
	for _, row := range g.rows.rows {
		if bytes.Equal(row.opening, row.current) {
			continue
		}
		if row.current == nil {
			deletedCount++
			continue
		}
		ids = append(ids, row.id)
		documents = append(documents, row.current)
	}
	if len(ids)+deletedCount == 0 {
		return nil
	}
	retained, scratch, err := nativeTypedStageCapacity(g.probe.meta, ids, documents)
	if err != nil {
		return err
	}
	deletionBytes, err := nativeTypedCheckedMul(uint64(deletedCount), uint64(unsafe.Sizeof([]byte{})))
	if err != nil {
		return err
	}
	if err = nativeTypedCheckedAdd(&retained, memtable.COWAllocationCharge(deletionBytes)); err != nil {
		return err
	}
	for _, row := range g.rows.rows {
		if bytes.Equal(row.opening, row.current) {
			continue
		}
		if row.current == nil && row.opening != nil {
			if err = nativeTypedCheckedAdd(&retained, memtable.COWAllocationCharge(uint64(len(row.id)))); err != nil {
				return err
			}
		}
	}
	lease, err := t.core.NativeAllocator().AdmitNativeBytes(retained)
	if err != nil {
		return err
	}
	defer func() {
		if lease != nil {
			lease.Close()
		}
	}()
	work, err := t.core.NativeAllocator().AdmitNativeBytes(scratch)
	if err != nil {
		return err
	}
	defer work.Close()
	docs := make([]columnWriteDocument, len(ids))
	for i := range ids {
		docs[i] = columnWriteDocument{ID: ids[i], Document: documents[i]}
	}
	rows, payloads, err := nativeTypedDocumentRows(g.probe.meta, docs)
	if err != nil {
		return err
	}
	if err = t.validateNativeOrdinaryProjection(g, rows); err != nil {
		return err
	}
	deleted := make([][]byte, 0, deletedCount)
	for _, row := range g.rows.rows {
		if row.current == nil && row.opening != nil {
			deleted = append(deleted, bytes.Clone(row.id))
		}
	}
	capacity, err := nativeTypedMutationBuildCapacity(g.probe.meta, rows, payloads, deleted)
	if err != nil {
		return err
	}
	operation := ColumnPublishOperationInsert
	for _, row := range g.rows.rows {
		if row.opening != nil && !bytes.Equal(row.opening, row.current) {
			operation = ColumnPublishOperationUpdate
			break
		}
	}
	if len(rows) == 0 {
		operation = ColumnPublishOperationDelete
	}
	if err = t.adoptNativeTyped(g, rows, payloads, deleted, operation, capacity, lease); err != nil {
		return err
	}
	lease = nil
	g.typed.memberOrdinal = uint32(len(t.members) - 1)
	g.plan.typed = g.typed
	if len(g.plan.additional) > maxCollectionInt-2 {
		return memtable.ErrCOWCapacity
	}
	if err = admitNativeInsertArray(len(g.plan.additional)+2, uint64(unsafe.Sizeof(nativeJSONDomainChange{})), admit); err != nil {
		return err
	}
	additional := make([]nativeJSONDomainChange, 0, len(g.plan.additional)+2)
	additional = append(additional, g.plan.additional...)
	for _, name := range []string{"column/manifest", "column/row-locator"} {
		policy, err := collectionRootStoragePolicy(g.probe.meta, g.name+"/"+name)
		if err != nil {
			return err
		}
		additional = append(additional, nativeJSONDomainChange{name: name, policy: policy, entries: g.plan.mutations})
	}
	g.plan.additional = additional
	return nil
}

// A canonical-JSON command must replay to exactly the supplied projection.
// Legacy projection publication is unchanged; this new native lane rejects an
// inconsistent trusted input rather than publishing unreplayable vector bytes.
// Headers borrow request-owned IDs/vectors only inside this synchronous scope.
type nativeOrdinaryProjectionEntry struct {
	id     []byte
	vector []float32
}
type nativeOrdinaryProjectionOrder []nativeOrdinaryProjectionEntry

func (s nativeOrdinaryProjectionOrder) Len() int { return len(s) }
func (s nativeOrdinaryProjectionOrder) Less(i, j int) bool {
	return bytes.Compare(s[i].id, s[j].id) < 0
}
func (s nativeOrdinaryProjectionOrder) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (t *NativeJSONTxn) validateNativeOrdinaryProjection(g *nativeJSONTxnBinding, rows []columnDeclaredRow) error {
	p := g.projection
	if p == nil {
		return nil
	}
	if len(p.ids) != len(rows) || len(p.vectors) != len(rows) {
		return fmt.Errorf("collections: native canonical projection row count mismatch")
	}
	admit := func(n uint64) error { return t.admitStage(g, n) }
	if err := admitNativeInsertArray(len(rows), uint64(unsafe.Sizeof(nativeOrdinaryProjectionEntry{})), admit); err != nil {
		return err
	}
	if err := admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeOrdinaryProjectionOrder{})))); err != nil {
		return err
	}
	order := make(nativeOrdinaryProjectionOrder, len(rows))
	for i := range order {
		order[i] = nativeOrdinaryProjectionEntry{id: p.ids[i], vector: p.vectors[i]}
	}
	sort.Sort(order)
	for i := 1; i < len(order); i++ {
		if bytes.Equal(order[i-1].id, order[i].id) {
			return ErrDocumentExists
		}
	}
	for _, row := range rows {
		lo, hi := 0, len(order)
		for lo < hi {
			mid := lo + (hi-lo)/2
			if bytes.Compare(order[mid].id, row.ID) < 0 {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo == len(order) || !bytes.Equal(order[lo].id, row.ID) || len(row.Values) != 1 || row.Values[0].Type != ColumnStoreValueFloat32Vector || !row.Values[0].Present {
			return fmt.Errorf("collections: native canonical projection ID/value mismatch")
		}
		got, want := row.Values[0].Float32Vector, order[lo].vector
		if len(got) != len(want) {
			return fmt.Errorf("collections: native canonical projection dimension mismatch")
		}
		for d := range got {
			if math.Float32bits(got[d]) != math.Float32bits(want[d]) {
				return fmt.Errorf("collections: native canonical projection differs from JSON at dimension %d", d)
			}
		}
	}
	return nil
}
