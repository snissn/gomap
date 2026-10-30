package collections

import (
	"bytes"
	"sort"
	"strings"
	"sync/atomic"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeTypedDelta retains declared values and their original retained JSON or
// semantic-stream carrier, including the complete document for Full.
// previous is an original immutable state edge, not a second visibility owner.
// appliedLSN and memberOrdinal are the original command identity, bound once
// before the private successor is installed. Physical insert-span materialization
// may cover several such members using the last original member's LSN; it never
// rewrites these identities or derives command coverage from checkpoint progress.
type nativeTypedDelta struct {
	refs          atomic.Int64
	previous      *nativeTypedDelta
	data          *nativeTypedRows
	rows          []columnDeclaredRow
	retained      [][]byte
	deleted       [][]byte
	operation     ColumnPublishOperation
	appliedLSN    uint64
	memberOrdinal uint32
	buildCapacity uint64
	// graphSourceRoot is the graph marker's authenticated physical basis.
	// Captured-prefix handoff advances only a private successor header; the
	// original recipe and its admitted source/capacity remain immutable.
	graphSourceRoot uint64
	refund          *cowbtree.OwnedMapRefund
}

// Vector backing has its own original receipt, so dropping a captured prefix
// need not retain consumed ancestors merely to preserve a newer row slice.
type nativeTypedRows struct {
	graph    *nativeVectorGraphRecipe
	source   *nativeSourceImportDeltaV2
	refs     atomic.Int64
	rows     []columnDeclaredRow
	retained [][]byte
	deleted  [][]byte
	refund   *cowbtree.OwnedMapRefund
}

func (r *nativeTypedRows) retain() {
	if r != nil && r.refs.Add(1) <= 1 {
		panic("collections: retain released typed vectors")
	}
}
func (r *nativeTypedRows) release() cowbtree.OwnedMapCompletion {
	if r == nil {
		return cowbtree.OwnedMapCompletion{}
	}
	n := r.refs.Add(-1)
	if n < 0 {
		panic("collections: typed vectors released twice")
	}
	if n != 0 {
		return cowbtree.OwnedMapCompletion{}
	}
	clear(r.rows)
	clear(r.retained)
	clear(r.deleted)
	r.rows, r.retained, r.deleted = nil, nil, nil
	var sourceDone cowbtree.OwnedMapCompletion
	if r.graph != nil {
		sourceDone.Append(r.graph.release())
		r.graph = nil
	}
	if r.source != nil {
		sourceDone.Append(r.source.release())
		r.source = nil
	}
	refund := r.refund
	r.refund = nil
	sourceDone.Append(refund.Completion())
	return sourceDone
}

func (d *nativeTypedDelta) retain() {
	if d != nil && d.refs.Add(1) <= 1 {
		panic("collections: retain released typed delta")
	}
}
func (d *nativeTypedDelta) release() (done cowbtree.OwnedMapCompletion) {
	for d != nil {
		refs := d.refs.Add(-1)
		if refs < 0 {
			panic("collections: typed delta released twice")
		}
		if refs != 0 {
			break
		}
		previous, refund := d.previous, d.refund
		d.previous, d.refund = nil, nil
		data := d.data
		d.data = nil

		d.rows, d.retained, d.deleted = nil, nil, nil
		if data != nil {
			done.Append(data.release())
		}
		d = nil
		done.Append(refund.Completion())
		d = previous
	}
	return done
}
func (d *nativeTypedDelta) find(id []byte) (*columnDeclaredRow, []byte, bool) {
	for ; d != nil; d = d.previous {
		i := sort.Search(len(d.rows), func(i int) bool { return bytes.Compare(d.rows[i].ID, id) >= 0 })
		if i < len(d.rows) && bytes.Equal(d.rows[i].ID, id) {
			return &d.rows[i], d.retained[i], true
		}
		for _, deleted := range d.deleted {
			if bytes.Equal(deleted, id) {
				return nil, nil, true
			}
		}
	}
	return nil, nil, false
}

// nativeTypedDeltaHeaderCharge is shared by staging and suffix reconstruction.
// Every clone retains the original vector receipt; it never copies historical
// document populations or invents a replacement owner.
func nativeTypedDeltaHeaderCharge() uint64 {
	return memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTypedDelta{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
}

func nativeTypedMeta(meta CollectionMeta) bool {
	cfg := meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || len(cfg.Columns) == 0 || (cfg.RetainedPayload != ColumnRetainedPayloadNonColumn && cfg.RetainedPayload != ColumnRetainedPayloadFull) {
		return false
	}
	encoding := columnRetainedPayloadEffectiveEncoding(cfg)
	full := cfg.RetainedPayload == ColumnRetainedPayloadFull
	if full && (encoding != ColumnRetainedPayloadEncodingJSON || normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON) {
		return false
	}
	// Preserve the maintained scalar semantic-stream owners and multi-column
	// producer while admitting Full only through its complete JSON carrier.
	if encoding != ColumnRetainedPayloadEncodingJSON && encoding != ColumnRetainedPayloadEncodingSemanticStreamV1 {
		return false
	}
	// Nested scalar parsing and aggregate asset fanout share the maintained
	// prepared producer's existing structural limits. Other eligible layouts
	// keep their original top-level/no-aggregate contract.
	boundedScalars := encoding == ColumnRetainedPayloadEncodingSemanticStreamV1 && preparedInsertBoundedScalarColumns(cfg.Columns)
	if len(cfg.AggregateMetadata) != 0 && (!boundedScalars || !preparedInsertBoundedAggregateMetadata(*cfg)) {
		return false
	}
	for _, col := range cfg.Columns {
		if col.Path == "" || (strings.Contains(col.Path, ".") && !boundedScalars) {
			return false
		}
		switch col.ValueType {
		case ColumnStoreValueInt64, ColumnStoreValueString:
		case ColumnStoreValueFloat32Vector:
			if col.Nullable || columnStoreFloat32VectorElementsPerRow(col) <= 0 {
				return false
			}
		default:
			return false
		}
		if full {
			// Ordinary Full inserts parse the declared values from the complete
			// document, retaining both through the existing typed publication.
			// Direct typed batches keep their separate NonColumn-only gate.
			if col.Nullable || (col.Owner != TypedStorageOwnerRowAsset && col.Owner != TypedStorageOwnerColumnPart) {
				return false
			}
		} else if encoding == ColumnRetainedPayloadEncodingJSON {
			if col.Nullable || !((col.ValueType == ColumnStoreValueString && col.Owner == TypedStorageOwnerRowAsset) || (col.ValueType == ColumnStoreValueFloat32Vector && col.Owner == TypedStorageOwnerColumnPart)) {
				return false
			}
		} else {
			if col.Owner != TypedStorageOwnerColumnPart && col.Owner != TypedStorageOwnerRowAsset {
				return false
			}
			// Keep the original single-vector semantic-stream lane. This scalar
			// producer migration does not widen vector layouts or their owners.
			if col.ValueType == ColumnStoreValueFloat32Vector && (len(cfg.Columns) != 1 || col.Owner != TypedStorageOwnerColumnPart) {
				return false
			}
		}
	}
	// The maintained direct typed producer indexes only declared, non-null
	// String RowAsset values carried alongside non-column JSON residuals.
	if full && len(meta.Indexes) != 0 && !nativeOrdinaryColumnGraphMeta(meta) {
		return false
	}
	for _, index := range meta.Indexes {
		if full && nativeOrdinaryColumnGraphMeta(meta) {
			continue // Complete JSON uses the ordinary scalar index planner.
		}
		if normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON || encoding != ColumnRetainedPayloadEncodingJSON || index.ValueType != IndexValueString || index.MultiKey || len(index.Components) != 0 || typedStringColumnIndex(cfg.Columns, index.Field) < 0 {
			return false
		}
	}
	return true
}

func nativeTypedProfile(database *backenddb.DB, meta CollectionMeta) error {
	if meta.Options.ColumnStore != nil && (database == nil || database.ResolvedProfile() != backenddb.ProfileCommandWALDurable) {
		return backenddb.ErrNativeSemanticUnsupported
	}
	return nil
}

// cloneNativeTypedSuffix copies only newer delta headers, retaining their SAME
// vector receipts. Captured-prefix rows become unreachable after old views end.
func cloneNativeTypedSuffix(current, boundary *nativeTypedDelta, admit cowbtree.OwnedMapAdmit) (head *nativeTypedDelta, err error) {
	var tail *nativeTypedDelta
	d := current
	for ; d != nil && d != boundary; d = d.previous {
		lease, e := admit(nativeTypedDeltaHeaderCharge())
		if e != nil {
			return head, e
		}
		refund := new(cowbtree.OwnedMapRefund)
		refund.Init(lease)
		next := &nativeTypedDelta{data: d.data, rows: d.rows, retained: d.retained, deleted: d.deleted, operation: d.operation, appliedLSN: d.appliedLSN, memberOrdinal: d.memberOrdinal, buildCapacity: d.buildCapacity, graphSourceRoot: d.graphSourceRoot, refund: refund}
		next.refs.Store(1)
		next.data.retain()
		if tail == nil {
			head = next
		} else {
			tail.previous = next
		}
		tail = next
	}
	if d != boundary {
		return head, backenddb.ErrNativeSemanticUnsupported
	}
	return head, nil
}
