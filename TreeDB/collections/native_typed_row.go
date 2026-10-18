package collections

import (
	"bytes"
	"reflect"
	"strings"
	"sync/atomic"
	"unicode/utf8"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeTypedRow is one identity-free staged declared row. The original planner
// has already validated the retained non-column payload and write capability.
// It owns no physical coordinates, manifest identity, LSN or reconstructed JSON
// projection. Its exact immutable schema and all row backing survive callers.
// OwnedMap row overlays use key and row.Deleted for newest-effect precedence.
type nativeTypedRow struct {
	refs     atomic.Int64
	schema   *nativeSchemaState
	key      string
	row      columnDeclaredRow
	retained []byte
	refund   *cowbtree.OwnedMapRefund
}

// newNativeTypedRow admits the original descriptor and each backing separately
// before allocation. There is no fallible materialization after admission.
// The supplied schema edge is borrowed; success retains its own original edge.
func newNativeTypedRow(schema *nativeSchemaState, row columnDeclaredRow, retained []byte, admit cowbtree.OwnedMapAdmit) (*nativeTypedRow, error) {
	if schema == nil || schema.refs.Load() <= 0 || schema.meta == nil || admit == nil || len(row.ID) == 0 || row.Preserved != nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	cfg := schema.meta.Options.ColumnStore
	if cfg == nil || len(cfg.Columns) == 0 {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	projection := trustedFloat32Projection{columns: cfg.Columns, schemaHash: cfg.SchemaHash}
	if err := validateTypedProjectionMeta(*schema.meta, &projection); err != nil {
		return nil, err
	}
	if row.Deleted {
		if len(row.Values) != 0 || len(retained) != 0 {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
	} else {
		if len(row.Values) != len(cfg.Columns) {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		for i, value := range row.Values {
			column := cfg.Columns[i]
			if value.Type != column.ValueType || !value.Present || value.Null {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
			// Only the established narrow typed carriers enter this overlay.
			// In particular, an asset-borrowed StringBytes or unrelated carrier
			// cannot silently extend its original read scope through staging.
			other := value
			other.Type, other.Present = "", false
			switch value.Type {
			case ColumnStoreValueString:
				if !utf8.ValidString(value.String) {
					return nil, backenddb.ErrNativeSemanticUnsupported
				}
				other.String = ""
			case ColumnStoreValueFloat32Vector:
				if err := validateColumnDeclaredPhysicalValueShape(column, value); err != nil {
					return nil, err
				}
				if err := validateFloat32Vector(value.Float32Vector); err != nil {
					return nil, err
				}
				// Normalization remains the existing trusted typed planner's
				// responsibility. Copying must not normalize a second time.
				if definition, ok := cosineNormalizedF32V1DefinitionForField(*schema.meta, column.Path); ok {
					if err := validateCosineNormalizedF32V1Canonical(value.Float32Vector, definition.Dimensions); err != nil {
						return nil, err
					}
				} else {
					for _, index := range schema.meta.VectorIndexes {
						if index.Field == column.Path && index.Metric == VectorMetricCosine && vectorNormSquared(value.Float32Vector) == 0 {
							return nil, backenddb.ErrNativeSemanticUnsupported
						}
					}
				}
				other.Float32Vector = nil
			default:
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
			if !reflect.ValueOf(other).IsZero() {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
		}
	}
	backing, err := nativeSchemaBackingCharge(reflect.ValueOf(row), 0)
	if err != nil {
		return nil, err
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTypedRow{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
	for _, bytes := range [3]uint64{backing, memtable.COWAllocationCharge(uint64(len(row.ID))), memtable.COWAllocationCharge(uint64(len(retained)))} {
		if bytes > ^uint64(0)-charge {
			return nil, memtable.ErrCOWCapacity
		}
		charge += bytes
	}
	lease, err := admit(charge)
	if err != nil {
		return nil, err
	}
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	owned := row
	owned.ID = bytes.Clone(row.ID)
	owned.Values = make([]columnDeclaredValue, len(row.Values))
	for i, value := range row.Values {
		value.Type = ColumnStoreValueType(strings.Clone(string(value.Type)))
		value.String = strings.Clone(value.String)
		value.Float32Vector = append([]float32(nil), value.Float32Vector...)
		owned.Values[i] = value
	}
	schema.retain()
	r := &nativeTypedRow{schema: schema, key: string(row.ID), row: owned, retained: bytes.Clone(retained), refund: refund}
	r.refs.Store(1)
	return r, nil
}

// The original immutable row edge is shared by sealed map nodes. Final release
// clears all schema, key and payload aliases before returning independent
// receipts. The map clears its value/key slots before completing those receipts.
type nativeTypedRowLifetime struct{}

func (nativeTypedRowLifetime) Retain(row *nativeTypedRow) {
	if row == nil || row.refs.Add(1) <= 1 {
		panic("collections: retain released native typed row")
	}
}

func (nativeTypedRowLifetime) Release(row *nativeTypedRow) (completion cowbtree.OwnedMapCompletion, err error) {
	if row == nil {
		return completion, nil
	}
	refs := row.refs.Add(-1)
	if refs < 0 {
		panic("collections: native typed row released twice")
	}
	if refs != 0 {
		return completion, nil
	}
	schema := row.schema
	row.schema = nil
	row.key, row.row, row.retained = "", columnDeclaredRow{}, nil
	completion.Append(schema.release())
	schema = nil
	refund := row.refund
	row.refund = nil
	completion.Append(refund.Completion())
	return completion, nil
}
