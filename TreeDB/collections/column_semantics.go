package collections

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func columnStoreSemanticLogicalType(valueType ColumnStoreValueType) (columnsemantics.LogicalType, bool) {
	switch valueType {
	case ColumnStoreValueBool:
		return columnsemantics.LogicalBool, true
	case ColumnStoreValueInt64:
		return columnsemantics.LogicalInt64, true
	case ColumnStoreValueFloat32:
		return columnsemantics.LogicalFloat32, true
	case ColumnStoreValueDouble:
		return columnsemantics.LogicalDouble, true
	case ColumnStoreValueString:
		return columnsemantics.LogicalString, true
	case ColumnStoreValueFloat32Vector:
		return columnsemantics.LogicalFloat32Vector, true
	case ColumnStoreValueAdjacencyList:
		return columnsemantics.LogicalAdjacencyList, true
	default:
		return "", false
	}
}

func typedColumnAdapterSemanticDescriptor(column typedColumnAdapterColumn) (columnsemantics.Descriptor, error) {
	logical, ok := columnStoreSemanticLogicalType(column.Field.ValueType)
	if !ok {
		return columnsemantics.Descriptor{}, fmt.Errorf("collections: typed-column semantic adapter unknown value_type %q", column.Field.ValueType)
	}
	return columnsemantics.Descriptor{
		Logical:  logical,
		Physical: column.Definition.Type,
		Encoding: column.Definition.Encoding,
		Nullable: column.Field.Nullable || column.Definition.Encoding == typedcolumn.EncodingNullableInt64,
	}, nil
}

func typedColumnAdapterCapability(column typedColumnAdapterColumn, op columnsemantics.Operation) (columnsemantics.Capability, error) {
	desc, err := typedColumnAdapterSemanticDescriptor(column)
	if err != nil {
		return columnsemantics.Unsupported(op, columnsemantics.ReasonUnknownLogicalType, err.Error()), err
	}
	return columnsemantics.CapabilityFor(desc, op), nil
}

func requireTypedColumnAdapterCapability(column typedColumnAdapterColumn, op columnsemantics.Operation, context string) error {
	capability, err := typedColumnAdapterCapability(column, op)
	if err != nil {
		return fmt.Errorf("%w: %s semantic capability %s reason=%s: %v", ErrColumnQueryPlanUnsupported, context, op, capability.Reason, err)
	}
	if capability.Supported() {
		return nil
	}
	return fmt.Errorf("%w: %s semantic capability %s status=%s reason=%s", ErrColumnQueryPlanUnsupported, context, op, capability.Status, capability.Reason)
}
