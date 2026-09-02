package collections

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
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
	case ColumnStoreValueInt8:
		return columnsemantics.LogicalInt8, true
	case ColumnStoreValueUint8:
		return columnsemantics.LogicalUint8, true
	case ColumnStoreValueInt16:
		return columnsemantics.LogicalInt16, true
	case ColumnStoreValueUint16:
		return columnsemantics.LogicalUint16, true
	case ColumnStoreValueInt32:
		return columnsemantics.LogicalInt32, true
	case ColumnStoreValueUint32:
		return columnsemantics.LogicalUint32, true
	case ColumnStoreValueUint64:
		return columnsemantics.LogicalUint64, true
	case ColumnStoreValueFloat16:
		return columnsemantics.LogicalFloat16, true
	case ColumnStoreValueBFloat16:
		return columnsemantics.LogicalBFloat16, true
	case ColumnStoreValueUint8Vector:
		return columnsemantics.LogicalUint8Vector, true
	case ColumnStoreValueInt8Vector:
		return columnsemantics.LogicalInt8Vector, true
	case ColumnStoreValueUint16Vector:
		return columnsemantics.LogicalUint16Vector, true
	case ColumnStoreValueInt16Vector:
		return columnsemantics.LogicalInt16Vector, true
	case ColumnStoreValueUint32Vector:
		return columnsemantics.LogicalUint32Vector, true
	case ColumnStoreValueInt32Vector:
		return columnsemantics.LogicalInt32Vector, true
	case ColumnStoreValueUint64Vector:
		return columnsemantics.LogicalUint64Vector, true
	case ColumnStoreValueInt64Vector:
		return columnsemantics.LogicalInt64Vector, true
	case ColumnStoreValueFloat16Vector:
		return columnsemantics.LogicalFloat16Vector, true
	case ColumnStoreValueBFloat16Vector:
		return columnsemantics.LogicalBFloat16Vector, true
	case ColumnStoreValueFloat32Vector:
		return columnsemantics.LogicalFloat32Vector, true
	case ColumnStoreValueFloat64Vector:
		return columnsemantics.LogicalFloat64Vector, true
	case ColumnStoreValueByteVector:
		return columnsemantics.LogicalByteVector, true
	case ColumnStoreValuePackedBitVector:
		return columnsemantics.LogicalPackedBitVector, true
	case ColumnStoreValuePackedUint2Vector:
		return columnsemantics.LogicalPackedUint2Vector, true
	case ColumnStoreValuePackedUint4Vector:
		return columnsemantics.LogicalPackedUint4Vector, true
	case ColumnStoreValueUint32List:
		return columnsemantics.LogicalUint32List, true
	case ColumnStoreValueBytes:
		return columnsemantics.LogicalBytes, true
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
		Nullable: column.Field.Nullable,
	}, nil
}

func typedColumnAdapterCapability(column typedColumnAdapterColumn, op columnsemantics.Operation) (columnsemantics.Capability, error) {
	desc, err := typedColumnAdapterSemanticDescriptor(column)
	if err != nil {
		return columnsemantics.Unsupported(op, columnsemantics.ReasonUnknownLogicalType, err.Error()), err
	}
	if op == columnsemantics.OpUint32ListDirectPayload && typedColumnAdapterUint32ListDirectPayloadSupported(column) {
		return columnsemantics.Supported(op), nil
	}
	if op == columnsemantics.OpAdjacencyDirectPayload && typedColumnAdapterOffsetsListAdjacencyDirectPayloadSupported(column) {
		return columnsemantics.Supported(op), nil
	}
	if op == columnsemantics.OpBytesDirectPayload && typedColumnAdapterFixedBytesDirectPayloadSupported(column) {
		return columnsemantics.Supported(op), nil
	}
	if op == columnsemantics.OpVectorDirectPayload && typedColumnAdapterPackedUintDirectPayloadSupported(column) {
		return columnsemantics.Supported(op), nil
	}
	return columnsemantics.CapabilityFor(desc, op), nil
}

type typedColumnSemanticCapabilityError struct {
	context   string
	operation columnsemantics.Operation
	status    columnsemantics.Status
	reason    columnsemantics.ReasonCode
	detail    string
	cause     error
}

func (e *typedColumnSemanticCapabilityError) Error() string {
	if e == nil {
		return "typed-column semantic capability error"
	}
	if e.detail != "" {
		return fmt.Sprintf("%s semantic capability %s status=%s reason=%s detail=%s", e.context, e.operation, e.status, e.reason, e.detail)
	}
	return fmt.Sprintf("%s semantic capability %s status=%s reason=%s", e.context, e.operation, e.status, e.reason)
}

func (e *typedColumnSemanticCapabilityError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func typedColumnSemanticCapabilityReason(err error) (columnsemantics.ReasonCode, bool) {
	var semanticErr *typedColumnSemanticCapabilityError
	if errors.As(err, &semanticErr) && semanticErr != nil {
		return semanticErr.reason, true
	}
	return "", false
}

func requireTypedColumnAdapterCapability(column typedColumnAdapterColumn, op columnsemantics.Operation, context string) error {
	capability, err := typedColumnAdapterCapability(column, op)
	if err != nil {
		semanticErr := &typedColumnSemanticCapabilityError{context: context, operation: op, status: capability.Status, reason: capability.Reason, detail: err.Error(), cause: err}
		return fmt.Errorf("%w: %w", ErrColumnQueryPlanUnsupported, semanticErr)
	}
	if capability.Supported() {
		return nil
	}
	capabilityDetail := capability.Error()
	if capabilityDetail == "" {
		capabilityDetail = string(capability.Reason)
	}
	semanticErr := &typedColumnSemanticCapabilityError{context: context, operation: op, status: capability.Status, reason: capability.Reason, detail: capabilityDetail}
	return fmt.Errorf("%w: %w", ErrColumnQueryPlanUnsupported, semanticErr)
}
