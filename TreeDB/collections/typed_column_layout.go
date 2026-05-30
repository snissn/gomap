package collections

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/columnlayout"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
)

func typedColumnLayoutDescriptorForAdapterColumn(column typedColumnAdapterColumn) columnlayout.Descriptor {
	logical, _ := columnStoreSemanticLogicalType(column.Field.ValueType)
	return columnlayout.Descriptor{
		Logical:             logical,
		Physical:            column.Definition.Type,
		Encoding:            column.Definition.Encoding,
		Compression:         column.Definition.Compression,
		Nullable:            column.Field.Nullable,
		Defaultable:         column.Field.Nullable,
		Dictionary:          column.Field.Dictionary || column.Dictionary != nil || column.ReverseDictionary != nil,
		DictionaryOrder:     false,
		DictionaryCollation: "",
		FixedWidthElements:  column.Definition.FixedWidthElements,
	}
}

func typedColumnLayoutCapabilitiesForAdapterColumn(column typedColumnAdapterColumn) columnlayout.Capabilities {
	return columnlayout.CapabilitiesFor(typedColumnLayoutDescriptorForAdapterColumn(column))
}

func requireTypedColumnLayoutCapability(column typedColumnAdapterColumn, op columnsemantics.Operation, context string) error {
	capability := typedColumnLayoutCapabilitiesForAdapterColumn(column).SupportsSemanticOperation(op)
	if capability.Supported() {
		return nil
	}
	return typedColumnLayoutCapabilityError(capability, context, op)
}

func typedColumnLayoutCapabilityError(capability columnlayout.Capability, context string, op columnsemantics.Operation) error {
	detail := capability.Error()
	if detail == "" {
		detail = string(capability.Reason)
	}
	semanticErr := &typedColumnSemanticCapabilityError{
		context:   context,
		operation: op,
		status:    capability.Status,
		reason:    columnsemantics.ReasonCode(capability.Reason),
		detail:    "layout capability " + detail,
	}
	return fmt.Errorf("%w: %w", ErrColumnQueryPlanUnsupported, semanticErr)
}
