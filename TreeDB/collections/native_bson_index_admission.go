package collections

import (
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Scalar BSON extraction borrows raw fields; it does not decode an unrelated
// document payload. Use those fields for the existing conservative encoder and
// numeric-work bounds. Array traversal retains its original whole-source bound.
// This inspection allocates no path/value arrays and never replaces validation
// or the maintained index extractor.
func nativeJSONIndexedDocumentSource(document []byte, meta CollectionMeta) (int, bool) {
	if normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatBSON {
		return len(document), false
	}
	source := 0
	add := func(field string) bool {
		value, scalar := nativeBSONScalarIndexPath(bson.Raw(document), field)
		if !scalar {
			return false
		}
		n := len(value.Value)
		if n > int(^uint(0)>>1)-16 {
			return false
		}
		n += 16
		switch value.Type {
		case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128:
			n = max(n, bsonIndexKeyNumericV2MaxDigits)
		}
		if n > int(^uint(0)>>1)-source {
			return false
		}
		source += n
		return true
	}
	for _, index := range meta.Indexes {
		if len(index.Components) == 0 {
			if !add(index.Field) {
				return len(document), false
			}
		} else {
			for _, component := range index.Components {
				if !add(component.Field) {
					return len(document), false
				}
			}
		}
	}
	return max(64, source), true
}

func nativeBSONScalarIndexPath(raw bson.Raw, field string) (bson.RawValue, bool) {
	for {
		head, tail, more := strings.Cut(field, ".")
		value := raw.Lookup(head)
		if value.Type == bson.TypeArray {
			return bson.RawValue{}, false
		}
		if !more || value.IsZero() {
			return value, true
		}
		next, ok := value.DocumentOK()
		if !ok {
			return bson.RawValue{}, true
		}
		raw, field = next, tail
	}
}
