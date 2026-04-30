package collections

import (
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func prepareBSONInsertDocuments(documents [][]byte) ([][]byte, []templateV1Record, templateV1Resolver, error) {
	for i, document := range documents {
		if err := validateBSONDocument(document); err != nil {
			return nil, nil, nil, fmt.Errorf("collections: BSON document %d: %w", i, err)
		}
	}
	return documents, nil, nil, nil
}

func validateBSONDocument(document []byte) error {
	if err := bson.Raw(document).Validate(); err != nil {
		return fmt.Errorf("invalid BSON: %w", err)
	}
	return nil
}

func bsonOrderedIndexStateForDocumentWithArena(document []byte, runtimes []indexRuntime, opts collectionOptions, encoder *indexEncodeArena) (orderedDocumentIndexState, error) {
	raw := bson.Raw(document)
	state := encoder.appendState(len(runtimes))
	for runtimeIdx, runtime := range runtimes {
		values, found, fromArray, err := bsonIndexValuesForPath(raw, runtime.path)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		if fromArray && !runtime.def.multiKey && !opts.allowArrayValuesInIndex {
			return nil, fmt.Errorf("collections: array value not allowed for index")
		}
		var encoded [][]byte
		for _, value := range values {
			var next []byte
			var ok bool
			encoder.buf, next, ok, err = appendBSONIndexScalar(encoder.buf, value)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			encoded = append(encoded, next)
		}
		switch len(encoded) {
		case 0:
			continue
		case 1:
			state[runtimeIdx] = encoder.appendSingleValueRef(encoded[0])
		default:
			state[runtimeIdx] = normalizeOwnedEncodedIndexValues(encoded)
		}
	}
	return state, nil
}

func bsonIndexValuesForPath(raw bson.Raw, path []string) ([]bson.RawValue, bool, bool, error) {
	if len(path) == 0 {
		return nil, false, false, nil
	}
	value := raw.Lookup(path[0])
	if value.IsZero() {
		return nil, false, false, nil
	}
	if len(path) == 1 {
		return bsonLeafIndexValues(value)
	}
	return bsonDescendantIndexValues(value, path[1:])
}

func bsonDescendantIndexValues(value bson.RawValue, path []string) ([]bson.RawValue, bool, bool, error) {
	if doc, ok := value.DocumentOK(); ok {
		return bsonIndexValuesForPath(doc, path)
	}
	array, ok := value.ArrayOK()
	if !ok {
		return nil, false, false, nil
	}
	values, err := array.Values()
	if err != nil {
		return nil, false, false, err
	}
	var out []bson.RawValue
	fromArray := false
	for _, item := range values {
		doc, ok := item.DocumentOK()
		if !ok {
			continue
		}
		itemValues, found, _, err := bsonIndexValuesForPath(doc, path)
		if err != nil {
			return nil, false, false, err
		}
		if found {
			out = append(out, itemValues...)
			fromArray = true
		}
	}
	return out, len(out) > 0, fromArray, nil
}

func bsonLeafIndexValues(value bson.RawValue) ([]bson.RawValue, bool, bool, error) {
	array, ok := value.ArrayOK()
	if !ok {
		return []bson.RawValue{value}, true, false, nil
	}
	values, err := array.Values()
	if err != nil {
		return nil, false, true, err
	}
	return values, true, true, nil
}

func appendBSONIndexScalar(dst []byte, value bson.RawValue) ([]byte, []byte, bool, error) {
	switch value.Type {
	case bson.TypeString:
		out, ok := value.StringValueOK()
		if !ok {
			return dst, nil, false, fmt.Errorf("collections: invalid indexed BSON string")
		}
		dst, encoded, err := appendIndexScalar(dst, out)
		return dst, encoded, true, err
	case bson.TypeBoolean:
		out, ok := value.BooleanOK()
		if !ok {
			return dst, nil, false, fmt.Errorf("collections: invalid indexed BSON boolean")
		}
		dst, encoded, err := appendIndexScalar(dst, out)
		return dst, encoded, true, err
	case bson.TypeDouble:
		out, ok := value.DoubleOK()
		if !ok || math.IsNaN(out) || math.IsInf(out, 0) {
			return dst, nil, false, fmt.Errorf("collections: unsupported indexed BSON double")
		}
		dst, encoded, err := appendIndexScalar(dst, out)
		return dst, encoded, true, err
	case bson.TypeInt32:
		out, ok := value.Int32OK()
		if !ok {
			return dst, nil, false, fmt.Errorf("collections: invalid indexed BSON int32")
		}
		dst, encoded, err := appendIndexScalar(dst, out)
		return dst, encoded, true, err
	case bson.TypeInt64:
		out, ok := value.Int64OK()
		if !ok {
			return dst, nil, false, fmt.Errorf("collections: invalid indexed BSON int64")
		}
		dst, encoded, err := appendIndexScalar(dst, out)
		return dst, encoded, true, err
	case bson.TypeNull:
		return dst, nil, false, nil
	default:
		return dst, nil, false, fmt.Errorf("collections: unsupported indexed BSON value type %s", value.Type)
	}
}
