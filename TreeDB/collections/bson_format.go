package collections

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const parallelBSONValidateMinDocs = 2048

func prepareBSONInsertDocuments(documents [][]byte) ([][]byte, []templateV1Record, []templateV1LearnedTemplate, templateV1Resolver, error) {
	if len(documents) >= parallelBSONValidateMinDocs {
		workers := runtime.GOMAXPROCS(0)
		if maxWorkers := len(documents) / 1024; workers > maxWorkers {
			workers = maxWorkers
		}
		if workers > 1 {
			errs := make([]error, workers)
			chunkSize := (len(documents) + workers - 1) / workers
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				start := w * chunkSize
				if start >= len(documents) {
					break
				}
				end := start + chunkSize
				if end > len(documents) {
					end = len(documents)
				}
				wg.Add(1)
				go func(workerIdx, lo, hi int) {
					defer wg.Done()
					for i := lo; i < hi; i++ {
						if err := validateBSONDocument(documents[i]); err != nil {
							errs[workerIdx] = fmt.Errorf("collections: BSON document %d: %w", i, err)
							return
						}
					}
				}(w, start, end)
			}
			wg.Wait()
			for _, err := range errs {
				if err != nil {
					return nil, nil, nil, nil, err
				}
			}
			return documents, nil, nil, nil, nil
		}
	}
	for i, document := range documents {
		if err := validateBSONDocument(document); err != nil {
			return nil, nil, nil, nil, fmt.Errorf("collections: BSON document %d: %w", i, err)
		}
	}
	return documents, nil, nil, nil, nil
}

func validateBSONDocument(document []byte) error {
	if validateBSONDocumentFast(document, 0) {
		return nil
	}
	if err := bson.Raw(document).Validate(); err != nil {
		return fmt.Errorf("invalid BSON: %w", err)
	}
	return nil
}

// validateBSONDocumentFast performs a strict, zero-allocation structural check
// matching bson.Raw.Validate for standard BSON types. It returns false on any
// malformed input or uncommon/legacy BSON type so the caller falls back to
// bson.Raw.Validate for exact validation and error reporting.
func validateBSONDocumentFast(doc []byte, depth int) bool {
	if len(doc) < 5 || depth > 64 {
		return false
	}
	docLen := int(int32(binary.LittleEndian.Uint32(doc[:4])))
	if docLen != len(doc) || doc[len(doc)-1] != 0 {
		return false
	}
	rem := doc[4 : len(doc)-1]
	for len(rem) > 0 {
		if len(rem) < 2 {
			return false
		}
		elemType := bson.Type(rem[0])
		nul := bytes.IndexByte(rem[1:], 0)
		if nul < 0 {
			return false
		}
		vStart := 2 + nul
		var vLen int
		switch elemType {
		case bson.TypeDouble, bson.TypeDateTime, bson.TypeTimestamp, bson.TypeInt64:
			vLen = 8
		case bson.TypeInt32:
			vLen = 4
		case bson.TypeBoolean:
			if vStart >= len(rem) || (rem[vStart] != 0 && rem[vStart] != 1) {
				return false
			}
			vLen = 1
		case bson.TypeNull, bson.TypeUndefined, bson.TypeMinKey, bson.TypeMaxKey:
			vLen = 0
		case bson.TypeObjectID:
			vLen = 12
		case bson.TypeDecimal128:
			vLen = 16
		case bson.TypeString, bson.TypeJavaScript, bson.TypeSymbol:
			if vStart+4 > len(rem) {
				return false
			}
			n := int(int32(binary.LittleEndian.Uint32(rem[vStart : vStart+4])))
			if n < 1 {
				return false
			}
			vLen = 4 + n
			if vStart+vLen > len(rem) || vStart+vLen < vStart || rem[vStart+vLen-1] != 0 {
				return false
			}
		case bson.TypeBinary:
			if vStart+5 > len(rem) {
				return false
			}
			n := int(int32(binary.LittleEndian.Uint32(rem[vStart : vStart+4])))
			// Subtype 0x02 (deprecated binary) has an inner 4-byte length prefix;
			// defer to bson.Raw.Validate.
			if n < 0 || rem[vStart+4] == 0x02 {
				return false
			}
			vLen = 5 + n
		case bson.TypeEmbeddedDocument, bson.TypeArray:
			if vStart+4 > len(rem) {
				return false
			}
			n := int(int32(binary.LittleEndian.Uint32(rem[vStart : vStart+4])))
			if n < 5 || vStart+n > len(rem) || vStart+n < vStart {
				return false
			}
			if !validateBSONDocumentFast(rem[vStart:vStart+n], depth+1) {
				return false
			}
			vLen = n
		default:
			return false
		}
		vEnd := vStart + vLen
		if vEnd > len(rem) || vEnd < vStart {
			return false
		}
		rem = rem[vEnd:]
	}
	return true
}

func bsonOrderedIndexStateForDocumentWithArena(document []byte, runtimes []indexRuntime, opts collectionOptions, encoder *indexEncodeArena) (orderedDocumentIndexState, error) {
	raw := bson.Raw(document)
	state := encoder.appendState(len(runtimes))
	if len(runtimes) >= 1 && len(runtimes) <= 64 && len(document) >= 5 && document[len(document)-1] == 0 {
		allTopLevelSimple := true
		for _, runtime := range runtimes {
			if len(runtime.def.components) > 1 ||
				(len(runtime.def.components) == 1 && runtime.def.components[0].Direction == IndexDirectionDescending) ||
				len(runtime.path) != 1 {
				allTopLevelSimple = false
				break
			}
		}
		if allTopLevelSimple {
			var values [64]bson.RawValue
			var foundMask uint64
			foundCount := 0
			rem := document[4 : len(document)-1]
			parsedOK := true
			for len(rem) > 0 && foundCount < len(runtimes) {
				elemType, keyBytes, valBytes, nextRem, ok := readBSONElementFast(rem)
				if !ok {
					parsedOK = false
					break
				}
				rem = nextRem
				keyLen := len(keyBytes)
				for i, runtime := range runtimes {
					bit := uint64(1) << uint(i)
					if foundMask&bit != 0 {
						continue
					}
					target := runtime.path[0]
					if keyLen == len(target) && string(keyBytes) == target {
						values[i] = bson.RawValue{Type: bson.Type(elemType), Value: valBytes}
						foundMask |= bit
						foundCount++
					}
				}
			}
			if parsedOK {
				for runtimeIdx, runtime := range runtimes {
					bit := uint64(1) << uint(runtimeIdx)
					if foundMask&bit == 0 {
						if runtime.def.valueType == IndexValueBSONOrderedV2 {
							if err := appendBSONIndexValueToState(state, runtimeIdx, runtime, bson.RawValue{}, opts, encoder); err != nil {
								return nil, err
							}
						}
						continue
					}
					if err := appendBSONIndexValueToState(state, runtimeIdx, runtime, values[runtimeIdx], opts, encoder); err != nil {
						return nil, err
					}
				}
				return state, nil
			}
		}
	}
	for runtimeIdx, runtime := range runtimes {
		if err := appendBSONIndexRuntimeState(raw, state, runtimeIdx, runtime, opts, encoder); err != nil {
			return nil, err
		}
	}
	return state, nil
}

// readBSONElementFast parses the next BSON element from rem without allocating
// or running bsoncore's general type dispatch for common scalar/document types,
// falling back to bsoncore.ReadElement for uncommon legacy BSON types.
func readBSONElementFast(rem []byte) (elemType byte, key, val, next []byte, ok bool) {
	if len(rem) < 2 {
		return 0, nil, nil, nil, false
	}
	elemType = rem[0]
	nul := bytes.IndexByte(rem[1:], 0)
	if nul < 0 {
		return 0, nil, nil, nil, false
	}
	key = rem[1 : 1+nul]
	vStart := 2 + nul
	var vLen int
	switch bson.Type(elemType) {
	case bson.TypeDouble, bson.TypeDateTime, bson.TypeTimestamp, bson.TypeInt64:
		vLen = 8
	case bson.TypeInt32:
		vLen = 4
	case bson.TypeBoolean:
		vLen = 1
	case bson.TypeNull, bson.TypeUndefined, bson.TypeMinKey, bson.TypeMaxKey:
		vLen = 0
	case bson.TypeObjectID:
		vLen = 12
	case bson.TypeDecimal128:
		vLen = 16
	case bson.TypeString, bson.TypeJavaScript, bson.TypeSymbol:
		if vStart+4 > len(rem) {
			return 0, nil, nil, nil, false
		}
		n := int(int32(binary.LittleEndian.Uint32(rem[vStart : vStart+4])))
		if n < 1 {
			return 0, nil, nil, nil, false
		}
		vLen = 4 + n
	case bson.TypeEmbeddedDocument, bson.TypeArray:
		if vStart+4 > len(rem) {
			return 0, nil, nil, nil, false
		}
		n := int(int32(binary.LittleEndian.Uint32(rem[vStart : vStart+4])))
		if n < 5 {
			return 0, nil, nil, nil, false
		}
		vLen = n
	case bson.TypeBinary:
		if vStart+5 > len(rem) {
			return 0, nil, nil, nil, false
		}
		n := int(int32(binary.LittleEndian.Uint32(rem[vStart : vStart+4])))
		if n < 0 {
			return 0, nil, nil, nil, false
		}
		vLen = 5 + n
	default:
		elem, rest, ok := bsoncore.ReadElement(rem)
		if !ok {
			return 0, nil, nil, nil, false
		}
		v := elem.Value()
		return byte(v.Type), elem[1 : 1+nul], v.Data, rest, true
	}
	vEnd := vStart + vLen
	if vEnd > len(rem) || vEnd < vStart {
		return 0, nil, nil, nil, false
	}
	return elemType, key, rem[vStart:vEnd], rem[vEnd:], true
}

func appendBSONIndexRuntimeState(raw bson.Raw, state orderedDocumentIndexState, runtimeIdx int, runtime indexRuntime, opts collectionOptions, encoder *indexEncodeArena) error {
	if len(runtime.def.components) > 1 || (len(runtime.def.components) == 1 && runtime.def.components[0].Direction == IndexDirectionDescending) {
		return appendBSONCompoundIndexRuntimeState(raw, state, runtimeIdx, runtime, encoder)
	}
	if len(runtime.path) == 1 {
		value := raw.Lookup(runtime.path[0])
		if value.IsZero() {
			if runtime.def.valueType == IndexValueBSONOrderedV2 {
				return appendBSONIndexValueToState(state, runtimeIdx, runtime, bson.RawValue{}, opts, encoder)
			}
			return nil
		}
		return appendBSONIndexValueToState(state, runtimeIdx, runtime, value, opts, encoder)
	}
	values, found, fromArray, err := bsonIndexValuesForPath(raw, runtime.path)
	if err != nil {
		return err
	}
	if !found {
		if runtime.def.valueType == IndexValueBSONOrderedV2 {
			return appendBSONIndexValueToState(state, runtimeIdx, runtime, bson.RawValue{}, opts, encoder)
		}
		return nil
	}
	if fromArray && !runtime.def.multiKey && !opts.allowArrayValuesInIndex {
		return fmt.Errorf("collections: array value not allowed for index")
	}
	var encodedInline [8][]byte
	encoded := encodedInline[:0]
	if len(values) > len(encodedInline) {
		encoded = make([][]byte, 0, len(values))
	}
	for _, value := range values {
		var next []byte
		var ok bool
		encoder.buf, next, ok, err = appendBSONIndexScalar(encoder.buf, runtime.def.valueType, value)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		encoded = append(encoded, next)
	}
	switch len(encoded) {
	case 0:
	case 1:
		state[runtimeIdx] = encoder.appendSingleValueRef(encoded[0])
	default:
		state[runtimeIdx] = normalizeOwnedEncodedIndexValues(encoded)
	}
	return nil
}

func appendBSONCompoundIndexRuntimeState(raw bson.Raw, state orderedDocumentIndexState, runtimeIdx int, runtime indexRuntime, encoder *indexEncodeArena) error {
	if runtime.def.valueType != IndexValueBSONOrderedV2 {
		return fmt.Errorf("collections: compound index requires BSON v2 key format")
	}
	start := len(encoder.buf)
	for i, component := range runtime.def.components {
		path := runtime.componentPaths[i]
		values, found, fromArray, err := bsonIndexValuesForPath(raw, path)
		if err != nil {
			return err
		}
		if fromArray || len(values) > 1 {
			return fmt.Errorf("collections: array value not allowed for compound index component %q", component.Field)
		}
		var value bson.RawValue
		if found {
			value = values[0]
		}
		next, encoded, err := appendBSONIndexKeyComponentV2(encoder.buf, value)
		if err != nil {
			return err
		}
		encoder.buf = next
		if component.Direction == IndexDirectionDescending {
			complementBSONIndexBytesV2(encoded)
		}
	}
	if len(encoder.buf)-start > bsonIndexKeyComponentV2MaxBytes {
		return errBSONIndexKeyV2TooLarge
	}
	state[runtimeIdx] = encoder.appendSingleValueRef(encoder.buf[start:])
	return nil
}

func appendBSONIndexValueToState(state orderedDocumentIndexState, runtimeIdx int, runtime indexRuntime, value bson.RawValue, opts collectionOptions, encoder *indexEncodeArena) error {
	if array, ok := value.ArrayOK(); ok {
		if !runtime.def.multiKey && !opts.allowArrayValuesInIndex {
			if err := array.Validate(); err != nil {
				return err
			}
			return fmt.Errorf("collections: array value not allowed for index")
		}
		values, err := array.Values()
		if err != nil {
			return err
		}
		var encodedInline [8][]byte
		encoded := encodedInline[:0]
		if len(values) > len(encodedInline) {
			encoded = make([][]byte, 0, len(values))
		}
		for _, item := range values {
			var next []byte
			var ok bool
			var err error
			encoder.buf, next, ok, err = appendBSONIndexScalar(encoder.buf, runtime.def.valueType, item)
			if err != nil {
				return err
			}
			if ok {
				encoded = append(encoded, next)
			}
		}
		switch len(encoded) {
		case 0:
			return nil
		case 1:
			state[runtimeIdx] = encoder.appendSingleValueRef(encoded[0])
		default:
			state[runtimeIdx] = normalizeOwnedEncodedIndexValues(encoded)
		}
		return nil
	}
	var next []byte
	var ok bool
	var err error
	encoder.buf, next, ok, err = appendBSONIndexScalar(encoder.buf, runtime.def.valueType, value)
	if err != nil {
		return err
	}
	if ok {
		state[runtimeIdx] = encoder.appendSingleValueRef(next)
	}
	return nil
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
	// Traversing an intermediate array is multikey even when no child matches
	// the remaining dotted path. Reporting it as missing would silently encode
	// a compound missing component instead of rejecting the unsupported shape.
	fromArray := true
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

func appendBSONIndexScalar(dst []byte, valueType IndexValueType, value bson.RawValue) ([]byte, []byte, bool, error) {
	if value.Type == bson.TypeNull && valueType != IndexValueBSONOrderedV2 {
		return dst, nil, false, nil
	}
	start := len(dst)
	switch valueType {
	case IndexValueBSONOrderedV2:
		out, encoded, err := appendBSONIndexKeyComponentV2(dst, value)
		if err != nil {
			return dst, nil, false, err
		}
		return out, encoded, true, nil
	case IndexValueString:
		out, ok := bsonRawStringBytes(value)
		if !ok {
			return dst, nil, false, fmt.Errorf("collections: indexed BSON value for type %q must be string, got %s", valueType, value.Type)
		}
		dst = appendIndexStringComponent(dst, out)
	case IndexValueBool:
		out, ok := value.BooleanOK()
		if !ok {
			return dst, nil, false, fmt.Errorf("collections: indexed BSON value for type %q must be bool, got %s", valueType, value.Type)
		}
		dst = appendIndexBoolComponent(dst, out)
	case IndexValueInt64:
		switch value.Type {
		case bson.TypeInt32:
			out, ok := value.Int32OK()
			if !ok {
				return dst, nil, false, fmt.Errorf("collections: invalid indexed BSON int32")
			}
			dst = appendIndexInt64Component(dst, int64(out))
		case bson.TypeInt64:
			out, ok := value.Int64OK()
			if !ok {
				return dst, nil, false, fmt.Errorf("collections: invalid indexed BSON int64")
			}
			dst = appendIndexInt64Component(dst, out)
		default:
			return dst, nil, false, fmt.Errorf("collections: indexed BSON value for type %q must be int32/int64, got %s", valueType, value.Type)
		}
	case IndexValueDouble:
		switch value.Type {
		case bson.TypeDouble:
			out, ok := value.DoubleOK()
			if !ok {
				return dst, nil, false, fmt.Errorf("collections: invalid indexed BSON double")
			}
			dst = appendIndexDoubleComponent(dst, out)
		case bson.TypeInt32:
			out, ok := value.Int32OK()
			if !ok {
				return dst, nil, false, fmt.Errorf("collections: invalid indexed BSON int32")
			}
			dst = appendIndexDoubleComponent(dst, float64(out))
		case bson.TypeInt64:
			out, ok := value.Int64OK()
			if !ok {
				return dst, nil, false, fmt.Errorf("collections: invalid indexed BSON int64")
			}
			doubleValue, err := int64IndexValueAsExactFloat64(out)
			if err != nil {
				return dst, nil, false, err
			}
			dst = appendIndexDoubleComponent(dst, doubleValue)
		default:
			return dst, nil, false, fmt.Errorf("collections: indexed BSON value for type %q must be double/int32/int64, got %s", valueType, value.Type)
		}
	default:
		return dst, nil, false, fmt.Errorf("collections: unsupported index value type %q", valueType)
	}
	return dst, dst[start:len(dst):len(dst)], true, nil
}

func bsonRawStringBytes(value bson.RawValue) ([]byte, bool) {
	if value.Type != bson.TypeString {
		return nil, false
	}
	if len(value.Value) < 5 {
		return nil, false
	}
	const lengthPrefixBytes = 4
	n32 := binary.LittleEndian.Uint32(value.Value[:lengthPrefixBytes])
	if n32 == 0 || n32 > uint32(len(value.Value)-lengthPrefixBytes) {
		return nil, false
	}
	n := int(n32)
	if value.Value[lengthPrefixBytes+n-1] != 0 {
		return nil, false
	}
	return value.Value[lengthPrefixBytes : lengthPrefixBytes+n-1], true
}
