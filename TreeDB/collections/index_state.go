package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
)

const documentIndexStateVersion = 1

type documentIndexState map[string][][]byte

func encodeDocumentIndexState(state documentIndexState) ([]byte, error) {
	if state == nil {
		state = make(documentIndexState)
	}
	names := make([]string, 0, len(state))
	for indexName, values := range state {
		if err := ValidateIndexName(indexName); err != nil {
			return nil, err
		}
		state[indexName] = normalizeEncodedIndexValues(values)
		names = append(names, indexName)
	}
	sort.Strings(names)

	out := make([]byte, 0, 32)
	out = append(out, documentIndexStateVersion)
	out = binary.BigEndian.AppendUint16(out, uint16(len(names)))
	for _, indexName := range names {
		values := state[indexName]
		if len(indexName) > 65535 {
			return nil, fmt.Errorf("collections: index state name too long")
		}
		out = binary.BigEndian.AppendUint16(out, uint16(len(indexName)))
		out = append(out, indexName...)
		out = binary.BigEndian.AppendUint16(out, uint16(len(values)))
		for _, value := range values {
			if len(value) > 65535 {
				return nil, fmt.Errorf("collections: index state value too large")
			}
			out = binary.BigEndian.AppendUint16(out, uint16(len(value)))
			out = append(out, value...)
		}
	}
	return out, nil
}

func decodeDocumentIndexState(raw []byte) (documentIndexState, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) < 3 {
		return nil, fmt.Errorf("collections: truncated index state")
	}
	if raw[0] != documentIndexStateVersion {
		return nil, fmt.Errorf("collections: unsupported index state version %d", raw[0])
	}
	cursor := 1
	count := int(binary.BigEndian.Uint16(raw[cursor : cursor+2]))
	cursor += 2
	state := make(documentIndexState, count)
	for i := 0; i < count; i++ {
		if cursor+2 > len(raw) {
			return nil, fmt.Errorf("collections: truncated index state name length")
		}
		nameLen := int(binary.BigEndian.Uint16(raw[cursor : cursor+2]))
		cursor += 2
		if cursor+nameLen > len(raw) {
			return nil, fmt.Errorf("collections: truncated index state name")
		}
		indexName := string(raw[cursor : cursor+nameLen])
		cursor += nameLen
		if err := ValidateIndexName(indexName); err != nil {
			return nil, err
		}
		if cursor+2 > len(raw) {
			return nil, fmt.Errorf("collections: truncated index state value count")
		}
		valueCount := int(binary.BigEndian.Uint16(raw[cursor : cursor+2]))
		cursor += 2
		values := make([][]byte, 0, valueCount)
		for j := 0; j < valueCount; j++ {
			if cursor+2 > len(raw) {
				return nil, fmt.Errorf("collections: truncated index state value length")
			}
			valueLen := int(binary.BigEndian.Uint16(raw[cursor : cursor+2]))
			cursor += 2
			if cursor+valueLen > len(raw) {
				return nil, fmt.Errorf("collections: truncated index state value")
			}
			values = append(values, bytes.Clone(raw[cursor:cursor+valueLen]))
			cursor += valueLen
		}
		state[indexName] = normalizeEncodedIndexValues(values)
	}
	if cursor != len(raw) {
		return nil, fmt.Errorf("collections: trailing bytes in index state")
	}
	return state, nil
}

func normalizeEncodedIndexValues(values [][]byte) [][]byte {
	if len(values) == 0 {
		return nil
	}
	sorted := make([][]byte, 0, len(values))
	for _, value := range values {
		if len(value) == 0 {
			continue
		}
		sorted = append(sorted, bytes.Clone(value))
	}
	if len(sorted) == 0 {
		return nil
	}
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i], sorted[j]) < 0
	})
	out := sorted[:1]
	for _, value := range sorted[1:] {
		if bytes.Equal(value, out[len(out)-1]) {
			continue
		}
		out = append(out, value)
	}
	return out
}

func encodedIndexValuesForDefinition(document any, idx IndexDefinition, opts CollectionOptions, path []string) ([][]byte, error) {
	value, found := extractIndexPathValue(document, path)
	if !found || value == nil {
		return nil, nil
	}
	values, err := normalizeIndexValues(value, idx.MultiKey, opts.AllowArrayValuesInIndex)
	if err != nil {
		return nil, err
	}
	encoded := make([][]byte, 0, len(values))
	for _, v := range values {
		next, err := encodeIndexScalar(v)
		if err != nil {
			return nil, err
		}
		encoded = append(encoded, next)
	}
	return normalizeEncodedIndexValues(encoded), nil
}

func (c *Collection) indexStateForDocument(document []byte) (documentIndexState, error) {
	if len(document) == 0 || len(c.meta.Indexes) == 0 {
		return make(documentIndexState), nil
	}
	var decoded any
	if err := json.Unmarshal(document, &decoded); err != nil {
		return nil, fmt.Errorf("collections: index extraction requires JSON document: %w", err)
	}
	obj, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("collections: index extraction requires JSON object document")
	}
	state := make(documentIndexState, len(c.meta.Indexes))
	for _, idx := range c.meta.Indexes {
		runtime, err := c.indexRuntime(idx)
		if err != nil {
			return nil, err
		}
		values, err := encodedIndexValuesForDefinition(obj, idx, c.meta.Options, runtime.path)
		if err != nil {
			return nil, err
		}
		if len(values) > 0 {
			state[idx.Name] = values
		}
	}
	return state, nil
}

func (c *Collection) indexEntriesForState(documentID []byte, state documentIndexState) ([][]byte, error) {
	if len(documentID) == 0 || len(state) == 0 || len(c.meta.Indexes) == 0 {
		return nil, nil
	}
	out := make([][]byte, 0, len(c.meta.Indexes))
	for _, idx := range c.meta.Indexes {
		values := state[idx.Name]
		if len(values) == 0 {
			continue
		}
		runtime, err := c.indexRuntime(idx)
		if err != nil {
			return nil, err
		}
		for _, encoded := range values {
			key, err := buildIndexEntryKeyWithPrefix(runtime.prefix, encoded, documentID)
			if err != nil {
				return nil, err
			}
			out = append(out, key)
		}
	}
	return out, nil
}
