package caching

var rawKVEmptyPointKey = []byte{}
var rawKVEmptyValue = []byte{}

func normalizeRawKVPointKey(key []byte) []byte {
	if key == nil {
		return rawKVEmptyPointKey
	}
	return key
}

func normalizeRawKVValue(value []byte) []byte {
	if value == nil {
		return rawKVEmptyValue
	}
	return value
}

func cloneRawKVPointKey(key []byte) []byte {
	key = normalizeRawKVPointKey(key)
	if len(key) == 0 {
		return rawKVEmptyPointKey
	}
	return append([]byte(nil), key...)
}

func normalizeRawKVPointKeys(keys [][]byte) [][]byte {
	for i, key := range keys {
		if key != nil {
			continue
		}
		out := append([][]byte(nil), keys...)
		out[i] = rawKVEmptyPointKey
		for j := i + 1; j < len(out); j++ {
			if out[j] == nil {
				out[j] = rawKVEmptyPointKey
			}
		}
		return out
	}
	return keys
}
