package treedb

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
